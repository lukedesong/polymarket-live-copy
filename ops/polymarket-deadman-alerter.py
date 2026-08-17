#!/usr/bin/env python3
"""polymarket-deadman-alerter — 独立存活告警器（仅标准库，零业务代码共享）。

设计原则（SRE dead-man 模式）：
  - 与业务进程零共享代码：只读 status.json / systemctl / 审计 JSONL；
  - 自身每次运行写状态文件，可被更外层 watchdog 检查（alerter 自己挂了也能被发现）；
  - 每个新问题只告警一次；持续存在不重复发送，恢复后再次出现才重新告警。

检查项：
  1. 必需 systemd 单元 is-active（主 + 热备）；
  2. zockdo status.json 的 heartbeat_at_ms 新鲜度（默认 < 180 秒）；
  3. status.json 文件 mtime 新鲜度（兜底，防心跳键缺失）；
  4. server_health_audit.jsonl 最新 overall_state（!= OK 时按 WARNING 级）。

配置：环境变量（建议放 /etc/polymarket-live/deadman-alerter.env，640 root:polymarket-live）
  ALERT_WEBHOOK_URL      必填，告警 webhook 地址
  ALERT_WEBHOOK_KIND     wecom(默认) | dingtalk | slack | generic
  HEARTBEAT_MAX_AGE_S    心跳最大年龄秒数，默认 180
  REPEAT_INTERVAL_S      持续异常重复提醒间隔，默认 1800
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

RUNTIME_DIR = Path("/srv/polymarket-live/runtime")
STATUS_JSON = RUNTIME_DIR / "zockdo_live" / "status.json"
AUDIT_JSONL = RUNTIME_DIR / "server_health" / "server_health_audit.jsonl"
STATE_FILE = RUNTIME_DIR / "server_health" / "deadman_alerter_state.json"

REQUIRED_UNITS = (
    "com.luke.polymarket.zockdo-live.service",
    "com.luke.polymarket.zockdo-live-hot-standby.service",
    "com.luke.polymarket.wallet-9506-live.service",
    "com.luke.polymarket.wallet-9506-live-hot-standby.service",
)

WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
WEBHOOK_KIND = os.environ.get("ALERT_WEBHOOK_KIND", "wecom").strip().lower()
HEARTBEAT_MAX_AGE_S = int(os.environ.get("HEARTBEAT_MAX_AGE_S", "180"))
REPEAT_INTERVAL_S = int(os.environ.get("REPEAT_INTERVAL_S", "1800"))

PROFILE_DISPLAY_NAMES = {
    "cd90": "已删除的 CD90 账本",
    "zockdo": "网球跟单 zockdo",
    "zockdo_full_wallet": "网球跟单 zockdo",
    "wallet_9506_full_wallet": "钱包 9506",
    "wallet_44b0_netflix": "已停的 Netflix 钱包",
    "fuu_full_wallet": "已停的 FUU 钱包",
    "tennis_atp_wta_mainline": "已停的网球主线",
}

UNIT_DISPLAY_NAMES = {
    "com.luke.polymarket.zockdo-live.service": "网球跟单 zockdo 主进程",
    "com.luke.polymarket.zockdo-live-hot-standby.service": "网球跟单 zockdo 热备",
    "com.luke.polymarket.wallet-9506-live.service": "钱包 9506 主进程",
    "com.luke.polymarket.wallet-9506-live-hot-standby.service": "钱包 9506 热备",
}

# Copy-completeness counters are not incidents. Barking them makes every
# skipped source action look like an outage.
SILENT_ISSUE_SUFFIXES = (
    "EXTERNAL_OR_CAUSAL_UNFILLED",
    "PENDING_ACTION_TARGETS",
    "POST_RELEASE_EXTERNAL_ERROR_EVENTS",
    "HOT_STANDBY_INACTIVE",
    "LIQUIDITY_RETRY_PENDING",
)

HEADLINES = {
    "发现需要关注的跟单问题。",
    "系统仍在运行，但存在未完成跟单或外部限制。",
    "健康检查发现异常，需要检查。",
}


def _latest_audit_payload() -> dict | None:
    """Read the current health authority once; callers fail closed on absence."""

    try:
        last = None
        with AUDIT_JSONL.open("rb") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    last = line
        if last is None:
            return None
        payload = json.loads(last)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def expected_active_units() -> tuple[str, ...]:
    """Use the current health registry so an operator-paused sleeve is silent."""

    payload = _latest_audit_payload()
    profiles = payload.get("profiles") if payload else None
    if not isinstance(profiles, dict):
        return REQUIRED_UNITS
    units = []
    for row in profiles.values():
        if not isinstance(row, dict) or row.get("paused") is True:
            continue
        for field in ("unit", "hot_standby_unit"):
            unit = row.get(field)
            if isinstance(unit, str) and unit in REQUIRED_UNITS and unit not in units:
                units.append(unit)
    return tuple(units) if units else REQUIRED_UNITS


def now_ms() -> int:
    return int(time.time() * 1000)


def check_units() -> list[str]:
    problems = []
    for unit in expected_active_units():
        try:
            out = subprocess.run(
                ["systemctl", "is-active", unit],
                capture_output=True, text=True, timeout=10,
            ).stdout.strip()
        except Exception as exc:  # systemctl 本身失败也要告警
            out = f"check_error:{type(exc).__name__}"
        if out != "active":
            who = UNIT_DISPLAY_NAMES.get(unit, "跟单进程")
            problems.append(
                _brief(
                    running="跟单可能停了",
                    who=who,
                    what=f"这个进程现在不是运行中，当前状态是{out}。",
                    you="不用自己下单。系统会尝试自动拉起。",
                )
            )
    return problems


def check_heartbeat() -> list[str]:
    problems = []
    try:
        payload = json.loads(STATUS_JSON.read_text())
    except Exception as exc:
        return [
            _brief(
                running="跟单可能停了",
                who="网球跟单 zockdo",
                what="状态文件读不到，没法确认这一轮有没有跟上。",
                you="不用自己下单。正在自动检查。",
            )
        ]
    hb = payload.get("heartbeat_at_ms") or (
        payload.get("runtime", {}) or {}
    ).get("heartbeat_at_ms")
    try:
        age_s = (now_ms() - int(hb)) / 1000
    except (TypeError, ValueError):
        problems.append(
            _brief(
                running="跟单可能停了",
                who="网球跟单 zockdo",
                what="状态文件里没有心跳时间，没法确认进程是否还在干活。",
                you="不用自己下单。正在自动检查。",
            )
        )
        age_s = None
    if age_s is not None and age_s > HEARTBEAT_MAX_AGE_S:
        problems.append(
            _brief(
                running="跟单可能停了",
                who="网球跟单 zockdo",
                what=(
                    f"心跳已经 {int(age_s)} 秒没更新，超过 {HEARTBEAT_MAX_AGE_S} 秒。"
                ),
                you="不用自己下单。正在自动检查。",
            )
        )
    try:
        mtime_age = time.time() - STATUS_JSON.stat().st_mtime
        if mtime_age > 300:
            problems.append(
                _brief(
                    running="跟单可能停了",
                    who="网球跟单 zockdo",
                    what=f"状态文件已经 {int(mtime_age)} 秒没更新。",
                    you="不用自己下单。正在自动检查。",
                )
            )
    except OSError:
        pass
    return problems


def _brief(*, running: str, who: str, what: str, you: str) -> str:
    return f"{running}\n谁：{who}\n事：{what}\n你：{you}"


def _plain_error_detail(detail: str) -> str:
    translations = {
        "database unavailable": "数据库打不开",
        "unable to open database file": "数据库文件打不开",
        "database is locked": "数据库被锁住了",
    }
    normalized = str(detail).strip()
    return translations.get(normalized.lower(), "程序自己报错了")


def _issue_suffix(issue: str) -> str:
    code = str(issue).split(":", 1)[0]
    suffixes = (
        "LIQUIDITY_RETRY_PENDING",
        "UNRESOLVED_ACTIONS",
        "PENDING_ACTION_TARGETS",
        "POST_RELEASE_EXTERNAL_ERROR_EVENTS",
        "EXTERNAL_OR_CAUSAL_UNFILLED",
        "POST_RELEASE_INTERNAL_ERROR_EVENTS",
        "ACTION_FIDELITY_INTERNAL_ERRORS",
        "ACTION_FIDELITY_NONCONSERVATION",
        "ACTION_FIDELITY_UNCLASSIFIED_TARGETS",
        "BOUNDED_RETRY_TARGET_NONCONSERVATION",
        "HOT_STANDBY_INACTIVE",
        "STATUS_MISSING_OR_MALFORMED",
        "WS_SUBSCRIPTION_INACTIVE",
        "LAST_CYCLE_NOT_SUCCESS",
        "CHAIN_CURSOR_LAG_INVALID",
        "LOSSLESS_HANDOFF_FAILURE",
        "AVAILABLE_CASH_INVALID",
        "INTERNAL_RUNTIME",
    )
    for suffix in suffixes:
        if code == suffix or code.endswith("_" + suffix):
            return suffix
    if code.startswith("INTERNAL_RUNTIME"):
        return "INTERNAL_RUNTIME"
    return code


def _is_silent_issue(issue: str) -> bool:
    return _issue_suffix(issue) in SILENT_ISSUE_SUFFIXES


def _humanize_audit_problem(*, profile: str, field: str, value: str) -> str | None:
    wallet = PROFILE_DISPLAY_NAMES.get(str(profile), "跟单账户")
    issue = str(value).strip()
    if _is_silent_issue(issue):
        return None
    suffix = _issue_suffix(issue)
    retry_match = re.fullmatch(
        r"[A-Z0-9_]*LIQUIDITY_RETRY_PENDING:(\{.*\})", issue
    )
    if retry_match:
        try:
            detail = json.loads(retry_match.group(1))
        except json.JSONDecodeError:
            detail = {}
        reason_text = {
            "CURRENT_BOOK_OUTSIDE_FIRST_ATTEMPT_PRICE": (
                "现在的可成交价比当时记下的上限差"
            ),
            "OFFICIAL_CONFIRMED_ZERO_FILL_RETRYABLE": (
                "上一笔官方确认没成交，还在等合适盘口"
            ),
            "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE": (
                "链上确认上一笔没成交，还在等合适盘口"
            ),
            "FAK_PARTIAL_FILL": "上一笔只成交一部分，还在补剩下的",
            "CURRENT_BOOK_UNAVAILABLE_FOR_LIQUIDITY_RETRY": (
                "这轮盘口暂时读不到，所以没再下单"
            ),
        }.get(str(detail.get("reason") or ""), "这轮盘口还不满足补单条件")
        market = str(detail.get("market_slug") or detail.get("event_slug") or "未知市场")
        side = "买" if str(detail.get("side") or "").upper() == "BUY" else (
            "卖" if str(detail.get("side") or "").upper() == "SELL" else "跟单"
        )
        return _brief(
            running="跟单还在跑",
            who=wallet,
            what=(
                f"{market} 这一笔{side}还没补完：目标 "
                f"{detail.get('target_quantity') or '未知'} 份，已成交 "
                f"{detail.get('cumulative_filled_quantity') or '未知'} 份，剩下 "
                f"{detail.get('remaining_quantity') or '未知'} 份。"
                f"{reason_text}。"
            ),
            you="不用管。系统不会为了补上而乱加仓。",
        )
    if suffix == "UNRESOLVED_ACTIONS":
        count = issue.rsplit(":", 1)[-1]
        return _brief(
            running="跟单还在跑",
            who=wallet,
            what=f"有 {count} 笔订单还没和官方结果对上。",
            you="不用管，更不要补单。系统只核对，不会重复下单。",
        )
    if suffix == "POST_RELEASE_EXTERNAL_ERROR_EVENTS":
        return _brief(
            running="跟单还在跑",
            who=wallet,
            what="官方对账接口这一阵对不上，成交确认可能慢一点。",
            you="不用管。系统不会改账，也不会重复下单。",
        )
    if suffix == "POST_RELEASE_INTERNAL_ERROR_EVENTS":
        return _brief(
            running="跟单可能受影响",
            who=wallet,
            what="程序自己记到了内部错误，可能影响这一路跟单。",
            you="需要马上看一下。你自己不用下单。",
        )
    if suffix == "INTERNAL_RUNTIME" or issue.startswith("INTERNAL_RUNTIME:"):
        detail = _plain_error_detail(issue.split(":", 1)[1] if ":" in issue else "")
        return _brief(
            running="跟单可能受影响",
            who=wallet,
            what=f"程序内部出错了：{detail}。",
            you="需要马上看一下。你自己不用下单。",
        )
    if suffix == "ACTION_FIDELITY_NONCONSERVATION":
        return _brief(
            running="跟单可能受影响",
            who=wallet,
            what="有源钱包动作对不上处理结果，可能漏记。",
            you="需要马上看一下。你自己不用下单。",
        )
    if suffix in {
        "ACTION_FIDELITY_INTERNAL_ERRORS",
        "ACTION_FIDELITY_UNCLASSIFIED_TARGETS",
        "BOUNDED_RETRY_TARGET_NONCONSERVATION",
    }:
        return _brief(
            running="跟单可能受影响",
            who=wallet,
            what="跟单记账对不齐，这一路可能记错或漏记。",
            you="需要马上看一下。你自己不用下单。",
        )
    if suffix == "HOT_STANDBY_INACTIVE":
        return _brief(
            running="跟单还在跑",
            who=wallet,
            what="热备进程停了。主进程若还在，跟单本身没停，只是少一层备份。",
            you="不用自己下单。系统会尝试拉起热备。",
        )
    if suffix == "STATUS_MISSING_OR_MALFORMED":
        return _brief(
            running="跟单可能停了",
            who=wallet,
            what="状态文件读不到，没法确认这一轮有没有跟上。",
            you="不用自己下单。正在自动检查。",
        )
    if suffix == "WS_SUBSCRIPTION_INACTIVE":
        return _brief(
            running="跟单可能变慢",
            who=wallet,
            what="实时行情断了，发现源钱包新动作可能变慢。",
            you="不用自己下单。系统会重连。",
        )
    if suffix == "LAST_CYCLE_NOT_SUCCESS":
        return _brief(
            running="跟单可能受影响",
            who=wallet,
            what="这一轮跟单循环没有成功结束。",
            you="需要马上看一下。你自己不用下单。",
        )
    if suffix == "CHAIN_CURSOR_LAG_INVALID":
        return _brief(
            running="跟单可能漏动作",
            who=wallet,
            what="链上进度落后或读数不对，可能暂时跟不上新动作。",
            you="需要马上看一下。你自己不用下单。",
        )
    if field == "status_issues":
        return _brief(
            running="跟单可能受影响",
            who=wallet,
            what="内部检查发现了问题，可能影响这一路跟单。",
            you="需要马上看一下。你自己不用下单。",
        )
    return _brief(
        running="跟单还在跑",
        who=wallet,
        what="官方接口或盘口限制了这一笔，所以没跟上。",
        you="不用管。系统不会因此重复下单。",
    )


def check_audit_state() -> str | None:
    try:
        last = None
        with AUDIT_JSONL.open("rb") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    last = line
        if last is None:
            return _brief(
                running="跟单可能停了",
                who="健康检查",
                what="健康记录是空的，没法确认现在正不正常。",
                you="需要马上看一下。你自己不用下单。",
            )
        payload = json.loads(last)
        state = payload.get("overall_state")
        if state == "OK":
            return None
        details = []
        profiles = payload.get("profiles")
        if isinstance(profiles, dict):
            for profile, row in sorted(profiles.items()):
                if not isinstance(row, dict):
                    continue
                for field in ("status_issues", "external_limitations"):
                    values = row.get(field)
                    if not isinstance(values, list):
                        continue
                    for value in values:
                        if not value:
                            continue
                        text = _humanize_audit_problem(
                            profile=str(profile),
                            field=field,
                            value=str(value),
                        )
                        if text:
                            details.append(text)
        if not details:
            return None
        return "\n\n".join(details[:8])
    except Exception:
        return _brief(
            running="跟单可能停了",
            who="健康检查",
            what="健康记录读失败，没法确认现在正不正常。",
            you="需要马上看一下。你自己不用下单。",
        )


def build_payload(text: str) -> bytes:
    if WEBHOOK_KIND in ("wecom", "dingtalk"):
        body = {"msgtype": "text", "text": {"content": text}}
    elif WEBHOOK_KIND == "bark":
        title = text.splitlines()[0][:40] if text.strip() else "跟单提醒"
        body = {"title": title, "body": text}
    else:  # slack / generic
        body = {"text": text}
    return json.dumps(body, ensure_ascii=False).encode("utf-8")


def send_alert(text: str) -> str:
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=build_payload(text),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return f"http_{resp.status}"


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["last_run_at_ms"] = now_ms()
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=1))
    tmp.replace(STATE_FILE)


def _individual_problems(warning: str | None) -> list[str]:
    """Drop the summary headline and return one current problem per line."""

    if not warning:
        return []
    text = str(warning).strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if lines and lines[0] in HEADLINES:
        text = "\n".join(lines[1:]).strip()
    return list(dict.fromkeys(block.strip() for block in text.split("\n\n") if block.strip()))


def _dedupe_key(fingerprint: str) -> str:
    """Keep a wording correction from re-sending the same active problem."""

    return str(fingerprint).replace("：累计历史上有 ", "：本版本发布后有 ")


def main() -> int:
    critical = check_units() + check_heartbeat()
    warning = check_audit_state()

    state = load_state()
    previous = {
        _dedupe_key(value)
        for value in (state.get("active_problem_fingerprints") or [])
    }
    current_rows = [
        (f"CRITICAL|{problem}", "CRITICAL", problem) for problem in critical
    ] + [
        (f"WARNING|{problem}", "WARNING", problem)
        for problem in _individual_problems(warning)
    ]
    current = {fingerprint for fingerprint, _, _ in current_rows}
    results: dict[str, str] = {}
    for fingerprint, severity, problem in current_rows:
        if _dedupe_key(fingerprint) in previous:
            continue
        message = problem
        results[fingerprint] = (
            send_alert(message) if WEBHOOK_URL else "no_webhook_configured"
        )
    had_critical = any(str(item).startswith("CRITICAL|") for item in previous)
    if not current and had_critical:
        recovered = _brief(
            running="跟单恢复了",
            who="实盘跟单",
            what="刚才可能停了的进程已经在跑，心跳正常。",
            you="不用管。",
        )
        results["RECOVERED"] = (
            send_alert(recovered) if WEBHOOK_URL else "no_webhook_configured"
        )
    state.update(
        alerting=bool(critical),
        active_problem_fingerprints=sorted(current),
        last_problem_results=results,
    )
    # Remove legacy aggregate state so a version cutover cannot resend history.
    state.pop("warning_fingerprint", None)
    if not current:
        state["recovered_at_ms"] = now_ms()
        print(json.dumps({"state": "OK"}, ensure_ascii=False))
    elif critical:
        print(json.dumps({"state": "CRITICAL", "problems": critical}, ensure_ascii=False))
    else:
        print(json.dumps({"state": "WARNING", "detail": warning}, ensure_ascii=False))
    save_state(state)
    return 1 if critical else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # 告警器自身异常也要留痕，绝不安静退出
        print(f"ALERTER_INTERNAL_ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(2)
