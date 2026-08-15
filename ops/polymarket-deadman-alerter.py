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
    "com.luke.polymarket.cd90-live.service",
    "com.luke.polymarket.cd90-live-hot-standby.service",
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
    "cd90": "CD90",
    "zockdo": "zockdo",
    "zockdo_full_wallet": "zockdo",
    "wallet_9506_full_wallet": "0x9506",
}


def now_ms() -> int:
    return int(time.time() * 1000)


def check_units() -> list[str]:
    problems = []
    for unit in REQUIRED_UNITS:
        try:
            out = subprocess.run(
                ["systemctl", "is-active", unit],
                capture_output=True, text=True, timeout=10,
            ).stdout.strip()
        except Exception as exc:  # systemctl 本身失败也要告警
            out = f"check_error:{type(exc).__name__}"
        if out != "active":
            problems.append(f"单元 {unit} 状态 {out}")
    return problems


def check_heartbeat() -> list[str]:
    problems = []
    try:
        payload = json.loads(STATUS_JSON.read_text())
    except Exception as exc:
        return [f"status.json 不可读/不可解析: {type(exc).__name__}"]
    hb = payload.get("heartbeat_at_ms") or (
        payload.get("runtime", {}) or {}
    ).get("heartbeat_at_ms")
    try:
        age_s = (now_ms() - int(hb)) / 1000
    except (TypeError, ValueError):
        problems.append("status.json 缺 heartbeat_at_ms")
        age_s = None
    if age_s is not None and age_s > HEARTBEAT_MAX_AGE_S:
        problems.append(f"心跳年龄 {int(age_s)}s 超过阈值 {HEARTBEAT_MAX_AGE_S}s")
    try:
        mtime_age = time.time() - STATUS_JSON.stat().st_mtime
        if mtime_age > 300:
            problems.append(f"status.json 文件 {int(mtime_age)}s 未更新")
    except OSError:
        pass
    return problems


def _plain_error_detail(detail: str) -> str:
    translations = {
        "database unavailable": "数据库不可用",
        "unable to open database file": "无法打开数据库文件",
        "database is locked": "数据库被锁定",
    }
    normalized = str(detail).strip()
    return translations.get(normalized.lower(), normalized or "未提供错误明细")


def _humanize_audit_problem(*, profile: str, field: str, value: str) -> str:
    wallet = PROFILE_DISPLAY_NAMES.get(str(profile), str(profile))
    issue = str(value).strip()
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
                "当前可成交价格差于首次冻结边界"
            ),
            "OFFICIAL_CONFIRMED_ZERO_FILL_RETRYABLE": (
                "上一笔已确认零成交，正在等待符合边界的流动性"
            ),
            "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE": (
                "链上已确认上一笔零成交，正在等待符合边界的流动性"
            ),
            "FAK_PARTIAL_FILL": "上一笔只成交一部分，正在补剩余数量",
            "CURRENT_BOOK_UNAVAILABLE_FOR_LIQUIDITY_RETRY": (
                "当前盘口暂时不可读取，本轮没有提交订单"
            ),
        }.get(str(detail.get("reason") or ""), "当前盘口没有满足受控重试条件")
        event_slug = str(detail.get("event_slug") or "未提供事件")
        market_slug = str(detail.get("market_slug") or "未提供市场")
        return (
            f"{wallet}：{event_slug} / {market_slug} 的 "
            f"{detail.get('side') or 'UNKNOWN'} 跟单，"
            f"目标 {detail.get('target_quantity') or '未知'} 份，"
            f"已成交 {detail.get('cumulative_filled_quantity') or '未知'} 份，"
            f"剩余 {detail.get('remaining_quantity') or '未知'} 份；"
            f"首次冻结价格边界 {detail.get('frozen_worst_price') or '未知'}；"
            f"本次未继续成交原因：{reason_text}。"
        )
    match = re.fullmatch(r"[A-Z0-9_]*UNRESOLVED_ACTIONS:(\d+)", issue)
    if match:
        return (
            f"{wallet}：有 {match.group(1)} 个跟单动作尚未完成处理。"
            "系统正在只读核对，不会重复下单，期间可能延迟确认成交。"
        )
    match = re.fullmatch(r"[A-Z0-9_]*PENDING_ACTION_TARGETS:(\d+)", issue)
    if match:
        return (
            f"{wallet}：有 {match.group(1)} 个源动作仍在等待处理，"
            "尚不能算作已完整跟单。"
        )
    match = re.fullmatch(
        r"[A-Z0-9_]*POST_RELEASE_EXTERNAL_ERROR_EVENTS:(\d+)", issue
    )
    if match:
        return (
            f"{wallet}：发布后外部核对接口累计失败 {match.group(1)} 次。"
            "这会延迟成交确认，但系统不会因此重复下单。"
        )
    match = re.fullmatch(r"[A-Z0-9_]*EXTERNAL_OR_CAUSAL_UNFILLED:(\d+)", issue)
    if match:
        return (
            f"{wallet}：本版本发布后有 {match.group(1)} 个源动作未完成跟单"
            "（可能包括盘口无深度、低于最低下单量或没有对应持仓）。"
            "这不代表服务停止。"
        )
    match = re.fullmatch(r"[A-Z0-9_]*POST_RELEASE_INTERNAL_ERROR_EVENTS:(\d+)", issue)
    if match:
        return (
            f"{wallet}：本次发布后记录到 {match.group(1)} 个内部错误，"
            "可能影响跟单，需要立即检查。"
        )
    if issue.startswith("INTERNAL_RUNTIME:"):
        detail = _plain_error_detail(issue.split(":", 1)[1])
        return f"{wallet}：内部运行错误：{detail}，可能影响跟单，需要立即检查。"
    if field == "status_issues":
        return (
            f"{wallet}：健康检查发现内部问题，可能影响跟单，需要立即检查。"
            f"详细标识：{issue}"
        )
    return (
        f"{wallet}：外部服务或市场条件限制了跟单，系统仍在运行。"
        f"详细标识：{issue}"
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
            return "审计 JSONL 为空"
        payload = json.loads(last)
        state = payload.get("overall_state")
        if state != "OK":
            details = []
            profiles = payload.get("profiles")
            if isinstance(profiles, dict):
                for profile, row in sorted(profiles.items()):
                    if not isinstance(row, dict):
                        continue
                    for field in ("status_issues", "external_limitations"):
                        values = row.get(field)
                        if isinstance(values, list):
                            details.extend(
                                _humanize_audit_problem(
                                    profile=str(profile),
                                    field=field,
                                    value=str(value),
                                )
                                for value in values
                                if value
                            )
            if state == "INTERNAL_DEGRADED":
                headline = "发现需要关注的跟单问题。"
            elif state == "EXTERNAL_DEGRADED":
                headline = "系统仍在运行，但存在未完成跟单或外部限制。"
            else:
                headline = "健康检查发现异常，需要检查。"
            return "\n".join((headline, *details[:8]))
    except Exception as exc:
        return f"审计 JSONL 读取失败: {type(exc).__name__}"
    return None


def build_payload(text: str) -> bytes:
    if WEBHOOK_KIND in ("wecom", "dingtalk"):
        body = {"msgtype": "text", "text": {"content": text}}
    elif WEBHOOK_KIND == "bark":
        body = {"title": "Polymarket 告警", "body": text}
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
    lines = [line.strip() for line in str(warning).splitlines() if line.strip()]
    if len(lines) > 1 and lines[0] in {
        "发现需要关注的跟单问题。",
        "系统仍在运行，但存在未完成跟单或外部限制。",
        "健康检查发现异常，需要检查。",
    }:
        lines = lines[1:]
    return list(dict.fromkeys(lines))


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
        message = f"【{severity}】Polymarket 跟单告警\n{problem}"
        results[fingerprint] = (
            send_alert(message) if WEBHOOK_URL else "no_webhook_configured"
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
