import importlib.util
import json
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "ops"
    / "polymarket-deadman-alerter.py"
)
SPEC = importlib.util.spec_from_file_location("polymarket_deadman_alerter", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
alerter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(alerter)


def test_health_warning_sends_once_and_a_changed_problem_sends_again(monkeypatch):
    state = {}
    sent = []
    warning = {"value": "发现内部故障：zockdo 数据库不可用，可能影响跟单。"}

    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(alerter, "check_audit_state", lambda: warning["value"])
    monkeypatch.setattr(alerter, "load_state", lambda: dict(state))
    monkeypatch.setattr(alerter, "now_ms", lambda: 1_000)
    monkeypatch.setattr(
        alerter,
        "save_state",
        lambda new_state: state.update(new_state),
    )
    monkeypatch.setattr(
        alerter,
        "send_alert",
        lambda message: sent.append(message) or "http_200",
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert len(sent) == 1
    assert "zockdo 数据库不可用" in sent[0] or "数据库" in sent[0]

    assert alerter.main() == 0
    assert len(sent) == 1

    warning["value"] = "系统仍在运行，但有一个源动作因盘口无深度未成交。"
    assert alerter.main() == 0
    assert len(sent) == 2
    assert "盘口无深度未成交" in sent[-1]


def test_health_warning_names_the_profile_and_problem(tmp_path, monkeypatch):
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps(
            {
                "overall_state": "INTERNAL_DEGRADED",
                "profiles": {
                    "zockdo": {
                        "status_issues": ["INTERNAL_RUNTIME:database unavailable"],
                        "external_limitations": [],
                    }
                },
            }
        )
        + "\n"
    )
    monkeypatch.setattr(alerter, "AUDIT_JSONL", audit_path)

    warning = alerter.check_audit_state()

    assert warning is not None
    assert "跟单可能受影响" in warning
    assert "谁：网球跟单 zockdo" in warning
    assert "数据库打不开" in warning
    assert "你自己不用下单" in warning
    assert "INTERNAL_RUNTIME" not in warning
    assert "overall_state" not in warning
    assert "详细标识" not in warning


def test_unfilled_only_health_does_not_send_any_bark(tmp_path, monkeypatch):
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps(
            {
                "overall_state": "EXTERNAL_DEGRADED",
                "profiles": {
                    "zockdo_full_wallet": {
                        "status_issues": [],
                        "external_limitations": [
                            "ZOCKDO_FULL_WALLET_EXTERNAL_OR_CAUSAL_UNFILLED:1"
                        ],
                    }
                },
            }
        )
        + "\n"
    )
    sent = []
    monkeypatch.setattr(alerter, "AUDIT_JSONL", audit_path)
    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(alerter, "load_state", lambda: {})
    monkeypatch.setattr(alerter, "save_state", lambda _state: None)
    monkeypatch.setattr(
        alerter, "send_alert", lambda message: sent.append(message) or "http_200"
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert sent == []


def test_external_unfilled_warning_is_plain_chinese_and_explains_impact(
    tmp_path, monkeypatch
):
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps(
            {
                "overall_state": "EXTERNAL_DEGRADED",
                "profiles": {
                    "cd90": {
                        "status_issues": [],
                        "external_limitations": [
                            "CD90_EXTERNAL_OR_CAUSAL_UNFILLED:55"
                        ],
                    },
                    "zockdo_full_wallet": {
                        "status_issues": [],
                        "external_limitations": [
                            "ZOCKDO_FULL_WALLET_EXTERNAL_OR_CAUSAL_UNFILLED:29"
                        ],
                    },
                },
            }
        )
        + "\n"
    )
    monkeypatch.setattr(alerter, "AUDIT_JSONL", audit_path)

    warning = alerter.check_audit_state()

    assert warning is None


def test_deadman_covers_all_registered_profiles_by_default():
    assert set(alerter.REQUIRED_UNITS) == {
        "com.luke.polymarket.zockdo-live.service",
        "com.luke.polymarket.zockdo-live-hot-standby.service",
        "com.luke.polymarket.wallet-9506-live.service",
        "com.luke.polymarket.wallet-9506-live-hot-standby.service",
    }


def test_user_paused_profile_is_not_treated_as_a_dead_executor(tmp_path, monkeypatch):
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps(
            {
                "profiles": {
                    "cd90": {
                        "paused": True,
                        "unit": "com.luke.polymarket.cd90-live.service",
                        "hot_standby_unit": (
                            "com.luke.polymarket.cd90-live-hot-standby.service"
                        ),
                    },
                    "zockdo_full_wallet": {
                        "paused": False,
                        "unit": "com.luke.polymarket.zockdo-live.service",
                        "hot_standby_unit": (
                            "com.luke.polymarket.zockdo-live-hot-standby.service"
                        ),
                    },
                }
            }
        )
        + "\n"
    )
    monkeypatch.setattr(alerter, "AUDIT_JSONL", audit_path)

    assert alerter.expected_active_units() == (
        "com.luke.polymarket.zockdo-live.service",
        "com.luke.polymarket.zockdo-live-hot-standby.service",
    )


def test_live_unresolved_submission_warning_explains_exact_safety_state(
    tmp_path, monkeypatch
):
    audit_path = tmp_path / "audit.jsonl"
    audit_path.write_text(
        json.dumps(
            {
                "overall_state": "INTERNAL_DEGRADED",
                "profiles": {
                    "zockdo_full_wallet": {
                        "status_issues": [
                            "ZOCKDO_FULL_WALLET_UNRESOLVED_ACTIONS:1"
                        ],
                        "external_limitations": [
                            "ZOCKDO_FULL_WALLET_PENDING_ACTION_TARGETS:1",
                            "ZOCKDO_FULL_WALLET_POST_RELEASE_EXTERNAL_ERROR_EVENTS:22",
                        ],
                    }
                },
            }
        )
        + "\n"
    )
    monkeypatch.setattr(alerter, "AUDIT_JSONL", audit_path)

    warning = alerter.check_audit_state()

    assert warning is not None
    assert "谁：网球跟单 zockdo" in warning
    assert "有 1 笔订单还没和官方结果对上" in warning
    assert "不会重复下单" in warning
    assert "官方对账接口这一阵对不上" not in warning
    assert "源动作仍在等待处理" not in warning
    assert "UNRESOLVED_ACTIONS" not in warning
    assert "PENDING_ACTION_TARGETS" not in warning
    assert "POST_RELEASE_EXTERNAL_ERROR_EVENTS" not in warning
    assert "详细标识" not in warning


def test_each_new_problem_is_sent_once_without_repeating_other_active_problems(
    monkeypatch,
):
    state = {}
    sent = []
    warning = {
        "value": (
            "跟单可能受影响\n谁：网球跟单 zockdo\n事：问题甲。\n你：需要马上看一下。\n\n"
            "跟单可能受影响\n谁：钱包 9506\n事：问题乙。\n你：需要马上看一下。"
        )
    }
    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(alerter, "check_audit_state", lambda: warning["value"])
    monkeypatch.setattr(alerter, "load_state", lambda: dict(state))
    monkeypatch.setattr(alerter, "save_state", lambda value: state.update(value))
    monkeypatch.setattr(alerter, "now_ms", lambda: 1_000)
    monkeypatch.setattr(
        alerter, "send_alert", lambda message: sent.append(message) or "http_200"
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert len(sent) == 2
    assert "问题甲" in sent[0] and "问题乙" not in sent[0]
    assert "问题乙" in sent[1] and "问题甲" not in sent[1]

    assert alerter.main() == 0
    assert len(sent) == 2

    warning["value"] += (
        "\n\n跟单可能受影响\n谁：网球跟单 zockdo\n事：问题丙。\n你：需要马上看一下。"
    )
    assert alerter.main() == 0
    assert len(sent) == 3
    assert "问题丙" in sent[-1]
    assert "问题甲" not in sent[-1]
    assert "问题乙" not in sent[-1]


def test_wording_fix_does_not_resend_the_same_current_version_problem(monkeypatch):
    old_problem = "CD90：累计历史上有 6 个源动作未完成跟单。"
    new_problem = "CD90：本版本发布后有 6 个源动作未完成跟单。"
    state = {"active_problem_fingerprints": [f"WARNING|{old_problem}"]}
    sent = []
    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(
        alerter,
        "check_audit_state",
        lambda: f"系统仍在运行，但存在未完成跟单或外部限制。\n{new_problem}",
    )
    monkeypatch.setattr(alerter, "load_state", lambda: dict(state))
    monkeypatch.setattr(alerter, "save_state", lambda value: state.update(value))
    monkeypatch.setattr(alerter, "now_ms", lambda: 2_000)
    monkeypatch.setattr(
        alerter, "send_alert", lambda message: sent.append(message) or "http_200"
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert sent == []
    assert state["active_problem_fingerprints"] == [f"WARNING|{new_problem}"]


def test_four_line_brief_is_one_bark_not_four(monkeypatch):
    state = {}
    sent = []
    brief = (
        "跟单可能受影响\n"
        "谁：网球跟单 zockdo\n"
        "事：程序内部出错了：数据库打不开。\n"
        "你：需要马上看一下。你自己不用下单。"
    )
    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(alerter, "check_audit_state", lambda: brief)
    monkeypatch.setattr(alerter, "load_state", lambda: dict(state))
    monkeypatch.setattr(alerter, "save_state", lambda value: state.update(value))
    monkeypatch.setattr(alerter, "now_ms", lambda: 1_000)
    monkeypatch.setattr(
        alerter, "send_alert", lambda message: sent.append(message) or "http_200"
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert len(sent) == 1
    assert sent[0] == brief
    assert "谁：网球跟单 zockdo" in sent[0]
    assert "你：需要马上看一下" in sent[0]


def test_unit_down_alert_does_not_name_systemd_unit():
    text = alerter._brief(
        running="跟单可能停了",
        who=alerter.UNIT_DISPLAY_NAMES["com.luke.polymarket.zockdo-live.service"],
        what="这个进程现在不是运行中，当前状态是inactive。",
        you="不用自己下单。系统会尝试自动拉起。",
    )
    assert "com.luke.polymarket" not in text
    assert "【WARNING】" not in text
    assert "谁：网球跟单 zockdo 主进程" in text
    assert "你：不用自己下单" in text


def test_unknown_internal_code_does_not_lead_with_the_code():
    text = alerter._humanize_audit_problem(
        profile="zockdo_full_wallet",
        field="status_issues",
        value="ZOCKDO_FULL_WALLET_SOME_NEW_INTERNAL_CODE:1",
    )
    assert text is not None
    assert "跟单可能受影响" in text
    assert "SOME_NEW_INTERNAL_CODE" not in text
    assert "详细标识" not in text
    assert "你自己不用下单" in text


def test_liquidity_retry_warning_names_market_target_fill_remainder_and_reason(
    tmp_path, monkeypatch
):
    audit_path = tmp_path / "audit.jsonl"
    detail = {
        "event_slug": "temperature-in-seoul",
        "market_slug": "seoul-35c-or-higher",
        "side": "BUY",
        "target_quantity": "10",
        "cumulative_filled_quantity": "4",
        "remaining_quantity": "6",
        "frozen_worst_price": "0.40",
        "reason": "CURRENT_BOOK_OUTSIDE_FIRST_ATTEMPT_PRICE",
    }
    audit_path.write_text(
        json.dumps(
            {
                "overall_state": "EXTERNAL_DEGRADED",
                "profiles": {
                    "cd90": {
                        "status_issues": [],
                        "external_limitations": [
                            "CD90_LIQUIDITY_RETRY_PENDING:"
                            + json.dumps(detail, separators=(",", ":"))
                        ],
                    }
                },
            }
        )
        + "\n"
    )
    monkeypatch.setattr(alerter, "AUDIT_JSONL", audit_path)

    warning = alerter.check_audit_state()

    assert warning is None


def test_process_recovery_sends_one_all_clear(monkeypatch):
    state = {
        "active_problem_fingerprints": [
            "CRITICAL|跟单可能停了\n谁：网球跟单 zockdo 主进程\n事：停了。\n你：不用自己下单。"
        ]
    }
    sent = []
    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(alerter, "check_audit_state", lambda: None)
    monkeypatch.setattr(alerter, "load_state", lambda: dict(state))
    monkeypatch.setattr(alerter, "save_state", lambda value: state.update(value))
    monkeypatch.setattr(alerter, "now_ms", lambda: 3_000)
    monkeypatch.setattr(
        alerter, "send_alert", lambda message: sent.append(message) or "http_200"
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert len(sent) == 1
    assert "跟单恢复了" in sent[0]
    assert "你：不用管" in sent[0]
    assert state["active_problem_fingerprints"] == []


def test_unfilled_fingerprint_clearing_does_not_send_recovery(monkeypatch):
    state = {
        "active_problem_fingerprints": [
            "WARNING|zockdo：本版本发布后有 29 个源动作未完成跟单。"
        ]
    }
    sent = []
    monkeypatch.setattr(alerter, "check_units", lambda: [])
    monkeypatch.setattr(alerter, "check_heartbeat", lambda: [])
    monkeypatch.setattr(alerter, "check_audit_state", lambda: None)
    monkeypatch.setattr(alerter, "load_state", lambda: dict(state))
    monkeypatch.setattr(alerter, "save_state", lambda value: state.update(value))
    monkeypatch.setattr(alerter, "now_ms", lambda: 3_000)
    monkeypatch.setattr(
        alerter, "send_alert", lambda message: sent.append(message) or "http_200"
    )
    monkeypatch.setattr(alerter, "WEBHOOK_URL", "https://example.invalid/bark")

    assert alerter.main() == 0
    assert sent == []
