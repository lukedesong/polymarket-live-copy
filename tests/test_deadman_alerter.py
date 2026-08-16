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
    assert "zockdo 数据库不可用" in sent[0]

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
    assert "发现需要关注的跟单问题" in warning
    assert "zockdo：内部运行错误：数据库不可用，可能影响跟单" in warning
    assert "INTERNAL_RUNTIME" not in warning
    assert "overall_state" not in warning


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

    assert warning is not None
    assert "系统仍在运行，但存在未完成跟单或外部限制" in warning
    assert "CD90：本版本发布后有 55 个源动作未完成跟单" in warning
    assert "zockdo：本版本发布后有 29 个源动作未完成跟单" in warning
    assert "累计历史" not in warning
    assert "不代表服务停止" in warning
    assert "EXTERNAL_OR_CAUSAL_UNFILLED" not in warning
    assert "overall_state" not in warning


def test_deadman_covers_all_registered_profiles_by_default():
    assert set(alerter.REQUIRED_UNITS) == {
        "com.luke.polymarket.cd90-live.service",
        "com.luke.polymarket.cd90-live-hot-standby.service",
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
    assert "zockdo：有 1 个跟单动作尚未完成处理" in warning
    assert "只读核对，不会重复下单" in warning
    assert "zockdo：有 1 个源动作仍在等待处理" in warning
    assert "zockdo：发布后外部核对接口累计失败 22 次" in warning
    assert "UNRESOLVED_ACTIONS" not in warning
    assert "PENDING_ACTION_TARGETS" not in warning
    assert "POST_RELEASE_EXTERNAL_ERROR_EVENTS" not in warning


def test_each_new_problem_is_sent_once_without_repeating_other_active_problems(
    monkeypatch,
):
    state = {}
    sent = []
    warning = {
        "value": "发现需要关注的跟单问题。\nzockdo：问题甲。\nCD90：问题乙。"
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

    warning["value"] += "\nzockdo：问题丙。"
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

    assert warning is not None
    assert "temperature-in-seoul / seoul-35c-or-higher" in warning
    assert "BUY" in warning
    assert "目标 10 份，已成交 4 份，剩余 6 份" in warning
    assert "首次冻结价格边界 0.40" in warning
    assert "当前可成交价格差于首次冻结边界" in warning
    assert "CURRENT_BOOK_OUTSIDE_FIRST_ATTEMPT_PRICE" not in warning
