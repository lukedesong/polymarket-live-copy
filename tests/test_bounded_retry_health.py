from app.server_health_heartbeat import (
    live_status_issues,
    profile_registry_issues,
    recovered_internal_before_success,
)


def test_residual_coordinator_sleeve_may_be_unmonitored():
    assert profile_registry_issues(
        expected_profiles={"cd90", "zockdo_full_wallet", "wallet_9506_full_wallet"},
        monitored_profiles={"zockdo_full_wallet", "wallet_9506_full_wallet"},
        residual_profiles={"cd90"},
    ) == []


def test_recovered_internal_before_later_success_does_not_latch():
    assert recovered_internal_before_success(
        internal_event_count=1,
        code_repair_event_count=0,
        latest_internal_occurred_at_ms=100,
        last_successful_cycle_at_ms=200,
        last_cycle_outcome="SUCCESS",
    ) is True


def test_unrecovered_or_code_repair_internal_still_latches():
    assert recovered_internal_before_success(
        internal_event_count=1,
        code_repair_event_count=1,
        latest_internal_occurred_at_ms=100,
        last_successful_cycle_at_ms=200,
        last_cycle_outcome="SUCCESS",
    ) is False
    assert recovered_internal_before_success(
        internal_event_count=1,
        code_repair_event_count=0,
        latest_internal_occurred_at_ms=300,
        last_successful_cycle_at_ms=200,
        last_cycle_outcome="SUCCESS",
    ) is False
    assert recovered_internal_before_success(
        internal_event_count=1,
        code_repair_event_count=0,
        latest_internal_occurred_at_ms=100,
        last_successful_cycle_at_ms=200,
        last_cycle_outcome="ERROR",
    ) is False


def test_reserved_coordinator_sleeve_cannot_drop_out_of_health():
    assert profile_registry_issues(
        expected_profiles={"cd90", "zockdo_full_wallet", "wallet_9506_full_wallet"},
        monitored_profiles={"wallet_9506_full_wallet"},
        residual_profiles={"cd90"},
    ) == ["UNMONITORED_COORDINATOR_PROFILES:zockdo_full_wallet"]


def _payload() -> dict:
    return {
        "mode": "CASH_LIVE",
        "paper_only": False,
        "real_order_submission_enabled": True,
        "account": {"available_cash_usd": "1"},
        "runtime": {
            "ws_subscription_active": "true",
            "current_head": "10",
            "last_processed_block": "10",
            "last_cycle_outcome": "success",
        },
        "action_receipt_count": 0,
        "action_terminal_counts": {},
        "action_fidelity": {
            "conservation_passed": True,
            "internal_error": 0,
            "missing_target": 0,
            "unclassified_target": 0,
            "retryable_target_terminal_transition_mismatch": 0,
        },
        "automatic_redemption": {
            "enabled": True,
            "worker_state": "idle",
            "terminal_counts": {},
        },
        "liquidity_retry": {
            "policy_id": "LIQUIDITY_ONLY_RETRY_V2",
            "effective_after_block": "9",
            "deadline_ms": None,
            "unknown_repost_violation_count": 0,
            "target_conservation_violation_count": 0,
            "pending_actions": [],
        },
    }


def test_live_health_accepts_valid_liquidity_retry_evidence():
    assert live_status_issues(_payload(), profile_key="fuu") == []


def test_live_health_fails_unknown_repost_and_target_nonconservation():
    payload = _payload()
    payload["liquidity_retry"]["unknown_repost_violation_count"] = 1
    payload["liquidity_retry"]["target_conservation_violation_count"] = 2
    assert live_status_issues(payload, profile_key="fuu") == [
        "FUU_LIQUIDITY_RETRY_UNKNOWN_REPOSTS:1",
        "FUU_LIQUIDITY_RETRY_TARGET_NONCONSERVATION:2",
    ]


def test_live_health_rejects_historical_retry_as_current_policy():
    payload = _payload()
    payload.pop("liquidity_retry")
    payload["bounded_retry"] = {
        "policy_id": "USER_AUTHORIZED_BOUNDED_LIVE_RETRY_V1",
        "effective_after_block": "9",
    }

    assert live_status_issues(payload, profile_key="fuu") == [
        "FUU_LIQUIDITY_RETRY_POLICY_MISSING"
    ]
