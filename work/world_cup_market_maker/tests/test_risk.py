from datetime import datetime, timedelta, timezone

import pytest

from world_cup_mm.risk import (
    RiskAction,
    RiskContext,
    RiskState,
    evaluate_risk,
)


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


def healthy_context(**overrides):
    values = {
        "condition_id": "condition-a",
        "market_open": True,
        "accepting_orders": True,
        "market_ws_connected": True,
        "book_ready": True,
        "sports_live": False,
        "cancel_armed": False,
    }
    values.update(overrides)
    return RiskContext(**values)


@pytest.mark.parametrize(
    ("minutes", "expected"),
    [
        (31, RiskState.PREMATCH_OPEN),
        (30, RiskState.NO_NEW_INVENTORY),
        (15, RiskState.REDUCE_ONLY),
        (5, RiskState.CANCELLED_BLOCKED),
    ],
)
def test_user_specified_boundaries_enter_conservative_state(minutes, expected):
    decision = evaluate_risk(
        start=NOW + timedelta(minutes=minutes),
        now=NOW,
        context=healthy_context(),
    )

    assert decision.state is expected


def test_live_sports_status_overrides_scheduled_start_and_cancels_when_armed():
    decision = evaluate_risk(
        start=NOW + timedelta(hours=1),
        now=NOW,
        context=healthy_context(sports_live=True, cancel_armed=True),
    )

    assert decision.state is RiskState.IN_PLAY_BLOCKED
    assert decision.actions == (
        RiskAction.CANCEL_MARKET_ORDERS,
        RiskAction.BLOCK_TRADING,
    )


def test_armed_disconnect_cancels_only_affected_market():
    decision = evaluate_risk(
        start=NOW + timedelta(hours=1),
        now=NOW,
        context=healthy_context(market_ws_connected=False, cancel_armed=True),
    )

    assert decision.state is RiskState.DATA_BLOCKED
    assert decision.market_condition_id == "condition-a"
    assert decision.actions == (
        RiskAction.CANCEL_MARKET_ORDERS,
        RiskAction.BLOCK_TRADING,
    )


def test_data_only_disconnect_blocks_without_claiming_to_cancel():
    decision = evaluate_risk(
        start=NOW + timedelta(hours=1),
        now=NOW,
        context=healthy_context(market_ws_connected=False, cancel_armed=False),
    )

    assert decision.actions == (RiskAction.BLOCK_TRADING,)


def test_closed_market_blocks_before_prematch_time_state():
    decision = evaluate_risk(
        start=NOW + timedelta(hours=1),
        now=NOW,
        context=healthy_context(market_open=False),
    )

    assert decision.state is RiskState.MARKET_BLOCKED
    assert decision.reason == "market_not_tradeable"


def test_missing_fresh_book_is_fail_closed():
    decision = evaluate_risk(
        start=NOW + timedelta(hours=1),
        now=NOW,
        context=healthy_context(book_ready=False),
    )

    assert decision.state is RiskState.DATA_BLOCKED


def test_risk_rejects_naive_clock_inputs():
    with pytest.raises(ValueError, match="risk_time_missing_timezone"):
        evaluate_risk(
            start=datetime(2026, 7, 18, 21),
            now=NOW,
            context=healthy_context(),
        )
