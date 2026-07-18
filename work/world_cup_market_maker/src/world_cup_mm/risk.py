from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum


NO_NEW_INVENTORY_WINDOW = timedelta(minutes=30)  # user-specified value
REDUCE_ONLY_WINDOW = timedelta(minutes=15)  # user-specified value
CANCEL_WINDOW = timedelta(minutes=5)  # user-specified value


class RiskState(str, Enum):
    PREMATCH_OPEN = "PREMATCH_OPEN"
    NO_NEW_INVENTORY = "NO_NEW_INVENTORY"
    REDUCE_ONLY = "REDUCE_ONLY"
    CANCELLED_BLOCKED = "CANCELLED_BLOCKED"
    IN_PLAY_BLOCKED = "IN_PLAY_BLOCKED"
    MARKET_BLOCKED = "MARKET_BLOCKED"
    DATA_BLOCKED = "DATA_BLOCKED"


class RiskAction(str, Enum):
    NO_NEW_INVENTORY = "NO_NEW_INVENTORY"
    REDUCE_ONLY = "REDUCE_ONLY"
    CANCEL_MARKET_ORDERS = "CANCEL_MARKET_ORDERS"
    BLOCK_TRADING = "BLOCK_TRADING"


@dataclass(frozen=True, slots=True)
class RiskContext:
    condition_id: str
    market_open: bool
    accepting_orders: bool
    market_ws_connected: bool
    book_ready: bool
    sports_live: bool
    cancel_armed: bool


@dataclass(frozen=True, slots=True)
class RiskDecision:
    state: RiskState
    actions: tuple[RiskAction, ...]
    market_condition_id: str
    reason: str
    seconds_to_start: int


def _blocked(
    state: RiskState,
    context: RiskContext,
    reason: str,
    *,
    seconds_to_start: int,
) -> RiskDecision:
    actions = (
        (RiskAction.CANCEL_MARKET_ORDERS, RiskAction.BLOCK_TRADING)
        if context.cancel_armed
        else (RiskAction.BLOCK_TRADING,)
    )
    return RiskDecision(
        state=state,
        actions=actions,
        market_condition_id=context.condition_id,
        reason=reason,
        seconds_to_start=seconds_to_start,
    )


def evaluate_risk(
    *,
    start: datetime,
    now: datetime,
    context: RiskContext,
) -> RiskDecision:
    if start.tzinfo is None or now.tzinfo is None:
        raise ValueError("risk_time_missing_timezone")
    remaining = start - now
    seconds_to_start = int(remaining.total_seconds())
    if context.sports_live or now >= start:
        return _blocked(
            RiskState.IN_PLAY_BLOCKED,
            context,
            "game_live_or_started",
            seconds_to_start=seconds_to_start,
        )
    if not context.market_open or not context.accepting_orders:
        return _blocked(
            RiskState.MARKET_BLOCKED,
            context,
            "market_not_tradeable",
            seconds_to_start=seconds_to_start,
        )
    if not context.market_ws_connected or not context.book_ready:
        return _blocked(
            RiskState.DATA_BLOCKED,
            context,
            "market_data_not_ready",
            seconds_to_start=seconds_to_start,
        )
    if remaining <= CANCEL_WINDOW:
        return _blocked(
            RiskState.CANCELLED_BLOCKED,
            context,
            "inside_cancel_window",
            seconds_to_start=seconds_to_start,
        )
    if remaining <= REDUCE_ONLY_WINDOW:
        return RiskDecision(
            RiskState.REDUCE_ONLY,
            (RiskAction.REDUCE_ONLY,),
            context.condition_id,
            "inside_reduce_window",
            seconds_to_start,
        )
    if remaining <= NO_NEW_INVENTORY_WINDOW:
        return RiskDecision(
            RiskState.NO_NEW_INVENTORY,
            (RiskAction.NO_NEW_INVENTORY,),
            context.condition_id,
            "inside_no_new_inventory_window",
            seconds_to_start,
        )
    return RiskDecision(
        RiskState.PREMATCH_OPEN,
        (),
        context.condition_id,
        "prematch_open",
        seconds_to_start,
    )
