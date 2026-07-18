from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from .order_control import OrderControl
from .risk import (
    CANCEL_WINDOW,
    NO_NEW_INVENTORY_WINDOW,
    REDUCE_ONLY_WINDOW,
    RiskAction,
    RiskContext,
    RiskDecision,
    evaluate_risk,
)
from .storage import StoredMarket


def next_transition_time(
    markets: Iterable[StoredMarket],
    now: datetime,
) -> datetime | None:
    candidates: list[datetime] = []
    for market in markets:
        candidates.extend(
            transition
            for transition in (
                market.game_start_time - NO_NEW_INVENTORY_WINDOW,
                market.game_start_time - REDUCE_ONLY_WINDOW,
                market.game_start_time - CANCEL_WINDOW,
                market.game_start_time,
            )
            if transition > now
        )
    return min(candidates) if candidates else None


class RiskRuntime:
    def __init__(
        self,
        store: Any,
        markets: Iterable[StoredMarket],
        order_control: OrderControl,
        *,
        cancel_capable: bool,
    ) -> None:
        self.store = store
        self.markets = tuple(markets)
        self.order_control = order_control
        self.cancel_capable = cancel_capable
        self._last_state: dict[str, str] = {}
        self._cancelled_conditions: set[str] = set()

    def evaluate_all(self, *, now: datetime | None = None) -> list[RiskDecision]:
        current = now or datetime.now(timezone.utc)
        session = self.store.latest_session_summary()
        connected = bool(session and session["connected"])
        decisions: list[RiskDecision] = []
        for market in self.markets:
            decision = evaluate_risk(
                start=market.game_start_time,
                now=current,
                context=RiskContext(
                    condition_id=market.condition_id,
                    market_open=True,
                    accepting_orders=True,
                    market_ws_connected=connected,
                    book_ready=all(
                        self.store.book_ready(asset_id)
                        for asset_id in market.token_ids
                    ),
                    sports_live=self.store.latest_sports_live(market.event_slug),
                    cancel_armed=self.cancel_capable,
                ),
            )
            decisions.append(decision)
            previous = self._last_state.get(market.condition_id)
            if previous != decision.state.value:
                self.store.record_risk_decision(decision, decided_at=current)
                self._last_state[market.condition_id] = decision.state.value
            if (
                RiskAction.CANCEL_MARKET_ORDERS in decision.actions
                and market.condition_id not in self._cancelled_conditions
            ):
                self._deliver_cancel(market.condition_id, current)
        return decisions

    def _deliver_cancel(self, condition_id: str, now: datetime) -> None:
        try:
            self.order_control.cancel_market_orders(condition_id)
        except Exception as exc:
            self.store.record_risk_action(
                condition_id,
                RiskAction.CANCEL_MARKET_ORDERS.value,
                "failed",
                created_at=now,
                detail=f"{type(exc).__name__}:{exc}",
            )
            raise
        self._cancelled_conditions.add(condition_id)
        self.store.record_risk_action(
            condition_id,
            RiskAction.CANCEL_MARKET_ORDERS.value,
            "delivered",
            created_at=now,
        )


async def monitor_risk(
    runtime: RiskRuntime,
    stop: asyncio.Event,
    *,
    now_fn: Callable[[], datetime] | None = None,
) -> None:
    clock = now_fn or (lambda: datetime.now(timezone.utc))
    while not stop.is_set():
        current = clock()
        runtime.evaluate_all(now=current)
        transition = next_transition_time(runtime.markets, current)
        if transition is None:
            await stop.wait()
            return
        delay = (transition - current).total_seconds()
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            continue
