from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable, Literal, Mapping

from .market_params import ClobMarketParams
from .paper_engine import PaperAsset, PaperEngine
from .risk import RiskContext, RiskDecision, evaluate_risk
from .storage import Store, StoredMarket


def build_paper_assets(
    markets: Iterable[StoredMarket],
    params_by_condition: Mapping[str, ClobMarketParams],
) -> list[PaperAsset]:
    assets: list[PaperAsset] = []
    for market in markets:
        params = params_by_condition[market.condition_id]
        for asset_id in market.token_ids:
            outcome = params.outcomes.get(asset_id)
            if not outcome:
                raise ValueError(f"missing_outcome:{asset_id}")
            assets.append(
                PaperAsset(
                    condition_id=market.condition_id,
                    market_id=market.market_id,
                    asset_id=asset_id,
                    outcome=outcome,
                    minimum_order_size=params.minimum_order_size,
                    maker_fee_bps=params.maker_fee_bps,
                )
            )
    return assets


def _event_hash(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaperRuntimeSink:
    def __init__(
        self,
        store: Store,
        session_id: str,
        markets: Iterable[StoredMarket],
        params_by_condition: Mapping[str, ClobMarketParams],
        *,
        fill_mode: Literal["strict", "touch"] = "strict",
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.markets = list(markets)
        self.market_by_asset = {
            asset_id: market
            for market in self.markets
            for asset_id in market.token_ids
        }
        self.engine = PaperEngine(
            store,
            build_paper_assets(self.markets, params_by_condition),
            fill_mode=fill_mode,
        )
        self.fill_mode = fill_mode
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.connected_flag = False

    def _now(self) -> datetime:
        now = self.now_fn()
        if now.tzinfo is None:
            raise ValueError("paper_runtime_time_missing_timezone")
        return now

    def _decision(self, market: StoredMarket, asset_id: str) -> RiskDecision:
        return evaluate_risk(
            start=market.game_start_time,
            now=self._now(),
            context=RiskContext(
                condition_id=market.condition_id,
                market_open=True,
                accepting_orders=True,
                market_ws_connected=self.connected_flag,
                book_ready=self.store.book_ready(asset_id),
                sports_live=self.store.latest_sports_live(market.event_slug),
                cancel_armed=True,
            ),
        )

    def _apply_asset_book(self, asset_id: str, timestamp: str | None) -> None:
        market = self.market_by_asset.get(asset_id)
        if market is None:
            return
        bid_text, ask_text = self.store.best_quotes(asset_id)
        decision = self._decision(market, asset_id)
        self.store.record_risk_decision(decision, decided_at=self._now())
        if bid_text is None or ask_text is None:
            self.engine.apply_risk(market.condition_id, decision.state, now=self._now())
            return
        self.engine.on_book(
            asset_id=asset_id,
            best_bid=Decimal(bid_text),
            best_ask=Decimal(ask_text),
            book_timestamp=timestamp,
            risk_state=decision.state,
            now=self._now(),
        )

    def evaluate_all(self) -> None:
        for market in self.markets:
            decisions = [self._decision(market, asset_id) for asset_id in market.token_ids]
            state = decisions[0].state
            self.engine.apply_risk(market.condition_id, state, now=self._now())

    async def connected(self) -> None:
        self.store.start_session(
            self.session_id,
            selection_mode=f"paper_frontier_{self.fill_mode}",
            started_at=self._now(),
        )
        self.connected_flag = True
        self.engine.reconnect()

    async def market_event(self, payload: dict[str, object]) -> None:
        if not self.store.record_market_event(
            self.session_id, payload, received_at=self._now()
        ):
            return
        event_type = str(payload.get("event_type") or "")
        if event_type == "price_change":
            asset_ids = [
                str(change.get("asset_id"))
                for change in (payload.get("price_changes") or [])  # type: ignore[union-attr]
                if isinstance(change, dict) and change.get("asset_id")
            ]
        else:
            asset_id = str(payload.get("asset_id") or "")
            asset_ids = [asset_id] if asset_id else []
        timestamp = str(payload.get("timestamp") or "") or None
        if event_type in {"book", "price_change"}:
            for asset_id in asset_ids:
                self._apply_asset_book(asset_id, timestamp)
            return
        if event_type != "last_trade_price":
            return
        for asset_id in asset_ids:
            market = self.market_by_asset.get(asset_id)
            bid_text, _ask_text = self.store.best_quotes(asset_id)
            if market is None or bid_text is None:
                continue
            try:
                trade_price = Decimal(str(payload.get("price")))
            except (InvalidOperation, TypeError, ValueError):
                continue
            self.engine.on_trade(
                asset_id=asset_id,
                trade_price=trade_price,
                trigger_event_hash=_event_hash(payload),
                best_bid=Decimal(bid_text),
                risk_state=self._decision(market, asset_id).state,
                now=self._now(),
            )

    async def sports_event(self, payload: dict[str, object]) -> None:
        if not self.store.record_sports_event(
            self.session_id, payload, received_at=self._now()
        ):
            return
        self.evaluate_all()

    async def disconnected(self) -> None:
        self.connected_flag = False
        self.engine.disconnect(now=self._now())
        self.store.invalidate_session_books(self.session_id, ended_at=self._now())
