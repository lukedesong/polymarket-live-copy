from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Iterable, Literal

from .risk import RiskState
from .storage import PaperOrder, Store


@dataclass(frozen=True, slots=True)
class PaperAsset:
    condition_id: str
    market_id: str
    asset_id: str
    outcome: str
    minimum_order_size: Decimal
    maker_fee_bps: int


class PaperEngine:
    def __init__(
        self,
        store: Store,
        assets: Iterable[PaperAsset],
        *,
        fill_mode: Literal["strict", "touch"] = "strict",
    ) -> None:
        if fill_mode not in {"strict", "touch"}:
            raise ValueError(f"unsupported_fill_mode:{fill_mode}")
        self.store = store
        self.assets = {asset.asset_id: asset for asset in assets}
        self.fill_mode = fill_mode
        self.connected = True

    def on_book(
        self,
        *,
        asset_id: str,
        best_bid: Decimal,
        best_ask: Decimal,
        book_timestamp: str | None,
        risk_state: RiskState,
        now: datetime,
    ) -> None:
        asset = self.assets.get(asset_id)
        if asset is None:
            return
        self.store.mark_paper_position(asset_id, best_bid, marked_at=now)
        if not self.connected:
            return
        if risk_state is RiskState.PREMATCH_OPEN:
            self._quote_buy(asset, best_bid, book_timestamp, now)
            self._quote_sell_if_inventory(asset, best_ask, book_timestamp, now)
            return
        if risk_state is RiskState.REDUCE_ONLY:
            self.store.cancel_paper_orders(
                asset_id=asset_id,
                side="BUY",
                reason="reduce_only",
                cancelled_at=now,
            )
            self._quote_sell_if_inventory(asset, best_ask, book_timestamp, now)
            return
        self.store.cancel_paper_orders(
            asset_id=asset_id,
            reason=f"risk_{risk_state.value.lower()}",
            cancelled_at=now,
        )

    def on_trade(
        self,
        *,
        asset_id: str,
        trade_price: Decimal,
        trigger_event_hash: str,
        best_bid: Decimal,
        risk_state: RiskState,
        now: datetime,
    ) -> list[int]:
        if not self.connected or asset_id not in self.assets:
            return []
        if self.store.paper_trigger_seen(trigger_event_hash):
            return []
        allowed_sides = {
            RiskState.PREMATCH_OPEN: {"BUY", "SELL"},
            RiskState.REDUCE_ONLY: {"SELL"},
        }.get(risk_state, set())
        filled: list[int] = []
        for order in self.store.open_paper_orders(asset_id):
            if order.side not in allowed_sides:
                continue
            if self.fill_mode == "touch":
                crosses = (
                    order.side == "BUY" and trade_price <= order.price
                ) or (
                    order.side == "SELL" and trade_price >= order.price
                )
            else:
                crosses = (
                    order.side == "BUY" and trade_price < order.price
                ) or (
                    order.side == "SELL" and trade_price > order.price
                )
            if not crosses:
                continue
            if self.store.apply_paper_fill(
                order.order_id,
                trigger_event_hash=trigger_event_hash,
                trigger_price=trade_price,
                filled_at=now,
                best_bid=best_bid,
            ):
                filled.append(order.order_id)
                break
        return filled

    def apply_risk(
        self, condition_id: str, state: RiskState, *, now: datetime
    ) -> None:
        if state is RiskState.PREMATCH_OPEN:
            return
        if state is RiskState.REDUCE_ONLY:
            self.store.cancel_paper_orders(
                condition_id=condition_id,
                side="BUY",
                reason="reduce_only",
                cancelled_at=now,
            )
            return
        self.store.cancel_paper_orders(
            condition_id=condition_id,
            reason=f"risk_{state.value.lower()}",
            cancelled_at=now,
        )

    def disconnect(self, *, now: datetime) -> None:
        self.connected = False
        self.store.cancel_paper_orders(
            reason="market_data_disconnected",
            cancelled_at=now,
        )

    def reconnect(self) -> None:
        self.connected = True

    def _quote_buy(
        self,
        asset: PaperAsset,
        best_bid: Decimal,
        book_timestamp: str | None,
        now: datetime,
    ) -> None:
        self.store.open_paper_order(
            condition_id=asset.condition_id,
            market_id=asset.market_id,
            asset_id=asset.asset_id,
            outcome=asset.outcome,
            side="BUY",
            price=best_bid,
            quantity=asset.minimum_order_size,
            maker_fee_bps=asset.maker_fee_bps,
            quote_book_timestamp=book_timestamp,
            created_at=now,
        )

    def _quote_sell_if_inventory(
        self,
        asset: PaperAsset,
        best_ask: Decimal,
        book_timestamp: str | None,
        now: datetime,
    ) -> None:
        try:
            position = self.store.paper_position(asset.asset_id)
        except ValueError:
            position = None
        if position is None or position.quantity <= 0:
            self.store.cancel_paper_orders(
                asset_id=asset.asset_id,
                side="SELL",
                reason="no_inventory",
                cancelled_at=now,
            )
            return
        quantity = min(asset.minimum_order_size, position.quantity)
        self.store.open_paper_order(
            condition_id=asset.condition_id,
            market_id=asset.market_id,
            asset_id=asset.asset_id,
            outcome=asset.outcome,
            side="SELL",
            price=best_ask,
            quantity=quantity,
            maker_fee_bps=asset.maker_fee_bps,
            quote_book_timestamp=book_timestamp,
            created_at=now,
        )
