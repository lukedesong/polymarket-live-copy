from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


class BookNotReady(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ApplyResult:
    applied_changes: int
    ignored_for_book: bool = False


def _decimal(value: Any, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid_{field_name}") from exc
    return parsed


def _parse_levels(levels: Any) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for level in levels or ():
        price = _decimal(level.get("price"), field_name="book_price")
        size = _decimal(level.get("size"), field_name="book_size")
        if size < 0:
            raise ValueError("negative_book_size")
        if size:
            result[price] = size
    return result


@dataclass(slots=True)
class OrderBookState:
    asset_id: str
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    ready: bool = False
    server_timestamp: str | None = None

    @property
    def best_bid(self) -> Decimal | None:
        if not self.ready:
            raise BookNotReady(self.asset_id)
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        if not self.ready:
            raise BookNotReady(self.asset_id)
        return min(self.asks) if self.asks else None

    def invalidate(self) -> None:
        self.ready = False
        self.bids.clear()
        self.asks.clear()

    def apply(self, payload: Mapping[str, Any]) -> ApplyResult:
        event_type = payload.get("event_type")
        if event_type == "book":
            if str(payload.get("asset_id") or "") != self.asset_id:
                return ApplyResult(0)
            bids = _parse_levels(payload.get("bids"))
            asks = _parse_levels(payload.get("asks"))
            self.bids = bids
            self.asks = asks
            self.server_timestamp = str(payload.get("timestamp") or "") or None
            self.ready = True
            return ApplyResult(len(bids) + len(asks))
        if event_type == "price_change":
            if not self.ready:
                raise BookNotReady(self.asset_id)
            applied = 0
            for change in payload.get("price_changes") or ():
                if str(change.get("asset_id") or "") != self.asset_id:
                    continue
                self._apply_change(change)
                applied += 1
            self.server_timestamp = str(payload.get("timestamp") or "") or self.server_timestamp
            return ApplyResult(applied)
        return ApplyResult(0, ignored_for_book=True)

    def _apply_change(self, change: Mapping[str, Any]) -> None:
        price = _decimal(change.get("price"), field_name="book_price")
        size = _decimal(change.get("size"), field_name="book_size")
        if size < 0:
            raise ValueError("negative_book_size")
        side = str(change.get("side") or "").upper()
        if side == "BUY":
            levels = self.bids
        elif side == "SELL":
            levels = self.asks
        else:
            raise ValueError("invalid_book_side")
        if size == 0:
            levels.pop(price, None)
        else:
            levels[price] = size

    def canonical_levels(self) -> tuple[tuple[str, str, str], ...]:
        if not self.ready:
            raise BookNotReady(self.asset_id)
        bids = (("BUY", str(price), str(size)) for price, size in self.bids.items())
        asks = (("SELL", str(price), str(size)) for price, size in self.asks.items())
        return tuple(sorted((*bids, *asks)))
