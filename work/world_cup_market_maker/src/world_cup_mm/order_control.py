from __future__ import annotations

from typing import Any, Protocol

from py_clob_client_v2.clob_types import OrderMarketCancelParams


class OrderControl(Protocol):
    def cancel_market_orders(self, condition_id: str) -> Any: ...


class RecordingOrderControl:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self._seen: set[str] = set()

    def cancel_market_orders(self, condition_id: str) -> dict[str, str]:
        if not condition_id:
            raise ValueError("missing_condition_id")
        if condition_id not in self._seen:
            self._seen.add(condition_id)
            self.calls.append(condition_id)
        return {"status": "recorded", "condition_id": condition_id}


class AuthenticatedOrderControl:
    def __init__(self, client: Any) -> None:
        self._client = client

    def cancel_market_orders(self, condition_id: str) -> Any:
        if not condition_id:
            raise ValueError("missing_condition_id")
        payload = OrderMarketCancelParams(market=condition_id)
        return self._client.cancel_market_orders(payload)
