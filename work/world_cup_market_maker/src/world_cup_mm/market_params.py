from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from urllib.parse import quote
from urllib.request import Request, urlopen


CLOB_MARKETS_URL = "https://clob.polymarket.com/clob-markets"


@dataclass(frozen=True, slots=True)
class ClobMarketParams:
    minimum_order_size: Decimal
    maker_fee_bps: int
    tick_size: Decimal
    outcomes: dict[str, str]
    taker_fee_rate: Decimal | None = None
    fee_exponent: int | None = None
    taker_only: bool = False


def _decimal(value: Any, error: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(error) from exc
    return result


def parse_clob_market_info(payload: Mapping[str, Any]) -> ClobMarketParams:
    minimum = _decimal(payload.get("mos"), "invalid_minimum_order_size")
    tick = _decimal(payload.get("mts"), "invalid_tick_size")
    if minimum <= 0:
        raise ValueError("invalid_minimum_order_size")
    if tick <= 0:
        raise ValueError("invalid_tick_size")
    fee_details = payload.get("fd")
    taker_only = isinstance(fee_details, Mapping) and fee_details.get("to") is True
    taker_fee_rate: Decimal | None = None
    fee_exponent: int | None = None
    if isinstance(fee_details, Mapping):
        taker_fee_rate = _decimal(
            fee_details.get("r"), "invalid_taker_fee_rate"
        )
        if taker_fee_rate < 0:
            raise ValueError("invalid_taker_fee_rate")
        try:
            fee_exponent = int(fee_details.get("e"))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid_fee_exponent") from exc
        if fee_exponent < 0:
            raise ValueError("invalid_fee_exponent")
    try:
        raw_maker_fee = int(payload.get("mbf"))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_maker_fee") from exc
    # CLOB V2 fee details are authoritative for platform fees. When `to` is
    # true, Polymarket documents that makers pay zero platform fee even though
    # the market response may expose non-zero base/builder fee fields.
    maker_fee = 0 if taker_only else raw_maker_fee
    if maker_fee != 0:
        raise ValueError("nonzero_maker_fee")
    outcomes = {
        str(token.get("t") or ""): str(token.get("o") or "")
        for token in (payload.get("t") or [])
        if isinstance(token, Mapping) and token.get("t")
    }
    return ClobMarketParams(
        minimum_order_size=minimum,
        maker_fee_bps=maker_fee,
        tick_size=tick,
        outcomes=outcomes,
        taker_fee_rate=taker_fee_rate,
        fee_exponent=fee_exponent,
        taker_only=taker_only,
    )


class ClobMarketParamsClient:
    def fetch(self, condition_id: str) -> ClobMarketParams:
        if not condition_id:
            raise ValueError("missing_condition_id")
        request = Request(
            f"{CLOB_MARKETS_URL}/{quote(condition_id, safe='')}",
            headers={
                "Accept": "application/json",
                "User-Agent": "world-cup-mm/0.1",
            },
        )
        with urlopen(request) as response:
            payload = json.load(response)
        if not isinstance(payload, Mapping):
            raise ValueError("clob_market_info_not_object")
        return parse_clob_market_info(payload)
