"""Read-only weather highest-temp NO qualification receipts.

This module never submits orders and never reads POLYMARKET_LIVE_TRADING as an
arming switch. Still-possible NO is blocked inside the 85% forecast coverage
band. YES remains an extra diagnostic with no claimed edge.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import parse_qs, urlparse
import json

from weather_high_temp_model import (
    WeatherModelError,
    attach_asks,
    buckets_from_gamma_event,
    parse_station_icao,
    qualify_no_books,
)


JsonReader = Callable[[str], Any]
BookReader = Callable[[str], Mapping[str, Any]]


class WeatherQualifyError(RuntimeError):
    """A qualification scan cannot finish from the supplied official reads."""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WeatherModelError(f"INVALID_{field.upper()}") from exc
    if not result.is_finite():
        raise WeatherModelError(f"INVALID_{field.upper()}")
    return result


def _best_ask(book: Mapping[str, Any]) -> Decimal | None:
    asks = book.get("asks")
    if not isinstance(asks, list) or not asks:
        return None
    prices: list[Decimal] = []
    for level in asks:
        if not isinstance(level, dict):
            continue
        raw = level.get("price")
        if raw is None:
            continue
        price = _decimal(raw, field="ask_price")
        if price > Decimal("0"):
            prices.append(price)
    if not prices:
        return None
    return min(prices)


def asks_from_books(
    buckets,
    *,
    book_for_token: BookReader,
) -> tuple[dict[str, Decimal | None], dict[str, Decimal | None]]:
    yes_asks: dict[str, Decimal | None] = {}
    no_asks: dict[str, Decimal | None] = {}
    for bucket in buckets:
        yes_asks[bucket.yes_token_id] = _best_ask(book_for_token(bucket.yes_token_id))
        no_asks[bucket.no_token_id] = _best_ask(book_for_token(bucket.no_token_id))
    return yes_asks, no_asks


def qualify_event(
    event: Mapping[str, Any],
    *,
    observed_max: Decimal | None,
    ensemble_members: Sequence[Decimal] | None,
    book_for_token: BookReader,
    ts_utc: str | None = None,
) -> dict[str, Any]:
    slug = _text(event.get("slug")).lower()
    if not slug:
        raise WeatherQualifyError("MISSING_EVENT_SLUG")
    receipt: dict[str, Any] = {
        "ts_utc": ts_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_slug": slug,
        "poly_live_trading_armed": False,
        "side": "NO",
        "yes_claimed_positive_edge": False,
    }
    try:
        station = parse_station_icao(str(event.get("resolutionSource") or ""))
    except WeatherModelError as exc:
        receipt["skip_reason"] = str(exc)
        return receipt
    receipt["station"] = station
    if observed_max is None:
        receipt["skip_reason"] = "MISSING_OBSERVED_MAX"
        return receipt
    if not ensemble_members:
        receipt["skip_reason"] = "MISSING_ENSEMBLE"
        return receipt
    buckets = buckets_from_gamma_event(event)
    yes_asks, no_asks = asks_from_books(buckets, book_for_token=book_for_token)
    priced = attach_asks(buckets, yes_asks=yes_asks, no_asks=no_asks)
    model_receipt = qualify_no_books(
        priced,
        observed_max=observed_max,
        members=tuple(ensemble_members),
    )
    receipt.update(model_receipt)
    receipt["unit"] = priced[0].unit
    receipt["observed_max"] = str(observed_max)
    return receipt


def validate_weather_public_get(url: str) -> None:
    """Separate allowlist from the live-copy public reader."""

    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    if parsed.scheme != "https" or parsed.username or parsed.password:
        raise WeatherQualifyError(f"public GET rejected: {url}")
    if parsed.hostname == "gamma-api.polymarket.com" and parsed.path == "/events":
        slugs = query.get("slug")
        if slugs and len(slugs) == 1 and slugs[0]:
            return
    if parsed.hostname == "clob.polymarket.com" and parsed.path == "/book":
        token = query.get("token_id")
        if token and len(token) == 1 and token[0].isdecimal():
            return
    raise WeatherQualifyError(f"public GET rejected: {url}")


def receipt_json(receipt: Mapping[str, Any]) -> str:
    return json.dumps(receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
