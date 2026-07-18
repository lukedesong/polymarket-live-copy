from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


def parse_clob_token_ids(value: object) -> tuple[str, ...]:
    try:
        payload = json.loads(value) if isinstance(value, str) else value
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_clob_token_ids") from exc
    if (
        not isinstance(payload, list)
        or not payload
        or not all(isinstance(item, str) and item for item in payload)
    ):
        raise ValueError("invalid_clob_token_ids")
    return tuple(payload)


def parse_utc_datetime(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing_datetime")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid_datetime") from exc
    if parsed.tzinfo is None:
        raise ValueError("datetime_missing_timezone")
    return parsed.astimezone(timezone.utc)


def parse_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid_decimal") from exc


@dataclass(frozen=True, slots=True)
class EligibleMarket:
    event_id: str
    event_title: str
    event_slug: str
    market_id: str
    question: str
    market_slug: str
    condition_id: str
    token_ids: tuple[str, ...]
    game_start_time: datetime
    liquidity: Decimal
    volume_24h: Decimal
    frontier: bool = False


@dataclass(frozen=True, slots=True)
class RejectedMarket:
    event_id: str
    event_title: str
    market_id: str
    question: str
    reasons: tuple[str, ...]
