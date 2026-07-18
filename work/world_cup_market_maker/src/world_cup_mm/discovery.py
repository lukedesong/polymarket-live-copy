from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import (
    EligibleMarket,
    RejectedMarket,
    parse_clob_token_ids,
    parse_decimal,
    parse_utc_datetime,
)


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
WORLD_CUP_TAG_SLUGS = frozenset({"fifa-world-cup", "2026-fifa-world-cup"})


class DiscoveryError(RuntimeError):
    pass


class GammaClient:
    def fetch_page(self, offset: int) -> list[dict[str, Any]]:
        query = urlencode(
            {
                "active": "true",
                "closed": "false",
                "tag_slug": "fifa-world-cup",
                "order": "liquidity",
                "ascending": "false",
                "offset": offset,
            }
        )
        request = Request(
            f"{GAMMA_EVENTS_URL}?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": "world-cup-mm/0.1",
            },
        )
        with urlopen(request) as response:
            payload = json.load(response)
        if not isinstance(payload, list):
            raise DiscoveryError("gamma_response_not_list")
        return payload


def fetch_all_events(
    fetch_page: Callable[[int], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    offset = 0
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    while True:
        page = fetch_page(offset)
        if not page:
            return result
        page_ids = [str(event.get("id") or "") for event in page]
        if not all(page_ids) or len(set(page_ids)) != len(page_ids):
            raise DiscoveryError("invalid_gamma_page_ids")
        if any(event_id in seen for event_id in page_ids):
            raise DiscoveryError("repeated_gamma_page")
        seen.update(page_ids)
        result.extend(page)
        offset += len(page)


def _tag_slugs(event: dict[str, Any]) -> set[str]:
    return {
        str(tag.get("slug") or "")
        for tag in (event.get("tags") or [])
        if isinstance(tag, dict)
    }


def _rejection_reasons(
    event_is_world_cup: bool,
    market: dict[str, Any],
    *,
    now: datetime,
) -> tuple[str, ...]:
    if not event_is_world_cup:
        return ("not_world_cup_event",)

    reasons: list[str] = []
    if market.get("active") is not True:
        reasons.append("market_not_active")
    if market.get("closed") is True:
        reasons.append("market_closed")
    if market.get("acceptingOrders") is not True:
        reasons.append("market_not_accepting_orders")
    if not market.get("conditionId"):
        reasons.append("missing_condition_id")
    try:
        parse_clob_token_ids(market.get("clobTokenIds"))
    except ValueError:
        reasons.append("invalid_clob_token_ids")
    try:
        start = parse_utc_datetime(market.get("gameStartTime"))
    except ValueError as exc:
        code = "missing_game_start_time" if str(exc) == "missing_datetime" else "invalid_game_start_time"
        reasons.append(code)
    else:
        if start <= now:
            reasons.append("game_already_started")
    return tuple(reasons)


def classify_events(
    events: Iterable[dict[str, Any]],
    *,
    now: datetime,
) -> tuple[list[EligibleMarket], list[RejectedMarket]]:
    if now.tzinfo is None:
        raise ValueError("now_missing_timezone")
    eligible: list[EligibleMarket] = []
    rejected: list[RejectedMarket] = []
    for event in events:
        event_id = str(event.get("id") or "")
        event_title = str(event.get("title") or "")
        event_slug = str(event.get("slug") or "")
        world_cup = bool(_tag_slugs(event) & WORLD_CUP_TAG_SLUGS)
        markets = event.get("markets") or []
        if not markets:
            rejected.append(
                RejectedMarket(
                    event_id=event_id,
                    event_title=event_title,
                    market_id="",
                    question="",
                    reasons=("event_has_no_markets",) if world_cup else ("not_world_cup_event",),
                )
            )
            continue
        for market in markets:
            market_id = str(market.get("id") or "")
            question = str(market.get("question") or "")
            reasons = _rejection_reasons(world_cup, market, now=now)
            if reasons:
                rejected.append(
                    RejectedMarket(
                        event_id=event_id,
                        event_title=event_title,
                        market_id=market_id,
                        question=question,
                        reasons=reasons,
                    )
                )
                continue
            eligible.append(
                EligibleMarket(
                    event_id=event_id,
                    event_title=event_title,
                    event_slug=event_slug,
                    market_id=market_id,
                    question=question,
                    market_slug=str(market.get("slug") or ""),
                    condition_id=str(market["conditionId"]),
                    token_ids=parse_clob_token_ids(market["clobTokenIds"]),
                    game_start_time=parse_utc_datetime(market["gameStartTime"]),
                    liquidity=parse_decimal(market.get("liquidity")),
                    volume_24h=parse_decimal(market.get("volume24hr")),
                )
            )
    return eligible, rejected


def liquidity_volume_frontier(
    markets: Iterable[EligibleMarket],
) -> list[EligibleMarket]:
    items = list(markets)
    frontier: list[EligibleMarket] = []
    for market in items:
        dominated = any(
            other.market_id != market.market_id
            and other.liquidity >= market.liquidity
            and other.volume_24h >= market.volume_24h
            and (
                other.liquidity > market.liquidity
                or other.volume_24h > market.volume_24h
            )
            for other in items
        )
        if not dominated:
            frontier.append(replace(market, frontier=True))
    return sorted(
        frontier,
        key=lambda item: (item.liquidity, item.volume_24h, item.market_id),
        reverse=True,
    )


def ranked_markets(markets: Iterable[EligibleMarket]) -> list[EligibleMarket]:
    frontier_ids = {market.market_id for market in liquidity_volume_frontier(markets)}
    return sorted(
        (replace(market, frontier=market.market_id in frontier_ids) for market in markets),
        key=lambda item: (item.frontier, item.liquidity, item.volume_24h, item.market_id),
        reverse=True,
    )
