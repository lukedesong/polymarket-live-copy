from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from world_cup_mm.discovery import (
    DiscoveryError,
    classify_events,
    fetch_all_events,
    liquidity_volume_frontier,
)
from world_cup_mm.models import EligibleMarket


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


def market_payload(**overrides):
    payload = {
        "id": "market-a",
        "question": "Will France win?",
        "slug": "france-england-france",
        "conditionId": "condition-a",
        "clobTokenIds": '["yes-a", "no-a"]',
        "gameStartTime": (NOW + timedelta(hours=9)).isoformat(),
        "active": True,
        "closed": False,
        "acceptingOrders": True,
        "liquidity": "100",
        "volume24hr": "80",
    }
    payload.update(overrides)
    return payload


def event_payload(*markets, tag_slug="fifa-world-cup"):
    return {
        "id": "event-a",
        "title": "France vs. England",
        "slug": "france-vs-england",
        "tags": [{"id": "external-id", "slug": tag_slug, "label": "FIFA World Cup"}],
        "markets": list(markets or (market_payload(),)),
    }


def candidate(market_id: str, liquidity: str, volume: str) -> EligibleMarket:
    return EligibleMarket(
        event_id="event-a",
        event_title="Event",
        event_slug="event",
        market_id=market_id,
        question=market_id,
        market_slug=market_id,
        condition_id=f"condition-{market_id}",
        token_ids=(f"yes-{market_id}", f"no-{market_id}"),
        game_start_time=NOW + timedelta(hours=1),
        liquidity=Decimal(liquidity),
        volume_24h=Decimal(volume),
    )


def test_fetch_all_events_stops_on_empty_page_and_advances_by_received_count():
    pages = {0: [{"id": "event-a"}], 1: [{"id": "event-b"}], 2: []}
    calls = []

    def fetch_page(offset):
        calls.append(offset)
        return pages[offset]

    assert [item["id"] for item in fetch_all_events(fetch_page)] == [
        "event-a",
        "event-b",
    ]
    assert calls == [0, 1, 2]


def test_repeated_gamma_page_is_an_error():
    with pytest.raises(DiscoveryError, match="repeated_gamma_page"):
        fetch_all_events(lambda _offset: [{"id": "same-event"}])


def test_world_cup_direct_match_is_eligible():
    eligible, rejected = classify_events([event_payload()], now=NOW)

    assert rejected == []
    assert len(eligible) == 1
    assert eligible[0].market_id == "market-a"
    assert eligible[0].token_ids == ("yes-a", "no-a")
    assert eligible[0].game_start_time.tzinfo is timezone.utc


def test_market_without_game_start_is_rejected_as_tournament_future():
    eligible, rejected = classify_events(
        [event_payload(market_payload(gameStartTime=None))], now=NOW
    )

    assert eligible == []
    assert rejected[0].reasons == ("missing_game_start_time",)


def test_non_world_cup_event_is_rejected_before_market_selection():
    eligible, rejected = classify_events(
        [event_payload(tag_slug="premier-league")], now=NOW
    )

    assert eligible == []
    assert rejected[0].reasons == ("not_world_cup_event",)


def test_closed_non_accepting_or_past_market_keeps_all_rejection_reasons():
    market = market_payload(
        closed=True,
        acceptingOrders=False,
        gameStartTime=(NOW - timedelta(minutes=1)).isoformat(),
    )

    eligible, rejected = classify_events([event_payload(market)], now=NOW)

    assert eligible == []
    assert rejected[0].reasons == (
        "market_closed",
        "market_not_accepting_orders",
        "game_already_started",
    )


def test_frontier_excludes_market_dominated_in_liquidity_and_volume():
    markets = [
        candidate("a", "100", "80"),
        candidate("b", "90", "70"),
        candidate("c", "80", "100"),
    ]

    assert {m.market_id for m in liquidity_volume_frontier(markets)} == {"a", "c"}
