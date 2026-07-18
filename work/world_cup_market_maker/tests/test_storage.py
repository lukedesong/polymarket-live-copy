from datetime import datetime, timezone
from decimal import Decimal

import pytest

from world_cup_mm.orderbook import BookNotReady
from world_cup_mm.models import EligibleMarket, RejectedMarket
from world_cup_mm.storage import Store, replay_events


RECEIVED = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


def snapshot_payload():
    return {
        "event_type": "book",
        "asset_id": "asset-a",
        "timestamp": "1000",
        "bids": [{"price": "0.49", "size": "2"}],
        "asks": [{"price": "0.52", "size": "3"}],
    }


def delta_payload():
    return {
        "event_type": "price_change",
        "timestamp": "1001",
        "price_changes": [
            {
                "asset_id": "asset-a",
                "side": "BUY",
                "price": "0.50",
                "size": "4",
            }
        ],
    }


def test_raw_event_and_book_update_commit_together(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)

    store.record_market_event("session-a", snapshot_payload(), received_at=RECEIVED)

    assert store.raw_event_count() == 1
    assert store.best_quotes("asset-a") == ("0.49", "0.52")
    assert store.book_ready("asset-a") is True


def test_duplicate_payload_hash_is_idempotent_within_session(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)

    assert store.record_market_event("session-a", snapshot_payload(), received_at=RECEIVED) is True
    assert store.record_market_event("session-a", snapshot_payload(), received_at=RECEIVED) is False
    assert store.raw_event_count() == 1


def test_same_snapshot_is_preserved_in_a_new_session(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    for session_id in ("session-a", "session-b"):
        store.start_session(session_id, selection_mode="frontier", started_at=RECEIVED)
        assert store.record_market_event(session_id, snapshot_payload(), received_at=RECEIVED)

    assert store.raw_event_count() == 2


def test_delta_updates_normalized_book(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)
    store.record_market_event("session-a", snapshot_payload(), received_at=RECEIVED)

    store.record_market_event("session-a", delta_payload(), received_at=RECEIVED)

    assert store.best_quotes("asset-a") == ("0.50", "0.52")


def test_delta_before_snapshot_rolls_back_raw_insert(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)

    with pytest.raises(BookNotReady):
        store.record_market_event("session-a", delta_payload(), received_at=RECEIVED)

    assert store.raw_event_count() == 0


def test_disconnect_invalidates_session_books(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)
    store.record_market_event("session-a", snapshot_payload(), received_at=RECEIVED)

    store.invalidate_session_books("session-a", ended_at=RECEIVED)

    assert store.book_ready("asset-a") is False
    assert store.best_quotes("asset-a") == (None, None)


def test_replay_reconstructs_same_quotes(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)
    store.record_market_event("session-a", snapshot_payload(), received_at=RECEIVED)
    store.record_market_event("session-a", delta_payload(), received_at=RECEIVED)

    replayed = replay_events(store.raw_events("session-a"))

    assert replayed["asset-a"].best_bid == Decimal("0.50")
    assert replayed["asset-a"].best_ask == Decimal("0.52")


def test_trade_message_is_stored_in_normalized_trade_table(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=RECEIVED)
    trade = {
        "event_type": "last_trade_price",
        "asset_id": "asset-a",
        "market": "condition-a",
        "price": "0.51",
        "size": "7",
        "side": "BUY",
        "timestamp": "1002",
    }

    store.record_market_event("session-a", trade, received_at=RECEIVED)

    assert store.trade_count() == 1


def test_scan_manifest_persists_frontier_assets_and_explicit_rejections(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    eligible = EligibleMarket(
        event_id="event-a",
        event_title="France vs England",
        event_slug="france-vs-england",
        market_id="market-a",
        question="Will France win?",
        market_slug="france-win",
        condition_id="condition-a",
        token_ids=("yes-a", "no-a"),
        game_start_time=datetime(2026, 7, 18, 21, tzinfo=timezone.utc),
        liquidity=Decimal("100"),
        volume_24h=Decimal("80"),
        frontier=True,
    )
    rejected = RejectedMarket(
        event_id="event-b",
        event_title="World Cup Winner",
        market_id="market-b",
        question="Will France win the World Cup?",
        reasons=("missing_game_start_time",),
    )

    store.record_scan(
        "scan-a",
        events=[
            {"id": "event-a", "title": "France vs England", "slug": "france-vs-england"},
            {"id": "event-b", "title": "World Cup Winner", "slug": "world-cup-winner"},
        ],
        eligible=[eligible],
        rejected=[rejected],
        started_at=RECEIVED,
        completed_at=RECEIVED,
        source_url="https://gamma-api.polymarket.com/events",
    )

    selected = store.selected_markets(all_eligible=False)
    assert [market.market_id for market in selected] == ["market-a"]
    assert selected[0].token_ids == ("yes-a", "no-a")
    assert store.latest_scan_summary() == {
        "scan_id": "scan-a",
        "eligible_count": 1,
        "frontier_count": 1,
        "rejected_count": 1,
    }


def test_all_eligible_mode_includes_non_frontier_markets(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    markets = []
    for market_id, frontier in (("frontier", True), ("other", False)):
        markets.append(
            EligibleMarket(
                event_id="event-a",
                event_title="Match",
                event_slug="match",
                market_id=market_id,
                question=market_id,
                market_slug=market_id,
                condition_id=f"condition-{market_id}",
                token_ids=(f"yes-{market_id}", f"no-{market_id}"),
                game_start_time=datetime(2026, 7, 18, 21, tzinfo=timezone.utc),
                liquidity=Decimal("1"),
                volume_24h=Decimal("1"),
                frontier=frontier,
            )
        )
    store.record_scan(
        "scan-a",
        events=[{"id": "event-a", "title": "Match", "slug": "match"}],
        eligible=markets,
        rejected=[],
        started_at=RECEIVED,
        completed_at=RECEIVED,
        source_url="https://gamma-api.polymarket.com/events",
    )

    assert len(store.selected_markets(all_eligible=False)) == 1
    assert len(store.selected_markets(all_eligible=True)) == 2


def test_multiple_event_level_rejections_get_stable_distinct_storage_keys(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    rejections = [
        RejectedMarket(
            event_id=event_id,
            event_title=event_id,
            market_id="",
            question="",
            reasons=("event_has_no_markets",),
        )
        for event_id in ("event-a", "event-b")
    ]

    store.record_scan(
        "scan-a",
        events=[
            {"id": "event-a", "title": "A", "slug": "a"},
            {"id": "event-b", "title": "B", "slug": "b"},
        ],
        eligible=[],
        rejected=rejections,
        started_at=RECEIVED,
        completed_at=RECEIVED,
        source_url="https://gamma-api.polymarket.com/events",
    )

    assert store.latest_scan_summary()["rejected_count"] == 2
