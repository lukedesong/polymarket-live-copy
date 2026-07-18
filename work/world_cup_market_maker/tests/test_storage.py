from datetime import datetime, timezone
from decimal import Decimal

import pytest

from world_cup_mm.orderbook import BookNotReady
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
