from decimal import Decimal

import pytest

from world_cup_mm.orderbook import BookNotReady, OrderBookState


def snapshot(*, bids=None, asks=None):
    return {
        "event_type": "book",
        "asset_id": "asset-a",
        "timestamp": "1000",
        "bids": [
            {"price": price, "size": size} for price, size in (bids or [])
        ],
        "asks": [
            {"price": price, "size": size} for price, size in (asks or [])
        ],
    }


def price_change(*, side, price, size, asset_id="asset-a"):
    return {
        "event_type": "price_change",
        "timestamp": "1001",
        "price_changes": [
            {
                "asset_id": asset_id,
                "side": side,
                "price": price,
                "size": size,
            }
        ],
    }


def seeded_book():
    book = OrderBookState("asset-a")
    book.apply(
        snapshot(
            bids=[("0.48", "2"), ("0.49", "1")],
            asks=[("0.52", "3"), ("0.53", "4")],
        )
    )
    return book


def test_snapshot_replaces_book_and_sorts_best_prices():
    book = seeded_book()

    assert book.best_bid == Decimal("0.49")
    assert book.best_ask == Decimal("0.52")
    assert book.ready is True
    assert book.server_timestamp == "1000"


def test_new_snapshot_replaces_old_levels_instead_of_merging():
    book = seeded_book()

    book.apply(snapshot(bids=[("0.40", "5")], asks=[("0.60", "6")]))

    assert book.bids == {Decimal("0.40"): Decimal("5")}
    assert book.asks == {Decimal("0.60"): Decimal("6")}


def test_zero_size_delta_removes_price_level():
    book = seeded_book()

    book.apply(price_change(side="BUY", price="0.49", size="0"))

    assert Decimal("0.49") not in book.bids
    assert book.best_bid == Decimal("0.48")


def test_delta_for_other_asset_is_ignored():
    book = seeded_book()

    result = book.apply(
        price_change(side="BUY", price="0.50", size="1", asset_id="asset-b")
    )

    assert result.applied_changes == 0
    assert book.best_bid == Decimal("0.49")


def test_reconnect_invalidates_book_until_fresh_snapshot():
    book = seeded_book()
    book.invalidate()

    with pytest.raises(BookNotReady):
        book.apply(price_change(side="BUY", price="0.50", size="1"))
    with pytest.raises(BookNotReady):
        _ = book.best_bid


def test_unknown_event_is_preserved_but_ignored_for_book():
    book = seeded_book()

    result = book.apply({"event_type": "new_future_event", "asset_id": "asset-a"})

    assert result.ignored_for_book is True
    assert result.applied_changes == 0


def test_negative_size_is_rejected():
    book = seeded_book()

    with pytest.raises(ValueError, match="negative_book_size"):
        book.apply(price_change(side="SELL", price="0.52", size="-1"))
