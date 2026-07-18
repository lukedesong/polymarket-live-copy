from datetime import datetime, timezone
from decimal import Decimal

import pytest

from world_cup_mm.storage import Store


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


def open_order(
    store: Store,
    *,
    side: str = "BUY",
    price: str = "0.49",
    quantity: str = "5",
) -> int:
    return store.open_paper_order(
        condition_id="condition-a",
        market_id="market-a",
        asset_id="asset-a",
        outcome="YES",
        side=side,
        price=Decimal(price),
        quantity=Decimal(quantity),
        maker_fee_bps=0,
        quote_book_timestamp="1000",
        created_at=NOW,
    )


def fill(
    store: Store,
    order_id: int,
    *,
    event_hash: str,
    trigger_price: str,
    best_bid: str,
) -> bool:
    return store.apply_paper_fill(
        order_id,
        trigger_event_hash=event_hash,
        trigger_price=Decimal(trigger_price),
        filled_at=NOW,
        best_bid=Decimal(best_bid),
    )


def test_buy_fill_is_idempotent_and_updates_position_atomically(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    order_id = open_order(store)

    assert fill(
        store,
        order_id,
        event_hash="trade-a",
        trigger_price="0.48",
        best_bid="0.48",
    ) is True
    assert fill(
        store,
        order_id,
        event_hash="trade-a",
        trigger_price="0.48",
        best_bid="0.48",
    ) is False

    position = store.paper_position("asset-a")
    assert position.quantity == Decimal("5")
    assert position.cost_basis == Decimal("2.45")
    assert position.average_cost == Decimal("0.49")
    assert store.paper_fill_count() == 1


def test_sell_fill_realizes_weighted_average_cost_and_never_shorts(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    buy_one = open_order(store, price="0.40", quantity="5")
    fill(store, buy_one, event_hash="buy-one", trigger_price="0.39", best_bid="0.39")
    buy_two = open_order(store, price="0.60", quantity="5")
    fill(store, buy_two, event_hash="buy-two", trigger_price="0.59", best_bid="0.59")

    sell = open_order(store, side="SELL", price="0.70", quantity="5")
    fill(store, sell, event_hash="sell-one", trigger_price="0.71", best_bid="0.69")

    position = store.paper_position("asset-a")
    assert position.quantity == Decimal("5")
    assert position.cost_basis == Decimal("2.50")
    assert position.realized_profit == Decimal("1.00")
    account = store.paper_account()
    assert account.buy_cost == Decimal("5.00")
    assert account.sell_proceeds == Decimal("3.50")
    assert account.realized_profit == Decimal("1.00")
    assert account.unrealized_profit == Decimal("0.95")
    assert account.total_profit == Decimal("1.95")

    excessive_sell = open_order(store, side="SELL", price="0.80", quantity="10")
    with pytest.raises(ValueError, match="paper_short_not_allowed"):
        fill(
            store,
            excessive_sell,
            event_hash="sell-too-large",
            trigger_price="0.81",
            best_bid="0.79",
        )
    assert store.paper_fill_count() == 3


def test_replace_and_cancel_keep_order_audit_history(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    first = open_order(store, price="0.49")
    second = open_order(store, price="0.50")

    assert first != second
    assert [order.order_id for order in store.open_paper_orders("asset-a")] == [second]
    assert store.cancel_paper_orders(
        condition_id="condition-a",
        reason="risk_blocked",
        cancelled_at=NOW,
    ) == 1
    assert store.open_paper_orders("asset-a") == []
    assert store.paper_order_status(first) == "CANCELLED"
    assert store.paper_order_status(second) == "CANCELLED"


def test_paper_ledger_survives_store_restart(tmp_path):
    path = tmp_path / "paper.sqlite3"
    store = Store(path)
    order_id = open_order(store)
    fill(store, order_id, event_hash="trade-a", trigger_price="0.48", best_bid="0.48")
    store.close()

    reopened = Store(path)
    assert reopened.paper_position("asset-a").quantity == Decimal("5")
    assert reopened.paper_account().total_profit == Decimal("-0.05")


def test_account_cash_totals_never_use_binary_float(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    for index in range(3):
        order_id = open_order(store, price="0.10", quantity="1")
        fill(
            store,
            order_id,
            event_hash=f"trade-{index}",
            trigger_price="0.09",
            best_bid="0.09",
        )

    assert store.paper_account().buy_cost == Decimal("0.30")
