from datetime import datetime, timezone
from decimal import Decimal

from world_cup_mm.paper_engine import PaperAsset, PaperEngine
from world_cup_mm.risk import RiskState
from world_cup_mm.storage import Store


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)
ASSET = PaperAsset(
    condition_id="condition-a",
    market_id="market-a",
    asset_id="asset-a",
    outcome="YES",
    minimum_order_size=Decimal("5"),
    maker_fee_bps=0,
    taker_fee_rate=Decimal("0.03"),
    fee_exponent=1,
)


def engine(tmp_path, *, fill_mode="strict"):
    store = Store(tmp_path / "paper.sqlite3")
    return store, PaperEngine(store, [ASSET], fill_mode=fill_mode)


def quote(paper: PaperEngine, *, state=RiskState.PREMATCH_OPEN):
    paper.on_book(
        asset_id="asset-a",
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.51"),
        book_timestamp="1000",
        risk_state=state,
        now=NOW,
    )


def queue_quote(
    paper: PaperEngine,
    *,
    bid="0.49",
    bid_size="10",
    ask="0.51",
    ask_size="10",
    timestamp="1000",
    state=RiskState.PREMATCH_OPEN,
):
    paper.on_book(
        asset_id="asset-a",
        best_bid=Decimal(bid),
        best_bid_size=Decimal(bid_size),
        best_ask=Decimal(ask),
        best_ask_size=Decimal(ask_size),
        bid_levels=((Decimal(bid), Decimal(bid_size)),),
        ask_levels=((Decimal(ask), Decimal(ask_size)),),
        book_timestamp=timestamp,
        risk_state=state,
        now=NOW,
    )


def queue_trade(
    paper: PaperEngine,
    *,
    price="0.49",
    side="SELL",
    size="1",
    event_hash="trade-a",
):
    return paper.on_trade(
        asset_id="asset-a",
        trade_price=Decimal(price),
        trade_side=side,
        trade_quantity=Decimal(size),
        trigger_event_hash=event_hash,
        best_bid=Decimal("0.49"),
        risk_state=RiskState.PREMATCH_OPEN,
        now=NOW,
    )


def trade(paper: PaperEngine, price: str, event_hash: str, *, state=RiskState.PREMATCH_OPEN):
    return paper.on_trade(
        asset_id="asset-a",
        trade_price=Decimal(price),
        trigger_event_hash=event_hash,
        best_bid=Decimal("0.48"),
        risk_state=state,
        now=NOW,
    )


def test_touch_does_not_fill_but_trade_through_does(tmp_path):
    store, paper = engine(tmp_path)
    quote(paper)

    assert trade(paper, "0.49", "touch") == []
    fills = trade(paper, "0.48", "through")

    assert len(fills) == 1
    assert store.paper_fill_count() == 1
    assert store.paper_position("asset-a").quantity == Decimal("5")


def test_touch_mode_fills_buy_and_sell_at_equal_quote_price(tmp_path):
    store, paper = engine(tmp_path, fill_mode="touch")
    quote(paper)

    assert len(trade(paper, "0.49", "buy-touch")) == 1
    quote(paper)
    assert len(trade(paper, "0.51", "sell-touch")) == 1

    assert store.paper_fill_count() == 2
    assert store.paper_position("asset-a").quantity == Decimal("0")


def test_unsupported_fill_mode_is_rejected(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")

    try:
        PaperEngine(store, [ASSET], fill_mode="unknown")
    except ValueError as error:
        assert str(error) == "unsupported_fill_mode:unknown"
    else:
        raise AssertionError("unsupported fill mode was accepted")


def test_sell_only_fills_above_ask_and_cannot_create_short(tmp_path):
    store, paper = engine(tmp_path)
    quote(paper)
    trade(paper, "0.48", "buy")
    quote(paper)

    assert len(store.open_paper_orders("asset-a")) == 2
    assert trade(paper, "0.51", "sell-touch") == []
    assert len(trade(paper, "0.52", "sell-through")) == 1
    assert store.paper_position("asset-a").quantity == Decimal("0")

    quote(paper)
    assert [order.side for order in store.open_paper_orders("asset-a")] == ["BUY"]


def test_risk_states_cancel_inventory_increasing_orders_and_then_all_orders(tmp_path):
    store, paper = engine(tmp_path)
    quote(paper)
    trade(paper, "0.48", "buy")
    quote(paper)

    paper.apply_risk("condition-a", RiskState.NO_NEW_INVENTORY, now=NOW)
    assert store.open_paper_orders("asset-a") == []

    quote(paper, state=RiskState.REDUCE_ONLY)
    assert [order.side for order in store.open_paper_orders("asset-a")] == ["SELL"]

    paper.apply_risk("condition-a", RiskState.CANCELLED_BLOCKED, now=NOW)
    assert store.open_paper_orders("asset-a") == []


def test_disconnect_cancels_orders_and_blocks_fill_processing(tmp_path):
    store, paper = engine(tmp_path)
    quote(paper)
    paper.disconnect(now=NOW)

    assert store.open_paper_orders("asset-a") == []
    assert trade(paper, "0.48", "after-disconnect") == []
    assert store.paper_fill_count() == 0


def test_repeated_trade_event_cannot_fill_twice(tmp_path):
    store, paper = engine(tmp_path)
    quote(paper)

    assert len(trade(paper, "0.48", "same-event")) == 1
    quote(paper)
    assert trade(paper, "0.48", "same-event") == []
    assert store.paper_fill_count() == 1


def test_queue_mode_keeps_same_price_order_and_original_queue_ahead(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid_size="10", timestamp="1000")
    first = store.open_paper_orders("asset-a")[0]

    queue_quote(paper, bid_size="20", timestamp="1001")
    second = store.open_paper_orders("asset-a")[0]

    assert second.order_id == first.order_id
    assert second.queue_ahead_initial == Decimal("10")
    assert second.queue_ahead_remaining == Decimal("10")


def test_queue_mode_does_not_treat_unmatched_size_decrease_as_front_cancel(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid_size="10")

    queue_quote(paper, bid_size="4", timestamp="1001")

    assert store.open_paper_orders("asset-a")[0].queue_ahead_remaining == Decimal("10")


def test_queue_mode_at_price_trade_consumes_ahead_then_partially_fills(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid_size="10")

    fills = queue_trade(paper, size="12")

    order = store.open_paper_orders("asset-a")[0]
    assert fills == [order.order_id]
    assert order.queue_ahead_remaining == Decimal("0")
    assert order.remaining_quantity == Decimal("3")
    assert store.paper_position("asset-a").quantity == Decimal("2")
    row = store.paper_fill_rows()[0]
    assert row["quantity_text"] == "2"
    assert row["official_trade_quantity_text"] == "12"
    assert row["queue_consumed_text"] == "10"
    assert row["proof_type"] == "AT_PRICE_QUEUE"


def test_queue_mode_partial_order_finishes_without_overfilling_or_replay(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid_size="0")
    queue_trade(paper, size="2", event_hash="partial")
    assert store.open_paper_orders("asset-a")[0].remaining_quantity == Decimal("3")

    assert queue_trade(paper, size="10", event_hash="finish")
    assert queue_trade(paper, size="10", event_hash="finish") == []

    assert store.paper_order_status(1) == "FILLED"
    assert store.paper_position("asset-a").quantity == Decimal("5")
    assert store.paper_fill_count() == 2


def test_queue_mode_wrong_trade_side_cannot_consume_buy_queue(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid_size="10")

    assert queue_trade(paper, side="BUY", size="20") == []

    assert store.paper_fill_count() == 0
    assert store.open_paper_orders("asset-a")[0].queue_ahead_remaining == Decimal("10")


def test_queue_mode_price_change_requeues_at_new_displayed_size(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid="0.49", bid_size="10")
    first = store.open_paper_orders("asset-a")[0]

    queue_quote(paper, bid="0.48", bid_size="7", timestamp="1001")
    second = store.open_paper_orders("asset-a")[0]

    assert second.order_id != first.order_id
    assert store.paper_order_status(first.order_id) == "CANCELLED"
    assert second.price == Decimal("0.48")
    assert second.queue_ahead_remaining == Decimal("7")


def test_queue_mode_trade_through_fills_valid_remainder(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid_size="100")

    fills = queue_trade(paper, price="0.48", side="SELL", size="1")

    assert fills == [1]
    assert store.paper_position("asset-a").quantity == Decimal("5")
    assert store.paper_fill_rows()[0]["proof_type"] == "TRADE_THROUGH"


def test_queue_mode_records_dynamic_depth_mark_and_fill_price_drift(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid="0.49", bid_size="0")
    queue_trade(paper, price="0.48", size="1")

    paper.on_book(
        asset_id="asset-a",
        best_bid=Decimal("0.47"),
        best_bid_size=Decimal("2"),
        best_ask=Decimal("0.49"),
        best_ask_size=Decimal("4"),
        bid_levels=(
            (Decimal("0.47"), Decimal("2")),
            (Decimal("0.46"), Decimal("1")),
        ),
        ask_levels=((Decimal("0.49"), Decimal("4")),),
        book_timestamp="1002",
        risk_state=RiskState.PREMATCH_OPEN,
        now=NOW,
    )

    row = store.connection.execute(
        "SELECT * FROM paper_inventory_marks ORDER BY mark_id DESC LIMIT 1"
    ).fetchone()
    assert row["liquidatable_quantity_text"] == "3"
    assert row["unliquidated_quantity_text"] == "2"
    assert Decimal(row["best_bid_drift_text"]) == Decimal("-0.02")
    assert Decimal(row["liquidation_vwap_drift_text"]) < Decimal("-0.02")


def test_queue_mode_blocking_risk_liquidates_real_depth_and_keeps_remainder(tmp_path):
    store, paper = engine(tmp_path, fill_mode="queue")
    queue_quote(paper, bid="0.49", bid_size="0")
    queue_trade(paper, price="0.48", size="1")

    paper.on_book(
        asset_id="asset-a",
        best_bid=Decimal("0.49"),
        best_bid_size=Decimal("2"),
        best_ask=Decimal("0.51"),
        best_ask_size=Decimal("2"),
        bid_levels=(
            (Decimal("0.49"), Decimal("2")),
            (Decimal("0.48"), Decimal("1")),
        ),
        ask_levels=((Decimal("0.51"), Decimal("2")),),
        book_timestamp="1003",
        risk_state=RiskState.CANCELLED_BLOCKED,
        now=NOW,
    )

    assert store.open_paper_orders("asset-a") == []
    assert store.paper_position("asset-a").quantity == Decimal("2")
    assert store.paper_liquidation_count() == 2
