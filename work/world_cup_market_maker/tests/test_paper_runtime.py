import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from world_cup_mm.market_params import ClobMarketParams
from world_cup_mm.paper_runtime import PaperRuntimeSink, build_paper_assets
from world_cup_mm.cli import build_paper_export
from world_cup_mm.storage import Store, StoredMarket


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


def market(start=NOW + timedelta(hours=1)):
    return StoredMarket(
        scan_id="scan-a",
        market_id="market-a",
        event_id="event-a",
        event_slug="france-vs-england",
        question="Will France win?",
        market_slug="france-win",
        condition_id="condition-a",
        token_ids=("asset-a", "asset-b"),
        game_start_time=start,
        liquidity_text="100",
        volume_24h_text="80",
        frontier=True,
    )


def params():
    return ClobMarketParams(
        minimum_order_size=Decimal("5"),
        maker_fee_bps=0,
        tick_size=Decimal("0.01"),
        outcomes={"asset-a": "Yes", "asset-b": "No"},
        taker_fee_rate=Decimal("0.03"),
        fee_exponent=1,
        taker_only=True,
    )


def book(asset_id="asset-a"):
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "market": "condition-a",
        "timestamp": "1000",
        "bids": [{"price": "0.49", "size": "10"}],
        "asks": [{"price": "0.51", "size": "10"}],
    }


def trade(price="0.48"):
    return {
        "event_type": "last_trade_price",
        "asset_id": "asset-a",
        "market": "condition-a",
        "price": price,
        "size": "5",
        "side": "SELL",
        "timestamp": "1001",
    }


def test_build_assets_uses_per_market_official_parameters():
    assets = build_paper_assets([market()], {"condition-a": params()})

    assert [asset.outcome for asset in assets] == ["Yes", "No"]
    assert all(asset.minimum_order_size == Decimal("5") for asset in assets)
    assert all(asset.taker_fee_rate == Decimal("0.03") for asset in assets)
    assert all(asset.fee_exponent == 1 for asset in assets)


def test_market_events_create_orders_and_conservative_fills(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.market_event(trade())

    asyncio.run(exercise())

    assert store.paper_fill_count() == 1
    assert store.paper_position("asset-a").quantity == Decimal("5")
    assert store.latest_session_summary()["selection_mode"] == "paper_frontier_strict"


def test_touch_runtime_fills_equal_price_trade(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        fill_mode="touch",
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.market_event(trade("0.49"))

    asyncio.run(exercise())

    assert store.paper_fill_count() == 1
    assert store.paper_position("asset-a").quantity == Decimal("5")
    assert store.latest_session_summary()["selection_mode"] == "paper_frontier_touch"


def test_disconnect_cancels_orders_and_invalidates_books(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.disconnected()

    asyncio.run(exercise())

    assert store.open_paper_orders() == []
    assert store.book_ready("asset-a") is False


def test_live_sports_event_cancels_all_market_orders(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.sports_event(
            {"slug": "france-vs-england", "live": True, "ended": False}
        )

    asyncio.run(exercise())

    assert store.open_paper_orders() == []


def test_no_new_inventory_boundary_creates_no_buy_order(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market(NOW + timedelta(minutes=30))],
        {"condition-a": params()},
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())

    asyncio.run(exercise())

    assert store.open_paper_orders() == []


def test_queue_runtime_uses_official_side_and_quantity_for_partial_fill(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        fill_mode="queue",
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.market_event(
            {
                **trade("0.49"),
                "size": "12",
                "side": "SELL",
            }
        )

    asyncio.run(exercise())

    assert store.paper_fill_count() == 1
    assert store.paper_position("asset-a").quantity == Decimal("2")
    assert store.open_paper_orders("asset-a")[0].remaining_quantity == Decimal("3")
    assert store.latest_session_summary()["selection_mode"] == "paper_frontier_queue"


def test_queue_runtime_invalid_trade_evidence_is_recorded_without_fill(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        fill_mode="queue",
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.market_event({**trade("0.49"), "size": "bad", "side": ""})

    asyncio.run(exercise())

    assert store.paper_fill_count() == 0
    assert store.paper_anomaly_count() == 1


def test_queue_runtime_out_of_order_trade_is_recorded_without_fill(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [market()],
        {"condition-a": params()},
        fill_mode="queue",
        now_fn=lambda: NOW,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.market_event({**trade("0.49"), "timestamp": "999", "size": "20"})

    asyncio.run(exercise())

    assert store.paper_fill_count() == 0
    assert store.paper_anomaly_count() == 1


def test_queue_runtime_replays_partial_reprice_trade_through_and_depth_exit(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    current = {"now": NOW}
    selected = market(NOW + timedelta(hours=1))
    sink = PaperRuntimeSink(
        store,
        "session-a",
        [selected],
        {"condition-a": params()},
        fill_mode="queue",
        now_fn=lambda: current["now"],
    )

    async def exercise():
        await sink.connected()
        await sink.market_event(book())
        await sink.market_event({**trade("0.49"), "size": "12", "side": "SELL"})
        await sink.market_event(
            {
                "event_type": "price_change",
                "market": "condition-a",
                "timestamp": "1002",
                "price_changes": [
                    {
                        "asset_id": "asset-a",
                        "price": "0.48",
                        "size": "7",
                        "side": "BUY",
                        "best_bid": "0.48",
                        "best_ask": "0.51",
                    },
                    {
                        "asset_id": "asset-a",
                        "price": "0.49",
                        "size": "0",
                        "side": "BUY",
                        "best_bid": "0.48",
                        "best_ask": "0.51",
                    },
                ],
            }
        )
        await sink.market_event(
            {**trade("0.47"), "size": "1", "side": "SELL", "timestamp": "1003"}
        )
        current["now"] = selected.game_start_time - timedelta(minutes=5)
        await sink.market_event(
            {
                "event_type": "book",
                "asset_id": "asset-a",
                "market": "condition-a",
                "timestamp": "1004",
                "bids": [
                    {"price": "0.47", "size": "4"},
                    {"price": "0.46", "size": "1"},
                ],
                "asks": [{"price": "0.49", "size": "4"}],
            }
        )

    asyncio.run(exercise())

    export = build_paper_export(store)
    overview = export["overview"]
    fills = export["fills"]
    independent_buy_cost = sum(
        Decimal(row["gross_amount_text"])
        for row in fills
        if row["side"] == "BUY"
    )
    independent_liquidation_net = sum(
        Decimal(row["gross_proceeds_text"]) - Decimal(row["fee_text"])
        for row in store.connection.execute(
            "SELECT gross_proceeds_text,fee_text FROM paper_liquidations"
        )
    )

    assert overview["authoritative"] is True
    assert overview["partial_fill_order_count"] == 1
    assert overview["full_fill_order_count"] == 1
    assert overview["unliquidated_quantity"] == "2"
    assert overview["buy_cost"] == str(independent_buy_cost)
    assert overview["sell_proceeds"] == str(independent_liquidation_net)
    assert len(fills) == 2
    assert {row["proof_type"] for row in fills} == {"AT_PRICE_QUEUE", "TRADE_THROUGH"}
