import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from world_cup_mm.market_params import ClobMarketParams
from world_cup_mm.paper_runtime import PaperRuntimeSink, build_paper_assets
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
