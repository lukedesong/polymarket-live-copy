import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from world_cup_mm.cli import (
    StoreSink,
    build_paper_status,
    build_parser,
    build_status,
    make_order_control,
    run_replay,
    run_scan,
)
from world_cup_mm.storage import Store


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


def event_payload(start):
    return {
        "id": "event-a",
        "title": "France vs England",
        "slug": "fifwc-fra-eng-2026-07-18",
        "tags": [{"slug": "fifa-world-cup"}],
        "markets": [
            {
                "id": "market-a",
                "question": "Will France win?",
                "slug": "france-win",
                "conditionId": "condition-a",
                "clobTokenIds": '["yes-a", "no-a"]',
                "gameStartTime": start.isoformat(),
                "active": True,
                "closed": False,
                "acceptingOrders": True,
                "liquidity": "100",
                "volume24hr": "80",
            }
        ],
    }


class FakeGammaClient:
    def __init__(self, events):
        self.events = events

    def fetch_page(self, offset):
        return self.events if offset == 0 else []


def test_parser_defaults_to_data_only_frontier_collection():
    args = build_parser().parse_args(["collect"])

    assert args.database == Path("data/world_cup_mm.sqlite3")
    assert args.all_eligible is False
    assert args.cancel_enabled is False
    assert args.max_messages is None


def test_parser_exposes_paper_only_runtime_commands():
    run = build_parser().parse_args(["paper-run", "--duration-seconds", "3600"])
    touch = build_parser().parse_args(["paper-run", "--fill-mode", "touch"])
    status = build_parser().parse_args(["paper-status"])
    export = build_parser().parse_args(["paper-export"])

    assert run.duration_seconds == 3600
    assert run.fill_mode == "queue"
    assert run.cancel_enabled is False
    assert touch.fill_mode == "touch"
    assert status.command == "paper-status"
    assert export.command == "paper-export"


def test_paper_status_reports_fill_mode_from_session(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    store.start_session(
        "session-a",
        selection_mode="paper_frontier_touch",
        started_at=NOW,
    )

    status = build_paper_status(store)
    assert status["fill_mode"] == "touch"
    assert status["authoritative"] is False
    assert status["result_role"] == "comparison_only"


def test_queue_paper_status_is_authoritative_and_exposes_queue_metrics(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    store.start_session(
        "session-a",
        selection_mode="paper_frontier_queue",
        started_at=NOW,
    )

    status = build_paper_status(store)

    assert status["fill_mode"] == "queue"
    assert status["authoritative"] is True
    assert status["result_role"] == "authoritative"
    assert status["official_trade_quantity"] == "0"
    assert status["partial_fill_order_count"] == 0
    assert status["full_fill_order_count"] == 0
    assert status["unliquidated_quantity"] == "0"


def test_run_scan_persists_ranked_manifest_without_fixed_liquidity_cutoff(tmp_path):
    store = Store(tmp_path / "market.sqlite3")

    result = run_scan(
        store,
        FakeGammaClient([event_payload(NOW + timedelta(hours=9))]),
        now=NOW,
        scan_id="scan-a",
    )

    assert result["scan_id"] == "scan-a"
    assert result["eligible_count"] == 1
    assert result["frontier_count"] == 1
    assert result["rejected_count"] == 0
    assert result["markets"][0]["condition_id"] == "condition-a"


def test_data_only_status_is_never_quote_capable(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    run_scan(
        store,
        FakeGammaClient([event_payload(NOW + timedelta(hours=9))]),
        now=NOW,
        scan_id="scan-a",
    )

    status = build_status(store, cancel_capable=False, now=NOW)

    assert status["mode"] == "data_only"
    assert status["quote_capable"] is False
    assert status["markets"][0]["risk_state"] == "DATA_BLOCKED"


def test_status_applies_user_specified_no_new_inventory_boundary(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    run_scan(
        store,
        FakeGammaClient([event_payload(NOW + timedelta(minutes=30))]),
        now=NOW,
        scan_id="scan-a",
    )
    store.start_session("session-a", selection_mode="frontier", started_at=NOW)
    for asset_id in ("yes-a", "no-a"):
        store.record_market_event(
            "session-a",
            {
                "event_type": "book",
                "asset_id": asset_id,
                "timestamp": "1000",
                "bids": [{"price": "0.49", "size": "1"}],
                "asks": [{"price": "0.51", "size": "1"}],
            },
            received_at=NOW,
        )

    status = build_status(store, cancel_capable=True, now=NOW)

    assert status["markets"][0]["risk_state"] == "NO_NEW_INVENTORY"
    assert status["quote_capable"] is False


def test_live_sports_slug_overrides_scheduled_start_in_status(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    run_scan(
        store,
        FakeGammaClient([event_payload(NOW + timedelta(hours=9))]),
        now=NOW,
        scan_id="scan-a",
    )
    store.start_session("session-a", selection_mode="frontier", started_at=NOW)
    for asset_id in ("yes-a", "no-a"):
        store.record_market_event(
            "session-a",
            {
                "event_type": "book",
                "asset_id": asset_id,
                "timestamp": "1000",
                "bids": [],
                "asks": [],
            },
            received_at=NOW,
        )
    store.record_sports_event(
        "session-a",
        {"slug": "fifwc-fra-eng-2026-07-18", "live": True, "ended": False},
        received_at=NOW,
    )

    status = build_status(store, cancel_capable=True, now=NOW)

    assert status["markets"][0]["risk_state"] == "IN_PLAY_BLOCKED"
    assert status["markets"][0]["risk_actions"] == [
        "CANCEL_MARKET_ORDERS",
        "BLOCK_TRADING",
    ]


def test_store_sink_records_and_invalidates_public_session(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    sink = StoreSink(store, "session-a", selection_mode="frontier", started_at=NOW)

    async def exercise():
        await sink.connected()
        await sink.market_event(
            {
                "event_type": "book",
                "asset_id": "asset-a",
                "timestamp": "1000",
                "bids": [],
                "asks": [],
            }
        )
        await sink.disconnected()

    asyncio.run(exercise())

    assert store.raw_event_count() == 1
    assert store.book_ready("asset-a") is False


def test_store_sink_rechecks_risk_on_connection_data_sports_and_disconnect(tmp_path):
    store = Store(tmp_path / "market.sqlite3")

    class FakeRuntime:
        def __init__(self):
            self.calls = 0

        def evaluate_all(self):
            self.calls += 1

    runtime = FakeRuntime()
    sink = StoreSink(
        store,
        "session-a",
        selection_mode="frontier",
        started_at=NOW,
        risk_runtime=runtime,
    )

    async def exercise():
        await sink.connected()
        await sink.market_event({"event_type": "unknown"})
        await sink.sports_event({"slug": "match", "live": False})
        await sink.disconnected()

    asyncio.run(exercise())

    assert runtime.calls == 4


def test_replay_reports_latest_session_quotes(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.start_session("session-a", selection_mode="frontier", started_at=NOW)
    store.record_market_event(
        "session-a",
        {
            "event_type": "book",
            "asset_id": "asset-a",
            "timestamp": "1000",
            "bids": [{"price": "0.49", "size": "1"}],
            "asks": [{"price": "0.51", "size": "1"}],
        },
        received_at=NOW,
    )

    result = run_replay(store)

    assert result["session_id"] == "session-a"
    assert result["books"]["asset-a"] == {"best_bid": "0.49", "best_ask": "0.51"}


def test_cancel_enabled_requires_runtime_credentials_only():
    with pytest.raises(RuntimeError, match="missing_cancel_credentials"):
        make_order_control(cancel_enabled=True, env={})

    recording = make_order_control(cancel_enabled=False, env={})
    assert recording.cancel_market_orders("condition-a")["status"] == "recorded"


def test_cancel_client_accepts_existing_wallet_address_name(monkeypatch):
    captured = {}

    class FakeClobClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr("world_cup_mm.cli.ClobClient", FakeClobClient)
    env = {
        "POLYMARKET_PRIVATE_KEY": "private",
        "POLYMARKET_API_KEY": "key",
        "POLYMARKET_API_SECRET": "secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "3",
        "POLYMARKET_WALLET_ADDRESS": "0xwallet",
    }

    make_order_control(cancel_enabled=True, env=env)

    assert captured["funder"] == "0xwallet"
