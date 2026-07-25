from decimal import Decimal
import sqlite3

import pytest
import weather_proportional_paper as tracker

from weather_proportional_paper import (
    PaperStore,
    ReadOnlyViolation,
    acquire_process_lock,
    apply_source_snapshot,
    render_status_files,
    run_cycle,
    run_daemon,
    run_settlement_cycle,
    settle_position,
    scaled_target,
    validate_public_request,
)


D = Decimal


def source_position(size: str, asset: str = "token-a") -> dict:
    return {
        "asset": asset,
        "conditionId": f"condition-{asset}",
        "title": "Will the highest temperature in Hong Kong be 32°C on July 25?",
        "eventSlug": "highest-temperature-in-hong-kong-on-july-25-2026",
        "outcome": "Yes",
        "size": size,
        "endDate": "2026-07-25",
    }


def book(minimum: str = "5") -> dict:
    return {
        "asset_id": "token-a",
        "min_order_size": minimum,
        "timestamp": "1784900000000",
        "hash": "book-hash",
        "asks": [
            {"price": "0.30", "size": "5"},
            {"price": "0.31", "size": "20"},
        ],
        "bids": [
            {"price": "0.29", "size": "20"},
            {"price": "0.28", "size": "20"},
        ],
    }


def test_fixed_scale_uses_source_position_and_not_extra_cash():
    assert scaled_target(D("12.5"), D("0"), D("0.4")) == D("5")
    assert scaled_target(D("100"), D("0"), D("0.4")) == D("40")


def test_startup_seeds_source_baseline_without_backfilling(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([source_position("100")], observed_at=1)

    assert store.cash() == D("300")
    assert store.paper_quantity("token-a") == D("0")
    assert store.source_anchor("token-a") == D("100")
    assert store.ledger_rows() == []


def test_new_source_position_buys_full_scaled_delta_at_current_asks(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([source_position("100")], observed_at=1)

    results = apply_source_snapshot(
        store,
        [source_position("125")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )

    assert results[0]["status"] == "FILLED"
    assert results[0]["side"] == "BUY"
    assert results[0]["quantity"] == D("10")
    assert store.paper_quantity("token-a") == D("10")
    assert store.target_quantity("token-a") == D("10")
    assert store.cash() < D("300")
    assert store.config()["initial_cash"] == "300"
    assert store.config()["scale"] == "0.4"


def test_legacy_source_sell_rolls_anchor_down_without_shorting(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([source_position("100")], observed_at=1)

    results = apply_source_snapshot(
        store,
        [source_position("80")],
        books_by_asset={},
        observed_at=2,
    )

    assert results == []
    assert store.source_anchor("token-a") == D("80")
    assert store.paper_quantity("token-a") == D("0")


def test_target_below_exchange_minimum_is_not_inflated_to_five(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)

    results = apply_source_snapshot(
        store,
        [source_position("10")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )

    assert results[0]["status"] == "SKIPPED"
    assert results[0]["reason"] == "BELOW_MIN_ORDER"
    assert results[0]["requested_quantity"] == D("4")
    assert store.paper_quantity("token-a") == D("0")
    assert store.cash() == D("300")


def test_skipped_change_is_not_retried_until_source_changes_again(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)
    apply_source_snapshot(
        store,
        [source_position("10")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )

    results = apply_source_snapshot(
        store,
        [source_position("10")],
        books_by_asset={},
        observed_at=3,
    )

    assert results == []
    assert len(store.ledger_rows()) == 1


def test_winning_settlement_releases_paper_cash(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)
    apply_source_snapshot(
        store,
        [source_position("25")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )
    cash_before = store.cash()

    result = settle_position(store, "token-a", winner=True, observed_at=3)

    assert result["status"] == "SETTLED"
    assert result["payout"] == D("10")
    assert store.paper_quantity("token-a") == D("0")
    assert store.cash() == cash_before + D("10")
    assert store.status()["occupied_capital"] == "0"


def test_transport_rejects_any_non_public_or_mutating_request():
    validate_public_request(
        "GET",
        "https://data-api.polymarket.com/positions?user=0x4989bfed5900ba096b08ba1f9b718464527c983e",
    )
    validate_public_request(
        "GET",
        "https://clob.polymarket.com/book?token_id=token-a",
    )
    validate_public_request(
        "GET",
        "https://gamma-api.polymarket.com/markets?condition_ids=condition-token-a",
    )

    with pytest.raises(ReadOnlyViolation):
        validate_public_request("POST", "https://clob.polymarket.com/order")
    with pytest.raises(ReadOnlyViolation):
        validate_public_request("GET", "https://example.com/positions")


def test_status_keeps_paper_safety_markers(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)

    status = store.status()

    assert status["paper_only"] is True
    assert status["real_order_submitted"] is False
    assert status["initial_cash"] == "300"
    assert status["scale"] == "0.4"


def test_first_cycle_seeds_and_second_cycle_fetches_book_only_for_changed_asset(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    calls: list[str] = []
    snapshots = [
        [source_position("100")],
        [source_position("125")],
    ]

    def getter(url: str) -> object:
        calls.append(url)
        if "/positions?" in url:
            return snapshots.pop(0)
        if "/book?" in url:
            return book()
        raise AssertionError(url)

    first = run_cycle(store, getter=getter, observed_at=1)
    second = run_cycle(store, getter=getter, observed_at=2)

    assert first["seeded"] is True
    assert first["results"] == []
    assert second["seeded"] is False
    assert second["results"][0]["status"] == "FILLED"
    assert sum("/book?" in url for url in calls) == 1


def test_status_files_auto_refresh_each_second_and_show_safety_state(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)

    render_status_files(store, tmp_path / "runtime", poll_seconds=1)

    html = (tmp_path / "runtime" / "status.html").read_text()
    payload = (tmp_path / "runtime" / "status.json").read_text()
    assert 'http-equiv="refresh" content="1"' in html
    assert "300 USD" in html
    assert "已实现盈亏（已结算/已卖出）" in html
    assert "可用现金" in html
    assert "占用资金（未结束持仓成本）" in html
    assert '"occupied_capital": "0"' in payload
    assert '"paper_only": true' in payload
    assert '"real_order_submitted": false' in payload


def test_weather_account_statement_separates_open_and_closed_records(tmp_path):
    store = PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=D("300"),
        scale=D("0.4"),
    )
    store.initialize([], observed_at=1)
    apply_source_snapshot(
        store,
        [source_position("25")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )
    apply_source_snapshot(
        store,
        [source_position("12.5")],
        books_by_asset={"token-a": book()},
        observed_at=3,
    )

    status = store.status()
    closed = status["closed_records"][0]
    assert status["pnl_reconciliation_ok"] is True
    assert status["replay_errors"] == []
    assert status["positions"][0]["position_status"] == "持仓中"
    assert status["positions"][0]["pnl_status"] == "尚未实现"
    assert D(status["positions"][0]["occupied_cost"]) == (
        D(status["positions"][0]["quantity"])
        * D(status["positions"][0]["average_cost"])
    )
    assert closed["close_type"] == "卖出"
    assert D(closed["realized_pnl"]) == (
        D(closed["net_recovered"]) - D(closed["cost_basis"])
    )

    cash_before = store.cash()
    positions_before = store.open_paper_positions()
    ledger_before = store.ledger_rows()
    source_anchor_before = store.source_anchor("token-a")
    target_before = store.target_quantity("token-a")
    runtime_dir = tmp_path / "runtime"

    render_status_files(store, runtime_dir, poll_seconds=1)

    html = (runtime_dir / "status.html").read_text()
    assert "<h2>持仓中</h2>" in html
    assert "<h2>已结束</h2>" in html
    assert "单笔盈亏" in html
    assert "尚未实现" in html
    assert "占用金额" in html
    assert store.cash() == cash_before
    assert store.open_paper_positions() == positions_before
    assert store.ledger_rows() == ledger_before
    assert store.source_anchor("token-a") == source_anchor_before
    assert store.target_quantity("token-a") == target_before


def test_weather_position_past_end_time_moves_to_pending_settlement(tmp_path):
    store = PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=D("300"),
        scale=D("0.4"),
    )
    ended = source_position("25")
    ended["endDate"] = "2000-01-01T00:00:00Z"
    store.initialize([], observed_at=1)
    apply_source_snapshot(
        store,
        [ended],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )

    status = store.status()

    assert status["active_positions"] == []
    assert status["pending_positions"][0]["position_status"] == "待结算"
    assert status["pending_positions"][0]["pnl_status"] == "等待官方结算"

    runtime_dir = tmp_path / "runtime"
    render_status_files(store, runtime_dir, poll_seconds=1)
    html = (runtime_dir / "status.html").read_text()
    assert "<h2>待结算</h2>" in html
    assert "活动时间已过，等待 Polymarket 正式判定" in html


def test_utc_market_end_is_rendered_in_shanghai_time():
    assert (
        tracker.format_end_time_shanghai("2026-07-25T12:00:00Z")
        == "2026-07-25 20:00（上海时间）"
    )


def test_status_adds_market_end_time_without_changing_existing_paper_records(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)
    apply_source_snapshot(
        store,
        [source_position("25")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )
    account_before = store.cash()
    positions_before = store.open_paper_positions()
    ledger_before = store.ledger_rows()
    gamma_calls: list[str] = []

    def getter(url: str) -> object:
        gamma_calls.append(url)
        return [
            {
                "conditionId": "condition-token-a",
                "endDate": "2026-07-25T12:00:00Z",
            }
        ]

    runtime_dir = tmp_path / "runtime"
    render_status_files(
        store,
        runtime_dir,
        poll_seconds=1,
        getter=getter,
    )
    render_status_files(
        store,
        runtime_dir,
        poll_seconds=1,
        getter=getter,
    )

    html = (runtime_dir / "status.html").read_text()
    payload = (runtime_dir / "status.json").read_text()
    expected_occupied_capital = sum(
        (
            D(row["quantity"]) * D(row["average_cost"])
            for row in store.open_paper_positions()
        ),
        D("0"),
    )
    assert "结束时间（上海）" in html
    assert "2026-07-25 20:00（上海时间）" in html
    assert f"{expected_occupied_capital:,.2f} USD" in html
    assert (
        f'"occupied_capital": "{expected_occupied_capital}"'
        in payload
    )
    assert '"end_time_shanghai": "2026-07-25 20:00（上海时间）"' in payload
    assert gamma_calls == [
        "https://gamma-api.polymarket.com/markets?condition_ids=condition-token-a"
    ]
    assert store.cash() == account_before
    assert store.open_paper_positions() == positions_before
    assert store.ledger_rows() == ledger_before


def test_settlement_cycle_uses_public_market_winner_and_releases_cash(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    store.initialize([], observed_at=1)
    apply_source_snapshot(
        store,
        [source_position("25")],
        books_by_asset={"token-a": book()},
        observed_at=2,
    )
    cash_before = store.cash()
    calls: list[str] = []

    def getter(url: str) -> object:
        calls.append(url)
        return {
            "closed": True,
            "tokens": [
                {"token_id": "token-a", "outcome": "Yes", "winner": True},
                {"token_id": "token-no", "outcome": "No", "winner": False},
            ],
        }

    results = run_settlement_cycle(store, getter=getter, observed_at=3)

    assert results[0]["status"] == "SETTLED"
    assert store.cash() == cash_before + D("10")
    assert store.status()["occupied_capital"] == "0"
    assert calls == ["https://clob.polymarket.com/markets/condition-token-a"]


def test_daemon_polls_each_second_and_refreshes_status(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))
    calls: list[str] = []
    sleeps: list[int] = []
    timestamps = iter([100, 101])

    def getter(url: str) -> object:
        calls.append(url)
        if "/positions?" in url:
            return []
        raise AssertionError(url)

    run_daemon(
        store,
        runtime_dir=tmp_path / "runtime",
        poll_seconds=1,
        getter=getter,
        sleeper=sleeps.append,
        clock=lambda: next(timestamps),
        max_cycles=2,
    )

    assert sum("/positions?" in url for url in calls) == 2
    assert sleeps == [1]
    assert store.status()["last_heartbeat"] == "101"
    assert (tmp_path / "runtime" / "status.html").exists()


def test_process_lock_allows_only_one_daemon(tmp_path):
    first = acquire_process_lock(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="already running"):
            acquire_process_lock(tmp_path)
    finally:
        first.close()


def test_store_connection_closes_after_context(tmp_path):
    store = PaperStore(tmp_path / "paper.sqlite3", initial_cash=D("300"), scale=D("0.4"))

    with store._connect() as conn:
        conn.execute("SELECT 1")

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")
