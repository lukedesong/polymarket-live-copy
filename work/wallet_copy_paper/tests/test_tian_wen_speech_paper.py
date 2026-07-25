import importlib.util
from decimal import Decimal
from pathlib import Path
import sqlite3

import pytest
import tian_wen_speech_paper as tracker


D = Decimal


def trade(
    *,
    tx: str,
    timestamp: int,
    side: str = "BUY",
    size: str = "0.22",
    price: str = "0.30",
    asset: str = "token-a",
    title: str = 'Will Trump say "AI" during his address?',
    end_date: str = "2026-07-24T23:59:00Z",
) -> dict:
    return {
        "proxyWallet": tracker.SOURCE_WALLET,
        "transactionHash": tx,
        "timestamp": timestamp,
        "side": side,
        "size": size,
        "price": price,
        "asset": asset,
        "conditionId": f"condition-{asset}",
        "title": title,
        "eventSlug": "what-will-trump-say-during-his-address",
        "slug": "will-trump-say-ai-during-his-address",
        "outcome": "Yes",
        "endDate": end_date,
    }


def source_position(
    size: str = "100",
    *,
    asset: str = "token-a",
    title: str = 'Will Trump say "AI" during his address?',
    end_date: str = "2026-07-24T23:59:00Z",
) -> dict:
    return {
        "asset": asset,
        "conditionId": f"condition-{asset}",
        "title": title,
        "eventSlug": "what-will-trump-say-during-his-address",
        "slug": "will-trump-say-ai-during-his-address",
        "outcome": "Yes",
        "size": size,
        "endDate": end_date,
    }


def book(*, minimum: str = "5", ask_size: str = "100", bid_size: str = "100") -> dict:
    return {
        "asset_id": "token-a",
        "min_order_size": minimum,
        "timestamp": "1784900000000",
        "hash": "book-hash",
        "asks": [{"price": "0.30", "size": ask_size}],
        "bids": [{"price": "0.29", "size": bid_size}],
    }


def market_info(*, minimum: int = 5, fee_rate: float = 0.04) -> dict:
    return {
        "mos": minimum,
        "fd": {"r": fee_rate, "e": 1, "to": True},
        "t": [{"t": "token-a", "o": "Yes"}],
    }


def test_tian_wen_paper_module_exists():
    assert importlib.util.find_spec("tian_wen_speech_paper") is not None


def test_paper_tracker_exposes_required_boundaries():
    required = (
        "PaperStore",
        "ReadOnlyViolation",
        "acquire_process_lock",
        "apply_source_actions",
        "fetch_trade_window",
        "format_end_time_shanghai",
        "group_trade_rows",
        "is_speech_word_trade",
        "render_status_files",
        "run_cycle",
        "run_daemon",
        "run_settlement_cycle",
        "validate_public_request",
        "walk_visible_depth",
    )
    missing = [name for name in required if not hasattr(tracker, name)]
    assert missing == []


def test_user_cash_and_fixed_position_scale_are_explicit():
    assert tracker.DEFAULT_SCALE == D("5") / D("68.7")
    assert tracker.DEFAULT_INITIAL_CASH == D("300")
    assert tracker.THEORETICAL_MIN_CASH_ESTIMATE == D("257.59")


@pytest.mark.parametrize(
    ("title", "accepted"),
    [
        ('Will Trump say "AI" during his address?', True),
        ('Will Donald Trump mention "China" in the speech?', True),
        ('Will Trump attend the NATO summit?', False),
        ('Will Trump meet Xi Jinping?', False),
        ('Will Trump speak to the President of France?', False),
        ('Will Trump post "AI" on Truth Social?', False),
        ("Will the Lakers win tonight?", False),
    ],
)
def test_only_trump_speech_word_markets_enter_the_sleeve(title, accepted):
    assert tracker.is_speech_word_trade({"title": title}) is accepted


def test_same_transaction_fragments_are_grouped_without_losing_size():
    rows = [
        trade(tx="0xabc", timestamp=101, size="0.22", price="0.30"),
        trade(tx="0xabc", timestamp=101, size="4.78", price="0.31"),
    ]

    actions = tracker.group_trade_rows(rows)

    assert len(actions) == 1
    assert actions[0]["source_quantity"] == D("5")
    assert actions[0]["source_vwap"] == D("0.30956")
    assert len(actions[0]["row_ids"]) == 2


def test_startup_seeds_positions_and_watermark_without_backfill(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    current = trade(tx="0xseed", timestamp=100)

    store.initialize([source_position()], [current], observed_at=100)

    assert store.cash() == tracker.DEFAULT_INITIAL_CASH
    assert store.source_anchor("token-a") == D("100")
    assert store.source_size("token-a") == D("100")
    assert store.paper_quantity("token-a") == D("0")
    assert store.is_trade_processed(tracker.trade_row_id(current))
    assert store.ledger_rows() == []


def test_position_snapshot_unit_becomes_exactly_five_shares_not_rounded(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([source_position()], [], observed_at=100)
    action = tracker.group_trade_rows(
        [trade(tx="0xbuy", timestamp=101, size="68.7")]
    )[0]

    results = tracker.apply_source_actions(
        store,
        [action],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )

    assert results[0]["status"] == "FILLED"
    assert results[0]["quantity"] == D("5")
    assert results[0]["fee"] == D("0.04200")
    assert store.paper_quantity("token-a") == D("5")
    assert store.target_quantity("token-a") == D("5")
    assert store.cash() == tracker.DEFAULT_INITIAL_CASH - D("1.54200")
    assert store.ledger_rows()[0]["end_date_utc"] == "2026-07-24T23:59:00Z"


def test_future_fragment_below_sample_minimum_is_skipped_not_inflated(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([], [], observed_at=100)
    action = tracker.group_trade_rows(
        [trade(tx="0xfuture-small", timestamp=101, size="0.10")]
    )[0]

    results = tracker.apply_source_actions(
        store,
        [action],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )

    assert results[0]["status"] == "SKIPPED"
    assert results[0]["reason"] == "BELOW_MIN_ORDER"
    assert results[0]["requested_quantity"] == D("0.10") * tracker.DEFAULT_SCALE
    assert store.paper_quantity("token-a") == D("0")


def test_source_sells_reduce_target_and_never_sell_more_than_held(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([source_position()], [], observed_at=100)
    buy = tracker.group_trade_rows(
        [trade(tx="0xbuy", timestamp=101, size="68.7")]
    )[0]
    tracker.apply_source_actions(
        store,
        [buy],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )
    sell = tracker.group_trade_rows(
        [trade(tx="0xsell", timestamp=102, side="SELL", size="68.7")]
    )[0]

    results = tracker.apply_source_actions(
        store,
        [sell],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=102,
    )

    assert results[0]["status"] == "FILLED"
    assert results[0]["side"] == "SELL"
    assert results[0]["quantity"] == D("5")
    assert store.paper_quantity("token-a") == D("0")
    assert store.source_anchor("token-a") == D("100")


def test_visible_depth_can_produce_a_partial_paper_fill(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([], [], observed_at=100)
    action = tracker.group_trade_rows(
        [trade(tx="0xlarge", timestamp=101, size="400")]
    )[0]

    results = tracker.apply_source_actions(
        store,
        [action],
        books_by_asset={"token-a": book(ask_size="10")},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )

    assert results[0]["status"] == "PARTIAL"
    assert results[0]["quantity"] == D("10")
    assert results[0]["requested_quantity"] == D("400") * tracker.DEFAULT_SCALE
    assert store.paper_quantity("token-a") == D("10")


def test_visible_depth_below_market_minimum_is_not_filled():
    fill = tracker.walk_visible_depth(
        [{"price": "0.30", "size": "4"}],
        requested=D("10"),
        ascending=True,
        fee_rate=D("0.04"),
        fee_exponent=1,
    )

    assert fill["quantity"] == D("4")
    assert fill["notional"] == D("1.20")
    assert fill["fee"] == D("0.03360")


def test_fetch_trade_window_stops_at_a_processed_watermark(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    known = trade(tx="0xknown", timestamp=100)
    new = trade(tx="0xnew", timestamp=101)
    store.initialize([], [known], observed_at=100)

    result = tracker.fetch_trade_window(
        store,
        getter=lambda _url: [new, known],
        page_size=2,
        max_pages=2,
    )

    assert result["cursor_gap"] is False
    assert result["pages"] == 1
    assert [row["transactionHash"] for row in result["rows"]] == ["0xnew"]


def test_cursor_gap_blocks_ambiguous_catch_up(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([], [trade(tx="0xknown", timestamp=1)], observed_at=1)
    pages = {
        0: [
            trade(tx="0xa", timestamp=5),
            trade(tx="0xb", timestamp=4),
        ],
        2: [
            trade(tx="0xc", timestamp=3),
            trade(tx="0xd", timestamp=2),
        ],
    }

    def getter(url: str) -> list[dict]:
        offset = int(url.split("offset=")[1].split("&")[0])
        return pages[offset]

    result = tracker.fetch_trade_window(
        store,
        getter=getter,
        page_size=2,
        max_pages=2,
    )

    assert result["cursor_gap"] is True
    assert result["rows"] == []


def test_transport_rejects_mutating_private_or_unknown_requests():
    tracker.validate_public_request(
        "GET",
        "https://data-api.polymarket.com/trades?user=0xabc&takerOnly=false",
    )
    tracker.validate_public_request(
        "GET",
        "https://clob.polymarket.com/book?token_id=token-a",
    )
    tracker.validate_public_request(
        "GET",
        "https://gamma-api.polymarket.com/markets?slug=market-slug",
    )

    with pytest.raises(tracker.ReadOnlyViolation):
        tracker.validate_public_request("POST", "https://clob.polymarket.com/order")
    with pytest.raises(tracker.ReadOnlyViolation):
        tracker.validate_public_request("GET", "https://example.com/trades")


def test_status_exposes_safety_and_numeric_provenance(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([], [], observed_at=100)

    status = store.status()

    assert status["paper_only"] is True
    assert status["real_order_submitted"] is False
    assert status["copy_scope"] == "tian_wen_trump_speech_words_only"
    assert status["scale_status"] == "PROVISIONAL_POSITION_SNAPSHOT_BOUND"
    assert status["data_block"] == "SOURCE_ORDER_ID_UNAVAILABLE"
    assert status["initial_cash_provenance_class"] == "user_specified"
    assert status["scale_provenance_class"] == "formula_derived"
    assert status["theoretical_min_cash_estimate"] == "257.59"


def test_utc_market_end_is_rendered_in_shanghai_time():
    assert (
        tracker.format_end_time_shanghai("2026-07-24T23:59:00Z")
        == "2026-07-25 07:59（上海时间）"
    )


def test_status_page_auto_refreshes_and_names_the_expert_sleeve(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([source_position()], [], observed_at=100)
    action = tracker.group_trade_rows(
        [trade(tx="0xbuy", timestamp=101, size="68.7")]
    )[0]
    tracker.apply_source_actions(
        store,
        [action],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )

    tracker.render_status_files(store, tmp_path / "runtime", poll_seconds=1)

    html = (tmp_path / "runtime" / "status.html").read_text()
    payload = (tmp_path / "runtime" / "status.json").read_text()
    expected_occupied_capital = sum(
        (
            D(row["quantity"]) * D(row["average_cost"])
            for row in store.open_paper_positions()
        ),
        D("0"),
    )
    assert 'http-equiv="refresh" content="1"' in html
    assert "Tian-Wen / Trump speech words" in html
    assert "300.00 USD" in html
    assert "已实现盈亏（已结算/已卖出）" in html
    assert "可用现金" in html
    assert "占用资金（未结束持仓成本）" in html
    assert f"{expected_occupied_capital:,.2f} USD" in html
    assert (
        f'"occupied_capital": "{expected_occupied_capital}"'
        in payload
    )
    assert "结束时间（上海）" in html
    assert "2026-07-25 07:59（上海时间）" in html
    assert '"paper_only": true' in payload
    assert '"real_order_submitted": false' in payload
    assert '"end_time_shanghai": "2026-07-25 07:59（上海时间）"' in payload


def test_tian_account_statement_separates_open_and_closed_records(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([source_position()], [], observed_at=100)
    buy = tracker.group_trade_rows(
        [trade(tx="0xbuy-large", timestamp=101, size="137.4")]
    )[0]
    tracker.apply_source_actions(
        store,
        [buy],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )
    sell = tracker.group_trade_rows(
        [
            trade(
                tx="0xsell-part",
                timestamp=102,
                side="SELL",
                size="68.7",
            )
        ]
    )[0]
    tracker.apply_source_actions(
        store,
        [sell],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=102,
    )

    status = store.status()
    closed = status["closed_records"][0]
    assert status["pnl_reconciliation_ok"] is True
    assert status["replay_errors"] == []
    assert status["active_positions"] == []
    assert status["pending_positions"][0]["position_status"] == "待结算"
    assert status["pending_positions"][0]["pnl_status"] == "等待官方结算"
    assert D(status["pending_positions"][0]["occupied_cost"]) == (
        D(status["pending_positions"][0]["quantity"])
        * D(status["pending_positions"][0]["average_cost"])
    )
    assert closed["close_type"] == "卖出"
    assert D(closed["realized_pnl"]) == (
        D(closed["net_recovered"]) - D(closed["cost_basis"])
    )

    cash_before = store.cash()
    positions_before = store.open_paper_positions()
    ledger_before = store.ledger_rows()
    source_before = store.source_rows()
    runtime_dir = tmp_path / "runtime"

    tracker.render_status_files(store, runtime_dir, poll_seconds=1)

    html = (runtime_dir / "status.html").read_text()
    assert "<h2>持仓中</h2>" in html
    assert "<h2>待结算</h2>" in html
    assert "<h2>已结束</h2>" in html
    assert "单笔盈亏" in html
    assert "等待官方结算" in html
    assert "活动时间已过，等待 Polymarket 正式判定" in html
    assert "占用金额" in html
    assert store.cash() == cash_before
    assert store.open_paper_positions() == positions_before
    assert store.ledger_rows() == ledger_before
    assert store.source_rows() == source_before


def test_settlement_releases_winning_paper_cash(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    store.initialize([source_position()], [], observed_at=100)
    action = tracker.group_trade_rows(
        [trade(tx="0xbuy", timestamp=101, size="68.7")]
    )[0]
    tracker.apply_source_actions(
        store,
        [action],
        books_by_asset={"token-a": book()},
        market_info_by_condition={"condition-token-a": market_info()},
        observed_at=101,
    )
    cash_before = store.cash()

    results = tracker.run_settlement_cycle(
        store,
        getter=lambda _url: {
            "closed": True,
            "tokens": [
                {"token_id": "token-a", "outcome": "Yes", "winner": True},
                {"token_id": "token-no", "outcome": "No", "winner": False},
            ],
        },
        observed_at=102,
    )

    assert results[0]["status"] == "SETTLED"
    assert store.cash() == cash_before + D("5")
    assert store.paper_quantity("token-a") == D("0")
    assert store.status()["occupied_capital"] == "0"
    closed = store.status()["closed_records"][0]
    assert closed["close_type"] == "结算盈利"
    assert D(closed["realized_pnl"]) == (
        D(closed["net_recovered"]) - D(closed["cost_basis"])
    )


def test_first_cycle_watermarks_and_second_cycle_processes_only_new_sleeve_trade(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    known = trade(tx="0xknown", timestamp=100)
    new = trade(tx="0xnew", timestamp=101, size="68.7")
    trade_pages = iter([[known], [new, known]])
    calls: list[str] = []

    def getter(url: str):
        calls.append(url)
        if "/positions?" in url:
            return [source_position()]
        if "/trades?" in url:
            return next(trade_pages)
        if "/book?" in url:
            return book()
        if "/clob-markets/" in url:
            return market_info()
        if "gamma-api.polymarket.com/markets?" in url:
            return [{"endDate": "2026-07-24T23:59:00Z"}]
        raise AssertionError(url)

    first = tracker.run_cycle(store, getter=getter, observed_at=100)
    second = tracker.run_cycle(store, getter=getter, observed_at=101)

    assert first["seeded"] is True
    assert first["results"] == []
    assert second["seeded"] is False
    assert second["results"][0]["status"] == "FILLED"
    assert store.paper_quantity("token-a") == D("5")
    assert sum("/book?" in url for url in calls) == 1


def test_first_cycle_enriches_date_only_end_date_from_public_gamma(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )
    calls: list[str] = []

    def getter(url: str):
        calls.append(url)
        if "/positions?" in url:
            return [source_position(end_date="2026-07-24")]
        if "/trades?" in url:
            return []
        if "gamma-api.polymarket.com/markets?" in url:
            return [{"endDate": "2026-07-24T23:59:00Z"}]
        raise AssertionError(url)

    result = tracker.run_cycle(store, getter=getter, observed_at=100)

    assert result["seeded"] is True
    assert store.source_rows()["token-a"]["end_date"] == "2026-07-24T23:59:00Z"
    assert sum("gamma-api.polymarket.com/markets?" in url for url in calls) == 1


def test_process_lock_allows_only_one_daemon(tmp_path):
    first = tracker.acquire_process_lock(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="already running"):
            tracker.acquire_process_lock(tmp_path)
    finally:
        first.close()


def test_store_connection_closes_after_context(tmp_path):
    store = tracker.PaperStore(
        tmp_path / "paper.sqlite3",
        initial_cash=tracker.DEFAULT_INITIAL_CASH,
        scale=tracker.DEFAULT_SCALE,
    )

    with store._connect() as conn:
        conn.execute("SELECT 1")

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        conn.execute("SELECT 1")


def test_source_file_has_no_order_submission_dependency():
    source = Path(tracker.__file__).read_text()
    forbidden = ("create_order", "post_order", "private_key", "py_clob_client")
    assert [token for token in forbidden if token in source.lower()] == []
