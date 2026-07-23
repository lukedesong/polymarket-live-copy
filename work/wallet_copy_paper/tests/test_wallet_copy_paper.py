from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from wallet_copy_paper import (
    ACCOUNTS,
    Book,
    BookLevel,
    MarketParams,
    PaperStore,
    PublicClient,
    ReadOnlyViolation,
    SourceAction,
    TrackerRuntime,
    TradeRow,
    build_status,
    decide,
    fee_for_leg,
    group_trade_rows,
    parse_epoch,
    validate_public_request,
    write_reports,
)


def row(
    *,
    wallet: str = ACCOUNTS[0].wallet,
    transaction_hash: str = "0xabc",
    asset: str = "asset-a",
    side: str = "BUY",
    size: str = "8",
    price: str = "0.40",
    timestamp: int = 100,
) -> TradeRow:
    return TradeRow(
        wallet=wallet,
        transaction_hash=transaction_hash,
        asset=asset,
        condition_id="condition-a",
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        timestamp=timestamp,
        title="Example market",
        outcome="Yes",
        slug="example-market",
        raw={"transactionHash": transaction_hash},
    )


def action(*, side: str = "BUY", timestamp: int = 100) -> SourceAction:
    return group_trade_rows([row(side=side, timestamp=timestamp)])[0]


def book(*, timestamp: int = 101, minimum: str = "5") -> Book:
    return Book(
        asset="asset-a",
        condition_id="condition-a",
        timestamp=timestamp,
        book_hash="book-hash",
        min_order_size=Decimal(minimum),
        bids=(BookLevel(Decimal("0.55"), Decimal("5")),),
        asks=(
            BookLevel(Decimal("0.50"), Decimal("2")),
            BookLevel(Decimal("0.60"), Decimal("3")),
        ),
    )


def params(*, rate: str = "0", exponent: int = 2) -> MarketParams:
    return MarketParams(
        condition_id="condition-a",
        min_order_size=Decimal("5"),
        fee_rate=Decimal(rate),
        fee_exponent=exponent,
    )


def test_accounts_are_independent_user_specified_cash():
    assert [account.starting_cash for account in ACCOUNTS] == [
        Decimal("100"),
        Decimal("100"),
        Decimal("100"),
    ]
    assert len({account.account_id for account in ACCOUNTS}) == 3
    assert len({account.wallet for account in ACCOUNTS}) == 3


def test_transport_is_public_get_only():
    validate_public_request("GET", "https://data-api.polymarket.com/trades?user=0x1")
    validate_public_request("GET", "https://clob.polymarket.com/book?token_id=a")
    validate_public_request("GET", "https://clob.polymarket.com/clob-markets/condition-a")
    validate_public_request("GET", "https://gamma-api.polymarket.com/markets?condition_ids=a")

    for method, url in (
        ("POST", "https://clob.polymarket.com/order"),
        ("GET", "https://clob.polymarket.com/data/orders"),
        ("GET", "https://evil.example/trades"),
        ("GET", "http://data-api.polymarket.com/trades"),
    ):
        with pytest.raises(ReadOnlyViolation):
            validate_public_request(method, url)


def test_trade_request_explicitly_includes_maker_and_taker_rows():
    seen: list[str] = []

    def transport(url: str):
        seen.append(url)
        return []

    PublicClient(transport=transport).trades(ACCOUNTS[0].wallet)
    query = parse_qs(urlsplit(seen[0]).query)
    assert query["takerOnly"] == ["false"]
    assert query["user"] == [ACCOUNTS[0].wallet]


def test_server_time_parser_normalizes_seconds_and_milliseconds():
    assert parse_epoch("1753200000") == parse_epoch("1753200000000")
    with pytest.raises(ValueError):
        parse_epoch("not-a-time")


def test_split_rows_group_to_one_source_action_but_assets_remain_separate():
    first = row(size="2")
    split = row(size="3", price="0.41")
    other_asset = row(asset="asset-b")

    grouped = group_trade_rows([first, split, other_asset])

    assert len(grouped) == 2
    asset_a = next(item for item in grouped if item.asset == "asset-a")
    assert asset_a.source_size == Decimal("5")
    assert len(asset_a.raw_rows) == 2


def test_buy_uses_current_minimum_and_walks_visible_asks():
    result = decide(
        action(),
        book(),
        params(),
        cash=Decimal("100"),
        position=Decimal("0"),
    )

    assert result.result == "FILLED"
    assert result.quantity == Decimal("5")
    assert result.gross == Decimal("2.80")
    assert result.fee == Decimal("0")
    assert result.cash_delta == Decimal("-2.80")
    assert [leg.quantity for leg in result.legs] == [Decimal("2"), Decimal("3")]


def test_official_fee_curve_is_applied_and_rounded_to_protocol_precision():
    assert fee_for_leg(
        quantity=Decimal("5"),
        price=Decimal("0.50"),
        fee_rate=Decimal("0.04"),
        exponent=2,
    ) == Decimal("0.01250")


def test_buy_skips_without_complete_cash_or_depth_or_fresh_book():
    assert decide(
        action(), book(), params(), cash=Decimal("2.79"), position=Decimal("0")
    ).result == "SKIP_INSUFFICIENT_CASH"

    shallow = Book(
        asset="asset-a",
        condition_id="condition-a",
        timestamp=101,
        book_hash="shallow",
        min_order_size=Decimal("5"),
        bids=(),
        asks=(BookLevel(Decimal("0.50"), Decimal("4")),),
    )
    assert decide(
        action(), shallow, params(), cash=Decimal("100"), position=Decimal("0")
    ).result == "SKIP_INSUFFICIENT_DEPTH"
    assert decide(
        action(timestamp=100), book(timestamp=99), params(), cash=Decimal("100"), position=Decimal("0")
    ).result == "SKIP_STALE_BOOK"


def test_sell_uses_current_minimum_and_never_creates_short_position():
    filled = decide(
        action(side="SELL"),
        book(),
        params(),
        cash=Decimal("95"),
        position=Decimal("5"),
    )
    assert filled.result == "FILLED"
    assert filled.quantity == Decimal("5")
    assert filled.gross == Decimal("2.75")
    assert filled.cash_delta == Decimal("2.75")

    assert decide(
        action(side="SELL"), book(), params(), cash=Decimal("95"), position=Decimal("0")
    ).result == "SKIP_NO_POSITION"
    assert decide(
        action(side="SELL"), book(), params(), cash=Decimal("95"), position=Decimal("4")
    ).result == "SKIP_BELOW_MINIMUM"


def test_store_isolates_accounts_and_deduplicates_source_actions(tmp_path: Path):
    store = PaperStore(tmp_path / "paper.sqlite3")
    store.initialize()
    source = action()
    decision = decide(
        source, book(), params(), cash=store.cash("russell"), position=Decimal("0")
    )

    assert store.apply("russell", source, book(), params(), decision, observed_at=102)
    assert not store.apply("russell", source, book(), params(), decision, observed_at=103)
    assert store.cash("russell") == Decimal("97.20")
    assert store.cash("zorro") == Decimal("100")
    assert store.position("russell", "asset-a") == Decimal("5")
    assert store.position("zorro", "asset-a") == Decimal("0")


def test_first_seed_marks_history_without_creating_paper_fill(tmp_path: Path):
    store = PaperStore(tmp_path / "paper.sqlite3")
    store.initialize()
    source = action()

    store.seed("russell", [source], observed_at=101)

    assert store.has_source(source.identity)
    assert store.paper_fill_count() == 0
    assert store.cash("russell") == Decimal("100")


def test_status_and_reports_keep_observed_and_paper_evidence_separate(tmp_path: Path):
    store = PaperStore(tmp_path / "paper.sqlite3")
    store.initialize()
    source = action()
    decision = decide(
        source, book(), params(), cash=store.cash("russell"), position=Decimal("0")
    )
    store.apply("russell", source, book(), params(), decision, observed_at=102)

    status = build_status(store, heartbeat={"state": "running", "updated_at": 102})
    assert status["paper_only"] is True
    assert status["real_order_submitted"] is False
    assert len(status["accounts"]) == 3
    assert status["accounts"][0]["source_actions"] == 1
    assert status["accounts"][0]["paper_fills"] == 1

    write_reports(store, tmp_path, heartbeat={"state": "running", "updated_at": 102})
    assert json.loads((tmp_path / "status.json").read_text())["paper_only"] is True
    assert "russell110320" in (tmp_path / "status.html").read_text()
    assert (tmp_path / "ledger.csv").read_text().count("\n") == 2
    with sqlite3.connect(tmp_path / "paper.sqlite3") as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


class FakeClient:
    def __init__(self):
        self.rows = {account.wallet: [row(wallet=account.wallet, transaction_hash="0xold")] for account in ACCOUNTS}
        self.current_book = book(timestamp=301)

    def trades(self, wallet: str):
        return list(self.rows[wallet])

    def book(self, asset: str):
        assert asset == "asset-a"
        return self.current_book

    def market_params(self, condition_id: str):
        assert condition_id == "condition-a"
        return params()


def test_runtime_seeds_history_then_processes_only_new_source_actions(tmp_path: Path):
    store = PaperStore(tmp_path / "paper.sqlite3")
    store.initialize()
    client = FakeClient()
    runtime = TrackerRuntime(store, client, tmp_path)

    first = runtime.run_once(now=150)

    assert first["seeded_accounts"] == 3
    assert store.paper_fill_count() == 0
    for account in ACCOUNTS:
        client.rows[account.wallet].append(
            row(
                wallet=account.wallet,
                transaction_hash=f"0xnew-{account.account_id}",
                timestamp=200,
            )
        )

    second = runtime.run_once(now=202)

    assert second["processed"] == 3
    assert second["filled"] == 3
    assert store.paper_fill_count() == 3
    assert all(item["paper_fills"] == 1 for item in build_status(store, heartbeat=second)["accounts"])


def test_runtime_refreshes_open_positions_at_executable_bid_depth(tmp_path: Path):
    store = PaperStore(tmp_path / "paper.sqlite3")
    store.initialize()
    client = FakeClient()
    runtime = TrackerRuntime(store, client, tmp_path)
    runtime.run_once(now=150)
    client.rows[ACCOUNTS[0].wallet].append(
        row(transaction_hash="0xnew", timestamp=200)
    )

    runtime.run_once(now=202)

    russell = build_status(store, heartbeat={})["accounts"][0]
    assert russell["executable_position_value"] == "2.75"
    assert russell["executable_equity"] == "99.95"
    assert russell["paper_pnl"] == "-0.05"
    assert (tmp_path / "status.html").exists()


def test_position_aggregation_never_round_trips_through_binary_float(tmp_path: Path):
    store = PaperStore(tmp_path / "paper.sqlite3")
    store.initialize()
    for transaction_hash, minimum in (("0xsmall-a", "0.1"), ("0xsmall-b", "0.2")):
        source = group_trade_rows([row(transaction_hash=transaction_hash)])[0]
        current_book = Book(
            asset="asset-a",
            condition_id="condition-a",
            timestamp=101,
            book_hash=transaction_hash,
            min_order_size=Decimal(minimum),
            bids=(BookLevel(Decimal("0.5"), Decimal("1")),),
            asks=(BookLevel(Decimal("0.5"), Decimal("1")),),
        )
        market_params = MarketParams(
            condition_id="condition-a",
            min_order_size=Decimal(minimum),
            fee_rate=Decimal("0"),
            fee_exponent=2,
        )
        decision = decide(
            source,
            current_book,
            market_params,
            cash=store.cash("russell"),
            position=store.position("russell", "asset-a"),
        )
        store.apply(
            "russell", source, current_book, market_params, decision, observed_at=102
        )

    assert store.open_positions()[0]["quantity"] == Decimal("0.3")
