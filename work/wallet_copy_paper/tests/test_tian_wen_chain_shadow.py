import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

import tian_wen_chain_shadow as shadow


D = Decimal


def word(value: int) -> str:
    return f"{value:064x}"


def address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def filled_log(
    *,
    side: int = 0,
    token_id: int = 123,
    maker_amount: int = 3_000_000,
    taker_amount: int = 10_000_000,
    maker: str = shadow.SOURCE_WALLET,
    taker: str = "0x1111111111111111111111111111111111111111",
    tx_hash: str = "0xabc",
    log_index: int = 7,
    block_number: int = 101,
    block_hash: str | None = None,
) -> dict:
    return {
        "address": shadow.V2_EXCHANGE_ADDRESSES[0],
        "blockNumber": hex(block_number),
        "blockHash": block_hash or f"0xh{block_number}",
        "transactionHash": tx_hash,
        "logIndex": hex(log_index),
        "topics": [
            shadow.ORDER_FILLED_TOPIC,
            "0x" + word(99),
            address_topic(maker),
            address_topic(taker),
        ],
        "data": "0x"
        + "".join(
            [
                word(side),
                word(token_id),
                word(maker_amount),
                word(taker_amount),
                word(12_345),
                word(0),
                word(0),
            ]
        ),
    }


def chain_event(
    *,
    tx_hash: str = "0xabc",
    log_index: int = 7,
    token_id: str = "123",
    side: str = "BUY",
    source_role: str = "maker",
    block_number: int = 101,
    block_hash: str = "0xblock101",
    block_timestamp: int = 1_000,
    chain_seen_at_ms: int = 1_004_000,
    catchup: bool = False,
) -> dict:
    return {
        "chain_id": shadow.CHAIN_ID,
        "exchange_address": shadow.V2_EXCHANGE_ADDRESSES[0],
        "block_number": block_number,
        "block_hash": block_hash,
        "block_timestamp": block_timestamp,
        "transaction_hash": tx_hash,
        "log_index": log_index,
        "order_hash": "0x" + word(99),
        "maker": shadow.SOURCE_WALLET,
        "taker": "0x1111111111111111111111111111111111111111",
        "source_role": source_role,
        "source_order": source_role == "maker",
        "side_code": 0 if side == "BUY" else 1,
        "side": side,
        "token_id": token_id,
        "maker_amount_raw": "3000000",
        "taker_amount_raw": "10000000",
        "fee_raw": "12345",
        "quantity": D("10"),
        "notional": D("3"),
        "price": D("0.3"),
        "chain_seen_at_ms": chain_seen_at_ms,
        "catchup": catchup,
        "orphaned": False,
        "raw_log": {"fixture": True},
    }


def trade_row(
    *,
    size: str,
    price: str,
    tx_hash: str = "0xabc",
    token_id: str = "123",
    side: str = "BUY",
    title: str = 'Will Trump say "AI" during his address?',
) -> dict:
    return {
        "proxyWallet": shadow.SOURCE_WALLET,
        "transactionHash": tx_hash,
        "timestamp": 1_000,
        "side": side,
        "asset": token_id,
        "conditionId": "0xcondition",
        "size": size,
        "price": price,
        "outcome": "Yes",
        "slug": "will-trump-say-ai",
        "eventSlug": "what-will-trump-say",
        "title": title,
        "endDate": "2026-07-26T23:59:00Z",
    }


def sample_book(token_id: str = "123", timestamp: str = "999999") -> dict:
    return {
        "asset_id": token_id,
        "timestamp": timestamp,
        "hash": "book-hash",
        "bids": [{"price": "0.29", "size": "20"}],
        "asks": [{"price": "0.31", "size": "30"}],
    }


def test_decode_v2_buy_source_order():
    event = shadow.decode_order_filled(filled_log(), shadow.SOURCE_WALLET)

    assert event is not None
    assert event["source_role"] == "maker"
    assert event["source_order"] is True
    assert event["side"] == "BUY"
    assert event["token_id"] == "123"
    assert event["quantity"] == D("10")
    assert event["notional"] == D("3")
    assert event["price"] == D("0.3")


def test_decode_v2_sell_source_order():
    event = shadow.decode_order_filled(
        filled_log(
            side=1,
            maker_amount=10_000_000,
            taker_amount=3_000_000,
        ),
        shadow.SOURCE_WALLET,
    )

    assert event is not None
    assert event["side"] == "SELL"
    assert event["quantity"] == D("10")
    assert event["notional"] == D("3")
    assert event["price"] == D("0.3")


def test_source_as_taker_is_recorded_but_direction_is_not_guessed():
    event = shadow.decode_order_filled(
        filled_log(
            maker="0x2222222222222222222222222222222222222222",
            taker=shadow.SOURCE_WALLET,
        ),
        shadow.SOURCE_WALLET,
    )

    assert event is not None
    assert event["source_role"] == "taker"
    assert event["source_order"] is False
    assert event["side"] == "UNKNOWN"
    assert event["quantity"] is None
    assert event["price"] is None


def test_unrelated_wallet_log_is_filtered():
    event = shadow.decode_order_filled(
        filled_log(
            maker="0x2222222222222222222222222222222222222222",
            taker="0x3333333333333333333333333333333333333333",
        ),
        shadow.SOURCE_WALLET,
    )

    assert event is None


@pytest.mark.parametrize(
    "method",
    [
        "eth_blockNumber",
        "eth_getBlockByNumber",
        "eth_getLogs",
        "eth_getTransactionReceipt",
    ],
)
def test_rpc_read_methods_are_allow_listed(method):
    shadow.validate_rpc_request(shadow.DEFAULT_RPC_URLS[0], method)


@pytest.mark.parametrize(
    "method",
    ["eth_sendRawTransaction", "eth_sendTransaction", "personal_sign"],
)
def test_rpc_write_or_sign_methods_are_rejected(method):
    with pytest.raises(shadow.ReadOnlyViolation):
        shadow.validate_rpc_request(shadow.DEFAULT_RPC_URLS[0], method)


def test_old_paper_database_is_opened_read_only(tmp_path):
    old_db = tmp_path / "paper.sqlite3"
    with sqlite3.connect(old_db) as connection:
        connection.execute("CREATE TABLE source_state(asset TEXT PRIMARY KEY, title TEXT)")
        connection.execute(
            "INSERT INTO source_state VALUES (?, ?)",
            ("123", 'Will Trump say "AI" during his address?'),
        )

    connection = shadow.open_old_paper_read_only(old_db)
    try:
        assert connection.execute("SELECT COUNT(*) FROM source_state").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute(
                "INSERT INTO source_state VALUES (?, ?)",
                ("456", "write must fail"),
            )
    finally:
        connection.close()


def test_store_is_idempotent_and_metrics_use_observation_times(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    event = chain_event()

    assert store.insert_chain_event(event) is True
    assert store.insert_chain_event(event) is False
    store.save_book_snapshot(
        transaction_hash="0xabc",
        token_id="123",
        channel="chain",
        request_started_at_ms=1_004_100,
        request_finished_at_ms=1_004_250,
        book=sample_book(timestamp="123"),
        error=None,
    )
    store.upsert_data_api_match(
        transaction_hash="0xabc",
        token_id="123",
        side="BUY",
        observed_at_ms=1_157_000,
        source_timestamp=1_000,
        source_quantity=D("10"),
        source_vwap=D("0.3"),
        raw_rows=[trade_row(size="10", price="0.3")],
    )

    metrics = store.metrics()

    assert store.chain_event_count() == 1
    assert metrics["live_chain_delay_ms_median"] == 4_000
    assert metrics["live_data_api_delay_ms_median"] == 157_000
    assert metrics["live_api_minus_chain_ms_median"] == 153_000


def test_catchup_event_does_not_enter_live_delay_metrics(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.insert_chain_event(chain_event(catchup=True))
    store.upsert_data_api_match(
        transaction_hash="0xabc",
        token_id="123",
        side="BUY",
        observed_at_ms=1_157_000,
        source_timestamp=1_000,
        source_quantity=D("10"),
        source_vwap=D("0.3"),
        raw_rows=[trade_row(size="10", price="0.3")],
    )

    metrics = store.metrics()

    assert metrics["live_chain_event_count"] == 0
    assert metrics["live_ab_match_count"] == 0
    assert metrics["live_chain_delay_ms_median"] is None


class FakeRpc:
    def __init__(self, blocks: dict[int, dict], maker_logs=None, taker_logs=None):
        self.blocks = blocks
        self.head = max(blocks)
        self.maker_logs = maker_logs or {}
        self.taker_logs = taker_logs or {}

    def latest_block_number(self) -> int:
        return self.head

    def get_block(self, block_number: int) -> dict:
        return self.blocks[block_number]

    def source_logs(self, block_number: int, source_wallet: str, role: str) -> list[dict]:
        assert source_wallet == shadow.SOURCE_WALLET
        mapping = self.maker_logs if role == "maker" else self.taker_logs
        return list(mapping.get(block_number, []))


class FakeClob:
    def __init__(self):
        self.calls: list[str] = []

    def get_book(self, token_id: str) -> dict:
        self.calls.append(token_id)
        return sample_book(token_id)


def block(number: int, block_hash: str, parent_hash: str, timestamp: int) -> dict:
    return {
        "number": hex(number),
        "hash": block_hash,
        "parentHash": parent_hash,
        "timestamp": hex(timestamp),
    }


def test_first_start_sets_watermark_without_fake_historical_samples(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    rpc = FakeRpc({100: block(100, "0xh100", "0xh99", 1_000)})
    observer = shadow.ChainObserver(
        store,
        rpc,
        FakeClob(),
        clock_ms=lambda: 1_004_000,
    )

    observer.initialize_watermark()

    assert store.chain_event_count() == 0
    assert store.runtime_value("last_processed_block") == "100"
    assert store.processed_block(100)["block_hash"] == "0xh100"


def test_observer_saves_primary_and_counterparty_leg_but_one_immediate_book(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    rpc = FakeRpc({100: block(100, "0xh100", "0xh99", 1_000)})
    clob = FakeClob()
    observer = shadow.ChainObserver(store, rpc, clob, clock_ms=lambda: 1_004_000)
    observer.initialize_watermark()

    rpc.blocks[101] = block(101, "0xh101", "0xh100", 1_002)
    rpc.head = 101
    rpc.maker_logs[101] = [filled_log(block_number=101)]
    rpc.taker_logs[101] = [
        filled_log(
            block_number=101,
            log_index=8,
            maker="0x2222222222222222222222222222222222222222",
            taker=shadow.SOURCE_WALLET,
        )
    ]

    observer.run_chain_cycle()

    assert store.chain_event_count() == 2
    assert store.snapshot_count(channel="chain") == 1
    assert clob.calls == ["123"]
    assert store.runtime_value("last_processed_block") == "101"


def test_restart_backlog_is_marked_catchup(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.save_processed_block(
        number=100,
        block_hash="0xh100",
        parent_hash="0xh99",
        block_timestamp=1_000,
        catchup=False,
    )
    store.set_runtime("last_processed_block", "100")
    rpc = FakeRpc(
        {
            100: block(100, "0xh100", "0xh99", 1_000),
            101: block(101, "0xh101", "0xh100", 1_002),
        },
        maker_logs={101: [filled_log(block_number=101)]},
    )
    observer = shadow.ChainObserver(store, rpc, FakeClob(), clock_ms=lambda: 1_010_000)

    observer.run_chain_cycle()

    assert store.chain_events()[0]["catchup"] == 1
    assert store.metrics()["live_chain_event_count"] == 0


class FakeRangeRpc(FakeRpc):
    def __init__(self, blocks: dict[int, dict], maker_logs=None, taker_logs=None):
        super().__init__(blocks, maker_logs, taker_logs)
        self.range_calls: list[tuple[int, int, str]] = []

    def source_logs_range(
        self,
        from_block: int,
        to_block: int,
        source_wallet: str,
        role: str,
    ) -> list[dict]:
        assert source_wallet == shadow.SOURCE_WALLET
        self.range_calls.append((from_block, to_block, role))
        mapping = self.maker_logs if role == "maker" else self.taker_logs
        return [
            row
            for number, rows in mapping.items()
            if from_block <= number <= to_block
            for row in rows
        ]

    def source_logs(self, block_number: int, source_wallet: str, role: str):
        raise AssertionError("range-capable RPC must not scan one block at a time")


def test_large_restart_gap_uses_two_log_ranges_not_one_request_per_block(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.save_processed_block(100, "0xh100", "0xh99", 1_000, False)
    store.set_runtime("last_processed_block", "100")
    rpc = FakeRangeRpc(
        {
            100: block(100, "0xh100", "0xh99", 1_000),
            200: block(200, "0xh200", "0xh199", 1_200),
        },
        maker_logs={200: [filled_log(block_number=200)]},
    )
    observer = shadow.ChainObserver(store, rpc, FakeClob(), clock_ms=lambda: 1_210_000)

    observer.run_chain_cycle()

    assert rpc.range_calls == [(101, 200, "maker"), (101, 200, "taker")]
    assert store.runtime_value("last_processed_block") == "200"
    assert store.chain_event_count() == 1


def test_rpc_ranges_are_chunked_at_empirically_verified_provider_span(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.save_processed_block(100, "0xh100", "0xh99", 1_000, False)
    store.set_runtime("last_processed_block", "100")
    rpc = FakeRangeRpc(
        {
            100: block(100, "0xh100", "0xh99", 1_000),
            301: block(301, "0xh301", "0xh300", 1_402),
        },
        maker_logs={301: [filled_log(block_number=301)]},
    )
    observer = shadow.ChainObserver(store, rpc, FakeClob(), clock_ms=lambda: 1_410_000)

    observer.run_chain_cycle()

    assert rpc.range_calls == [
        (101, 200, "maker"),
        (201, 300, "maker"),
        (301, 301, "maker"),
        (101, 200, "taker"),
        (201, 300, "taker"),
        (301, 301, "taker"),
    ]


def test_reorg_rewinds_orphans_and_reprocesses_from_common_ancestor(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.save_processed_block(100, "0xh100", "0xh99", 1_000, False)
    store.save_processed_block(101, "0xold101", "0xh100", 1_002, False)
    store.set_runtime("last_processed_block", "101")
    store.insert_chain_event(
        chain_event(block_number=101, block_hash="0xold101")
    )
    rpc = FakeRpc(
        {
            100: block(100, "0xh100", "0xh99", 1_000),
            101: block(101, "0xnew101", "0xh100", 1_002),
        },
        maker_logs={
            101: [
                    filled_log(
                        tx_hash="0xnew",
                        block_number=101,
                        block_hash="0xnew101",
                        log_index=9,
                    )
            ]
        },
    )
    observer = shadow.ChainObserver(store, rpc, FakeClob(), clock_ms=lambda: 1_004_000)

    observer.run_chain_cycle()

    rows = store.chain_events(include_orphaned=True)
    assert any(row["transaction_hash"] == "0xabc" and row["orphaned"] == 1 for row in rows)
    assert any(row["transaction_hash"] == "0xnew" and row["orphaned"] == 0 for row in rows)
    assert store.runtime_value("last_processed_block") == "101"


class FakeDataApi:
    def __init__(self, rows: list[dict]):
        self.rows = rows

    def get_trades(self, source_wallet: str) -> list[dict]:
        assert source_wallet == shadow.SOURCE_WALLET
        return list(self.rows)


def test_data_api_fragments_match_once_and_capture_one_comparison_book(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.insert_chain_event(chain_event())
    rows = [
        trade_row(size="4", price="0.2"),
        trade_row(size="6", price="0.4"),
        trade_row(size="6", price="0.4"),
    ]
    clob = FakeClob()
    times = iter([1_157_000, 1_157_100, 1_200_000, 1_200_100])
    matcher = shadow.DataApiMatcher(
        store,
        FakeDataApi(rows),
        clob,
        clock_ms=lambda: next(times),
    )

    matcher.run_once()
    matcher.run_once()

    matches = store.data_api_matches()
    assert len(store.data_api_rows()) == 2
    assert len(matches) == 1
    assert matches[0]["source_quantity"] == "10"
    assert matches[0]["source_vwap"] == "0.32"
    assert matches[0]["data_api_seen_at_ms"] == 1_157_000
    assert store.snapshot_count(channel="data_api") == 1
    assert clob.calls == ["123"]


def test_data_api_worker_uses_one_batch_insert_not_one_transaction_per_row(
    tmp_path,
    monkeypatch,
):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.insert_chain_event(chain_event())
    rows = [
        trade_row(size="4", price="0.2"),
        trade_row(size="6", price="0.4"),
    ]

    def reject_row_at_a_time(*_args, **_kwargs):
        raise AssertionError("row-at-a-time SQLite writes are forbidden")

    monkeypatch.setattr(store, "insert_data_api_row", reject_row_at_a_time)
    matcher = shadow.DataApiMatcher(
        store,
        FakeDataApi(rows),
        FakeClob(),
        clock_ms=lambda: 1_157_000,
    )

    matcher.run_once()

    assert len(store.data_api_rows()) == 2


def test_metadata_is_copied_from_old_db_read_only_and_unknown_stays_pending(tmp_path):
    old_db = tmp_path / "old.sqlite3"
    with sqlite3.connect(old_db) as connection:
        connection.execute(
            """
            CREATE TABLE source_state(
                asset TEXT PRIMARY KEY,
                condition_id TEXT,
                title TEXT,
                event_slug TEXT,
                slug TEXT,
                outcome TEXT,
                end_date TEXT,
                anchor_size TEXT,
                last_size TEXT,
                target_size TEXT,
                observed_at INTEGER
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_state VALUES(
                '123', '0xcondition', 'Will Trump say "AI" during his address?',
                'event', 'slug', 'Yes', '2026-07-26T23:59:00Z',
                '0', '0', '0', 1000
            )
            """
        )

    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    copied = store.load_metadata_from_old_paper(old_db)

    assert copied == 1
    assert store.scope_for_token("123") == "IN_SCOPE"
    assert store.scope_for_token("999") == "PENDING"


def test_status_files_show_safety_and_real_timestamps(tmp_path):
    store = shadow.ShadowStore(tmp_path / "shadow.sqlite3")
    store.initialize()
    store.insert_chain_event(chain_event())
    store.set_runtime("heartbeat_at_ms", "1005000")
    store.set_runtime("status", "running")

    shadow.render_status_files(store, tmp_path)

    status = json.loads((tmp_path / "status.json").read_text())
    html = (tmp_path / "status.html").read_text()
    assert status["paper_only"] is True
    assert status["real_order_submitted"] is False
    assert status["old_paper_database_mode"] == "read_only"
    assert status["recent_events"][0]["chain_seen_at_ms"] == 1_004_000
    assert "只读链上影子" in html
    assert "real_order_submitted=false" in html
