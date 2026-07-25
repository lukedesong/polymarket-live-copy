#!/usr/bin/env python3
"""Read-only Polygon shadow observer for Tian-Wen public Polymarket fills.

This process has no signing, order-submission, cancellation, or paper-ledger
write path.  It observes public chain logs and public books into an independent
SQLite database so chain discovery can be compared with the public Data API.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import signal
import sqlite3
import statistics
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


D = Decimal
ZERO = D("0")

# External constraint: Polygon PoS mainnet chain ID.
CHAIN_ID = 137
# Empirical identifier: public proxy wallet already confirmed for @tian-wen.
SOURCE_WALLET = "0x66c1a6fe836ff555ca32848646acedbbe93bfa3f"
# External constraints: official CTF Exchange V2 Polygon deployment addresses.
V2_EXCHANGE_ADDRESSES = (
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)
# External constraint: official V2 Events.sol event signature.
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3"
    "b9b1a28262980ba90b1a89f2ea84d8ee"
)
# External constraint: Polymarket collateral and CTF quantities use six decimals.
TOKEN_SCALE = D("1000000")

MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_RUNTIME_DIR = MODULE_DIR / "tian_wen_chain_shadow_runtime"
DEFAULT_OLD_PAPER_DB = MODULE_DIR / "tian_wen_speech_runtime" / "paper.sqlite3"

# Empirically reachable public read-only Polygon endpoints on 2026-07-25.
# They carry no credentials and have no guaranteed service-level agreement.
DEFAULT_RPC_URLS = (
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.drpc.org",
)
# Estimate: research-only polling idle time. It changes measurement granularity
# but cannot submit an order; the actual cycle duration is recorded.
DEFAULT_CHAIN_IDLE_SECONDS = 0.5
DEFAULT_DATA_API_IDLE_SECONDS = 1.0
# Estimate: failure containment for public network calls, not an order lifetime.
HTTP_TIMEOUT_SECONDS = 10
MAX_BACKOFF_SECONDS = 30
# External public endpoint page-size constraint used by the existing tracker.
DATA_API_PAGE_SIZE = 1000
# Empirical public-RPC constraint: both registered endpoints accepted a
# 100-block indexed log query and rejected 200 blocks on 2026-07-25.
MAX_LOG_RANGE_BLOCKS = 100
# Estimate: SQLite contention timeout for two local read/write workers.
SQLITE_TIMEOUT_SECONDS = 30

RPC_READ_METHODS = frozenset(
    {
        "eth_chainId",
        "eth_blockNumber",
        "eth_getBlockByNumber",
        "eth_getLogs",
        "eth_getTransactionReceipt",
    }
)
RPC_ALLOWED_HOSTS = frozenset(urlparse(url).hostname for url in DEFAULT_RPC_URLS)


class ReadOnlyViolation(RuntimeError):
    """Raised if a caller attempts a non-public or state-changing operation."""


class RpcError(RuntimeError):
    """Raised when every configured public RPC endpoint fails."""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def normalize_address(value: str) -> str:
    raw = str(value or "").lower()
    if not raw.startswith("0x") or len(raw) != 42:
        raise ValueError(f"invalid address: {value}")
    int(raw[2:], 16)
    return raw


def address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + normalize_address(address)[2:]


def topic_address(topic: str) -> str:
    raw = str(topic or "").lower().removeprefix("0x")
    if len(raw) != 64:
        raise ValueError("address topic must contain 32 bytes")
    int(raw, 16)
    return "0x" + raw[-40:]


def hex_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return int(str(value), 16)


def split_words(data: str) -> list[str]:
    raw = str(data or "").removeprefix("0x")
    if len(raw) % 64:
        raise ValueError("ABI data is not aligned to 32-byte words")
    return ["0x" + raw[index : index + 64] for index in range(0, len(raw), 64)]


def decode_order_filled(
    log: dict[str, Any],
    source_wallet: str = SOURCE_WALLET,
) -> dict[str, Any] | None:
    """Decode one official V2 OrderFilled log without guessing counterparty side."""
    topics = [str(value).lower() for value in log.get("topics", [])]
    if len(topics) != 4 or topics[0] != ORDER_FILLED_TOPIC:
        return None
    maker = topic_address(topics[2])
    taker = topic_address(topics[3])
    source = normalize_address(source_wallet)
    if source not in {maker, taker}:
        return None

    words = split_words(str(log.get("data", "")))
    if len(words) != 7:
        raise ValueError(f"OrderFilled expected 7 data words, got {len(words)}")
    side_code = hex_int(words[0])
    if side_code not in {0, 1}:
        raise ValueError(f"unsupported Side enum: {side_code}")
    token_id = hex_int(words[1])
    maker_amount_raw = hex_int(words[2])
    taker_amount_raw = hex_int(words[3])
    fee_raw = hex_int(words[4])
    source_role = "maker" if maker == source else "taker"
    source_order = source_role == "maker"

    side = "UNKNOWN"
    quantity: Decimal | None = None
    notional: Decimal | None = None
    price: Decimal | None = None
    if source_order:
        side = "BUY" if side_code == 0 else "SELL"
        if side == "BUY":
            notional = D(maker_amount_raw) / TOKEN_SCALE
            quantity = D(taker_amount_raw) / TOKEN_SCALE
        else:
            quantity = D(maker_amount_raw) / TOKEN_SCALE
            notional = D(taker_amount_raw) / TOKEN_SCALE
        if quantity > ZERO:
            price = notional / quantity

    return {
        "chain_id": CHAIN_ID,
        "exchange_address": normalize_address(str(log.get("address", ""))),
        "block_number": hex_int(log.get("blockNumber", "0x0")),
        "block_hash": str(log.get("blockHash", "")).lower(),
        "transaction_hash": str(log.get("transactionHash", "")).lower(),
        "log_index": hex_int(log.get("logIndex", "0x0")),
        "order_hash": topics[1],
        "maker": maker,
        "taker": taker,
        "source_role": source_role,
        "source_order": source_order,
        "side_code": side_code,
        "side": side,
        "token_id": str(token_id),
        "maker_amount_raw": str(maker_amount_raw),
        "taker_amount_raw": str(taker_amount_raw),
        "fee_raw": str(fee_raw),
        "quantity": quantity,
        "notional": notional,
        "price": price,
        "builder": words[5].lower(),
        "metadata": words[6].lower(),
        "raw_log": log,
    }


def validate_rpc_request(url: str, method: str) -> None:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in RPC_ALLOWED_HOSTS
        or parsed.username
        or parsed.password
        or method not in RPC_READ_METHODS
    ):
        raise ReadOnlyViolation(f"RPC request rejected: {method} {url}")


def validate_public_get(url: str) -> None:
    parsed = urlparse(url)
    allowed = (
        parsed.scheme == "https"
        and (
            (
                parsed.hostname == "data-api.polymarket.com"
                and parsed.path == "/trades"
            )
            or (
                parsed.hostname == "clob.polymarket.com"
                and parsed.path == "/book"
            )
        )
    )
    if not allowed:
        raise ReadOnlyViolation(f"public GET rejected: {url}")


def open_old_paper_read_only(path: Path) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    connection = sqlite3.connect(
        f"file:{resolved}?mode=ro",
        uri=True,
        timeout=SQLITE_TIMEOUT_SECONDS,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def is_speech_word_title(title: str) -> bool:
    lowered = str(title or "").strip().lower()
    if not lowered or not re.search(r"\b(?:donald\s+)?trump\b", lowered):
        return False
    if re.search(
        r"\b(?:truth social|tweet|post(?:ed|s|ing)?|attend(?:s|ed|ing)?|"
        r"meet(?:s|ing)?|visit(?:s|ed|ing)?|speak(?:s|ing)?\s+to|"
        r"talk(?:s|ing)?\s+to)\b",
        lowered,
    ):
        return False
    return bool(
        re.search(
            r"\b(?:say|says|said|mention|mentions|mentioned|utter|utters|"
            r"name|names|use\s+the\s+(?:word|phrase))\b",
            lowered,
        )
    )


def data_row_id(row: dict[str, Any]) -> str:
    fields = (
        "proxyWallet",
        "transactionHash",
        "timestamp",
        "side",
        "asset",
        "conditionId",
        "size",
        "price",
        "outcome",
        "slug",
        "eventSlug",
        "title",
    )
    canonical = [str(row.get(field, "")) for field in fields]
    return hashlib.sha256(json_text(canonical).encode("utf-8")).hexdigest()


class RpcClient:
    """Minimal allow-listed JSON-RPC client with endpoint failover."""

    def __init__(
        self,
        urls: Iterable[str] = DEFAULT_RPC_URLS,
        *,
        timeout: int = HTTP_TIMEOUT_SECONDS,
    ):
        self.urls = tuple(urls)
        if not self.urls:
            raise ValueError("at least one RPC URL is required")
        self.timeout = timeout
        self.last_url: str | None = None
        self.last_latency_ms: int | None = None

    def call(self, method: str, params: list[Any]) -> Any:
        errors: list[str] = []
        for url in self.urls:
            validate_rpc_request(url, method)
            payload = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                }
            ).encode("utf-8")
            request = Request(
                url,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "tian-wen-chain-shadow/1.0",
                },
                method="POST",
            )
            started = now_ms()
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    body = json.load(response)
                if body.get("error"):
                    raise RpcError(json_text(body["error"]))
                self.last_url = url
                self.last_latency_ms = now_ms() - started
                return body.get("result")
            except Exception as exc:
                errors.append(f"{url}: {type(exc).__name__}: {exc}")
        raise RpcError("; ".join(errors))

    def chain_id(self) -> int:
        return hex_int(self.call("eth_chainId", []))

    def latest_block_number(self) -> int:
        return hex_int(self.call("eth_blockNumber", []))

    def get_block(self, block_number: int) -> dict[str, Any]:
        result = self.call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(result, dict):
            raise RpcError(f"block {block_number} not found")
        return result

    def get_receipt(self, transaction_hash: str) -> dict[str, Any]:
        result = self.call("eth_getTransactionReceipt", [transaction_hash])
        if not isinstance(result, dict):
            raise RpcError(f"receipt {transaction_hash} not found")
        return result

    def source_logs(
        self,
        block_number: int,
        source_wallet: str,
        role: str,
    ) -> list[dict[str, Any]]:
        return self.source_logs_range(
            block_number,
            block_number,
            source_wallet,
            role,
        )

    def source_logs_range(
        self,
        from_block: int,
        to_block: int,
        source_wallet: str,
        role: str,
    ) -> list[dict[str, Any]]:
        if role not in {"maker", "taker"}:
            raise ValueError(f"invalid indexed role: {role}")
        if from_block > to_block:
            return []
        topics: list[str | None] = [ORDER_FILLED_TOPIC, None]
        if role == "maker":
            topics.append(address_topic(source_wallet))
        else:
            topics.extend([None, address_topic(source_wallet)])
        result = self.call(
            "eth_getLogs",
            [
                {
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "address": list(V2_EXCHANGE_ADDRESSES),
                    "topics": topics,
                }
            ],
        )
        if not isinstance(result, list):
            raise RpcError("eth_getLogs returned a non-list result")
        return [row for row in result if isinstance(row, dict)]


class PublicPolymarketClient:
    """Read-only public CLOB/Data API client."""

    def __init__(self, *, timeout: int = HTTP_TIMEOUT_SECONDS):
        self.timeout = timeout

    def get_json(self, url: str) -> Any:
        validate_public_get(url)
        request = Request(
            url,
            headers={"User-Agent": "tian-wen-chain-shadow/1.0"},
            method="GET",
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def get_book(self, token_id: str) -> dict[str, Any]:
        url = "https://clob.polymarket.com/book?" + urlencode(
            {"token_id": str(token_id)}
        )
        result = self.get_json(url)
        if not isinstance(result, dict):
            raise ValueError("CLOB book response is not an object")
        return result

    def get_trades(self, source_wallet: str) -> list[dict[str, Any]]:
        url = "https://data-api.polymarket.com/trades?" + urlencode(
            {
                "user": normalize_address(source_wallet),
                "takerOnly": "false",
                "limit": DATA_API_PAGE_SIZE,
                "offset": 0,
            }
        )
        result = self.get_json(url)
        if not isinstance(result, list):
            raise ValueError("Data API trades response is not a list")
        return [row for row in result if isinstance(row, dict)]


class ShadowStore:
    """Independent SQLite evidence store; never points at the paper ledger."""

    def __init__(self, path: Path):
        self.path = Path(path).resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=SQLITE_TIMEOUT_SECONDS,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(
            f"PRAGMA busy_timeout = {SQLITE_TIMEOUT_SECONDS * 1000}"
        )
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS processed_blocks(
                    number INTEGER PRIMARY KEY,
                    block_hash TEXT NOT NULL,
                    parent_hash TEXT NOT NULL,
                    block_timestamp INTEGER NOT NULL,
                    catchup INTEGER NOT NULL,
                    processed_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS chain_events(
                    chain_id INTEGER NOT NULL,
                    exchange_address TEXT NOT NULL,
                    block_number INTEGER NOT NULL,
                    block_hash TEXT NOT NULL,
                    block_timestamp INTEGER NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    order_hash TEXT NOT NULL,
                    maker TEXT NOT NULL,
                    taker TEXT NOT NULL,
                    source_role TEXT NOT NULL,
                    source_order INTEGER NOT NULL,
                    side_code INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    maker_amount_raw TEXT NOT NULL,
                    taker_amount_raw TEXT NOT NULL,
                    fee_raw TEXT NOT NULL,
                    quantity TEXT,
                    notional TEXT,
                    price TEXT,
                    chain_seen_at_ms INTEGER NOT NULL,
                    catchup INTEGER NOT NULL,
                    orphaned INTEGER NOT NULL DEFAULT 0,
                    raw_log_json TEXT NOT NULL,
                    PRIMARY KEY(chain_id, transaction_hash, log_index)
                );
                CREATE INDEX IF NOT EXISTS idx_chain_match
                    ON chain_events(transaction_hash, token_id, side);
                CREATE INDEX IF NOT EXISTS idx_chain_block
                    ON chain_events(block_number, orphaned);

                CREATE TABLE IF NOT EXISTS book_snapshots(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_hash TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    request_started_at_ms INTEGER NOT NULL,
                    request_finished_at_ms INTEGER NOT NULL,
                    book_timestamp TEXT,
                    book_hash TEXT,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    raw_book_json TEXT NOT NULL,
                    error TEXT,
                    UNIQUE(transaction_hash, token_id, channel)
                );

                CREATE TABLE IF NOT EXISTS data_api_rows(
                    row_id TEXT PRIMARY KEY,
                    transaction_hash TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    source_timestamp INTEGER NOT NULL,
                    size TEXT NOT NULL,
                    price TEXT NOT NULL,
                    first_seen_at_ms INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS data_api_matches(
                    transaction_hash TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    data_api_seen_at_ms INTEGER NOT NULL,
                    source_timestamp INTEGER NOT NULL,
                    source_quantity TEXT NOT NULL,
                    source_vwap TEXT NOT NULL,
                    raw_rows_json TEXT NOT NULL,
                    PRIMARY KEY(transaction_hash, token_id, side)
                );

                CREATE TABLE IF NOT EXISTS token_metadata(
                    token_id TEXT PRIMARY KEY,
                    condition_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    event_slug TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    scope_status TEXT NOT NULL,
                    source TEXT NOT NULL,
                    observed_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runtime_state(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
        self.set_runtime("paper_only", "true")
        self.set_runtime("real_order_submitted", "false")
        self.set_runtime("old_paper_database_mode", "read_only")
        if self.runtime_value("created_at_ms") is None:
            self.set_runtime("created_at_ms", str(now_ms()))

    def set_runtime(self, key: str, value: Any) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runtime_state(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(key), str(value)),
            )

    def runtime_value(self, key: str) -> str | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM runtime_state WHERE key = ?",
                (key,),
            ).fetchone()
        return None if row is None else str(row["value"])

    def runtime_values(self) -> dict[str, str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT key, value FROM runtime_state ORDER BY key"
            ).fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def save_processed_block(
        self,
        number: int,
        block_hash: str,
        parent_hash: str,
        block_timestamp: int,
        catchup: bool,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO processed_blocks(
                    number, block_hash, parent_hash, block_timestamp,
                    catchup, processed_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(number) DO UPDATE SET
                    block_hash = excluded.block_hash,
                    parent_hash = excluded.parent_hash,
                    block_timestamp = excluded.block_timestamp,
                    catchup = excluded.catchup,
                    processed_at_ms = excluded.processed_at_ms
                """,
                (
                    int(number),
                    str(block_hash).lower(),
                    str(parent_hash).lower(),
                    int(block_timestamp),
                    int(bool(catchup)),
                    now_ms(),
                ),
            )

    def processed_block(self, number: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM processed_blocks WHERE number = ?",
                (int(number),),
            ).fetchone()
        return None if row is None else dict(row)

    def processed_block_numbers_desc(self) -> list[int]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT number FROM processed_blocks ORDER BY number DESC"
            ).fetchall()
        return [int(row["number"]) for row in rows]

    def insert_chain_event(self, event: dict[str, Any]) -> bool:
        key = (
            int(event["chain_id"]),
            str(event["transaction_hash"]).lower(),
            int(event["log_index"]),
        )
        with self.connect() as connection:
            existing = connection.execute(
                """
                SELECT block_hash, orphaned FROM chain_events
                WHERE chain_id = ? AND transaction_hash = ? AND log_index = ?
                """,
                key,
            ).fetchone()
            if (
                existing is not None
                and str(existing["block_hash"]).lower()
                == str(event["block_hash"]).lower()
                and int(existing["orphaned"]) == 0
            ):
                return False
            values = (
                int(event["chain_id"]),
                str(event["exchange_address"]).lower(),
                int(event["block_number"]),
                str(event["block_hash"]).lower(),
                int(event["block_timestamp"]),
                str(event["transaction_hash"]).lower(),
                int(event["log_index"]),
                str(event["order_hash"]).lower(),
                str(event["maker"]).lower(),
                str(event["taker"]).lower(),
                str(event["source_role"]),
                int(bool(event["source_order"])),
                int(event["side_code"]),
                str(event["side"]),
                str(event["token_id"]),
                str(event["maker_amount_raw"]),
                str(event["taker_amount_raw"]),
                str(event["fee_raw"]),
                decimal_text(event.get("quantity")),
                decimal_text(event.get("notional")),
                decimal_text(event.get("price")),
                int(event["chain_seen_at_ms"]),
                int(bool(event["catchup"])),
                int(bool(event.get("orphaned", False))),
                json_text(event.get("raw_log", {})),
            )
            connection.execute(
                """
                INSERT INTO chain_events(
                    chain_id, exchange_address, block_number, block_hash,
                    block_timestamp, transaction_hash, log_index, order_hash,
                    maker, taker, source_role, source_order, side_code, side,
                    token_id, maker_amount_raw, taker_amount_raw, fee_raw,
                    quantity, notional, price, chain_seen_at_ms, catchup,
                    orphaned, raw_log_json
                ) VALUES(
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(chain_id, transaction_hash, log_index) DO UPDATE SET
                    exchange_address = excluded.exchange_address,
                    block_number = excluded.block_number,
                    block_hash = excluded.block_hash,
                    block_timestamp = excluded.block_timestamp,
                    order_hash = excluded.order_hash,
                    maker = excluded.maker,
                    taker = excluded.taker,
                    source_role = excluded.source_role,
                    source_order = excluded.source_order,
                    side_code = excluded.side_code,
                    side = excluded.side,
                    token_id = excluded.token_id,
                    maker_amount_raw = excluded.maker_amount_raw,
                    taker_amount_raw = excluded.taker_amount_raw,
                    fee_raw = excluded.fee_raw,
                    quantity = excluded.quantity,
                    notional = excluded.notional,
                    price = excluded.price,
                    chain_seen_at_ms = excluded.chain_seen_at_ms,
                    catchup = excluded.catchup,
                    orphaned = excluded.orphaned,
                    raw_log_json = excluded.raw_log_json
                """,
                values,
            )
        return True

    def chain_event_count(self) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM chain_events WHERE orphaned = 0"
            ).fetchone()
        return int(row["count"])

    def chain_events(self, *, include_orphaned: bool = False) -> list[dict[str, Any]]:
        where = "" if include_orphaned else "WHERE orphaned = 0"
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM chain_events
                {where}
                ORDER BY block_number, log_index
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def has_primary_event(
        self,
        transaction_hash: str,
        token_id: str,
        side: str,
    ) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM chain_events
                WHERE transaction_hash = ? AND token_id = ? AND side = ?
                  AND source_order = 1 AND orphaned = 0
                LIMIT 1
                """,
                (transaction_hash.lower(), str(token_id), side.upper()),
            ).fetchone()
        return row is not None

    def has_snapshot(
        self,
        transaction_hash: str,
        token_id: str,
        channel: str,
    ) -> bool:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM book_snapshots
                WHERE transaction_hash = ? AND token_id = ? AND channel = ?
                """,
                (transaction_hash.lower(), str(token_id), channel),
            ).fetchone()
        return row is not None

    def save_book_snapshot(
        self,
        *,
        transaction_hash: str,
        token_id: str,
        channel: str,
        request_started_at_ms: int,
        request_finished_at_ms: int,
        book: dict[str, Any],
        error: str | None,
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO book_snapshots(
                    transaction_hash, token_id, channel,
                    request_started_at_ms, request_finished_at_ms,
                    book_timestamp, book_hash, bids_json, asks_json,
                    raw_book_json, error
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_hash.lower(),
                    str(token_id),
                    str(channel),
                    int(request_started_at_ms),
                    int(request_finished_at_ms),
                    None if not book else str(book.get("timestamp", "")),
                    None if not book else str(book.get("hash", "")),
                    json_text(book.get("bids", [])),
                    json_text(book.get("asks", [])),
                    json_text(book),
                    error,
                ),
            )
        return cursor.rowcount == 1

    def snapshot_count(self, *, channel: str) -> int:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM book_snapshots WHERE channel = ?",
                (channel,),
            ).fetchone()
        return int(row["count"])

    def insert_data_api_row(
        self,
        row: dict[str, Any],
        observed_at_ms: int,
    ) -> bool:
        return self.insert_data_api_rows([row], observed_at_ms) == 1

    def insert_data_api_rows(
        self,
        rows: Iterable[dict[str, Any]],
        observed_at_ms: int,
    ) -> int:
        values = [
            (
                data_row_id(row),
                str(row.get("transactionHash", "")).lower(),
                str(row.get("asset", "")),
                str(row.get("side", "")).upper(),
                int(row.get("timestamp", 0)),
                str(row.get("size", "0")),
                str(row.get("price", "0")),
                int(observed_at_ms),
                json_text(row),
            )
            for row in rows
        ]
        if not values:
            return 0
        with self.connect() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO data_api_rows(
                    row_id, transaction_hash, token_id, side, source_timestamp,
                    size, price, first_seen_at_ms, raw_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            inserted = connection.total_changes - before
        return int(inserted)

    def data_api_rows(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM data_api_rows ORDER BY row_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_data_api_match(
        self,
        *,
        transaction_hash: str,
        token_id: str,
        side: str,
        observed_at_ms: int,
        source_timestamp: int,
        source_quantity: Decimal,
        source_vwap: Decimal,
        raw_rows: list[dict[str, Any]],
    ) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO data_api_matches(
                    transaction_hash, token_id, side, data_api_seen_at_ms,
                    source_timestamp, source_quantity, source_vwap,
                    raw_rows_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_hash.lower(),
                    str(token_id),
                    side.upper(),
                    int(observed_at_ms),
                    int(source_timestamp),
                    decimal_text(source_quantity),
                    decimal_text(source_vwap),
                    json_text(raw_rows),
                ),
            )
        return cursor.rowcount == 1

    def data_api_matches(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM data_api_matches
                ORDER BY data_api_seen_at_ms, transaction_hash
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def upsert_token_metadata(
        self,
        *,
        token_id: str,
        condition_id: str,
        title: str,
        event_slug: str,
        slug: str,
        outcome: str,
        end_date: str,
        source: str,
        observed_at_ms: int,
    ) -> None:
        scope_status = "IN_SCOPE" if is_speech_word_title(title) else "OUT_OF_SCOPE"
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO token_metadata(
                    token_id, condition_id, title, event_slug, slug, outcome,
                    end_date, scope_status, source, observed_at_ms
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(token_id) DO UPDATE SET
                    condition_id = excluded.condition_id,
                    title = excluded.title,
                    event_slug = excluded.event_slug,
                    slug = excluded.slug,
                    outcome = excluded.outcome,
                    end_date = excluded.end_date,
                    scope_status = excluded.scope_status,
                    source = excluded.source,
                    observed_at_ms = excluded.observed_at_ms
                """,
                (
                    str(token_id),
                    str(condition_id or ""),
                    str(title or ""),
                    str(event_slug or ""),
                    str(slug or ""),
                    str(outcome or ""),
                    str(end_date or ""),
                    scope_status,
                    str(source),
                    int(observed_at_ms),
                ),
            )

    def load_metadata_from_old_paper(self, old_path: Path) -> int:
        if not Path(old_path).exists():
            return 0
        connection = open_old_paper_read_only(old_path)
        try:
            rows = connection.execute(
                """
                SELECT asset, condition_id, title, event_slug, slug,
                       outcome, end_date, observed_at
                FROM source_state
                """
            ).fetchall()
        finally:
            connection.close()
        for row in rows:
            self.upsert_token_metadata(
                token_id=str(row["asset"]),
                condition_id=str(row["condition_id"]),
                title=str(row["title"]),
                event_slug=str(row["event_slug"]),
                slug=str(row["slug"]),
                outcome=str(row["outcome"]),
                end_date=str(row["end_date"]),
                source="old_paper_read_only",
                observed_at_ms=int(row["observed_at"]) * 1000,
            )
        return len(rows)

    def enrich_metadata_from_trade(
        self,
        row: dict[str, Any],
        observed_at_ms: int,
    ) -> None:
        self.upsert_token_metadata(
            token_id=str(row.get("asset", "")),
            condition_id=str(row.get("conditionId", "")),
            title=str(row.get("title", "")),
            event_slug=str(row.get("eventSlug", "")),
            slug=str(row.get("slug", "")),
            outcome=str(row.get("outcome", "")),
            end_date=str(row.get("endDate", "")),
            source="data_api_match",
            observed_at_ms=observed_at_ms,
        )

    def scope_for_token(self, token_id: str) -> str:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT scope_status FROM token_metadata WHERE token_id = ?",
                (str(token_id),),
            ).fetchone()
        return "PENDING" if row is None else str(row["scope_status"])

    def mark_orphaned_after(self, common_block: int) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE chain_events
                SET orphaned = 1
                WHERE block_number > ? AND orphaned = 0
                """,
                (int(common_block),),
            )
            connection.execute(
                "DELETE FROM processed_blocks WHERE number > ?",
                (int(common_block),),
            )
        self.set_runtime("last_processed_block", str(common_block))

    @staticmethod
    def _median(values: list[int]) -> int | float | None:
        if not values:
            return None
        value = statistics.median(values)
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

    def metrics(self) -> dict[str, Any]:
        with self.connect() as connection:
            live_chain = connection.execute(
                """
                SELECT block_timestamp, chain_seen_at_ms
                FROM chain_events
                WHERE source_order = 1 AND catchup = 0 AND orphaned = 0
                """
            ).fetchall()
            live_ab = connection.execute(
                """
                SELECT ce.block_timestamp, ce.chain_seen_at_ms,
                       dm.data_api_seen_at_ms
                FROM chain_events AS ce
                JOIN data_api_matches AS dm
                  ON dm.transaction_hash = ce.transaction_hash
                 AND dm.token_id = ce.token_id
                 AND dm.side = ce.side
                WHERE ce.source_order = 1
                  AND ce.catchup = 0
                  AND ce.orphaned = 0
                """
            ).fetchall()
            catchup_count = connection.execute(
                """
                SELECT COUNT(*) AS count FROM chain_events
                WHERE source_order = 1 AND catchup = 1 AND orphaned = 0
                """
            ).fetchone()
        chain_delays = [
            int(row["chain_seen_at_ms"]) - int(row["block_timestamp"]) * 1000
            for row in live_chain
        ]
        api_delays = [
            int(row["data_api_seen_at_ms"]) - int(row["block_timestamp"]) * 1000
            for row in live_ab
        ]
        differences = [
            int(row["data_api_seen_at_ms"]) - int(row["chain_seen_at_ms"])
            for row in live_ab
        ]
        return {
            "live_chain_event_count": len(live_chain),
            "catchup_chain_event_count": int(catchup_count["count"]),
            "live_ab_match_count": len(live_ab),
            "live_chain_delay_ms_median": self._median(chain_delays),
            "live_data_api_delay_ms_median": self._median(api_delays),
            "live_api_minus_chain_ms_median": self._median(differences),
        }

    def recent_events(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT
                    ce.transaction_hash, ce.log_index, ce.block_number,
                    ce.block_timestamp, ce.chain_seen_at_ms, ce.source_role,
                    ce.source_order, ce.side, ce.token_id, ce.quantity,
                    ce.price, ce.catchup, ce.orphaned,
                    tm.title, tm.scope_status,
                    dm.data_api_seen_at_ms, dm.source_quantity,
                    dm.source_vwap,
                    cb.id AS chain_book_id,
                    db.id AS data_api_book_id
                FROM chain_events AS ce
                LEFT JOIN token_metadata AS tm ON tm.token_id = ce.token_id
                LEFT JOIN data_api_matches AS dm
                  ON dm.transaction_hash = ce.transaction_hash
                 AND dm.token_id = ce.token_id
                 AND dm.side = ce.side
                LEFT JOIN book_snapshots AS cb
                  ON cb.transaction_hash = ce.transaction_hash
                 AND cb.token_id = ce.token_id
                 AND cb.channel = 'chain'
                LEFT JOIN book_snapshots AS db
                  ON db.transaction_hash = ce.transaction_hash
                 AND db.token_id = ce.token_id
                 AND db.channel = 'data_api'
                ORDER BY ce.block_number DESC, ce.log_index DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["scope_status"] = item.get("scope_status") or "PENDING"
            result.append(item)
        return result

    def integrity_check(self) -> str:
        with self.connect() as connection:
            row = connection.execute("PRAGMA integrity_check").fetchone()
        return str(row[0])


class ChainObserver:
    """Sequential block observer with restart catch-up and reorg repair."""

    def __init__(
        self,
        store: ShadowStore,
        rpc: Any,
        clob: Any,
        *,
        clock_ms: Callable[[], int] = now_ms,
        source_wallet: str = SOURCE_WALLET,
    ):
        self.store = store
        self.rpc = rpc
        self.clob = clob
        self.clock_ms = clock_ms
        self.source_wallet = normalize_address(source_wallet)
        self.had_persisted_watermark = (
            self.store.runtime_value("last_processed_block") is not None
        )
        self.boot_head = int(self.rpc.latest_block_number())
        self.reorg_replay = False

    @staticmethod
    def _header_values(header: dict[str, Any]) -> tuple[int, str, str, int]:
        return (
            hex_int(header["number"]),
            str(header["hash"]).lower(),
            str(header["parentHash"]).lower(),
            hex_int(header["timestamp"]),
        )

    def initialize_watermark(self) -> None:
        if self.store.runtime_value("last_processed_block") is not None:
            return
        header = self.rpc.get_block(self.boot_head)
        number, block_hash, parent_hash, timestamp = self._header_values(header)
        self.store.save_processed_block(
            number,
            block_hash,
            parent_hash,
            timestamp,
            False,
        )
        self.store.set_runtime("last_processed_block", str(number))
        self.store.set_runtime("current_head", str(number))
        self.store.set_runtime("status", "initialized_at_current_head")
        self.store.set_runtime("heartbeat_at_ms", str(self.clock_ms()))

    def _snapshot_book(self, event: dict[str, Any], channel: str) -> None:
        transaction_hash = str(event["transaction_hash"]).lower()
        token_id = str(event["token_id"])
        if self.store.has_snapshot(transaction_hash, token_id, channel):
            return
        started = self.clock_ms()
        book: dict[str, Any] = {}
        error: str | None = None
        try:
            book = self.clob.get_book(token_id)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        finished = self.clock_ms()
        self.store.save_book_snapshot(
            transaction_hash=transaction_hash,
            token_id=token_id,
            channel=channel,
            request_started_at_ms=started,
            request_finished_at_ms=finished,
            book=book,
            error=error,
        )

    def _ingest_logs(
        self,
        logs: Iterable[dict[str, Any]],
        *,
        block_number: int,
        block_hash: str,
        block_timestamp: int,
        catchup: bool,
    ) -> None:
        for log in logs:
            observed_at = self.clock_ms()
            event = decode_order_filled(log, self.source_wallet)
            if event is None:
                continue
            event["block_number"] = block_number
            event["block_hash"] = block_hash
            event["block_timestamp"] = block_timestamp
            event["chain_seen_at_ms"] = observed_at
            event["catchup"] = bool(catchup)
            event["orphaned"] = False
            inserted = self.store.insert_chain_event(event)
            if inserted:
                self._snapshot_book(event, "chain")

    def _source_logs_between(
        self,
        from_block: int,
        to_block: int,
        role: str,
    ) -> list[dict[str, Any]]:
        if hasattr(self.rpc, "source_logs_range"):
            result: list[dict[str, Any]] = []
            for chunk_start in range(
                from_block,
                to_block + 1,
                MAX_LOG_RANGE_BLOCKS,
            ):
                chunk_end = min(
                    chunk_start + MAX_LOG_RANGE_BLOCKS - 1,
                    to_block,
                )
                result.extend(
                    self.rpc.source_logs_range(
                        chunk_start,
                        chunk_end,
                        self.source_wallet,
                        role,
                    )
                )
            return result
        result: list[dict[str, Any]] = []
        for number in range(from_block, to_block + 1):
            result.extend(
                self.rpc.source_logs(
                    number,
                    self.source_wallet,
                    role,
                )
            )
        return result

    def _ingest_log_batch(
        self,
        logs: Iterable[dict[str, Any]],
        *,
        header_cache: dict[int, dict[str, Any]],
    ) -> None:
        by_block: dict[int, list[dict[str, Any]]] = {}
        for log in logs:
            block_number = hex_int(log.get("blockNumber", "0x0"))
            by_block.setdefault(block_number, []).append(log)
        for block_number in sorted(by_block):
            header = header_cache.get(block_number)
            if header is None:
                header = self.rpc.get_block(block_number)
                header_cache[block_number] = header
            (
                actual_number,
                block_hash,
                parent_hash,
                block_timestamp,
            ) = self._header_values(header)
            if actual_number != block_number:
                raise RpcError(
                    f"requested block {block_number}, received {actual_number}"
                )
            for log in by_block[block_number]:
                log_hash = str(log.get("blockHash", "")).lower()
                if log_hash and log_hash != block_hash:
                    raise RpcError(
                        f"log/header hash mismatch at block {block_number}"
                    )
            catchup = bool(
                (self.had_persisted_watermark and block_number <= self.boot_head)
                or self.reorg_replay
            )
            self._ingest_logs(
                by_block[block_number],
                block_number=block_number,
                block_hash=block_hash,
                block_timestamp=block_timestamp,
                catchup=catchup,
            )
            self.store.save_processed_block(
                block_number,
                block_hash,
                parent_hash,
                block_timestamp,
                catchup,
            )

    def _find_common_ancestor(self, last_processed: int) -> int:
        for number in self.store.processed_block_numbers_desc():
            if number > last_processed:
                continue
            stored = self.store.processed_block(number)
            if stored is None:
                continue
            current = self.rpc.get_block(number)
            if str(current["hash"]).lower() == str(stored["block_hash"]).lower():
                return number
        raise RpcError("no stored common ancestor remains after chain reorg")

    def _reconcile_last_block(self, last_processed: int) -> int:
        stored = self.store.processed_block(last_processed)
        if stored is None:
            raise RpcError(f"missing stored header for block {last_processed}")
        current = self.rpc.get_block(last_processed)
        if str(current["hash"]).lower() == str(stored["block_hash"]).lower():
            return last_processed
        common = self._find_common_ancestor(last_processed - 1)
        self.store.mark_orphaned_after(common)
        self.store.set_runtime(
            "last_reorg",
            json_text(
                {
                    "detected_at_ms": self.clock_ms(),
                    "old_tip": last_processed,
                    "common_ancestor": common,
                }
            ),
        )
        self.reorg_replay = True
        return common

    def run_chain_cycle(self) -> None:
        cycle_started = self.clock_ms()
        if self.store.runtime_value("last_processed_block") is None:
            self.initialize_watermark()
            return
        last_processed = int(self.store.runtime_value("last_processed_block") or 0)
        last_processed = self._reconcile_last_block(last_processed)
        latest = int(self.rpc.latest_block_number())
        self.store.set_runtime("current_head", str(latest))
        if latest > last_processed:
            latest_header = self.rpc.get_block(latest)
            header_cache = {latest: latest_header}
            maker_logs = self._source_logs_between(
                last_processed + 1,
                latest,
                "maker",
            )
            self._ingest_log_batch(
                maker_logs,
                header_cache=header_cache,
            )
            taker_logs = self._source_logs_between(
                last_processed + 1,
                latest,
                "taker",
            )
            self._ingest_log_batch(
                taker_logs,
                header_cache=header_cache,
            )
            (
                actual_number,
                block_hash,
                parent_hash,
                block_timestamp,
            ) = self._header_values(latest_header)
            if actual_number != latest:
                raise RpcError(
                    f"requested head {latest}, received block {actual_number}"
                )
            catchup = bool(
                (self.had_persisted_watermark and latest <= self.boot_head)
                or self.reorg_replay
            )
            self.store.save_processed_block(
                latest,
                block_hash,
                parent_hash,
                block_timestamp,
                catchup,
            )
            self.store.set_runtime("last_processed_block", str(latest))
        self.store.set_runtime("status", "running")
        self.store.set_runtime("last_error", "")
        self.store.set_runtime("heartbeat_at_ms", str(self.clock_ms()))
        self.store.set_runtime(
            "last_chain_cycle_duration_ms",
            str(self.clock_ms() - cycle_started),
        )
        self.reorg_replay = False


def group_data_rows(
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen: set[str] = set()
    for row in rows:
        row_id = data_row_id(row)
        if row_id in seen:
            continue
        seen.add(row_id)
        transaction_hash = str(row.get("transactionHash", "")).lower()
        token_id = str(row.get("asset", ""))
        side = str(row.get("side", "")).upper()
        if not transaction_hash or not token_id or side not in {"BUY", "SELL"}:
            continue
        try:
            size = D(str(row.get("size", "0")))
            price = D(str(row.get("price", "0")))
        except Exception:
            continue
        if size <= ZERO:
            continue
        key = (transaction_hash, token_id, side)
        group = groups.setdefault(
            key,
            {
                "transaction_hash": transaction_hash,
                "token_id": token_id,
                "side": side,
                "source_timestamp": int(row.get("timestamp", 0)),
                "source_quantity": ZERO,
                "source_notional": ZERO,
                "rows": [],
            },
        )
        group["source_timestamp"] = max(
            int(group["source_timestamp"]),
            int(row.get("timestamp", 0)),
        )
        group["source_quantity"] += size
        group["source_notional"] += size * price
        group["rows"].append(row)
    result: list[dict[str, Any]] = []
    for group in groups.values():
        quantity = group["source_quantity"]
        group["source_vwap"] = group["source_notional"] / quantity
        result.append(group)
    return sorted(
        result,
        key=lambda item: (
            int(item["source_timestamp"]),
            str(item["transaction_hash"]),
            str(item["token_id"]),
            str(item["side"]),
        ),
    )


class DataApiMatcher:
    """Records the first public Data API observation for new chain events."""

    def __init__(
        self,
        store: ShadowStore,
        data_api: Any,
        clob: Any,
        *,
        clock_ms: Callable[[], int] = now_ms,
        source_wallet: str = SOURCE_WALLET,
    ):
        self.store = store
        self.data_api = data_api
        self.clob = clob
        self.clock_ms = clock_ms
        self.source_wallet = normalize_address(source_wallet)

    def _snapshot_book(
        self,
        transaction_hash: str,
        token_id: str,
        observed_at_ms: int,
    ) -> None:
        if self.store.has_snapshot(transaction_hash, token_id, "data_api"):
            return
        book: dict[str, Any] = {}
        error: str | None = None
        try:
            book = self.clob.get_book(token_id)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        finished = self.clock_ms()
        self.store.save_book_snapshot(
            transaction_hash=transaction_hash,
            token_id=token_id,
            channel="data_api",
            request_started_at_ms=observed_at_ms,
            request_finished_at_ms=finished,
            book=book,
            error=error,
        )

    def run_once(self) -> None:
        rows = self.data_api.get_trades(self.source_wallet)
        observed_at = self.clock_ms()
        self.store.insert_data_api_rows(rows, observed_at)
        for group in group_data_rows(rows):
            transaction_hash = str(group["transaction_hash"])
            token_id = str(group["token_id"])
            side = str(group["side"])
            if not self.store.has_primary_event(
                transaction_hash,
                token_id,
                side,
            ):
                continue
            inserted = self.store.upsert_data_api_match(
                transaction_hash=transaction_hash,
                token_id=token_id,
                side=side,
                observed_at_ms=observed_at,
                source_timestamp=int(group["source_timestamp"]),
                source_quantity=group["source_quantity"],
                source_vwap=group["source_vwap"],
                raw_rows=group["rows"],
            )
            for row in group["rows"]:
                self.store.enrich_metadata_from_trade(row, observed_at)
            if inserted:
                self._snapshot_book(
                    transaction_hash,
                    token_id,
                    observed_at,
                )
        self.store.set_runtime("last_data_api_seen_at_ms", str(observed_at))
        self.store.set_runtime("last_data_api_error", "")


def build_status(store: ShadowStore) -> dict[str, Any]:
    runtime = store.runtime_values()
    return {
        "title": "Tian-Wen 只读链上影子",
        "paper_only": True,
        "real_order_submitted": False,
        "old_paper_database_mode": "read_only",
        "source_wallet": SOURCE_WALLET,
        "chain_id": CHAIN_ID,
        "exchange_addresses": list(V2_EXCHANGE_ADDRESSES),
        "runtime": runtime,
        "metrics": store.metrics(),
        "recent_events": store.recent_events(),
        "sqlite_integrity_check": store.integrity_check(),
    }


def _status_html(status: dict[str, Any]) -> str:
    metrics = status["metrics"]
    runtime = status["runtime"]
    rows: list[str] = []
    for event in status["recent_events"]:
        block_time = int(event["block_timestamp"]) * 1000
        chain_delay = int(event["chain_seen_at_ms"]) - block_time
        api_seen = event.get("data_api_seen_at_ms")
        api_delay = "" if api_seen is None else str(int(api_seen) - block_time)
        rows.append(
            "<tr>"
            f"<td>{escape(str(event['block_number']))}</td>"
            f"<td>{escape(str(event['transaction_hash']))}</td>"
            f"<td>{escape(str(event['source_role']))}</td>"
            f"<td>{escape(str(event['side']))}</td>"
            f"<td>{escape(str(event['title'] or '元数据待匹配'))}</td>"
            f"<td>{escape(str(event['scope_status']))}</td>"
            f"<td>{escape(str(event['chain_seen_at_ms']))}</td>"
            f"<td>{escape(str(chain_delay))}</td>"
            f"<td>{escape(str(api_seen or '待出现'))}</td>"
            f"<td>{escape(api_delay or '—')}</td>"
            f"<td>{escape(str(event['chain_book_id'] or '失败/待写入'))}</td>"
            f"<td>{escape(str(event['data_api_book_id'] or '待匹配'))}</td>"
            f"<td>{'是' if event['catchup'] else '否'}</td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>Tian-Wen 只读链上影子</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #17202a; }}
.safe {{ padding: 12px; border-radius: 8px; background: #eafaf1; color: #176b3a; }}
.cards {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(190px,1fr)); gap: 12px; margin: 16px 0; }}
.card {{ border: 1px solid #dfe6e9; border-radius: 8px; padding: 12px; }}
.value {{ font-size: 22px; font-weight: 650; margin-top: 6px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ border-bottom: 1px solid #e7ecef; padding: 8px; text-align: left; vertical-align: top; }}
th {{ position: sticky; top: 0; background: white; }}
.scroll {{ overflow-x: auto; }}
</style>
</head>
<body>
<h1>Tian-Wen 只读链上影子</h1>
<div class="safe">paper_only=true · real_order_submitted=false · 旧纸面数据库只读</div>
<div class="cards">
  <div class="card">状态<div class="value">{escape(runtime.get("status", "未启动"))}</div></div>
  <div class="card">最后处理区块<div class="value">{escape(runtime.get("last_processed_block", "—"))}</div></div>
  <div class="card">实时链上事件<div class="value">{metrics["live_chain_event_count"]}</div></div>
  <div class="card">实时 A/B 匹配<div class="value">{metrics["live_ab_match_count"]}</div></div>
  <div class="card">链上发现延迟中位数<div class="value">{metrics["live_chain_delay_ms_median"] if metrics["live_chain_delay_ms_median"] is not None else "待样本"} ms</div></div>
  <div class="card">Data API 发现延迟中位数<div class="value">{metrics["live_data_api_delay_ms_median"] if metrics["live_data_api_delay_ms_median"] is not None else "待样本"} ms</div></div>
</div>
<p>延迟均使用本进程真实首次观察时间；盘口返回的 timestamp 不替代观察时间。catch-up 样本不进入实时统计。</p>
<div class="scroll">
<table>
<thead><tr><th>区块</th><th>交易</th><th>链上角色</th><th>方向</th><th>市场</th><th>范围</th><th>链上观察(ms)</th><th>链延迟(ms)</th><th>API观察(ms)</th><th>API延迟(ms)</th><th>链盘口ID</th><th>API盘口ID</th><th>补扫</th></tr></thead>
<tbody>{''.join(rows) or '<tr><td colspan="13">等待新的前瞻成交</td></tr>'}</tbody>
</table>
</div>
</body>
</html>
"""


def _atomic_write(path: Path, text_value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text_value, encoding="utf-8")
    os.replace(temporary, path)


def render_status_files(store: ShadowStore, runtime_dir: Path) -> dict[str, Any]:
    status = build_status(store)
    target = Path(runtime_dir)
    _atomic_write(
        target / "status.json",
        json.dumps(status, ensure_ascii=False, indent=2, default=str) + "\n",
    )
    _atomic_write(target / "status.html", _status_html(status))
    return status


@contextmanager
def acquire_process_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(f"shadow observer is already running: {path}") from exc
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def run_daemon(
    *,
    runtime_dir: Path = DEFAULT_RUNTIME_DIR,
    old_paper_db: Path = DEFAULT_OLD_PAPER_DB,
    rpc_urls: Iterable[str] = DEFAULT_RPC_URLS,
    once: bool = False,
) -> None:
    runtime_dir = Path(runtime_dir).resolve()
    store = ShadowStore(runtime_dir / "shadow.sqlite3")
    store.initialize()
    copied = store.load_metadata_from_old_paper(old_paper_db)
    store.set_runtime("metadata_rows_seeded_read_only", str(copied))
    store.set_runtime("pid", str(os.getpid()))
    store.set_runtime("started_at_ms", str(now_ms()))

    rpc = RpcClient(rpc_urls)
    if rpc.chain_id() != CHAIN_ID:
        raise RpcError(f"RPC endpoint is not Polygon chain {CHAIN_ID}")
    public = PublicPolymarketClient()
    observer = ChainObserver(store, rpc, public)
    observer.initialize_watermark()
    matcher = DataApiMatcher(store, public, public)
    render_status_files(store, runtime_dir)

    if once:
        observer.run_chain_cycle()
        matcher.run_once()
        render_status_files(store, runtime_dir)
        return

    stopped = threading.Event()

    def request_stop(_signum: int, _frame: Any) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    def matcher_loop() -> None:
        backoff = DEFAULT_DATA_API_IDLE_SECONDS
        while not stopped.is_set():
            try:
                matcher.run_once()
                backoff = DEFAULT_DATA_API_IDLE_SECONDS
            except Exception as exc:
                store.set_runtime(
                    "last_data_api_error",
                    f"{type(exc).__name__}: {exc}",
                )
                backoff = min(
                    max(DEFAULT_DATA_API_IDLE_SECONDS, backoff * 2),
                    MAX_BACKOFF_SECONDS,
                )
            stopped.wait(backoff)

    thread = threading.Thread(
        target=matcher_loop,
        name="public-data-api-matcher",
        daemon=True,
    )
    thread.start()

    chain_backoff = DEFAULT_CHAIN_IDLE_SECONDS
    while not stopped.is_set():
        try:
            observer.run_chain_cycle()
            store.set_runtime("rpc_url", rpc.last_url or "")
            store.set_runtime("rpc_last_latency_ms", rpc.last_latency_ms or "")
            render_status_files(store, runtime_dir)
            chain_backoff = DEFAULT_CHAIN_IDLE_SECONDS
        except Exception as exc:
            store.set_runtime("status", "rpc_error_retrying")
            store.set_runtime("last_error", f"{type(exc).__name__}: {exc}")
            store.set_runtime("heartbeat_at_ms", str(now_ms()))
            render_status_files(store, runtime_dir)
            chain_backoff = min(
                max(DEFAULT_CHAIN_IDLE_SECONDS, chain_backoff * 2),
                MAX_BACKOFF_SECONDS,
            )
        stopped.wait(chain_backoff)
    thread.join(timeout=HTTP_TIMEOUT_SECONDS + DEFAULT_DATA_API_IDLE_SECONDS)
    store.set_runtime("status", "stopped")
    store.set_runtime("heartbeat_at_ms", str(now_ms()))
    render_status_files(store, runtime_dir)


def verify_transaction(
    transaction_hash: str,
    *,
    rpc_urls: Iterable[str] = DEFAULT_RPC_URLS,
) -> dict[str, Any]:
    rpc = RpcClient(rpc_urls)
    if rpc.chain_id() != CHAIN_ID:
        raise RpcError(f"RPC endpoint is not Polygon chain {CHAIN_ID}")
    receipt = rpc.get_receipt(transaction_hash)
    block_number = hex_int(receipt["blockNumber"])
    header = rpc.get_block(block_number)
    block_timestamp = hex_int(header["timestamp"])
    decoded: list[dict[str, Any]] = []
    for log in receipt.get("logs", []):
        if not isinstance(log, dict):
            continue
        try:
            event = decode_order_filled(log, SOURCE_WALLET)
        except (ValueError, KeyError):
            continue
        if event is None:
            continue
        event["block_timestamp"] = block_timestamp
        event["quantity"] = decimal_text(event.get("quantity"))
        event["notional"] = decimal_text(event.get("notional"))
        event["price"] = decimal_text(event.get("price"))
        event.pop("raw_log", None)
        decoded.append(event)
    return {
        "paper_only": True,
        "real_order_submitted": False,
        "transaction_hash": transaction_hash.lower(),
        "block_number": block_number,
        "block_timestamp": block_timestamp,
        "source_events": decoded,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tian-Wen read-only Polygon chain shadow observer"
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=DEFAULT_RUNTIME_DIR,
    )
    parser.add_argument(
        "--old-paper-db",
        type=Path,
        default=DEFAULT_OLD_PAPER_DB,
    )
    parser.add_argument(
        "--rpc-url",
        action="append",
        dest="rpc_urls",
        help="allow-listed public Polygon RPC; repeat for fallback",
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--verify-tx")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rpc_urls = tuple(args.rpc_urls or DEFAULT_RPC_URLS)
    if args.verify_tx:
        print(
            json.dumps(
                verify_transaction(args.verify_tx, rpc_urls=rpc_urls),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    runtime_dir = Path(args.runtime_dir).resolve()
    with acquire_process_lock(runtime_dir / "daemon.lock"):
        run_daemon(
            runtime_dir=runtime_dir,
            old_paper_db=Path(args.old_paper_db),
            rpc_urls=rpc_urls,
            once=bool(args.once),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
