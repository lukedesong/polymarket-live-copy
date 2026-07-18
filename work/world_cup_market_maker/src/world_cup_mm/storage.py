from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from .models import EligibleMarket, RejectedMarket, parse_utc_datetime
from .orderbook import BookNotReady, OrderBookState


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scan_runs (
    scan_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,
    source_url TEXT NOT NULL,
    selection_mode TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS events (
    scan_id TEXT NOT NULL REFERENCES scan_runs(scan_id),
    event_id TEXT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (scan_id, event_id)
);

CREATE TABLE IF NOT EXISTS markets (
    scan_id TEXT NOT NULL REFERENCES scan_runs(scan_id),
    market_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    question TEXT NOT NULL,
    slug TEXT NOT NULL,
    condition_id TEXT,
    game_start_time TEXT,
    liquidity_text TEXT,
    volume_24h_text TEXT,
    eligible INTEGER NOT NULL,
    frontier INTEGER NOT NULL,
    rejection_reasons_json TEXT NOT NULL,
    PRIMARY KEY (scan_id, market_id)
);

CREATE TABLE IF NOT EXISTS assets (
    scan_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    token_index INTEGER NOT NULL,
    PRIMARY KEY (scan_id, market_id, token_id),
    FOREIGN KEY (scan_id, market_id) REFERENCES markets(scan_id, market_id)
);

CREATE TABLE IF NOT EXISTS collector_sessions (
    session_id TEXT PRIMARY KEY,
    selection_mode TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    connected INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_ws_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES collector_sessions(session_id),
    event_hash TEXT NOT NULL,
    event_type TEXT NOT NULL,
    asset_id TEXT,
    server_timestamp TEXT,
    received_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE (session_id, event_hash)
);

CREATE TABLE IF NOT EXISTS book_readiness (
    asset_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES collector_sessions(session_id),
    ready INTEGER NOT NULL,
    server_timestamp TEXT
);

CREATE TABLE IF NOT EXISTS book_levels (
    asset_id TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES collector_sessions(session_id),
    side TEXT NOT NULL,
    price_text TEXT NOT NULL,
    size_text TEXT NOT NULL,
    PRIMARY KEY (asset_id, side, price_text)
);

CREATE TABLE IF NOT EXISTS book_snapshots (
    session_id TEXT NOT NULL REFERENCES collector_sessions(session_id),
    event_hash TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    server_timestamp TEXT,
    PRIMARY KEY (session_id, event_hash, asset_id)
);

CREATE TABLE IF NOT EXISTS trades (
    session_id TEXT NOT NULL REFERENCES collector_sessions(session_id),
    event_hash TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    condition_id TEXT,
    price_text TEXT NOT NULL,
    size_text TEXT NOT NULL,
    side TEXT NOT NULL,
    server_timestamp TEXT,
    PRIMARY KEY (session_id, event_hash)
);

CREATE TABLE IF NOT EXISTS sports_status (
    session_id TEXT NOT NULL REFERENCES collector_sessions(session_id),
    event_hash TEXT NOT NULL,
    slug TEXT,
    live INTEGER,
    ended INTEGER,
    status TEXT,
    received_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (session_id, event_hash)
);

CREATE TABLE IF NOT EXISTS risk_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    seconds_to_start INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    action TEXT NOT NULL,
    delivery_status TEXT NOT NULL,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS paper_orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('BUY','SELL')),
    price_text TEXT NOT NULL,
    quantity_text TEXT NOT NULL,
    original_quantity_text TEXT NOT NULL DEFAULT '0',
    remaining_quantity_text TEXT NOT NULL DEFAULT '0',
    queue_ahead_initial_text TEXT NOT NULL DEFAULT '0',
    queue_ahead_remaining_text TEXT NOT NULL DEFAULT '0',
    maker_fee_bps INTEGER NOT NULL,
    quote_book_timestamp TEXT,
    last_book_timestamp TEXT,
    requeue_reason TEXT,
    status TEXT NOT NULL CHECK(status IN ('OPEN','FILLED','CANCELLED')),
    created_at TEXT NOT NULL,
    cancelled_at TEXT,
    cancel_reason TEXT,
    filled_at TEXT
);

CREATE INDEX IF NOT EXISTS paper_orders_open_asset_side
ON paper_orders(asset_id,side,status);

CREATE TABLE IF NOT EXISTS paper_fills (
    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES paper_orders(order_id),
    trigger_event_hash TEXT NOT NULL UNIQUE,
    trigger_price_text TEXT NOT NULL,
    fill_price_text TEXT NOT NULL,
    quantity_text TEXT NOT NULL,
    gross_amount_text TEXT NOT NULL,
    fee_text TEXT NOT NULL,
    filled_at TEXT NOT NULL,
    position_quantity_after_text TEXT NOT NULL,
    realized_profit_after_text TEXT NOT NULL,
    official_trade_quantity_text TEXT NOT NULL DEFAULT '0',
    queue_consumed_text TEXT NOT NULL DEFAULT '0',
    order_quantity_before_text TEXT NOT NULL DEFAULT '0',
    order_quantity_after_text TEXT NOT NULL DEFAULT '0',
    proof_type TEXT NOT NULL DEFAULT 'LEGACY',
    UNIQUE(order_id,trigger_event_hash)
);

CREATE TABLE IF NOT EXISTS paper_queue_events (
    queue_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL REFERENCES paper_orders(order_id),
    event_type TEXT NOT NULL,
    trigger_event_hash TEXT,
    official_trade_quantity_text TEXT,
    queue_before_text TEXT NOT NULL,
    queue_after_text TEXT NOT NULL,
    order_remaining_before_text TEXT NOT NULL,
    order_remaining_after_text TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS paper_queue_trade_event_once
ON paper_queue_events(trigger_event_hash)
WHERE trigger_event_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS paper_inventory_marks (
    mark_id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    marked_at TEXT NOT NULL,
    book_timestamp TEXT,
    best_bid_text TEXT,
    best_ask_text TEXT,
    liquidatable_quantity_text TEXT NOT NULL,
    liquidation_proceeds_text TEXT NOT NULL,
    liquidation_vwap_text TEXT,
    liquidation_fee_text TEXT NOT NULL,
    unliquidated_quantity_text TEXT NOT NULL,
    depth_adjusted_unrealized_profit_text TEXT NOT NULL,
    reference_fill_price_text TEXT,
    best_bid_drift_text TEXT,
    liquidation_vwap_drift_text TEXT
);

CREATE TABLE IF NOT EXISTS paper_liquidations (
    liquidation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    price_text TEXT NOT NULL,
    quantity_text TEXT NOT NULL,
    gross_proceeds_text TEXT NOT NULL,
    fee_text TEXT NOT NULL,
    liquidated_at TEXT NOT NULL,
    book_timestamp TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS paper_liquidation_layer_once
ON paper_liquidations(batch_key,price_text);

CREATE TABLE IF NOT EXISTS paper_liquidation_runs (
    batch_key TEXT PRIMARY KEY,
    asset_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    book_timestamp TEXT,
    sold_quantity_text TEXT NOT NULL,
    unliquidated_quantity_text TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_positions (
    asset_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    quantity_text TEXT NOT NULL,
    cost_basis_text TEXT NOT NULL,
    realized_profit_text TEXT NOT NULL,
    mark_price_text TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_account_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    buy_cost_text TEXT NOT NULL,
    sell_proceeds_text TEXT NOT NULL,
    realized_profit_text TEXT NOT NULL,
    unrealized_profit_text TEXT NOT NULL,
    total_profit_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_report_sync (
    sync_id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempted_at TEXT NOT NULL,
    status TEXT NOT NULL,
    spreadsheet_id TEXT,
    detail TEXT
);
"""


def _iso(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("storage_time_missing_timezone")
    return current.astimezone(timezone.utc).isoformat()


def _canonical(payload: Mapping[str, Any]) -> tuple[str, str]:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text, hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class StoredMarket:
    scan_id: str
    market_id: str
    event_id: str
    event_slug: str
    question: str
    market_slug: str
    condition_id: str
    token_ids: tuple[str, ...]
    game_start_time: datetime
    liquidity_text: str
    volume_24h_text: str
    frontier: bool


@dataclass(frozen=True, slots=True)
class PaperOrder:
    order_id: int
    condition_id: str
    market_id: str
    asset_id: str
    outcome: str
    side: str
    price: Decimal
    quantity: Decimal
    original_quantity: Decimal
    remaining_quantity: Decimal
    queue_ahead_initial: Decimal
    queue_ahead_remaining: Decimal
    maker_fee_bps: int
    quote_book_timestamp: str | None
    status: str
    created_at: str


@dataclass(frozen=True, slots=True)
class PaperPosition:
    asset_id: str
    condition_id: str
    market_id: str
    outcome: str
    quantity: Decimal
    cost_basis: Decimal
    realized_profit: Decimal
    mark_price: Decimal

    @property
    def average_cost(self) -> Decimal:
        return self.cost_basis / self.quantity if self.quantity else Decimal("0")

    @property
    def unrealized_profit(self) -> Decimal:
        return self.quantity * self.mark_price - self.cost_basis


@dataclass(frozen=True, slots=True)
class PaperAccount:
    buy_cost: Decimal
    sell_proceeds: Decimal
    realized_profit: Decimal
    unrealized_profit: Decimal
    total_profit: Decimal


@dataclass(frozen=True, slots=True)
class DepthFill:
    price: Decimal
    quantity: Decimal
    gross_proceeds: Decimal
    fee: Decimal


@dataclass(frozen=True, slots=True)
class DepthSale:
    requested_quantity: Decimal
    sold_quantity: Decimal
    gross_proceeds: Decimal
    fee: Decimal
    net_proceeds: Decimal
    vwap: Decimal | None
    unliquidated_quantity: Decimal
    fills: tuple[DepthFill, ...]


@dataclass(frozen=True, slots=True)
class PaperInventoryMark:
    asset_id: str
    liquidatable_quantity: Decimal
    liquidation_proceeds: Decimal
    liquidation_vwap: Decimal | None
    liquidation_fee: Decimal
    unliquidated_quantity: Decimal
    depth_adjusted_unrealized_profit: Decimal
    reference_fill_price: Decimal | None
    best_bid_drift: Decimal | None
    liquidation_vwap_drift: Decimal | None


def executable_sale(
    quantity: Decimal,
    bid_levels: Iterable[tuple[Decimal, Decimal]],
    *,
    taker_fee_rate: Decimal,
    fee_exponent: int,
) -> DepthSale:
    if quantity < 0:
        raise ValueError("negative_liquidation_quantity")
    if taker_fee_rate < 0 or fee_exponent < 0:
        raise ValueError("invalid_liquidation_fee_curve")
    remaining = quantity
    gross = Decimal("0")
    fee = Decimal("0")
    fills: list[DepthFill] = []
    for price, available in sorted(bid_levels, key=lambda item: item[0], reverse=True):
        if price <= 0 or price >= 1 or available < 0:
            raise ValueError("invalid_liquidation_book_level")
        if not available or not remaining:
            continue
        filled = min(remaining, available)
        layer_gross = filled * price
        layer_fee = (
            filled
            * taker_fee_rate
            * (price * (Decimal("1") - price)) ** fee_exponent
        )
        fills.append(DepthFill(price, filled, layer_gross, layer_fee))
        gross += layer_gross
        fee += layer_fee
        remaining -= filled
    sold = quantity - remaining
    return DepthSale(
        requested_quantity=quantity,
        sold_quantity=sold,
        gross_proceeds=gross,
        fee=fee,
        net_proceeds=gross - fee,
        vwap=gross / sold if sold else None,
        unliquidated_quantity=remaining,
        fills=tuple(fills),
    )


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self._migrate_schema()
        self._books: dict[tuple[str, str], OrderBookState] = {}

    def _migrate_schema(self) -> None:
        order_columns = {
            str(row["name"])
            for row in self.connection.execute("PRAGMA table_info(paper_orders)")
        }
        additions = {
            "original_quantity_text": "TEXT NOT NULL DEFAULT '0'",
            "remaining_quantity_text": "TEXT NOT NULL DEFAULT '0'",
            "queue_ahead_initial_text": "TEXT NOT NULL DEFAULT '0'",
            "queue_ahead_remaining_text": "TEXT NOT NULL DEFAULT '0'",
            "last_book_timestamp": "TEXT",
            "requeue_reason": "TEXT",
        }
        with self.connection:
            for name, definition in additions.items():
                if name not in order_columns:
                    self.connection.execute(
                        f"ALTER TABLE paper_orders ADD COLUMN {name} {definition}"
                    )
            self.connection.execute(
                """
                UPDATE paper_orders SET
                    original_quantity_text=quantity_text,
                    remaining_quantity_text=CASE
                        WHEN status='OPEN' THEN quantity_text ELSE '0' END
                WHERE original_quantity_text='0'
                """
            )

    def close(self) -> None:
        self.connection.close()

    def record_scan(
        self,
        scan_id: str,
        *,
        events: Iterable[Mapping[str, Any]],
        eligible: Iterable[EligibleMarket],
        rejected: Iterable[RejectedMarket],
        started_at: datetime,
        completed_at: datetime,
        source_url: str,
    ) -> None:
        eligible_items = list(eligible)
        rejected_items = list(rejected)
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO scan_runs(
                    scan_id,started_at,completed_at,status,source_url,selection_mode
                ) VALUES(?,?,?,'complete',?,'frontier')
                """,
                (scan_id, _iso(started_at), _iso(completed_at), source_url),
            )
            self.connection.executemany(
                "INSERT INTO events(scan_id,event_id,title,slug,payload_json) VALUES(?,?,?,?,?)",
                (
                    (
                        scan_id,
                        str(event.get("id") or ""),
                        str(event.get("title") or ""),
                        str(event.get("slug") or ""),
                        json.dumps(event, sort_keys=True, ensure_ascii=False),
                    )
                    for event in events
                    if event.get("id")
                ),
            )
            for market in eligible_items:
                self.connection.execute(
                    """
                    INSERT INTO markets(
                        scan_id,market_id,event_id,question,slug,condition_id,game_start_time,
                        liquidity_text,volume_24h_text,eligible,frontier,rejection_reasons_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,1,?, '[]')
                    """,
                    (
                        scan_id,
                        market.market_id,
                        market.event_id,
                        market.question,
                        market.market_slug,
                        market.condition_id,
                        market.game_start_time.isoformat(),
                        str(market.liquidity),
                        str(market.volume_24h),
                        int(market.frontier),
                    ),
                )
                self.connection.executemany(
                    "INSERT INTO assets(scan_id,market_id,token_id,token_index) VALUES(?,?,?,?)",
                    (
                        (scan_id, market.market_id, token_id, index)
                        for index, token_id in enumerate(market.token_ids)
                    ),
                )
            for market in rejected_items:
                storage_market_id = market.market_id or f"event:{market.event_id}"
                self.connection.execute(
                    """
                    INSERT INTO markets(
                        scan_id,market_id,event_id,question,slug,condition_id,game_start_time,
                        liquidity_text,volume_24h_text,eligible,frontier,rejection_reasons_json
                    ) VALUES(?,?,?,?, '',NULL,NULL,'0','0',0,0,?)
                    """,
                    (
                        scan_id,
                        storage_market_id,
                        market.event_id,
                        market.question,
                        json.dumps(market.reasons),
                    ),
                )

    def latest_scan_summary(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT scan_id FROM scan_runs WHERE status='complete' ORDER BY completed_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        scan_id = str(row["scan_id"])
        counts = self.connection.execute(
            """
            SELECT
                SUM(CASE WHEN eligible=1 THEN 1 ELSE 0 END) AS eligible_count,
                SUM(CASE WHEN frontier=1 THEN 1 ELSE 0 END) AS frontier_count,
                SUM(CASE WHEN eligible=0 THEN 1 ELSE 0 END) AS rejected_count
            FROM markets WHERE scan_id=?
            """,
            (scan_id,),
        ).fetchone()
        return {
            "scan_id": scan_id,
            "eligible_count": int(counts["eligible_count"] or 0),
            "frontier_count": int(counts["frontier_count"] or 0),
            "rejected_count": int(counts["rejected_count"] or 0),
        }

    def selected_markets(self, *, all_eligible: bool) -> list[StoredMarket]:
        summary = self.latest_scan_summary()
        if summary is None:
            return []
        scan_id = str(summary["scan_id"])
        frontier_clause = "" if all_eligible else "AND m.frontier=1"
        rows = self.connection.execute(
            f"""
            SELECT m.*, e.slug AS event_slug FROM markets AS m
            JOIN events AS e ON e.scan_id=m.scan_id AND e.event_id=m.event_id
            WHERE m.scan_id=? AND m.eligible=1 {frontier_clause}
            ORDER BY m.frontier DESC, CAST(m.liquidity_text AS REAL) DESC,
                     CAST(m.volume_24h_text AS REAL) DESC, m.market_id
            """,
            (scan_id,),
        ).fetchall()
        result: list[StoredMarket] = []
        for row in rows:
            token_ids = tuple(
                str(token["token_id"])
                for token in self.connection.execute(
                    "SELECT token_id FROM assets WHERE scan_id=? AND market_id=? ORDER BY token_index",
                    (scan_id, row["market_id"]),
                )
            )
            result.append(
                StoredMarket(
                    scan_id=scan_id,
                    market_id=str(row["market_id"]),
                    event_id=str(row["event_id"]),
                    event_slug=str(row["event_slug"]),
                    question=str(row["question"]),
                    market_slug=str(row["slug"]),
                    condition_id=str(row["condition_id"]),
                    token_ids=token_ids,
                    game_start_time=parse_utc_datetime(row["game_start_time"]),
                    liquidity_text=str(row["liquidity_text"]),
                    volume_24h_text=str(row["volume_24h_text"]),
                    frontier=bool(row["frontier"]),
                )
            )
        return result

    def start_session(
        self,
        session_id: str,
        *,
        selection_mode: str,
        started_at: datetime | None = None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO collector_sessions(session_id,selection_mode,started_at,connected) VALUES(?,?,?,1)",
                (session_id, selection_mode, _iso(started_at)),
            )

    def record_market_event(
        self,
        session_id: str,
        payload: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> bool:
        canonical, event_hash = _canonical(payload)
        event_type = str(payload.get("event_type") or "unknown")
        asset_id = str(payload.get("asset_id") or "") or None
        server_timestamp = str(payload.get("timestamp") or "") or None
        try:
            with self.connection:
                inserted = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO raw_ws_events(
                        session_id,event_hash,event_type,asset_id,server_timestamp,received_at,payload_json
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        session_id,
                        event_hash,
                        event_type,
                        asset_id,
                        server_timestamp,
                        _iso(received_at),
                        canonical,
                    ),
                ).rowcount
                if not inserted:
                    return False
                if event_type == "book":
                    self._apply_snapshot(session_id, event_hash, payload)
                elif event_type == "price_change":
                    self._apply_delta(session_id, payload)
                elif event_type == "last_trade_price":
                    self._insert_trade(session_id, event_hash, payload)
        except Exception:
            self._reload_session_books(session_id)
            raise
        return True

    def _apply_snapshot(
        self,
        session_id: str,
        event_hash: str,
        payload: Mapping[str, Any],
    ) -> None:
        asset_id = str(payload.get("asset_id") or "")
        if not asset_id:
            raise ValueError("book_missing_asset_id")
        book = OrderBookState(asset_id)
        book.apply(payload)
        self._books[(session_id, asset_id)] = book
        self._persist_book(session_id, book)
        self.connection.execute(
            "INSERT INTO book_snapshots(session_id,event_hash,asset_id,server_timestamp) VALUES(?,?,?,?)",
            (session_id, event_hash, asset_id, book.server_timestamp),
        )

    def _apply_delta(self, session_id: str, payload: Mapping[str, Any]) -> None:
        asset_ids = {
            str(change.get("asset_id") or "")
            for change in (payload.get("price_changes") or [])
            if change.get("asset_id")
        }
        if not asset_ids:
            raise ValueError("price_change_missing_assets")
        for asset_id in asset_ids:
            book = self._books.get((session_id, asset_id))
            if book is None or not book.ready:
                raise BookNotReady(asset_id)
            book.apply(payload)
            self._persist_book(session_id, book)

    def _insert_trade(
        self,
        session_id: str,
        event_hash: str,
        payload: Mapping[str, Any],
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO trades(
                session_id,event_hash,asset_id,condition_id,price_text,size_text,side,server_timestamp
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                session_id,
                event_hash,
                str(payload.get("asset_id") or ""),
                str(payload.get("market") or "") or None,
                str(payload.get("price") or ""),
                str(payload.get("size") or ""),
                str(payload.get("side") or ""),
                str(payload.get("timestamp") or "") or None,
            ),
        )

    def _persist_book(self, session_id: str, book: OrderBookState) -> None:
        self.connection.execute("DELETE FROM book_levels WHERE asset_id=?", (book.asset_id,))
        self.connection.executemany(
            "INSERT INTO book_levels(asset_id,session_id,side,price_text,size_text) VALUES(?,?,?,?,?)",
            (
                (book.asset_id, session_id, side, price, size)
                for side, price, size in book.canonical_levels()
            ),
        )
        self.connection.execute(
            """
            INSERT INTO book_readiness(asset_id,session_id,ready,server_timestamp)
            VALUES(?,?,1,?)
            ON CONFLICT(asset_id) DO UPDATE SET
                session_id=excluded.session_id,
                ready=1,
                server_timestamp=excluded.server_timestamp
            """,
            (book.asset_id, session_id, book.server_timestamp),
        )

    def _reload_session_books(self, session_id: str) -> None:
        for key in [key for key in self._books if key[0] == session_id]:
            self._books.pop(key, None)
        rows = self.connection.execute(
            "SELECT asset_id,ready,server_timestamp FROM book_readiness WHERE session_id=?",
            (session_id,),
        ).fetchall()
        for row in rows:
            if not row["ready"]:
                continue
            book = OrderBookState(str(row["asset_id"]), ready=True)
            book.server_timestamp = row["server_timestamp"]
            for level in self.connection.execute(
                "SELECT side,price_text,size_text FROM book_levels WHERE asset_id=? AND session_id=?",
                (book.asset_id, session_id),
            ):
                target = book.bids if level["side"] == "BUY" else book.asks
                target[Decimal(level["price_text"])] = Decimal(level["size_text"])
            self._books[(session_id, book.asset_id)] = book

    def invalidate_session_books(
        self,
        session_id: str,
        *,
        ended_at: datetime | None = None,
    ) -> None:
        with self.connection:
            asset_ids = [
                str(row["asset_id"])
                for row in self.connection.execute(
                    "SELECT asset_id FROM book_readiness WHERE session_id=?", (session_id,)
                )
            ]
            self.connection.execute(
                "UPDATE book_readiness SET ready=0 WHERE session_id=?", (session_id,)
            )
            if asset_ids:
                self.connection.executemany(
                    "DELETE FROM book_levels WHERE asset_id=?", ((asset_id,) for asset_id in asset_ids)
                )
            self.connection.execute(
                "UPDATE collector_sessions SET connected=0,ended_at=? WHERE session_id=?",
                (_iso(ended_at), session_id),
            )
        for asset_id in asset_ids:
            book = self._books.pop((session_id, asset_id), None)
            if book:
                book.invalidate()

    def raw_event_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM raw_ws_events").fetchone()[0])

    def trade_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0])

    def book_ready(self, asset_id: str) -> bool:
        row = self.connection.execute(
            "SELECT ready FROM book_readiness WHERE asset_id=?", (asset_id,)
        ).fetchone()
        return bool(row and row["ready"])

    def best_quotes(self, asset_id: str) -> tuple[str | None, str | None]:
        if not self.book_ready(asset_id):
            return None, None
        bid = self.connection.execute(
            "SELECT price_text FROM book_levels WHERE asset_id=? AND side='BUY' ORDER BY CAST(price_text AS REAL) DESC LIMIT 1",
            (asset_id,),
        ).fetchone()
        ask = self.connection.execute(
            "SELECT price_text FROM book_levels WHERE asset_id=? AND side='SELL' ORDER BY CAST(price_text AS REAL) ASC LIMIT 1",
            (asset_id,),
        ).fetchone()
        return (bid["price_text"] if bid else None, ask["price_text"] if ask else None)

    def raw_events(self, session_id: str) -> list[dict[str, Any]]:
        return [
            json.loads(row["payload_json"])
            for row in self.connection.execute(
                "SELECT payload_json FROM raw_ws_events WHERE session_id=? ORDER BY id",
                (session_id,),
            )
        ]

    def latest_session_summary(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT session_id,selection_mode,started_at,ended_at,connected
            FROM collector_sessions ORDER BY started_at DESC,rowid DESC LIMIT 1
            """
        ).fetchone()
        if row is None:
            return None
        return {
            "session_id": str(row["session_id"]),
            "selection_mode": str(row["selection_mode"]),
            "started_at": str(row["started_at"]),
            "ended_at": str(row["ended_at"]) if row["ended_at"] else None,
            "connected": bool(row["connected"]),
        }

    def record_sports_event(
        self,
        session_id: str,
        payload: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> bool:
        canonical, event_hash = _canonical(payload)
        with self.connection:
            inserted = self.connection.execute(
                """
                INSERT OR IGNORE INTO sports_status(
                    session_id,event_hash,slug,live,ended,status,received_at,payload_json
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    session_id,
                    event_hash,
                    str(payload.get("slug") or "") or None,
                    int(bool(payload.get("live"))),
                    int(bool(payload.get("ended"))),
                    str(payload.get("status") or "") or None,
                    _iso(received_at),
                    canonical,
                ),
            ).rowcount
        return bool(inserted)

    def latest_sports_live(self, slug: str) -> bool:
        row = self.connection.execute(
            "SELECT live FROM sports_status WHERE slug=? ORDER BY rowid DESC LIMIT 1",
            (slug,),
        ).fetchone()
        return bool(row and row["live"])

    def record_risk_decision(
        self,
        decision: Any,
        *,
        decided_at: datetime,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO risk_decisions(
                    condition_id,decided_at,state,reason,seconds_to_start
                ) VALUES(?,?,?,?,?)
                """,
                (
                    decision.market_condition_id,
                    _iso(decided_at),
                    decision.state.value,
                    decision.reason,
                    decision.seconds_to_start,
                ),
            )

    def record_risk_action(
        self,
        condition_id: str,
        action: str,
        delivery_status: str,
        *,
        created_at: datetime,
        detail: str | None = None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO risk_actions(
                    condition_id,created_at,action,delivery_status,detail
                ) VALUES(?,?,?,?,?)
                """,
                (condition_id, _iso(created_at), action, delivery_status, detail),
            )

    def open_paper_order(
        self,
        *,
        condition_id: str,
        market_id: str,
        asset_id: str,
        outcome: str,
        side: str,
        price: Decimal,
        quantity: Decimal,
        maker_fee_bps: int,
        quote_book_timestamp: str | None,
        created_at: datetime,
    ) -> int:
        if side not in {"BUY", "SELL"}:
            raise ValueError("invalid_paper_side")
        if price <= 0 or quantity <= 0:
            raise ValueError("invalid_paper_order_value")
        if maker_fee_bps != 0:
            raise ValueError("paper_nonzero_maker_fee_unsupported")
        timestamp = _iso(created_at)
        with self.connection:
            self.connection.execute(
                """
                UPDATE paper_orders SET status='CANCELLED',cancelled_at=?,cancel_reason='requote'
                WHERE asset_id=? AND side=? AND status='OPEN'
                """,
                (timestamp, asset_id, side),
            )
            cursor = self.connection.execute(
                """
                INSERT INTO paper_orders(
                    condition_id,market_id,asset_id,outcome,side,price_text,quantity_text,
                    original_quantity_text,remaining_quantity_text,
                    queue_ahead_initial_text,queue_ahead_remaining_text,
                    maker_fee_bps,quote_book_timestamp,last_book_timestamp,status,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)
                """,
                (
                    condition_id,
                    market_id,
                    asset_id,
                    outcome,
                    side,
                    str(price),
                    str(quantity),
                    str(quantity),
                    str(quantity),
                    "0",
                    "0",
                    maker_fee_bps,
                    quote_book_timestamp,
                    quote_book_timestamp,
                    timestamp,
                ),
            )
        return int(cursor.lastrowid)

    def open_queue_paper_order(
        self,
        *,
        condition_id: str,
        market_id: str,
        asset_id: str,
        outcome: str,
        side: str,
        price: Decimal,
        quantity: Decimal,
        queue_ahead: Decimal,
        maker_fee_bps: int,
        quote_book_timestamp: str | None,
        created_at: datetime,
    ) -> int:
        if side not in {"BUY", "SELL"}:
            raise ValueError("invalid_paper_side")
        if price <= 0 or quantity <= 0 or queue_ahead < 0:
            raise ValueError("invalid_queue_paper_order_value")
        if maker_fee_bps != 0:
            raise ValueError("paper_nonzero_maker_fee_unsupported")
        timestamp = _iso(created_at)
        current = self.connection.execute(
            """
            SELECT * FROM paper_orders
            WHERE asset_id=? AND side=? AND status='OPEN'
            ORDER BY order_id DESC LIMIT 1
            """,
            (asset_id, side),
        ).fetchone()
        if current is not None and Decimal(current["price_text"]) == price:
            with self.connection:
                self.connection.execute(
                    "UPDATE paper_orders SET last_book_timestamp=? WHERE order_id=?",
                    (quote_book_timestamp, current["order_id"]),
                )
            return int(current["order_id"])
        with self.connection:
            if current is not None:
                self.connection.execute(
                    """
                    UPDATE paper_orders
                    SET status='CANCELLED',cancelled_at=?,cancel_reason='best_price_changed',
                        requeue_reason='best_price_changed'
                    WHERE order_id=?
                    """,
                    (timestamp, current["order_id"]),
                )
                self.connection.execute(
                    """
                    INSERT INTO paper_queue_events(
                        order_id,event_type,queue_before_text,queue_after_text,
                        order_remaining_before_text,order_remaining_after_text,created_at
                    ) VALUES(?,'BEST_PRICE_CHANGED',?,?,?,?,?)
                    """,
                    (
                        current["order_id"],
                        current["queue_ahead_remaining_text"],
                        current["queue_ahead_remaining_text"],
                        current["remaining_quantity_text"],
                        current["remaining_quantity_text"],
                        timestamp,
                    ),
                )
            cursor = self.connection.execute(
                """
                INSERT INTO paper_orders(
                    condition_id,market_id,asset_id,outcome,side,price_text,quantity_text,
                    original_quantity_text,remaining_quantity_text,
                    queue_ahead_initial_text,queue_ahead_remaining_text,
                    maker_fee_bps,quote_book_timestamp,last_book_timestamp,status,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)
                """,
                (
                    condition_id,
                    market_id,
                    asset_id,
                    outcome,
                    side,
                    str(price),
                    str(quantity),
                    str(quantity),
                    str(quantity),
                    str(queue_ahead),
                    str(queue_ahead),
                    maker_fee_bps,
                    quote_book_timestamp,
                    quote_book_timestamp,
                    timestamp,
                ),
            )
            order_id = int(cursor.lastrowid)
            self.connection.execute(
                """
                INSERT INTO paper_queue_events(
                    order_id,event_type,queue_before_text,queue_after_text,
                    order_remaining_before_text,order_remaining_after_text,created_at
                ) VALUES(?,'ENQUEUED',?,?,?,?,?)
                """,
                (
                    order_id,
                    str(queue_ahead),
                    str(queue_ahead),
                    str(quantity),
                    str(quantity),
                    timestamp,
                ),
            )
        return order_id

    def open_paper_orders(self, asset_id: str | None = None) -> list[PaperOrder]:
        clause = "AND asset_id=?" if asset_id else ""
        params: tuple[Any, ...] = (asset_id,) if asset_id else ()
        rows = self.connection.execute(
            f"SELECT * FROM paper_orders WHERE status='OPEN' {clause} ORDER BY order_id",
            params,
        ).fetchall()
        return [self._paper_order(row) for row in rows]

    def paper_order_status(self, order_id: int) -> str | None:
        row = self.connection.execute(
            "SELECT status FROM paper_orders WHERE order_id=?", (order_id,)
        ).fetchone()
        return str(row["status"]) if row else None

    def cancel_paper_orders(
        self,
        *,
        reason: str,
        cancelled_at: datetime,
        condition_id: str | None = None,
        asset_id: str | None = None,
        side: str | None = None,
    ) -> int:
        filters = ["status='OPEN'"]
        values: list[Any] = [_iso(cancelled_at), reason]
        for column, value in (
            ("condition_id", condition_id),
            ("asset_id", asset_id),
            ("side", side),
        ):
            if value is not None:
                filters.append(f"{column}=?")
                values.append(value)
        with self.connection:
            cursor = self.connection.execute(
                f"UPDATE paper_orders SET status='CANCELLED',cancelled_at=?,cancel_reason=? WHERE {' AND '.join(filters)}",
                values,
            )
        return int(cursor.rowcount)

    def apply_paper_fill(
        self,
        order_id: int,
        *,
        trigger_event_hash: str,
        trigger_price: Decimal,
        filled_at: datetime,
        best_bid: Decimal,
    ) -> bool:
        timestamp = _iso(filled_at)
        with self.connection:
            if self.paper_trigger_seen(trigger_event_hash):
                return False
            row = self.connection.execute(
                "SELECT * FROM paper_orders WHERE order_id=?", (order_id,)
            ).fetchone()
            if row is None:
                raise ValueError("paper_order_not_found")
            if row["status"] != "OPEN":
                return False
            quantity = Decimal(row["quantity_text"])
            price = Decimal(row["price_text"])
            gross = price * quantity
            fee = Decimal("0")
            position_row = self.connection.execute(
                "SELECT * FROM paper_positions WHERE asset_id=?", (row["asset_id"],)
            ).fetchone()
            current_quantity = (
                Decimal(position_row["quantity_text"]) if position_row else Decimal("0")
            )
            current_cost = (
                Decimal(position_row["cost_basis_text"]) if position_row else Decimal("0")
            )
            realized = (
                Decimal(position_row["realized_profit_text"])
                if position_row
                else Decimal("0")
            )
            if row["side"] == "BUY":
                next_quantity = current_quantity + quantity
                next_cost = current_cost + gross + fee
            else:
                if quantity > current_quantity:
                    raise ValueError("paper_short_not_allowed")
                average = current_cost / current_quantity if current_quantity else Decimal("0")
                removed_cost = average * quantity
                next_quantity = current_quantity - quantity
                next_cost = current_cost - removed_cost
                realized += gross - fee - removed_cost
            self.connection.execute(
                """
                INSERT INTO paper_positions(
                    asset_id,condition_id,market_id,outcome,quantity_text,cost_basis_text,
                    realized_profit_text,mark_price_text,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    quantity_text=excluded.quantity_text,
                    cost_basis_text=excluded.cost_basis_text,
                    realized_profit_text=excluded.realized_profit_text,
                    mark_price_text=excluded.mark_price_text,
                    updated_at=excluded.updated_at
                """,
                (
                    row["asset_id"],
                    row["condition_id"],
                    row["market_id"],
                    row["outcome"],
                    str(next_quantity),
                    str(next_cost),
                    str(realized),
                    str(best_bid),
                    timestamp,
                ),
            )
            self.connection.execute(
                """
                INSERT INTO paper_fills(
                    order_id,trigger_event_hash,trigger_price_text,fill_price_text,quantity_text,
                    gross_amount_text,fee_text,filled_at,position_quantity_after_text,
                    realized_profit_after_text
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    order_id,
                    trigger_event_hash,
                    str(trigger_price),
                    str(price),
                    str(quantity),
                    str(gross),
                    str(fee),
                    timestamp,
                    str(next_quantity),
                    str(realized),
                ),
            )
            self.connection.execute(
                "UPDATE paper_orders SET status='FILLED',filled_at=? WHERE order_id=?",
                (timestamp, order_id),
            )
            self._insert_paper_account_snapshot(timestamp)
        return True

    def consume_paper_trade(
        self,
        order_id: int,
        *,
        trigger_event_hash: str,
        trigger_price: Decimal,
        official_trade_quantity: Decimal,
        proof_type: str,
        filled_at: datetime,
        best_bid: Decimal,
    ) -> Decimal:
        if proof_type not in {"AT_PRICE_QUEUE", "TRADE_THROUGH"}:
            raise ValueError("invalid_paper_fill_proof")
        if official_trade_quantity <= 0:
            raise ValueError("invalid_official_trade_quantity")
        timestamp = _iso(filled_at)
        with self.connection:
            if self.paper_trigger_seen(trigger_event_hash):
                return Decimal("0")
            row = self.connection.execute(
                "SELECT * FROM paper_orders WHERE order_id=?", (order_id,)
            ).fetchone()
            if row is None:
                raise ValueError("paper_order_not_found")
            if row["status"] != "OPEN":
                return Decimal("0")
            queue_before = Decimal(row["queue_ahead_remaining_text"])
            order_before = Decimal(row["remaining_quantity_text"])
            if proof_type == "AT_PRICE_QUEUE":
                queue_consumed = min(queue_before, official_trade_quantity)
                queue_after = queue_before - queue_consumed
                fill_capacity = official_trade_quantity - queue_consumed
            else:
                queue_consumed = queue_before
                queue_after = Decimal("0")
                fill_capacity = order_before
            position_row = self.connection.execute(
                "SELECT * FROM paper_positions WHERE asset_id=?", (row["asset_id"],)
            ).fetchone()
            current_quantity = (
                Decimal(position_row["quantity_text"])
                if position_row is not None
                else Decimal("0")
            )
            if row["side"] == "SELL":
                fill_capacity = min(fill_capacity, current_quantity)
            fill_quantity = min(order_before, fill_capacity)
            order_after = order_before - fill_quantity
            self.connection.execute(
                """
                UPDATE paper_orders
                SET queue_ahead_remaining_text=?,remaining_quantity_text=?,
                    status=CASE WHEN ?='0' THEN 'FILLED' ELSE 'OPEN' END,
                    filled_at=CASE WHEN ?='0' THEN ? ELSE filled_at END
                WHERE order_id=?
                """,
                (
                    str(queue_after),
                    str(order_after),
                    str(order_after),
                    str(order_after),
                    timestamp,
                    order_id,
                ),
            )
            self.connection.execute(
                """
                INSERT INTO paper_queue_events(
                    order_id,event_type,trigger_event_hash,official_trade_quantity_text,
                    queue_before_text,queue_after_text,order_remaining_before_text,
                    order_remaining_after_text,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    order_id,
                    proof_type,
                    trigger_event_hash,
                    str(official_trade_quantity),
                    str(queue_before),
                    str(queue_after),
                    str(order_before),
                    str(order_after),
                    timestamp,
                ),
            )
            if fill_quantity == 0:
                return Decimal("0")
            price = Decimal(row["price_text"])
            gross = price * fill_quantity
            fee = Decimal("0")
            current_cost = (
                Decimal(position_row["cost_basis_text"])
                if position_row is not None
                else Decimal("0")
            )
            realized = (
                Decimal(position_row["realized_profit_text"])
                if position_row is not None
                else Decimal("0")
            )
            if row["side"] == "BUY":
                next_quantity = current_quantity + fill_quantity
                next_cost = current_cost + gross
            else:
                if fill_quantity > current_quantity:
                    raise ValueError("paper_short_not_allowed")
                average = current_cost / current_quantity if current_quantity else Decimal("0")
                removed_cost = average * fill_quantity
                next_quantity = current_quantity - fill_quantity
                next_cost = current_cost - removed_cost
                realized += gross - fee - removed_cost
            self.connection.execute(
                """
                INSERT INTO paper_positions(
                    asset_id,condition_id,market_id,outcome,quantity_text,cost_basis_text,
                    realized_profit_text,mark_price_text,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(asset_id) DO UPDATE SET
                    quantity_text=excluded.quantity_text,
                    cost_basis_text=excluded.cost_basis_text,
                    realized_profit_text=excluded.realized_profit_text,
                    mark_price_text=excluded.mark_price_text,
                    updated_at=excluded.updated_at
                """,
                (
                    row["asset_id"],
                    row["condition_id"],
                    row["market_id"],
                    row["outcome"],
                    str(next_quantity),
                    str(next_cost),
                    str(realized),
                    str(best_bid),
                    timestamp,
                ),
            )
            self.connection.execute(
                """
                INSERT INTO paper_fills(
                    order_id,trigger_event_hash,trigger_price_text,fill_price_text,quantity_text,
                    gross_amount_text,fee_text,filled_at,position_quantity_after_text,
                    realized_profit_after_text,official_trade_quantity_text,queue_consumed_text,
                    order_quantity_before_text,order_quantity_after_text,proof_type
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    order_id,
                    trigger_event_hash,
                    str(trigger_price),
                    str(price),
                    str(fill_quantity),
                    str(gross),
                    str(fee),
                    timestamp,
                    str(next_quantity),
                    str(realized),
                    str(official_trade_quantity),
                    str(queue_consumed),
                    str(order_before),
                    str(order_after),
                    proof_type,
                ),
            )
            self._insert_paper_account_snapshot(timestamp)
        return fill_quantity

    def paper_trigger_seen(self, trigger_event_hash: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1 FROM paper_fills WHERE trigger_event_hash=?
            UNION ALL
            SELECT 1 FROM paper_queue_events WHERE trigger_event_hash=?
            LIMIT 1
            """,
            (trigger_event_hash, trigger_event_hash),
        ).fetchone()
        return row is not None

    def mark_paper_position(
        self, asset_id: str, best_bid: Decimal, *, marked_at: datetime
    ) -> bool:
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE paper_positions SET mark_price_text=?,updated_at=? WHERE asset_id=?",
                (str(best_bid), _iso(marked_at), asset_id),
            )
            if cursor.rowcount:
                self._insert_paper_account_snapshot(_iso(marked_at))
        return bool(cursor.rowcount)

    def record_inventory_mark(
        self,
        *,
        asset_id: str,
        bid_levels: Iterable[tuple[Decimal, Decimal]],
        best_ask: Decimal,
        taker_fee_rate: Decimal,
        fee_exponent: int,
        book_timestamp: str | None,
        marked_at: datetime,
    ) -> PaperInventoryMark | None:
        position_row = self.connection.execute(
            "SELECT * FROM paper_positions WHERE asset_id=?", (asset_id,)
        ).fetchone()
        if position_row is None:
            return None
        quantity = Decimal(position_row["quantity_text"])
        cost_basis = Decimal(position_row["cost_basis_text"])
        levels = tuple(bid_levels)
        sale = executable_sale(
            quantity,
            levels,
            taker_fee_rate=taker_fee_rate,
            fee_exponent=fee_exponent,
        )
        average_cost = cost_basis / quantity if quantity else Decimal("0")
        liquidated_cost = average_cost * sale.sold_quantity
        depth_profit = sale.net_proceeds - liquidated_cost
        reference_row = self.connection.execute(
            """
            SELECT f.fill_price_text
            FROM paper_fills AS f
            JOIN paper_orders AS o ON o.order_id=f.order_id
            WHERE o.asset_id=? AND o.side='BUY'
            ORDER BY f.fill_id DESC LIMIT 1
            """,
            (asset_id,),
        ).fetchone()
        reference = (
            Decimal(reference_row["fill_price_text"])
            if reference_row is not None
            else None
        )
        sorted_levels = sorted(levels, key=lambda item: item[0], reverse=True)
        best_bid = sorted_levels[0][0] if sorted_levels else None
        best_bid_drift = (
            best_bid - reference
            if best_bid is not None and reference is not None
            else None
        )
        vwap_drift = (
            sale.vwap - reference
            if sale.vwap is not None and reference is not None
            else None
        )
        mark = PaperInventoryMark(
            asset_id=asset_id,
            liquidatable_quantity=sale.sold_quantity,
            liquidation_proceeds=sale.gross_proceeds,
            liquidation_vwap=sale.vwap,
            liquidation_fee=sale.fee,
            unliquidated_quantity=sale.unliquidated_quantity,
            depth_adjusted_unrealized_profit=depth_profit,
            reference_fill_price=reference,
            best_bid_drift=best_bid_drift,
            liquidation_vwap_drift=vwap_drift,
        )
        timestamp = _iso(marked_at)
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO paper_inventory_marks(
                    asset_id,condition_id,marked_at,book_timestamp,best_bid_text,best_ask_text,
                    liquidatable_quantity_text,liquidation_proceeds_text,liquidation_vwap_text,
                    liquidation_fee_text,unliquidated_quantity_text,
                    depth_adjusted_unrealized_profit_text,reference_fill_price_text,
                    best_bid_drift_text,liquidation_vwap_drift_text
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    asset_id,
                    position_row["condition_id"],
                    timestamp,
                    book_timestamp,
                    str(best_bid) if best_bid is not None else None,
                    str(best_ask),
                    str(mark.liquidatable_quantity),
                    str(mark.liquidation_proceeds),
                    str(mark.liquidation_vwap) if mark.liquidation_vwap is not None else None,
                    str(mark.liquidation_fee),
                    str(mark.unliquidated_quantity),
                    str(mark.depth_adjusted_unrealized_profit),
                    str(mark.reference_fill_price) if mark.reference_fill_price is not None else None,
                    str(mark.best_bid_drift) if mark.best_bid_drift is not None else None,
                    str(mark.liquidation_vwap_drift) if mark.liquidation_vwap_drift is not None else None,
                ),
            )
            if best_bid is not None:
                self.connection.execute(
                    "UPDATE paper_positions SET mark_price_text=?,updated_at=? WHERE asset_id=?",
                    (str(best_bid), timestamp, asset_id),
                )
            self._insert_paper_account_snapshot(timestamp)
        return mark

    def liquidate_paper_position(
        self,
        *,
        asset_id: str,
        bid_levels: Iterable[tuple[Decimal, Decimal]],
        taker_fee_rate: Decimal,
        fee_exponent: int,
        reason: str,
        book_timestamp: str | None,
        liquidated_at: datetime,
    ) -> DepthSale:
        position_row = self.connection.execute(
            "SELECT * FROM paper_positions WHERE asset_id=?", (asset_id,)
        ).fetchone()
        quantity = (
            Decimal(position_row["quantity_text"])
            if position_row is not None
            else Decimal("0")
        )
        empty = DepthSale(
            requested_quantity=quantity,
            sold_quantity=Decimal("0"),
            gross_proceeds=Decimal("0"),
            fee=Decimal("0"),
            net_proceeds=Decimal("0"),
            vwap=None,
            unliquidated_quantity=quantity,
            fills=(),
        )
        if position_row is None or quantity == 0:
            return empty
        batch_key = f"{asset_id}:{book_timestamp or 'none'}:{reason}"
        if self.connection.execute(
            "SELECT 1 FROM paper_liquidation_runs WHERE batch_key=?", (batch_key,)
        ).fetchone():
            return empty
        sale = executable_sale(
            quantity,
            bid_levels,
            taker_fee_rate=taker_fee_rate,
            fee_exponent=fee_exponent,
        )
        timestamp = _iso(liquidated_at)
        current_cost = Decimal(position_row["cost_basis_text"])
        average_cost = current_cost / quantity
        removed_cost = average_cost * sale.sold_quantity
        next_quantity = quantity - sale.sold_quantity
        next_cost = current_cost - removed_cost
        realized = Decimal(position_row["realized_profit_text"])
        realized += sale.net_proceeds - removed_cost
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO paper_liquidation_runs(
                    batch_key,asset_id,condition_id,reason,book_timestamp,
                    sold_quantity_text,unliquidated_quantity_text,created_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    batch_key,
                    asset_id,
                    position_row["condition_id"],
                    reason,
                    book_timestamp,
                    str(sale.sold_quantity),
                    str(sale.unliquidated_quantity),
                    timestamp,
                ),
            )
            self.connection.executemany(
                """
                INSERT INTO paper_liquidations(
                    asset_id,condition_id,reason,batch_key,price_text,quantity_text,
                    gross_proceeds_text,fee_text,liquidated_at,book_timestamp
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    (
                        asset_id,
                        position_row["condition_id"],
                        reason,
                        batch_key,
                        str(layer.price),
                        str(layer.quantity),
                        str(layer.gross_proceeds),
                        str(layer.fee),
                        timestamp,
                        book_timestamp,
                    )
                    for layer in sale.fills
                ),
            )
            self.connection.execute(
                """
                UPDATE paper_positions SET quantity_text=?,cost_basis_text=?,
                    realized_profit_text=?,updated_at=? WHERE asset_id=?
                """,
                (str(next_quantity), str(next_cost), str(realized), timestamp, asset_id),
            )
            self._insert_paper_account_snapshot(timestamp)
        return sale

    def paper_position(self, asset_id: str) -> PaperPosition:
        row = self.connection.execute(
            "SELECT * FROM paper_positions WHERE asset_id=?", (asset_id,)
        ).fetchone()
        if row is None:
            raise ValueError("paper_position_not_found")
        return self._paper_position(row)

    def paper_positions(self) -> list[PaperPosition]:
        return [
            self._paper_position(row)
            for row in self.connection.execute(
                "SELECT * FROM paper_positions ORDER BY condition_id,asset_id"
            )
        ]

    def paper_fill_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM paper_fills").fetchone()[0])

    def paper_liquidation_count(self) -> int:
        return int(
            self.connection.execute("SELECT COUNT(*) FROM paper_liquidations").fetchone()[0]
        )

    def paper_fill_rows(self) -> list[dict[str, str | int]]:
        rows = self.connection.execute(
            """
            SELECT f.fill_id,f.filled_at,o.condition_id,o.market_id,o.asset_id,o.outcome,
                   o.side,f.fill_price_text,f.quantity_text,f.gross_amount_text,
                   f.fee_text,f.trigger_price_text,f.position_quantity_after_text,
                   f.realized_profit_after_text,f.official_trade_quantity_text,
                   f.queue_consumed_text,f.order_quantity_before_text,
                   f.order_quantity_after_text,f.proof_type
            FROM paper_fills AS f
            JOIN paper_orders AS o ON o.order_id=f.order_id
            ORDER BY f.fill_id
            """
        ).fetchall()
        return [
            {key: int(row[key]) if key == "fill_id" else str(row[key]) for key in row.keys()}
            for row in rows
        ]

    def paper_account(self) -> PaperAccount:
        cash_rows = self.connection.execute(
            """
            SELECT o.side,f.gross_amount_text,f.fee_text
            FROM paper_fills f JOIN paper_orders o ON o.order_id=f.order_id
            """
        ).fetchall()
        buy = sum(
            (
                Decimal(row["gross_amount_text"]) + Decimal(row["fee_text"])
                for row in cash_rows
                if row["side"] == "BUY"
            ),
            Decimal("0"),
        )
        sell = sum(
            (
                Decimal(row["gross_amount_text"]) - Decimal(row["fee_text"])
                for row in cash_rows
                if row["side"] == "SELL"
            ),
            Decimal("0"),
        )
        sell += sum(
            (
                Decimal(row["gross_proceeds_text"]) - Decimal(row["fee_text"])
                for row in self.connection.execute(
                    "SELECT gross_proceeds_text,fee_text FROM paper_liquidations"
                )
            ),
            Decimal("0"),
        )
        positions = self.paper_positions()
        realized = sum((item.realized_profit for item in positions), Decimal("0"))
        unrealized = sum((item.unrealized_profit for item in positions), Decimal("0"))
        return PaperAccount(
            buy_cost=buy,
            sell_proceeds=sell,
            realized_profit=realized,
            unrealized_profit=unrealized,
            total_profit=realized + unrealized,
        )

    def _insert_paper_account_snapshot(self, timestamp: str) -> None:
        account = self.paper_account()
        self.connection.execute(
            """
            INSERT INTO paper_account_snapshots(
                created_at,buy_cost_text,sell_proceeds_text,realized_profit_text,
                unrealized_profit_text,total_profit_text
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                timestamp,
                str(account.buy_cost),
                str(account.sell_proceeds),
                str(account.realized_profit),
                str(account.unrealized_profit),
                str(account.total_profit),
            ),
        )

    @staticmethod
    def _paper_order(row: sqlite3.Row) -> PaperOrder:
        return PaperOrder(
            order_id=int(row["order_id"]),
            condition_id=str(row["condition_id"]),
            market_id=str(row["market_id"]),
            asset_id=str(row["asset_id"]),
            outcome=str(row["outcome"]),
            side=str(row["side"]),
            price=Decimal(row["price_text"]),
            quantity=Decimal(row["quantity_text"]),
            original_quantity=Decimal(row["original_quantity_text"]),
            remaining_quantity=Decimal(row["remaining_quantity_text"]),
            queue_ahead_initial=Decimal(row["queue_ahead_initial_text"]),
            queue_ahead_remaining=Decimal(row["queue_ahead_remaining_text"]),
            maker_fee_bps=int(row["maker_fee_bps"]),
            quote_book_timestamp=(
                str(row["quote_book_timestamp"])
                if row["quote_book_timestamp"] is not None
                else None
            ),
            status=str(row["status"]),
            created_at=str(row["created_at"]),
        )

    @staticmethod
    def _paper_position(row: sqlite3.Row) -> PaperPosition:
        return PaperPosition(
            asset_id=str(row["asset_id"]),
            condition_id=str(row["condition_id"]),
            market_id=str(row["market_id"]),
            outcome=str(row["outcome"]),
            quantity=Decimal(row["quantity_text"]),
            cost_basis=Decimal(row["cost_basis_text"]),
            realized_profit=Decimal(row["realized_profit_text"]),
            mark_price=Decimal(row["mark_price_text"]),
        )


def replay_events(events: Iterable[Mapping[str, Any]]) -> dict[str, OrderBookState]:
    books: dict[str, OrderBookState] = {}
    for payload in events:
        event_type = payload.get("event_type")
        if event_type == "book":
            asset_id = str(payload.get("asset_id") or "")
            if not asset_id:
                raise ValueError("book_missing_asset_id")
            book = books.setdefault(asset_id, OrderBookState(asset_id))
            book.apply(payload)
        elif event_type == "price_change":
            asset_ids = {
                str(change.get("asset_id") or "")
                for change in (payload.get("price_changes") or [])
                if change.get("asset_id")
            }
            for asset_id in asset_ids:
                book = books.get(asset_id)
                if book is None:
                    raise BookNotReady(asset_id)
                book.apply(payload)
    return books
