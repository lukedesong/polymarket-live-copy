from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

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
"""


def _iso(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("storage_time_missing_timezone")
    return current.astimezone(timezone.utc).isoformat()


def _canonical(payload: Mapping[str, Any]) -> tuple[str, str]:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return text, hashlib.sha256(text.encode()).hexdigest()


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        self._books: dict[tuple[str, str], OrderBookState] = {}

    def close(self) -> None:
        self.connection.close()

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
