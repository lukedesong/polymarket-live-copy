#!/usr/bin/env python3
"""Read-only forward paper tracker for Tian-Wen's Trump speech-word sleeve."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any, Iterable, Iterator
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from paper_dashboard_statement import (
    build_account_statement,
    position_display_state,
)


D = Decimal
ZERO = D("0")
ONE = D("1")
# External constraint: USD is displayed and reconciled at cent precision.
USD_CENT = D("0.01")
SOURCE_WALLET = "0x66c1a6fe836ff555ca32848646acedbbe93bfa3f"
SOURCE_PROFILE = "https://polymarket.com/@tian-wen"
COPY_SCOPE = "tian_wen_trump_speech_words_only"
MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_RUNTIME_DIR = MODULE_DIR / "tian_wen_speech_runtime"

# External-constraint snapshot: all nine in-sleeve open markets returned mos=5
# from the public CLOB market-info endpoint on 2026-07-25.
CURRENT_MARKET_MINIMUM_SAMPLE = D("5")
# Empirical value: smallest current in-sleeve source net position at the
# 2026-07-25 onboarding snapshot. It is not a proven minimum future action.
SOURCE_POSITION_MINIMUM_SAMPLE = D("68.7")
# Formula-derived provisional position scale: 5 / 68.7.
DEFAULT_SCALE = CURRENT_MARKET_MINIMUM_SAMPLE / SOURCE_POSITION_MINIMUM_SAMPLE

# Empirical source-price cash-flow replay, before follower fees.
SOURCE_NO_FEE_CAPITAL_BOUND = D("2810.689707")
# Estimate: the source-price replay charges the currently observed 0.04 taker
# fee curve to every historical sleeve trade because historical order role and
# market-specific fee configuration are not reconstructable from the public rows.
SOURCE_FEE_ADJUSTED_CAPITAL_ESTIMATE = D("3539.229446142568099189494913")
# Formula-derived from the fee-adjusted estimate and provisional position scale,
# then rounded upward to the next USD cent.
THEORETICAL_MIN_CASH_ESTIMATE = D("257.59")
# User-specified paper cash on 2026-07-25. The extra buffer never changes scale.
DEFAULT_INITIAL_CASH = D("300")

# Formula-derived rate-limit use: one normal Data API request per second is
# 10 requests per 10 seconds, below the published 200 per 10 seconds limit.
DEFAULT_POLL_SECONDS = 1
# Estimated settlement refresh cadence; it affects paper cash release timing
# only and is recorded in config.
DEFAULT_SETTLEMENT_INTERVAL_SECONDS = 60
# External API pagination constraints used by the public user-trades endpoint.
TRADE_PAGE_SIZE = 1000
MAX_TRADE_PAGES = 10
# Estimated network timeout; it never changes an order because this module has
# no real-order path.
HTTP_TIMEOUT_SECONDS = 20


class ReadOnlyViolation(RuntimeError):
    pass


class CursorGap(RuntimeError):
    pass


def _decimal(value: Any) -> Decimal:
    return D(str(value))


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _scaled_target(source_delta: Decimal, scale: Decimal) -> Decimal:
    if scale == DEFAULT_SCALE:
        sampled_units = source_delta / SOURCE_POSITION_MINIMUM_SAMPLE
        if sampled_units == sampled_units.to_integral_value():
            return sampled_units * CURRENT_MARKET_MINIMUM_SAMPLE
    return source_delta * scale


def format_end_time_shanghai(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "未知"
    if "T" not in raw:
        return f"{raw}（原始数据仅含日期）"
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    local = parsed.astimezone(ZoneInfo("Asia/Shanghai"))
    return local.strftime("%Y-%m-%d %H:%M（上海时间）")


def is_speech_word_trade(item: dict[str, Any]) -> bool:
    """Return True only for Trump word/phrase prediction markets."""
    title = str(item.get("title", "")).strip().lower()
    if not title or not re.search(r"\b(?:donald\s+)?trump\b", title):
        return False
    if re.search(
        r"\b(?:truth social|tweet|post(?:ed|s|ing)?|attend(?:s|ed|ing)?|"
        r"meet(?:s|ing)?|visit(?:s|ed|ing)?|speak(?:s|ing)?\s+to|"
        r"talk(?:s|ing)?\s+to)\b",
        title,
    ):
        return False
    return bool(
        re.search(
            r"\b(?:say|says|said|mention|mentions|mentioned|utter|utters|"
            r"name|names|use\s+the\s+(?:word|phrase))\b",
            title,
        )
    )


def trade_row_id(row: dict[str, Any]) -> str:
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
    return hashlib.sha256(_json_text(canonical).encode("utf-8")).hexdigest()


def group_trade_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group public fragments sharing transaction, token, and direction."""
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    seen_rows: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or not is_speech_word_trade(row):
            continue
        row_id = trade_row_id(row)
        if row_id in seen_rows:
            continue
        seen_rows.add(row_id)
        transaction_hash = str(row.get("transactionHash", "")).lower()
        asset = str(row.get("asset", ""))
        side = str(row.get("side", "")).upper()
        if side not in {"BUY", "SELL"} or not asset:
            continue
        group_anchor = transaction_hash or row_id
        key = (group_anchor, asset, side)
        quantity = _decimal(row.get("size", "0"))
        price = _decimal(row.get("price", "0"))
        if quantity <= ZERO:
            continue
        if key not in groups:
            groups[key] = {
                "transaction_hash": transaction_hash,
                "asset": asset,
                "condition_id": str(row.get("conditionId", "")),
                "title": str(row.get("title", "")),
                "event_slug": str(row.get("eventSlug", "")),
                "slug": str(row.get("slug", "")),
                "outcome": str(row.get("outcome", "")),
                "end_date_utc": str(row.get("endDate", "")),
                "side": side,
                "source_timestamp": int(row.get("timestamp", 0)),
                "source_quantity": ZERO,
                "source_notional": ZERO,
                "row_ids": [],
            }
        group = groups[key]
        group["source_timestamp"] = max(
            int(group["source_timestamp"]),
            int(row.get("timestamp", 0)),
        )
        group["source_quantity"] += quantity
        group["source_notional"] += quantity * price
        if not group["end_date_utc"] and row.get("endDate"):
            group["end_date_utc"] = str(row["endDate"])
        group["row_ids"].append(row_id)

    actions: list[dict[str, Any]] = []
    for key, group in groups.items():
        quantity = group["source_quantity"]
        group["source_vwap"] = (
            group["source_notional"] / quantity if quantity > ZERO else ZERO
        )
        group["row_ids"] = sorted(group["row_ids"])
        group["action_id"] = hashlib.sha256(
            _json_text([*key, *group["row_ids"]]).encode("utf-8")
        ).hexdigest()
        actions.append(group)
    return sorted(
        actions,
        key=lambda action: (int(action["source_timestamp"]), str(action["action_id"])),
    )


def validate_public_request(method: str, url: str) -> None:
    parsed = urlparse(url)
    allowed = False
    if parsed.scheme == "https" and parsed.hostname == "data-api.polymarket.com":
        allowed = parsed.path in {"/positions", "/trades", "/activity"}
    elif parsed.scheme == "https" and parsed.hostname == "clob.polymarket.com":
        allowed = (
            parsed.path == "/book"
            or parsed.path.startswith("/clob-markets/")
            or parsed.path.startswith("/markets/")
        )
    elif parsed.scheme == "https" and parsed.hostname == "gamma-api.polymarket.com":
        allowed = parsed.path == "/markets"
    if method.upper() != "GET" or not allowed:
        raise ReadOnlyViolation(f"public GET request rejected: {method} {url}")


def public_get_json(url: str, timeout: int = HTTP_TIMEOUT_SECONDS) -> Any:
    validate_public_request("GET", url)
    request = Request(url, headers={"User-Agent": "tian-wen-speech-paper/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def walk_visible_depth(
    levels: Iterable[dict[str, Any]],
    *,
    requested: Decimal,
    ascending: bool,
    fee_rate: Decimal,
    fee_exponent: int,
) -> dict[str, Any]:
    ordered = sorted(
        levels,
        key=lambda level: _decimal(level["price"]),
        reverse=not ascending,
    )
    remaining = requested
    quantity = ZERO
    notional = ZERO
    fee = ZERO
    fills: list[dict[str, str]] = []
    for level in ordered:
        price = _decimal(level.get("price", "0"))
        available = _decimal(level.get("size", "0"))
        take = min(remaining, available)
        if take <= ZERO or price <= ZERO or price >= ONE:
            continue
        level_fee = take * fee_rate * ((price * (ONE - price)) ** fee_exponent)
        quantity += take
        notional += take * price
        fee += level_fee
        fills.append(
            {
                "price": str(price),
                "quantity": str(take),
                "fee": str(level_fee),
            }
        )
        remaining -= take
        if remaining <= ZERO:
            break
    return {
        "quantity": quantity,
        "requested_quantity": requested,
        "notional": notional,
        "fee": fee,
        "fills": fills,
        "fully_filled": quantity == requested,
    }


class PaperStore:
    def __init__(self, path: Path, *, initial_cash: Decimal, scale: Decimal):
        self.path = Path(path)
        self.initial_cash = initial_cash
        self.scale = scale

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                cash TEXT NOT NULL,
                total_fees TEXT NOT NULL,
                realized_pnl TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS source_state (
                asset TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                title TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                slug TEXT NOT NULL,
                outcome TEXT NOT NULL,
                end_date TEXT NOT NULL,
                anchor_size TEXT NOT NULL,
                last_size TEXT NOT NULL,
                target_size TEXT NOT NULL,
                observed_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                asset TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                title TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                slug TEXT NOT NULL,
                outcome TEXT NOT NULL,
                end_date_utc TEXT NOT NULL,
                quantity TEXT NOT NULL,
                average_cost TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS processed_source_trades (
                row_id TEXT PRIMARY KEY,
                source_timestamp INTEGER NOT NULL,
                in_sleeve INTEGER NOT NULL,
                action_id TEXT NOT NULL,
                seen_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at INTEGER NOT NULL,
                source_timestamp INTEGER NOT NULL,
                action_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                asset TEXT NOT NULL,
                title TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                source_quantity TEXT NOT NULL,
                source_price TEXT NOT NULL,
                source_size TEXT NOT NULL,
                target_size TEXT NOT NULL,
                requested_quantity TEXT NOT NULL,
                quantity TEXT NOT NULL,
                notional TEXT NOT NULL,
                fee TEXT NOT NULL,
                cash_before TEXT NOT NULL,
                cash_after TEXT NOT NULL,
                end_date_utc TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        for table in ("paper_positions", "ledger"):
            columns = {
                str(row["name"])
                for row in conn.execute(f"PRAGMA table_info({table})")
            }
            if "end_date_utc" not in columns:
                conn.execute(
                    f"ALTER TABLE {table} "
                    "ADD COLUMN end_date_utc TEXT NOT NULL DEFAULT ''"
                )

    def initialize(
        self,
        source_positions: list[dict[str, Any]],
        watermark_trades: list[dict[str, Any]],
        *,
        observed_at: int,
    ) -> None:
        with self._connect() as conn:
            self._create_schema(conn)
            if conn.execute(
                "SELECT 1 FROM config WHERE key = 'initialized'"
            ).fetchone():
                return
            config = {
                "initialized": "true",
                "paper_only": "true",
                "real_order_submitted": "false",
                "source_wallet": SOURCE_WALLET,
                "source_profile": SOURCE_PROFILE,
                "copy_scope": COPY_SCOPE,
                "initial_cash": str(self.initial_cash),
                "initial_cash_provenance_class": "user_specified",
                "initial_cash_provenance": "user_specified_2026-07-25",
                "scale": str(self.scale),
                "scale_provenance_class": "formula_derived",
                "scale_provenance": (
                    "current_market_mos_5_divided_by_"
                    "current_position_snapshot_min_68.7"
                ),
                "scale_status": "PROVISIONAL_POSITION_SNAPSHOT_BOUND",
                "data_block": "SOURCE_ORDER_ID_UNAVAILABLE",
                "source_position_minimum_sample": str(
                    SOURCE_POSITION_MINIMUM_SAMPLE
                ),
                "source_position_minimum_provenance_class": "empirical",
                "market_minimum_sample": str(CURRENT_MARKET_MINIMUM_SAMPLE),
                "market_minimum_provenance_class": "external_constraint",
                "source_no_fee_capital_bound": str(SOURCE_NO_FEE_CAPITAL_BOUND),
                "source_no_fee_capital_provenance_class": "empirical",
                "source_fee_adjusted_capital_estimate": str(
                    SOURCE_FEE_ADJUSTED_CAPITAL_ESTIMATE
                ),
                "source_fee_adjusted_capital_provenance_class": "estimate",
                "theoretical_min_cash_estimate": str(
                    THEORETICAL_MIN_CASH_ESTIMATE
                ),
                "theoretical_min_cash_provenance_class": "estimate",
                "buffer_cash_does_not_rescale": "true",
                "poll_seconds": str(DEFAULT_POLL_SECONDS),
                "poll_seconds_provenance_class": "formula_derived",
                "settlement_interval_seconds": str(DEFAULT_SETTLEMENT_INTERVAL_SECONDS),
                "settlement_interval_provenance_class": "estimate",
                "historical_audit_start": "2025-01-01T00:35:17Z",
                "historical_audit_end": "2026-07-24T16:38:49Z",
                "startup_policy": "seed_current_source_positions_and_trade_watermark_no_backfill",
                "execution_policy": "delayed_public_trade_then_current_clob_depth",
                "minimum_policy": "skip_below_current_market_minimum_never_round_up",
            }
            conn.executemany(
                "INSERT INTO config(key, value) VALUES (?, ?)",
                list(config.items()),
            )
            conn.execute(
                """
                INSERT INTO account(singleton, cash, total_fees, realized_pnl)
                VALUES (1, ?, '0', '0')
                """,
                (str(self.initial_cash),),
            )
            for position in source_positions:
                if not is_speech_word_trade(position) or not position.get("asset"):
                    continue
                size = _decimal(position.get("size", "0"))
                conn.execute(
                    """
                    INSERT OR REPLACE INTO source_state(
                        asset, condition_id, title, event_slug, slug, outcome, end_date,
                        anchor_size, last_size, target_size, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '0', ?)
                    """,
                    (
                        str(position["asset"]),
                        str(position.get("conditionId", "")),
                        str(position.get("title", "")),
                        str(position.get("eventSlug", "")),
                        str(position.get("slug", "")),
                        str(position.get("outcome", "")),
                        str(position.get("endDate", "")),
                        str(size),
                        str(size),
                        observed_at,
                    ),
                )
            for row in watermark_trades:
                if not isinstance(row, dict):
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_source_trades(
                        row_id, source_timestamp, in_sleeve, action_id, seen_at
                    ) VALUES (?, ?, ?, '', ?)
                    """,
                    (
                        trade_row_id(row),
                        int(row.get("timestamp", 0)),
                        1 if is_speech_word_trade(row) else 0,
                        observed_at,
                    ),
                )
            conn.executemany(
                "INSERT INTO runtime_state(key, value) VALUES (?, ?)",
                [
                    ("started_at", str(observed_at)),
                    ("last_heartbeat", str(observed_at)),
                    ("last_error", ""),
                    ("cursor_gap", "false"),
                ],
            )

    def is_initialized(self) -> bool:
        if not self.path.exists():
            return False
        with self._connect() as conn:
            self._create_schema(conn)
            return (
                conn.execute(
                    "SELECT 1 FROM config WHERE key = 'initialized'"
                ).fetchone()
                is not None
            )

    def config(self) -> dict[str, str]:
        with self._connect() as conn:
            self._create_schema(conn)
            return {
                row["key"]: row["value"]
                for row in conn.execute("SELECT key, value FROM config")
            }

    def set_runtime_state(self, key: str, value: str) -> None:
        with self._connect() as conn:
            self._create_schema(conn)
            conn.execute(
                "INSERT OR REPLACE INTO runtime_state(key, value) VALUES (?, ?)",
                (key, value),
            )

    def runtime_state(self) -> dict[str, str]:
        with self._connect() as conn:
            self._create_schema(conn)
            return {
                row["key"]: row["value"]
                for row in conn.execute("SELECT key, value FROM runtime_state")
            }

    def cash(self) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT cash FROM account WHERE singleton = 1"
            ).fetchone()
            return _decimal(row["cash"])

    def _state_decimal(self, asset: str, field: str) -> Decimal:
        if field not in {"anchor_size", "last_size", "target_size"}:
            raise ValueError(field)
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT {field} FROM source_state WHERE asset = ?",
                (asset,),
            ).fetchone()
            return _decimal(row[field]) if row else ZERO

    def source_anchor(self, asset: str) -> Decimal:
        return self._state_decimal(asset, "anchor_size")

    def source_size(self, asset: str) -> Decimal:
        return self._state_decimal(asset, "last_size")

    def target_quantity(self, asset: str) -> Decimal:
        return self._state_decimal(asset, "target_size")

    def paper_quantity(self, asset: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT quantity FROM paper_positions WHERE asset = ?",
                (asset,),
            ).fetchone()
            return _decimal(row["quantity"]) if row else ZERO

    def is_trade_processed(self, row_id: str) -> bool:
        with self._connect() as conn:
            self._create_schema(conn)
            return (
                conn.execute(
                    "SELECT 1 FROM processed_source_trades WHERE row_id = ?",
                    (row_id,),
                ).fetchone()
                is not None
            )

    def mark_processed_rows(
        self,
        rows: Iterable[dict[str, Any]],
        *,
        in_sleeve: bool,
        action_id: str,
        observed_at: int,
    ) -> None:
        with self._connect() as conn:
            self._create_schema(conn)
            for row in rows:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO processed_source_trades(
                        row_id, source_timestamp, in_sleeve, action_id, seen_at
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        trade_row_id(row),
                        int(row.get("timestamp", 0)),
                        1 if in_sleeve else 0,
                        action_id,
                        observed_at,
                    ),
                )

    def source_rows(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            self._create_schema(conn)
            return {
                row["asset"]: dict(row)
                for row in conn.execute("SELECT * FROM source_state")
            }

    def open_paper_positions(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self._connect() as conn:
            self._create_schema(conn)
            return [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT * FROM paper_positions
                    WHERE CAST(quantity AS REAL) > 0
                    ORDER BY title, outcome
                    """
                )
            ]

    def ledger_rows(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            self._create_schema(conn)
            return [
                dict(row)
                for row in conn.execute("SELECT * FROM ledger ORDER BY id")
            ]

    def status(self) -> dict[str, Any]:
        cfg = self.config()
        runtime = self.runtime_state()
        with self._connect() as conn:
            account = conn.execute(
                "SELECT * FROM account WHERE singleton = 1"
            ).fetchone()
            positions = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT * FROM paper_positions
                    WHERE CAST(quantity AS REAL) > 0
                    ORDER BY title, outcome
                    """
                )
            ]
            recent = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM ledger ORDER BY id DESC LIMIT 30"
                )
            ]
            counts = {
                row["status"]: int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM ledger GROUP BY status"
                )
            }
            processed = int(
                conn.execute(
                    "SELECT COUNT(*) AS count FROM processed_source_trades"
                ).fetchone()["count"]
            )
            ledger = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM ledger ORDER BY id"
                )
            ]
            metadata_by_asset = {
                str(row["asset"]): {
                    "title": str(row["title"]),
                    "outcome": str(row["outcome"]),
                }
                for row in conn.execute(
                    """
                    SELECT asset, title, outcome
                    FROM paper_positions
                    """
                )
            }
        statement = build_account_statement(
            ledger,
            metadata_by_asset=metadata_by_asset,
        )
        for row in positions:
            row["position_status"] = position_display_state(
                row.get("end_date_utc", "")
            )
            row["occupied_cost"] = str(
                _decimal(row["quantity"])
                * _decimal(row["average_cost"])
            )
            row["pnl_status"] = (
                "等待官方结算"
                if row["position_status"] == "待结算"
                else "尚未实现"
            )
            row["end_time_shanghai"] = format_end_time_shanghai(
                str(row.get("end_date_utc", ""))
            )
        active_positions = [
            row for row in positions
            if row["position_status"] == "持仓中"
        ]
        pending_positions = [
            row for row in positions
            if row["position_status"] == "待结算"
        ]
        for row in recent:
            row["end_time_shanghai"] = format_end_time_shanghai(
                str(row.get("end_date_utc", ""))
            )
        open_cost = sum(
            (
                _decimal(row["occupied_cost"])
                for row in positions
            ),
            ZERO,
        )
        pnl_reconciliation_ok = (
            not statement["replay_errors"]
            and _decimal(
                statement["reconstructed_realized_pnl"]
            ).quantize(USD_CENT)
            == _decimal(account["realized_pnl"]).quantize(USD_CENT)
        )
        return {
            "paper_only": cfg.get("paper_only") == "true",
            "real_order_submitted": cfg.get("real_order_submitted") == "true",
            "source_wallet": cfg["source_wallet"],
            "source_profile": cfg["source_profile"],
            "copy_scope": cfg["copy_scope"],
            "initial_cash": cfg["initial_cash"],
            "initial_cash_provenance_class": cfg[
                "initial_cash_provenance_class"
            ],
            "scale": cfg["scale"],
            "scale_provenance_class": cfg["scale_provenance_class"],
            "scale_status": cfg["scale_status"],
            "data_block": cfg["data_block"],
            "source_no_fee_capital_bound": cfg["source_no_fee_capital_bound"],
            "source_fee_adjusted_capital_estimate": cfg[
                "source_fee_adjusted_capital_estimate"
            ],
            "theoretical_min_cash_estimate": cfg[
                "theoretical_min_cash_estimate"
            ],
            "cash": account["cash"],
            "total_fees": account["total_fees"],
            "realized_pnl": account["realized_pnl"],
            "open_cost": str(open_cost),
            "occupied_capital": str(open_cost),
            "positions": positions,
            "active_positions": active_positions,
            "pending_positions": pending_positions,
            "closed_records": statement["closed_records"],
            "reconstructed_realized_pnl": statement[
                "reconstructed_realized_pnl"
            ],
            "replay_errors": statement["replay_errors"],
            "pnl_reconciliation_ok": pnl_reconciliation_ok,
            "recent_ledger": recent,
            "ledger_counts": counts,
            "processed_source_rows": processed,
            "last_heartbeat": runtime.get("last_heartbeat"),
            "last_error": runtime.get("last_error", ""),
            "cursor_gap": runtime.get("cursor_gap", "false") == "true",
            "last_cycle_summary": runtime.get("last_cycle_summary", ""),
        }


def _record_ledger(
    conn: sqlite3.Connection,
    *,
    observed_at: int,
    source_timestamp: int,
    action_id: str,
    kind: str,
    asset: str,
    title: str,
    side: str,
    status: str,
    reason: str,
    source_quantity: Decimal,
    source_price: Decimal,
    source_size: Decimal,
    target_size: Decimal,
    requested_quantity: Decimal,
    quantity: Decimal,
    notional: Decimal,
    fee: Decimal,
    cash_before: Decimal,
    cash_after: Decimal,
    detail: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO ledger(
            observed_at, source_timestamp, action_id, kind, asset, title, side,
            status, reason, source_quantity, source_price, source_size, target_size,
            requested_quantity, quantity, notional, fee, cash_before, cash_after,
            end_date_utc, detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            observed_at,
            source_timestamp,
            action_id,
            kind,
            asset,
            title,
            side,
            status,
            reason,
            str(source_quantity),
            str(source_price),
            str(source_size),
            str(target_size),
            str(requested_quantity),
            str(quantity),
            str(notional),
            str(fee),
            str(cash_before),
            str(cash_after),
            str(detail.get("end_date_utc", "")),
            _json_text(detail),
        ),
    )


def _action_metadata(
    action: dict[str, Any],
    prior: sqlite3.Row | None,
) -> tuple[str, str, str, str, str, str]:
    if action:
        action_end_date = str(
            action.get("end_date_utc") or action.get("endDate") or ""
        )
        if not action_end_date and prior:
            action_end_date = str(prior["end_date"])
        return (
            str(action.get("condition_id", "")),
            str(action.get("title", "")),
            str(action.get("event_slug", "")),
            str(action.get("slug", "")),
            str(action.get("outcome", "")),
            action_end_date,
        )
    if prior:
        return (
            str(prior["condition_id"]),
            str(prior["title"]),
            str(prior["event_slug"]),
            str(prior["slug"]),
            str(prior["outcome"]),
            str(prior["end_date"]),
        )
    return ("", "", "", "", "", "")


def _market_constraints(
    book: dict[str, Any] | None,
    market_info: dict[str, Any] | None,
) -> tuple[Decimal, Decimal, int] | None:
    if not book or not market_info:
        return None
    minimums: list[Decimal] = []
    if book.get("min_order_size") is not None:
        minimums.append(_decimal(book["min_order_size"]))
    if market_info.get("mos") is not None:
        minimums.append(_decimal(market_info["mos"]))
    fee_data = market_info.get("fd")
    if not minimums or not isinstance(fee_data, dict):
        return None
    taker_fee_enabled = bool(fee_data.get("to"))
    fee_rate = _decimal(fee_data.get("r", "0")) if taker_fee_enabled else ZERO
    fee_exponent = int(fee_data.get("e", 1))
    return max(minimums), fee_rate, fee_exponent


def _mark_action_rows(
    conn: sqlite3.Connection,
    action: dict[str, Any],
    *,
    observed_at: int,
) -> None:
    for row_id in action["row_ids"]:
        conn.execute(
            """
            INSERT OR IGNORE INTO processed_source_trades(
                row_id, source_timestamp, in_sleeve, action_id, seen_at
            ) VALUES (?, ?, 1, ?, ?)
            """,
            (
                row_id,
                int(action["source_timestamp"]),
                str(action["action_id"]),
                observed_at,
            ),
        )


def apply_source_actions(
    store: PaperStore,
    actions: list[dict[str, Any]],
    *,
    books_by_asset: dict[str, dict[str, Any]],
    market_info_by_condition: dict[str, dict[str, Any]],
    observed_at: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    with store._connect() as conn:
        store._create_schema(conn)
        for action in actions:
            row_ids = list(action.get("row_ids", []))
            if row_ids and all(
                conn.execute(
                    "SELECT 1 FROM processed_source_trades WHERE row_id = ?",
                    (row_id,),
                ).fetchone()
                for row_id in row_ids
            ):
                continue

            asset = str(action["asset"])
            prior = conn.execute(
                "SELECT * FROM source_state WHERE asset = ?",
                (asset,),
            ).fetchone()
            anchor = _decimal(prior["anchor_size"]) if prior else ZERO
            last_source = _decimal(prior["last_size"]) if prior else ZERO
            source_quantity = _decimal(action["source_quantity"])
            source_price = _decimal(action["source_vwap"])
            source_side = str(action["side"]).upper()
            if source_side == "BUY":
                source_size = last_source + source_quantity
            else:
                source_size = max(ZERO, last_source - source_quantity)
                if source_size < anchor:
                    anchor = source_size
            target = _scaled_target(max(ZERO, source_size - anchor), store.scale)
            condition_id, title, event_slug, slug, outcome, end_date = _action_metadata(
                action,
                prior,
            )
            conn.execute(
                """
                INSERT INTO source_state(
                    asset, condition_id, title, event_slug, slug, outcome, end_date,
                    anchor_size, last_size, target_size, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    condition_id = excluded.condition_id,
                    title = excluded.title,
                    event_slug = excluded.event_slug,
                    slug = excluded.slug,
                    outcome = excluded.outcome,
                    end_date = excluded.end_date,
                    anchor_size = excluded.anchor_size,
                    last_size = excluded.last_size,
                    target_size = excluded.target_size,
                    observed_at = excluded.observed_at
                """,
                (
                    asset,
                    condition_id,
                    title,
                    event_slug,
                    slug,
                    outcome,
                    end_date,
                    str(anchor),
                    str(source_size),
                    str(target),
                    observed_at,
                ),
            )
            paper_row = conn.execute(
                "SELECT * FROM paper_positions WHERE asset = ?",
                (asset,),
            ).fetchone()
            held = _decimal(paper_row["quantity"]) if paper_row else ZERO
            delta = target - held
            side = "BUY" if delta > ZERO else "SELL"
            requested = abs(delta)
            account = conn.execute(
                "SELECT * FROM account WHERE singleton = 1"
            ).fetchone()
            cash_before = _decimal(account["cash"])
            base_detail = {
                "transaction_hash": action.get("transaction_hash", ""),
                "row_ids": row_ids,
                "source_side": source_side,
                "source_notional": str(action.get("source_notional", ZERO)),
                "end_date_utc": end_date,
            }

            if requested == ZERO:
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    source_timestamp=int(action["source_timestamp"]),
                    action_id=str(action["action_id"]),
                    kind="SOURCE_ACTION",
                    asset=asset,
                    title=title,
                    side="",
                    status="SKIPPED",
                    reason="NO_TARGET_CHANGE",
                    source_quantity=source_quantity,
                    source_price=source_price,
                    source_size=source_size,
                    target_size=target,
                    requested_quantity=ZERO,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail=base_detail,
                )
                _mark_action_rows(conn, action, observed_at=observed_at)
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": "NO_TARGET_CHANGE",
                        "side": "",
                        "requested_quantity": ZERO,
                        "quantity": ZERO,
                    }
                )
                continue

            book = books_by_asset.get(asset)
            market_info = market_info_by_condition.get(condition_id)
            constraints = _market_constraints(book, market_info)
            if constraints is None:
                reason = "MISSING_BOOK_OR_MARKET_CONSTRAINTS"
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    source_timestamp=int(action["source_timestamp"]),
                    action_id=str(action["action_id"]),
                    kind="REBALANCE",
                    asset=asset,
                    title=title,
                    side=side,
                    status="SKIPPED",
                    reason=reason,
                    source_quantity=source_quantity,
                    source_price=source_price,
                    source_size=source_size,
                    target_size=target,
                    requested_quantity=requested,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail=base_detail,
                )
                _mark_action_rows(conn, action, observed_at=observed_at)
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": reason,
                        "side": side,
                        "requested_quantity": requested,
                        "quantity": ZERO,
                    }
                )
                continue

            minimum, fee_rate, fee_exponent = constraints
            base_detail.update(
                {
                    "minimum_order_size": str(minimum),
                    "fee_rate": str(fee_rate),
                    "fee_exponent": fee_exponent,
                    "book_timestamp": str(book.get("timestamp", "")),
                    "book_hash": str(book.get("hash", "")),
                }
            )
            if requested < minimum:
                reason = "BELOW_MIN_ORDER"
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    source_timestamp=int(action["source_timestamp"]),
                    action_id=str(action["action_id"]),
                    kind="REBALANCE",
                    asset=asset,
                    title=title,
                    side=side,
                    status="SKIPPED",
                    reason=reason,
                    source_quantity=source_quantity,
                    source_price=source_price,
                    source_size=source_size,
                    target_size=target,
                    requested_quantity=requested,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail=base_detail,
                )
                _mark_action_rows(conn, action, observed_at=observed_at)
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": reason,
                        "side": side,
                        "requested_quantity": requested,
                        "quantity": ZERO,
                    }
                )
                continue

            levels = book.get("asks", []) if side == "BUY" else book.get("bids", [])
            fill = walk_visible_depth(
                levels,
                requested=requested,
                ascending=side == "BUY",
                fee_rate=fee_rate,
                fee_exponent=fee_exponent,
            )
            quantity = _decimal(fill["quantity"])
            if quantity < minimum:
                reason = "INSUFFICIENT_VISIBLE_DEPTH"
                detail = {**base_detail, **fill}
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    source_timestamp=int(action["source_timestamp"]),
                    action_id=str(action["action_id"]),
                    kind="REBALANCE",
                    asset=asset,
                    title=title,
                    side=side,
                    status="SKIPPED",
                    reason=reason,
                    source_quantity=source_quantity,
                    source_price=source_price,
                    source_size=source_size,
                    target_size=target,
                    requested_quantity=requested,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail=detail,
                )
                _mark_action_rows(conn, action, observed_at=observed_at)
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": reason,
                        "side": side,
                        "requested_quantity": requested,
                        "quantity": ZERO,
                    }
                )
                continue

            notional = _decimal(fill["notional"])
            fee = _decimal(fill["fee"])
            if side == "BUY":
                cash_after = cash_before - notional - fee
                if cash_after < ZERO:
                    reason = "INSUFFICIENT_CASH"
                    _record_ledger(
                        conn,
                        observed_at=observed_at,
                        source_timestamp=int(action["source_timestamp"]),
                        action_id=str(action["action_id"]),
                        kind="REBALANCE",
                        asset=asset,
                        title=title,
                        side=side,
                        status="SKIPPED",
                        reason=reason,
                        source_quantity=source_quantity,
                        source_price=source_price,
                        source_size=source_size,
                        target_size=target,
                        requested_quantity=requested,
                        quantity=ZERO,
                        notional=notional,
                        fee=fee,
                        cash_before=cash_before,
                        cash_after=cash_before,
                        detail={**base_detail, **fill},
                    )
                    _mark_action_rows(conn, action, observed_at=observed_at)
                    results.append(
                        {
                            "status": "SKIPPED",
                            "reason": reason,
                            "side": side,
                            "requested_quantity": requested,
                            "quantity": ZERO,
                        }
                    )
                    continue
                old_cost = _decimal(paper_row["average_cost"]) if paper_row else ZERO
                new_quantity = held + quantity
                new_average = (
                    (old_cost * held + notional + fee) / new_quantity
                    if new_quantity > ZERO
                    else ZERO
                )
                realized_change = ZERO
            else:
                quantity = min(quantity, held)
                cash_after = cash_before + notional - fee
                old_cost = _decimal(paper_row["average_cost"]) if paper_row else ZERO
                new_quantity = held - quantity
                new_average = old_cost if new_quantity > ZERO else ZERO
                realized_change = notional - fee - old_cost * quantity

            status = "FILLED" if quantity == requested else "PARTIAL"
            conn.execute(
                """
                UPDATE account
                SET cash = ?, total_fees = ?, realized_pnl = ?
                WHERE singleton = 1
                """,
                (
                    str(cash_after),
                    str(_decimal(account["total_fees"]) + fee),
                    str(_decimal(account["realized_pnl"]) + realized_change),
                ),
            )
            conn.execute(
                """
                INSERT INTO paper_positions(
                    asset, condition_id, title, event_slug, slug, outcome,
                    end_date_utc, quantity, average_cost, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    condition_id = excluded.condition_id,
                    title = excluded.title,
                    event_slug = excluded.event_slug,
                    slug = excluded.slug,
                    outcome = excluded.outcome,
                    end_date_utc = excluded.end_date_utc,
                    quantity = excluded.quantity,
                    average_cost = excluded.average_cost,
                    updated_at = excluded.updated_at
                """,
                (
                    asset,
                    condition_id,
                    title,
                    event_slug,
                    slug,
                    outcome,
                    end_date,
                    str(new_quantity),
                    str(new_average),
                    observed_at,
                ),
            )
            _record_ledger(
                conn,
                observed_at=observed_at,
                source_timestamp=int(action["source_timestamp"]),
                action_id=str(action["action_id"]),
                kind="REBALANCE",
                asset=asset,
                title=title,
                side=side,
                status=status,
                reason="",
                source_quantity=source_quantity,
                source_price=source_price,
                source_size=source_size,
                target_size=target,
                requested_quantity=requested,
                quantity=quantity,
                notional=notional,
                fee=fee,
                cash_before=cash_before,
                cash_after=cash_after,
                detail={**base_detail, **fill},
            )
            _mark_action_rows(conn, action, observed_at=observed_at)
            results.append(
                {
                    "status": status,
                    "reason": "",
                    "side": side,
                    "requested_quantity": requested,
                    "quantity": quantity,
                    "notional": notional,
                    "fee": fee,
                }
            )
        conn.execute(
            "INSERT OR REPLACE INTO runtime_state(key, value) VALUES ('last_heartbeat', ?)",
            (str(observed_at),),
        )
    return results


def settle_position(
    store: PaperStore,
    asset: str,
    *,
    winner: bool,
    observed_at: int,
) -> dict[str, Any]:
    with store._connect() as conn:
        store._create_schema(conn)
        row = conn.execute(
            "SELECT * FROM paper_positions WHERE asset = ?",
            (asset,),
        ).fetchone()
        if not row or _decimal(row["quantity"]) <= ZERO:
            return {"status": "NO_POSITION", "payout": ZERO}
        quantity = _decimal(row["quantity"])
        average_cost = _decimal(row["average_cost"])
        payout = quantity if winner else ZERO
        account = conn.execute(
            "SELECT * FROM account WHERE singleton = 1"
        ).fetchone()
        cash_before = _decimal(account["cash"])
        cash_after = cash_before + payout
        realized_change = payout - average_cost * quantity
        conn.execute(
            """
            UPDATE account SET cash = ?, realized_pnl = ?
            WHERE singleton = 1
            """,
            (
                str(cash_after),
                str(_decimal(account["realized_pnl"]) + realized_change),
            ),
        )
        conn.execute(
            """
            UPDATE paper_positions
            SET quantity = '0', average_cost = '0', updated_at = ?
            WHERE asset = ?
            """,
            (observed_at, asset),
        )
        conn.execute(
            """
            UPDATE source_state
            SET anchor_size = '0', last_size = '0', target_size = '0',
                observed_at = ?
            WHERE asset = ?
            """,
            (observed_at, asset),
        )
        _record_ledger(
            conn,
            observed_at=observed_at,
            source_timestamp=0,
            action_id=f"settlement:{asset}:{observed_at}",
            kind="SETTLEMENT",
            asset=asset,
            title=str(row["title"]),
            side="",
            status="SETTLED",
            reason="WINNER" if winner else "LOSER",
            source_quantity=ZERO,
            source_price=ZERO,
            source_size=ZERO,
            target_size=ZERO,
            requested_quantity=quantity,
            quantity=quantity,
            notional=payout,
            fee=ZERO,
            cash_before=cash_before,
            cash_after=cash_after,
            detail={
                "winner": winner,
                "end_date_utc": str(row["end_date_utc"]),
            },
        )
        return {"status": "SETTLED", "payout": payout, "winner": winner}


def _positions_url() -> str:
    return "https://data-api.polymarket.com/positions?" + urlencode(
        {
            "user": SOURCE_WALLET,
            "sizeThreshold": "0",
            "limit": "500",
            "offset": "0",
        }
    )


def _trades_url(*, limit: int, offset: int) -> str:
    return "https://data-api.polymarket.com/trades?" + urlencode(
        {
            "user": SOURCE_WALLET,
            "takerOnly": "false",
            "limit": str(limit),
            "offset": str(offset),
        }
    )


def _book_url(asset: str) -> str:
    return "https://clob.polymarket.com/book?" + urlencode({"token_id": asset})


def _clob_market_url(condition_id: str) -> str:
    return f"https://clob.polymarket.com/clob-markets/{condition_id}"


def _resolution_market_url(condition_id: str) -> str:
    return f"https://clob.polymarket.com/markets/{condition_id}"


def _gamma_market_url(slug: str) -> str:
    return "https://gamma-api.polymarket.com/markets?" + urlencode(
        {"slug": slug}
    )


def _has_precise_end_date(value: Any) -> bool:
    return "T" in str(value or "")


def _enrich_end_date(
    item: dict[str, Any],
    *,
    getter,
    cache: dict[str, str],
) -> dict[str, Any]:
    enriched = dict(item)
    field = "end_date_utc" if "end_date_utc" in enriched else "endDate"
    current = str(enriched.get(field, ""))
    if _has_precise_end_date(current):
        return enriched
    slug = str(enriched.get("slug", "")).strip()
    if not slug:
        return enriched
    if slug not in cache:
        try:
            payload = getter(_gamma_market_url(slug))
        except Exception:
            payload = []
        market = (
            next((row for row in payload if isinstance(row, dict)), None)
            if isinstance(payload, list)
            else None
        )
        cache[slug] = str(market.get("endDate", "")) if market else ""
    if cache[slug]:
        enriched[field] = cache[slug]
    return enriched


def fetch_trade_window(
    store: PaperStore,
    *,
    getter=public_get_json,
    page_size: int = TRADE_PAGE_SIZE,
    max_pages: int = MAX_TRADE_PAGES,
) -> dict[str, Any]:
    collected: list[dict[str, Any]] = []
    collected_ids: set[str] = set()
    found_boundary = False
    pages = 0
    for page_index in range(max_pages):
        payload = getter(
            _trades_url(limit=page_size, offset=page_index * page_size)
        )
        pages += 1
        if not isinstance(payload, list):
            raise ValueError("trades payload must be a list")
        for row in payload:
            if not isinstance(row, dict):
                continue
            row_id = trade_row_id(row)
            if store.is_trade_processed(row_id):
                found_boundary = True
                break
            if row_id not in collected_ids:
                collected.append(row)
                collected_ids.add(row_id)
        if found_boundary or len(payload) < page_size:
            found_boundary = True
            break
    if not found_boundary:
        return {
            "rows": [],
            "cursor_gap": True,
            "pages": pages,
            "newest_timestamp": None,
        }
    newest = max((int(row.get("timestamp", 0)) for row in collected), default=None)
    return {
        "rows": collected,
        "cursor_gap": False,
        "pages": pages,
        "newest_timestamp": newest,
    }


def run_settlement_cycle(
    store: PaperStore,
    *,
    getter=public_get_json,
    observed_at: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for position in store.open_paper_positions():
        payload = getter(
            _resolution_market_url(str(position["condition_id"]))
        )
        if not isinstance(payload, dict) or not payload.get("closed"):
            continue
        matching = next(
            (
                token
                for token in payload.get("tokens", [])
                if isinstance(token, dict)
                and str(token.get("token_id", "")) == str(position["asset"])
            ),
            None,
        )
        if not matching or not isinstance(matching.get("winner"), bool):
            continue
        results.append(
            settle_position(
                store,
                str(position["asset"]),
                winner=matching["winner"],
                observed_at=observed_at,
            )
        )
    return results


def run_cycle(
    store: PaperStore,
    *,
    getter=public_get_json,
    observed_at: int,
) -> dict[str, Any]:
    if not store.is_initialized():
        positions_payload = getter(_positions_url())
        trades_payload = getter(
            _trades_url(limit=TRADE_PAGE_SIZE, offset=0)
        )
        if not isinstance(positions_payload, list):
            raise ValueError("positions payload must be a list")
        if not isinstance(trades_payload, list):
            raise ValueError("trades payload must be a list")
        end_date_cache: dict[str, str] = {}
        source_positions = [
            _enrich_end_date(row, getter=getter, cache=end_date_cache)
            for row in positions_payload
            if isinstance(row, dict) and is_speech_word_trade(row)
        ]
        watermark = [row for row in trades_payload if isinstance(row, dict)]
        store.initialize(
            source_positions,
            watermark,
            observed_at=observed_at,
        )
        return {
            "seeded": True,
            "blocked": False,
            "results": [],
            "source_positions": len(source_positions),
            "watermark_rows": len(watermark),
        }

    window = fetch_trade_window(store, getter=getter)
    if window["cursor_gap"]:
        store.set_runtime_state("cursor_gap", "true")
        store.set_runtime_state(
            "last_error",
            "BLOCK_CURSOR_GAP: public trade history exceeded bounded catch-up window",
        )
        store.set_runtime_state("last_heartbeat", str(observed_at))
        return {
            "seeded": False,
            "blocked": True,
            "block_reason": "CURSOR_GAP",
            "results": [],
            "pages": window["pages"],
        }

    store.set_runtime_state("cursor_gap", "false")
    rows = window["rows"]
    irrelevant = [row for row in rows if not is_speech_word_trade(row)]
    if irrelevant:
        store.mark_processed_rows(
            irrelevant,
            in_sleeve=False,
            action_id="",
            observed_at=observed_at,
        )
    sleeve_rows = [row for row in rows if is_speech_word_trade(row)]
    actions = group_trade_rows(sleeve_rows)
    prior_source_rows = store.source_rows()
    end_date_cache: dict[str, str] = {}
    enriched_actions: list[dict[str, Any]] = []
    for action in actions:
        prior_end_date = str(
            prior_source_rows.get(str(action["asset"]), {}).get("end_date", "")
        )
        if (
            not _has_precise_end_date(action.get("end_date_utc"))
            and _has_precise_end_date(prior_end_date)
        ):
            action = {**action, "end_date_utc": prior_end_date}
        enriched_actions.append(
            _enrich_end_date(action, getter=getter, cache=end_date_cache)
        )
    actions = enriched_actions
    books: dict[str, dict[str, Any]] = {}
    market_info: dict[str, dict[str, Any]] = {}
    for action in actions:
        asset = str(action["asset"])
        condition_id = str(action["condition_id"])
        if asset not in books:
            payload = getter(_book_url(asset))
            if not isinstance(payload, dict):
                raise ValueError("book payload must be an object")
            books[asset] = payload
        if condition_id not in market_info:
            payload = getter(_clob_market_url(condition_id))
            if not isinstance(payload, dict):
                raise ValueError("market-info payload must be an object")
            market_info[condition_id] = payload
    results = apply_source_actions(
        store,
        actions,
        books_by_asset=books,
        market_info_by_condition=market_info,
        observed_at=observed_at,
    )
    store.set_runtime_state("last_error", "")
    store.set_runtime_state("last_heartbeat", str(observed_at))
    return {
        "seeded": False,
        "blocked": False,
        "results": results,
        "new_public_rows": len(rows),
        "sleeve_rows": len(sleeve_rows),
        "source_actions": len(actions),
        "pages": window["pages"],
    }


def _atomic_text(path: Path, content: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _format_usd(value: str) -> str:
    return f"{_decimal(value):,.2f}"


def render_status_files(
    store: PaperStore,
    runtime_dir: Path,
    *,
    poll_seconds: int,
) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    status = store.status()
    status["poll_seconds"] = poll_seconds
    _atomic_text(
        runtime_dir / "status.json",
        json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True),
    )
    active_position_rows = "".join(
        "<tr>"
        f"<td>{escape(str(row['title']))}</td>"
        f"<td>{escape(str(row['outcome']))}</td>"
        f"<td>{escape(str(row['quantity']))}</td>"
        f"<td>{escape(str(row['average_cost']))}</td>"
        f"<td>{_format_usd(row['occupied_cost'])} USD</td>"
        f"<td class=\"neutral\">{escape(row['pnl_status'])}</td>"
        f"<td>{escape(row['position_status'])}</td>"
        f"<td>{escape(str(row['end_time_shanghai']))}</td>"
        "</tr>"
        for row in status["active_positions"]
    ) or '<tr><td colspan="8">当前没有持仓中项目</td></tr>'
    pending_position_rows = "".join(
        "<tr>"
        f"<td>{escape(str(row['title']))}</td>"
        f"<td>{escape(str(row['outcome']))}</td>"
        f"<td>{escape(str(row['quantity']))}</td>"
        f"<td>{escape(str(row['average_cost']))}</td>"
        f"<td>{_format_usd(row['occupied_cost'])} USD</td>"
        f"<td class=\"neutral\">{escape(row['pnl_status'])}</td>"
        f"<td>{escape(row['position_status'])}</td>"
        f"<td>{escape(str(row['end_time_shanghai']))}</td>"
        "</tr>"
        for row in status["pending_positions"]
    ) or '<tr><td colspan="8">当前没有待结算项目</td></tr>'
    closed_rows = "".join(
        "<tr>"
        f"<td>{escape(row['close_time_shanghai'])}</td>"
        f"<td>{escape(row['title'])}</td>"
        f"<td>{escape(row['outcome'] or '—')}</td>"
        f"<td>{escape(row['close_type'])}</td>"
        f"<td>{escape(row['quantity'])}</td>"
        f"<td>{_format_usd(row['cost_basis'])} USD</td>"
        f"<td>{_format_usd(row['net_recovered'])} USD</td>"
        f"<td>{_format_usd(row['fee'])} USD</td>"
        f"<td class=\"{'profit' if _decimal(row['realized_pnl']) > ZERO else 'loss' if _decimal(row['realized_pnl']) < ZERO else 'neutral'}\">{_format_usd(row['realized_pnl'])} USD</td>"
        "</tr>"
        for row in status["closed_records"]
    ) or '<tr><td colspan="9">尚无已卖出或已结算记录</td></tr>'
    ledger_rows = "".join(
        "<tr>"
        f"<td>{escape(str(row['observed_at']))}</td>"
        f"<td>{escape(str(row['title']))}</td>"
        f"<td>{escape(str(row['side']))}</td>"
        f"<td>{escape(str(row['status']))}</td>"
        f"<td>{escape(str(row['reason'] or '—'))}</td>"
        f"<td>{escape(str(row['quantity']))}</td>"
        f"<td>{escape(str(row['end_time_shanghai']))}</td>"
        "</tr>"
        for row in status["recent_ledger"]
    ) or '<tr><td colspan="7">尚无启动后的源动作</td></tr>'
    safety_class = "safe" if not status["cursor_gap"] else "blocked"
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="{poll_seconds}">
  <title>Tian-Wen 言论策略等比例纸面实盘</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "PingFang SC", sans-serif; margin: 28px; color: #17202a; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(190px,1fr)); gap: 12px; }}
    .card {{ padding: 16px; border: 1px solid #dfe6e9; border-radius: 12px; background: #f8fafc; }}
    .value {{ font-size: 23px; font-weight: 700; margin-top: 6px; }}
    .safe {{ color: #087f5b; font-weight: 700; }}
    .blocked {{ color: #c92a2a; font-weight: 700; }}
    .profit {{ color: #087f5b; font-weight: 700; }}
    .loss {{ color: #c92a2a; font-weight: 700; }}
    .neutral {{ color: #5f6b76; font-weight: 600; }}
    .warning {{ color: #c92a2a; font-weight: 700; }}
    .note {{ padding: 12px 14px; background: #fff8e1; border-radius: 10px; }}
    .section-note {{ color: #5f6b76; margin-top: -6px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ text-align: left; border-bottom: 1px solid #e5e7eb; padding: 9px 6px; vertical-align: top; }}
  </style>
</head>
<body>
  <h1>Tian-Wen / Trump speech words</h1>
  <p class="{safety_class}">paper_only={str(status['paper_only']).lower()} · real_order_submitted={str(status['real_order_submitted']).lower()} · cursor_gap={str(status['cursor_gap']).lower()}</p>
  <div class="cards">
    <div class="card">已实现盈亏（已结算/已卖出）<div class="value">{_format_usd(status['realized_pnl'])} USD</div></div>
    <div class="card">可用现金<div class="value">{_format_usd(status['cash'])} USD</div></div>
    <div class="card">占用资金（未结束持仓成本）<div class="value">{_format_usd(status['occupied_capital'])} USD</div></div>
    <div class="card">纸面总资金（用户指定）<div class="value">{_format_usd(status['initial_cash'])} USD</div></div>
    <div class="card">固定缩放比例<div class="value">{escape(status['scale'])}×</div></div>
    <div class="card">扫描频率<div class="value">{poll_seconds} 秒</div></div>
  </div>
  <p class="note">比例状态：{escape(status['scale_status'])}。固定比例由当前市场最低 5 份（外部约束值）÷ 当前源仓位样本最小 68.7 份（实证值）推导；公开数据没有源订单 ID，因此仍是暂定仓位快照边界。低于市场最低量的目标只记跳过，不向上补齐；300 USD 的额外缓冲不会改变比例。</p>
  <p>理论最低资金估算：{_format_usd(status['theoretical_min_cash_estimate'])} USD　累计纸面费用：{_format_usd(status['total_fees'])} USD　已实现纸面盈亏：{_format_usd(status['realized_pnl'])} USD</p>
  <p class="{'safe' if status['pnl_reconciliation_ok'] else 'warning'}">单笔已实现盈亏合计：{'已与账户累计盈亏核对' if status['pnl_reconciliation_ok'] else '账本待核对'}</p>
  <p>最后心跳：{escape(str(status['last_heartbeat']))}　最后错误：{escape(status['last_error'] or '无')}</p>
  <h2>持仓中</h2>
  <table><thead><tr><th>市场</th><th>方向</th><th>份数</th><th>含费平均成本</th><th>占用金额</th><th>盈亏</th><th>状态</th><th>结束时间（上海）</th></tr></thead><tbody>{active_position_rows}</tbody></table>
  <h2>待结算</h2>
  <p class="section-note">活动时间已过，等待 Polymarket 正式判定；在正式结算前仍占用纸面资金，不计算真实盈亏。</p>
  <table><thead><tr><th>市场</th><th>方向</th><th>份数</th><th>含费平均成本</th><th>占用金额</th><th>盈亏</th><th>状态</th><th>结束时间（上海）</th></tr></thead><tbody>{pending_position_rows}</tbody></table>
  <h2>已结束</h2>
  <p class="section-note">这里逐笔列出已经卖出的数量或已经结算的仓位；同一市场部分卖出后，也可能同时仍有持仓。</p>
  <table><thead><tr><th>结束时间</th><th>市场</th><th>方向</th><th>结束方式</th><th>份数</th><th>对应成本</th><th>净回款</th><th>费用</th><th>单笔盈亏</th></tr></thead><tbody>{closed_rows}</tbody></table>
  <h2>最近动作</h2>
  <table><thead><tr><th>观察时间</th><th>市场</th><th>我方方向</th><th>结果</th><th>原因</th><th>成交份数</th><th>结束时间（上海）</th></tr></thead><tbody>{ledger_rows}</tbody></table>
</body>
</html>
"""
    _atomic_text(runtime_dir / "status.html", html)


def acquire_process_lock(runtime_dir: Path):
    runtime_dir.mkdir(parents=True, exist_ok=True)
    lock_file = (runtime_dir / "daemon.lock").open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError("Tian-Wen speech paper daemon is already running") from exc
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def run_daemon(
    store: PaperStore,
    *,
    runtime_dir: Path,
    poll_seconds: int,
    settlement_interval_seconds: int = DEFAULT_SETTLEMENT_INTERVAL_SECONDS,
    getter=public_get_json,
    sleeper=time.sleep,
    clock=time.time,
    max_cycles: int | None = None,
) -> None:
    completed = 0
    last_settlement_at: int | None = None
    while max_cycles is None or completed < max_cycles:
        observed_at = int(clock())
        try:
            cycle = run_cycle(store, getter=getter, observed_at=observed_at)
            settlements: list[dict[str, Any]] = []
            if (
                store.is_initialized()
                and (
                    last_settlement_at is None
                    or observed_at - last_settlement_at >= settlement_interval_seconds
                )
            ):
                settlements = run_settlement_cycle(
                    store,
                    getter=getter,
                    observed_at=observed_at,
                )
                last_settlement_at = observed_at
            store.set_runtime_state(
                "last_cycle_summary",
                _json_text({"cycle": cycle, "settlements": settlements}),
            )
            if not cycle.get("blocked"):
                store.set_runtime_state("last_error", "")
        except Exception as exc:
            if store.is_initialized():
                store.set_runtime_state(
                    "last_error",
                    f"{type(exc).__name__}: {exc}",
                )
                store.set_runtime_state("last_heartbeat", str(observed_at))
        if store.is_initialized():
            render_status_files(
                store,
                runtime_dir,
                poll_seconds=poll_seconds,
            )
        completed += 1
        if max_cycles is not None and completed >= max_cycles:
            break
        sleeper(poll_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Tian-Wen Trump speech-word fixed-ratio paper tracker"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="run one public paper cycle")
    mode.add_argument("--status", action="store_true", help="print current paper status")
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=DEFAULT_RUNTIME_DIR,
        help="independent runtime database and status directory",
    )
    args = parser.parse_args(argv)
    runtime_dir = args.runtime_dir.resolve()
    store = PaperStore(
        runtime_dir / "paper.sqlite3",
        initial_cash=DEFAULT_INITIAL_CASH,
        scale=DEFAULT_SCALE,
    )
    if args.status:
        if not store.is_initialized():
            parser.error("paper tracker has not been initialized")
        render_status_files(
            store,
            runtime_dir,
            poll_seconds=DEFAULT_POLL_SECONDS,
        )
        print(json.dumps(store.status(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    lock_file = acquire_process_lock(runtime_dir)
    try:
        if args.once:
            observed_at = int(time.time())
            cycle = run_cycle(store, observed_at=observed_at)
            settlements = run_settlement_cycle(
                store,
                observed_at=observed_at,
            )
            render_status_files(
                store,
                runtime_dir,
                poll_seconds=DEFAULT_POLL_SECONDS,
            )
            print(
                _json_text({"cycle": cycle, "settlements": settlements})
            )
            return 0
        run_daemon(
            store,
            runtime_dir=runtime_dir,
            poll_seconds=DEFAULT_POLL_SECONDS,
        )
    finally:
        lock_file.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
