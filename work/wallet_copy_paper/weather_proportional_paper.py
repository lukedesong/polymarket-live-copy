#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
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

from paper_dashboard_statement import build_account_statement


D = Decimal
ZERO = D("0")
# External constraint: USD is displayed and reconciled at cent precision.
USD_CENT = D("0.01")
WEATHER_TAKER_FEE_RATE = D("0.05")
SOURCE_WALLET = "0x4989bfed5900ba096b08ba1f9b718464527c983e"
MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_RUNTIME_DIR = MODULE_DIR / "weather_proportional_runtime"
# User-specified paper capital. It is a cash buffer and never changes the fixed scale.
DEFAULT_INITIAL_CASH = D("300")
# Formula-derived scale: 5-share platform minimum / 12.5-share empirical source minimum.
DEFAULT_SCALE = D("0.4")
# Estimated polling interval, kept below the published public API rate limits.
DEFAULT_POLL_SECONDS = 1


class ReadOnlyViolation(RuntimeError):
    pass


def validate_public_request(method: str, url: str) -> None:
    parsed = urlparse(url)
    allowed = False
    if parsed.scheme == "https" and parsed.hostname == "data-api.polymarket.com":
        allowed = parsed.path == "/positions"
    elif parsed.scheme == "https" and parsed.hostname == "clob.polymarket.com":
        allowed = parsed.path == "/book" or parsed.path.startswith("/markets/")
    elif parsed.scheme == "https" and parsed.hostname == "gamma-api.polymarket.com":
        allowed = parsed.path == "/markets"
    if method.upper() != "GET" or not allowed:
        raise ReadOnlyViolation(f"public GET request rejected: {method} {url}")


def scaled_target(source_size: Decimal, anchor_size: Decimal, scale: Decimal) -> Decimal:
    return max(ZERO, source_size - anchor_size) * scale


def _decimal(value: Any) -> Decimal:
    return D(str(value))


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


def _is_temperature_position(position: dict[str, Any]) -> bool:
    return bool(position.get("asset")) and "temperature" in str(position.get("title", "")).lower()


def _walk_depth(levels: Iterable[dict[str, Any]], quantity: Decimal, ascending: bool) -> dict[str, Any] | None:
    ordered = sorted(
        levels,
        key=lambda level: _decimal(level["price"]),
        reverse=not ascending,
    )
    remaining = quantity
    notional = ZERO
    fee = ZERO
    fills: list[dict[str, str]] = []
    for level in ordered:
        price = _decimal(level["price"])
        available = _decimal(level["size"])
        take = min(remaining, available)
        if take <= ZERO:
            continue
        notional += take * price
        fee += take * WEATHER_TAKER_FEE_RATE * price * (D("1") - price)
        fills.append({"price": str(price), "quantity": str(take)})
        remaining -= take
        if remaining <= ZERO:
            break
    if remaining > ZERO:
        return None
    return {"notional": notional, "fee": fee, "fills": fills}


class PaperStore:
    def __init__(self, path: Path, initial_cash: Decimal, scale: Decimal):
        self.path = Path(path)
        self.initial_cash = initial_cash
        self.scale = scale

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
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
                outcome TEXT NOT NULL,
                quantity TEXT NOT NULL,
                average_cost TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observed_at INTEGER NOT NULL,
                kind TEXT NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                source_size TEXT NOT NULL,
                target_size TEXT NOT NULL,
                quantity TEXT NOT NULL,
                notional TEXT NOT NULL,
                fee TEXT NOT NULL,
                cash_before TEXT NOT NULL,
                cash_after TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS market_metadata (
                condition_id TEXT PRIMARY KEY,
                end_date_utc TEXT NOT NULL,
                fetched_at INTEGER NOT NULL
            );
            """
        )

    def initialize(self, source_positions: list[dict[str, Any]], observed_at: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            self._create_schema(conn)
            existing = conn.execute("SELECT value FROM config WHERE key = 'initialized'").fetchone()
            if existing:
                return
            config = {
                "initialized": "true",
                "initial_cash": str(self.initial_cash),
                "initial_cash_provenance": "user_specified_2026-07-24",
                "scale": str(self.scale),
                "scale_provenance": "formula_5_platform_shares_divided_by_12.5_source_shares",
                "scale_source_snapshot": "macau.weather_active_temperature_positions_2026-07-24",
                "paper_only": "true",
                "real_order_submitted": "false",
                "sizing_rule": "fixed_source_net_position_ratio",
                "buffer_cash_does_not_rescale": "true",
                "poll_seconds": str(DEFAULT_POLL_SECONDS),
                "poll_seconds_provenance": "estimated_and_checked_against_public_api_limits",
            }
            conn.executemany(
                "INSERT INTO config(key, value) VALUES (?, ?)",
                list(config.items()),
            )
            conn.execute(
                "INSERT INTO account(singleton, cash, total_fees, realized_pnl) VALUES (1, ?, '0', '0')",
                (str(self.initial_cash),),
            )
            for position in source_positions:
                if not _is_temperature_position(position):
                    continue
                size = _decimal(position["size"])
                conn.execute(
                    """
                    INSERT INTO source_state(
                        asset, condition_id, title, event_slug, outcome, end_date,
                        anchor_size, last_size, target_size, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', ?)
                    """,
                    (
                        str(position["asset"]),
                        str(position.get("conditionId", "")),
                        str(position.get("title", "")),
                        str(position.get("eventSlug", "")),
                        str(position.get("outcome", "")),
                        str(position.get("endDate", "")),
                        str(size),
                        str(size),
                        observed_at,
                    ),
                )
            conn.execute(
                "INSERT OR REPLACE INTO runtime_state(key, value) VALUES ('last_heartbeat', ?)",
                (str(observed_at),),
            )

    def is_initialized(self) -> bool:
        if not self.path.exists():
            return False
        with self._connect() as conn:
            self._create_schema(conn)
            return (
                conn.execute("SELECT 1 FROM config WHERE key = 'initialized'").fetchone()
                is not None
            )

    def source_rows(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
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
                    "SELECT * FROM paper_positions WHERE CAST(quantity AS REAL) > 0 ORDER BY asset"
                )
            ]

    def set_runtime_state(self, key: str, value: str) -> None:
        with self._connect() as conn:
            self._create_schema(conn)
            conn.execute(
                "INSERT OR REPLACE INTO runtime_state(key, value) VALUES (?, ?)",
                (key, value),
            )

    def config(self) -> dict[str, str]:
        with self._connect() as conn:
            return {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM config")}

    def cash(self) -> Decimal:
        with self._connect() as conn:
            row = conn.execute("SELECT cash FROM account WHERE singleton = 1").fetchone()
            return _decimal(row["cash"])

    def paper_quantity(self, asset: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT quantity FROM paper_positions WHERE asset = ?",
                (asset,),
            ).fetchone()
            return _decimal(row["quantity"]) if row else ZERO

    def source_anchor(self, asset: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT anchor_size FROM source_state WHERE asset = ?",
                (asset,),
            ).fetchone()
            return _decimal(row["anchor_size"]) if row else ZERO

    def target_quantity(self, asset: str) -> Decimal:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT target_size FROM source_state WHERE asset = ?",
                (asset,),
            ).fetchone()
            return _decimal(row["target_size"]) if row else ZERO

    def ledger_rows(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM ledger ORDER BY id")]

    def status(self) -> dict[str, Any]:
        cfg = self.config()
        with self._connect() as conn:
            account = conn.execute("SELECT * FROM account WHERE singleton = 1").fetchone()
            positions = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT paper_positions.*,
                           COALESCE(market_metadata.end_date_utc, source_state.end_date, '')
                               AS end_date_utc
                    FROM paper_positions
                    LEFT JOIN source_state
                        ON source_state.asset = paper_positions.asset
                    LEFT JOIN market_metadata
                        ON market_metadata.condition_id = paper_positions.condition_id
                    WHERE CAST(paper_positions.quantity AS REAL) > 0
                    ORDER BY paper_positions.title
                    """
                )
            ]
            heartbeat = conn.execute(
                "SELECT value FROM runtime_state WHERE key = 'last_heartbeat'"
            ).fetchone()
            last_error = conn.execute(
                "SELECT value FROM runtime_state WHERE key = 'last_error'"
            ).fetchone()
            recent = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT ledger.*,
                           COALESCE(market_metadata.end_date_utc, source_state.end_date, '')
                               AS end_date_utc
                    FROM ledger
                    LEFT JOIN source_state
                        ON source_state.asset = ledger.asset
                    LEFT JOIN market_metadata
                        ON market_metadata.condition_id = source_state.condition_id
                    ORDER BY ledger.id DESC
                    LIMIT 30
                    """
                )
            ]
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
        positions = [
            {
                **row,
                "occupied_cost": str(
                    _decimal(row["quantity"])
                    * _decimal(row["average_cost"])
                ),
                "position_status": "持仓中",
                "pnl_status": "尚未实现",
                "end_time_shanghai": format_end_time_shanghai(
                    str(row.get("end_date_utc", ""))
                ),
            }
            for row in positions
        ]
        recent = [
            {
                **row,
                "end_time_shanghai": format_end_time_shanghai(
                    str(row.get("end_date_utc", ""))
                ),
            }
            for row in recent
        ]
        occupied_capital = sum(
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
            "initial_cash": cfg["initial_cash"],
            "scale": cfg["scale"],
            "cash": account["cash"],
            "total_fees": account["total_fees"],
            "realized_pnl": account["realized_pnl"],
            "occupied_capital": str(occupied_capital),
            "positions": positions,
            "closed_records": statement["closed_records"],
            "reconstructed_realized_pnl": statement[
                "reconstructed_realized_pnl"
            ],
            "replay_errors": statement["replay_errors"],
            "pnl_reconciliation_ok": pnl_reconciliation_ok,
            "last_heartbeat": heartbeat["value"] if heartbeat else None,
            "last_error": last_error["value"] if last_error else "",
            "recent_ledger": recent,
        }


def _position_metadata(position: dict[str, Any] | None, prior: sqlite3.Row | None) -> tuple[str, ...]:
    if position:
        return (
            str(position.get("conditionId", "")),
            str(position.get("title", "")),
            str(position.get("eventSlug", "")),
            str(position.get("outcome", "")),
            str(position.get("endDate", "")),
        )
    if prior:
        return (
            str(prior["condition_id"]),
            str(prior["title"]),
            str(prior["event_slug"]),
            str(prior["outcome"]),
            str(prior["end_date"]),
        )
    return ("", "", "", "", "")


def _record_ledger(
    conn: sqlite3.Connection,
    *,
    observed_at: int,
    kind: str,
    asset: str,
    side: str,
    status: str,
    reason: str,
    source_size: Decimal,
    target_size: Decimal,
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
            observed_at, kind, asset, side, status, reason, source_size,
            target_size, quantity, notional, fee, cash_before, cash_after, detail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            observed_at,
            kind,
            asset,
            side,
            status,
            reason,
            str(source_size),
            str(target_size),
            str(quantity),
            str(notional),
            str(fee),
            str(cash_before),
            str(cash_after),
            json.dumps(detail, ensure_ascii=False, sort_keys=True, default=str),
        ),
    )


def apply_source_snapshot(
    store: PaperStore,
    source_positions: list[dict[str, Any]],
    books_by_asset: dict[str, dict[str, Any]],
    observed_at: int,
) -> list[dict[str, Any]]:
    current = {
        str(position["asset"]): position
        for position in source_positions
        if _is_temperature_position(position)
    }
    results: list[dict[str, Any]] = []
    with store._connect() as conn:
        store._create_schema(conn)
        prior_rows = {
            row["asset"]: row
            for row in conn.execute("SELECT * FROM source_state")
        }
        for asset in sorted(set(prior_rows) | set(current)):
            position = current.get(asset)
            prior = prior_rows.get(asset)
            source_size = _decimal(position["size"]) if position else ZERO
            anchor = _decimal(prior["anchor_size"]) if prior else ZERO
            last_size = _decimal(prior["last_size"]) if prior else ZERO
            if source_size < anchor:
                anchor = source_size
            target = scaled_target(source_size, anchor, store.scale)
            condition_id, title, event_slug, outcome, end_date = _position_metadata(position, prior)
            conn.execute(
                """
                INSERT INTO source_state(
                    asset, condition_id, title, event_slug, outcome, end_date,
                    anchor_size, last_size, target_size, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    condition_id = excluded.condition_id,
                    title = excluded.title,
                    event_slug = excluded.event_slug,
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
            if prior is not None and source_size == last_size:
                continue
            delta = target - held
            if delta == ZERO:
                continue
            side = "BUY" if delta > ZERO else "SELL"
            requested = abs(delta)
            book = books_by_asset.get(asset)
            cash_before = _decimal(
                conn.execute("SELECT cash FROM account WHERE singleton = 1").fetchone()["cash"]
            )
            if not book:
                reason = "MISSING_BOOK"
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    kind="REBALANCE",
                    asset=asset,
                    side=side,
                    status="SKIPPED",
                    reason=reason,
                    source_size=source_size,
                    target_size=target,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail={},
                )
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": reason,
                        "side": side,
                        "requested_quantity": requested,
                    }
                )
                continue
            minimum = _decimal(book["min_order_size"])
            if requested < minimum:
                reason = "BELOW_MIN_ORDER"
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    kind="REBALANCE",
                    asset=asset,
                    side=side,
                    status="SKIPPED",
                    reason=reason,
                    source_size=source_size,
                    target_size=target,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail={"minimum_order_size": str(minimum)},
                )
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": reason,
                        "side": side,
                        "requested_quantity": requested,
                    }
                )
                continue
            fill = _walk_depth(
                book["asks"] if side == "BUY" else book["bids"],
                requested,
                ascending=side == "BUY",
            )
            if not fill:
                reason = "INSUFFICIENT_VISIBLE_DEPTH"
                _record_ledger(
                    conn,
                    observed_at=observed_at,
                    kind="REBALANCE",
                    asset=asset,
                    side=side,
                    status="SKIPPED",
                    reason=reason,
                    source_size=source_size,
                    target_size=target,
                    quantity=ZERO,
                    notional=ZERO,
                    fee=ZERO,
                    cash_before=cash_before,
                    cash_after=cash_before,
                    detail={"minimum_order_size": str(minimum)},
                )
                results.append(
                    {
                        "status": "SKIPPED",
                        "reason": reason,
                        "side": side,
                        "requested_quantity": requested,
                    }
                )
                continue
            notional = fill["notional"]
            fee = fill["fee"]
            if side == "BUY":
                cash_after = cash_before - notional - fee
                if cash_after < ZERO:
                    reason = "INSUFFICIENT_CASH"
                    _record_ledger(
                        conn,
                        observed_at=observed_at,
                        kind="REBALANCE",
                        asset=asset,
                        side=side,
                        status="SKIPPED",
                        reason=reason,
                        source_size=source_size,
                        target_size=target,
                        quantity=ZERO,
                        notional=notional,
                        fee=fee,
                        cash_before=cash_before,
                        cash_after=cash_before,
                        detail=fill,
                    )
                    results.append(
                        {
                            "status": "SKIPPED",
                            "reason": reason,
                            "side": side,
                            "requested_quantity": requested,
                        }
                    )
                    continue
                old_cost = _decimal(paper_row["average_cost"]) if paper_row else ZERO
                old_quantity = held
                new_quantity = old_quantity + requested
                new_average = (
                    (old_cost * old_quantity + notional + fee) / new_quantity
                    if new_quantity > ZERO
                    else ZERO
                )
                realized_change = ZERO
            else:
                cash_after = cash_before + notional - fee
                new_quantity = held - requested
                old_cost = _decimal(paper_row["average_cost"]) if paper_row else ZERO
                new_average = old_cost if new_quantity > ZERO else ZERO
                realized_change = notional - fee - old_cost * requested
            account = conn.execute(
                "SELECT total_fees, realized_pnl FROM account WHERE singleton = 1"
            ).fetchone()
            conn.execute(
                "UPDATE account SET cash = ?, total_fees = ?, realized_pnl = ? WHERE singleton = 1",
                (
                    str(cash_after),
                    str(_decimal(account["total_fees"]) + fee),
                    str(_decimal(account["realized_pnl"]) + realized_change),
                ),
            )
            conn.execute(
                """
                INSERT INTO paper_positions(
                    asset, condition_id, title, event_slug, outcome,
                    quantity, average_cost, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset) DO UPDATE SET
                    condition_id = excluded.condition_id,
                    title = excluded.title,
                    event_slug = excluded.event_slug,
                    outcome = excluded.outcome,
                    quantity = excluded.quantity,
                    average_cost = excluded.average_cost,
                    updated_at = excluded.updated_at
                """,
                (
                    asset,
                    condition_id,
                    title,
                    event_slug,
                    outcome,
                    str(new_quantity),
                    str(new_average),
                    observed_at,
                ),
            )
            _record_ledger(
                conn,
                observed_at=observed_at,
                kind="REBALANCE",
                asset=asset,
                side=side,
                status="FILLED",
                reason="",
                source_size=source_size,
                target_size=target,
                quantity=requested,
                notional=notional,
                fee=fee,
                cash_before=cash_before,
                cash_after=cash_after,
                detail=fill,
            )
            results.append(
                {
                    "status": "FILLED",
                    "reason": "",
                    "side": side,
                    "quantity": requested,
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
            "SELECT cash, realized_pnl FROM account WHERE singleton = 1"
        ).fetchone()
        cash_before = _decimal(account["cash"])
        cash_after = cash_before + payout
        realized_change = payout - average_cost * quantity
        conn.execute(
            "UPDATE account SET cash = ?, realized_pnl = ? WHERE singleton = 1",
            (
                str(cash_after),
                str(_decimal(account["realized_pnl"]) + realized_change),
            ),
        )
        conn.execute(
            "UPDATE paper_positions SET quantity = '0', average_cost = '0', updated_at = ? WHERE asset = ?",
            (observed_at, asset),
        )
        _record_ledger(
            conn,
            observed_at=observed_at,
            kind="SETTLEMENT",
            asset=asset,
            side="",
            status="SETTLED",
            reason="WINNER" if winner else "LOSER",
            source_size=ZERO,
            target_size=ZERO,
            quantity=quantity,
            notional=payout,
            fee=ZERO,
            cash_before=cash_before,
            cash_after=cash_after,
            detail={"winner": winner},
        )
        return {"status": "SETTLED", "payout": payout}


def public_get_json(url: str, timeout: int = 20) -> Any:
    validate_public_request("GET", url)
    request = Request(url, headers={"User-Agent": "weather-proportional-paper/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _positions_url() -> str:
    query = urlencode(
        {
            "user": SOURCE_WALLET,
            "sizeThreshold": "0",
            "limit": "500",
        }
    )
    return f"https://data-api.polymarket.com/positions?{query}"


def _book_url(asset: str) -> str:
    return f"https://clob.polymarket.com/book?{urlencode({'token_id': asset})}"


def _market_url(condition_id: str) -> str:
    return f"https://clob.polymarket.com/markets/{condition_id}"


def _gamma_market_url(condition_id: str) -> str:
    return "https://gamma-api.polymarket.com/markets?" + urlencode(
        {"condition_ids": condition_id}
    )


def refresh_market_end_times(
    store: PaperStore,
    *,
    getter=public_get_json,
    observed_at: int,
) -> None:
    with store._connect() as conn:
        store._create_schema(conn)
        missing = [
            str(row["condition_id"])
            for row in conn.execute(
                """
                SELECT DISTINCT source_state.condition_id
                FROM source_state
                LEFT JOIN market_metadata
                    ON market_metadata.condition_id = source_state.condition_id
                WHERE source_state.condition_id <> ''
                  AND market_metadata.condition_id IS NULL
                ORDER BY source_state.condition_id
                """
            )
        ]
    resolved: list[tuple[str, str, int]] = []
    for condition_id in missing:
        try:
            payload = getter(_gamma_market_url(condition_id))
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        market = next(
            (
                row
                for row in payload
                if isinstance(row, dict)
                and str(row.get("conditionId", "")) == condition_id
            ),
            None,
        )
        end_date_utc = str(market.get("endDate", "")) if market else ""
        if end_date_utc:
            resolved.append((condition_id, end_date_utc, observed_at))
    if resolved:
        with store._connect() as conn:
            store._create_schema(conn)
            conn.executemany(
                """
                INSERT INTO market_metadata(condition_id, end_date_utc, fetched_at)
                VALUES (?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    end_date_utc = excluded.end_date_utc,
                    fetched_at = excluded.fetched_at
                """,
                resolved,
            )


def run_settlement_cycle(
    store: PaperStore,
    *,
    getter=public_get_json,
    observed_at: int,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for position in store.open_paper_positions():
        payload = getter(_market_url(str(position["condition_id"])))
        if not isinstance(payload, dict) or not payload.get("closed"):
            continue
        matching_token = next(
            (
                token
                for token in payload.get("tokens", [])
                if isinstance(token, dict)
                and str(token.get("token_id", "")) == str(position["asset"])
            ),
            None,
        )
        if not matching_token or not isinstance(matching_token.get("winner"), bool):
            continue
        result = settle_position(
            store,
            str(position["asset"]),
            winner=matching_token["winner"],
            observed_at=observed_at,
        )
        results.append(result)
    return results


def _needed_book_assets(store: PaperStore, source_positions: list[dict[str, Any]]) -> list[str]:
    current = {
        str(position["asset"]): position
        for position in source_positions
        if _is_temperature_position(position)
    }
    prior = store.source_rows()
    needed: list[str] = []
    for asset in sorted(set(current) | set(prior)):
        row = prior.get(asset)
        source_size = _decimal(current[asset]["size"]) if asset in current else ZERO
        anchor = _decimal(row["anchor_size"]) if row else ZERO
        last_size = _decimal(row["last_size"]) if row else ZERO
        if source_size < anchor:
            anchor = source_size
        target = scaled_target(source_size, anchor, store.scale)
        if source_size != last_size and target != store.paper_quantity(asset):
            needed.append(asset)
    return needed


def run_cycle(
    store: PaperStore,
    *,
    getter=public_get_json,
    observed_at: int,
) -> dict[str, Any]:
    payload = getter(_positions_url())
    if not isinstance(payload, list):
        raise ValueError("positions payload must be a list")
    source_positions = [
        position
        for position in payload
        if isinstance(position, dict) and _is_temperature_position(position)
    ]
    if not store.is_initialized():
        store.initialize(source_positions, observed_at=observed_at)
        return {"seeded": True, "results": [], "source_positions": len(source_positions)}
    books = {
        asset: getter(_book_url(asset))
        for asset in _needed_book_assets(store, source_positions)
    }
    results = apply_source_snapshot(
        store,
        source_positions,
        books_by_asset=books,
        observed_at=observed_at,
    )
    store.set_runtime_state("last_error", "")
    return {
        "seeded": False,
        "results": results,
        "source_positions": len(source_positions),
    }


def _atomic_text(path: Path, content: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def _format_usd(value: str) -> str:
    return f"{_decimal(value):,.2f}"


def acquire_process_lock(runtime_dir: Path):
    runtime_dir.mkdir(parents=True, exist_ok=True)
    lock_file = (runtime_dir / "daemon.lock").open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise RuntimeError("weather proportional paper daemon is already running") from exc
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    return lock_file


def render_status_files(
    store: PaperStore,
    runtime_dir: Path,
    poll_seconds: int,
    *,
    getter=None,
) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    if getter is not None:
        refresh_market_end_times(
            store,
            getter=getter,
            observed_at=int(time.time()),
        )
    status = store.status()
    status["source_wallet"] = SOURCE_WALLET
    status["poll_seconds"] = poll_seconds
    _atomic_text(
        runtime_dir / "status.json",
        json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True),
    )
    position_rows = "".join(
        "<tr>"
        f"<td>{escape(row['title'])}</td>"
        f"<td>{escape(row['outcome'])}</td>"
        f"<td>{escape(row['quantity'])}</td>"
        f"<td>{escape(row['average_cost'])}</td>"
        f"<td>{_format_usd(row['occupied_cost'])} USD</td>"
        f"<td class=\"neutral\">{escape(row['pnl_status'])}</td>"
        f"<td>{escape(row['position_status'])}</td>"
        f"<td>{escape(row['end_time_shanghai'])}</td>"
        "</tr>"
        for row in status["positions"]
    ) or '<tr><td colspan="8">当前没有持仓中项目</td></tr>'
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
        f"<td>{escape(row['kind'])}</td>"
        f"<td>{escape(row['side'])}</td>"
        f"<td>{escape(row['status'])}</td>"
        f"<td>{escape(row['reason'])}</td>"
        f"<td>{escape(row['quantity'])}</td>"
        f"<td>{escape(row['end_time_shanghai'])}</td>"
        "</tr>"
        for row in status["recent_ledger"]
    ) or '<tr><td colspan="7">尚无启动后动作</td></tr>'
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="{poll_seconds}">
  <title>macau.weather 等比例纸面实盘</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 28px; color: #17202a; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap: 12px; }}
    .card {{ padding: 16px; border: 1px solid #dfe6e9; border-radius: 12px; background: #f8fafc; }}
    .value {{ font-size: 24px; font-weight: 700; margin-top: 6px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ text-align: left; border-bottom: 1px solid #e5e7eb; padding: 9px 6px; }}
    .safe {{ color: #087f5b; font-weight: 700; }}
    .profit {{ color: #087f5b; font-weight: 700; }}
    .loss {{ color: #c92a2a; font-weight: 700; }}
    .neutral {{ color: #5f6b76; font-weight: 600; }}
    .warning {{ color: #c92a2a; font-weight: 700; }}
    .section-note {{ color: #5f6b76; margin-top: -6px; }}
  </style>
</head>
<body>
  <h1>macau.weather 等比例纸面实盘</h1>
  <p class="safe">paper_only={str(status['paper_only']).lower()} · real_order_submitted={str(status['real_order_submitted']).lower()}</p>
  <div class="cards">
    <div class="card">已实现盈亏（已结算/已卖出）<div class="value">{_format_usd(status['realized_pnl'])} USD</div></div>
    <div class="card">可用现金<div class="value">{_format_usd(status['cash'])} USD</div></div>
    <div class="card">占用资金（未结束持仓成本）<div class="value">{_format_usd(status['occupied_capital'])} USD</div></div>
    <div class="card">总纸面资金<div class="value">{escape(status['initial_cash'])} USD</div></div>
    <div class="card">固定缩放比例<div class="value">{escape(status['scale'])}</div></div>
    <div class="card">扫描频率<div class="value">{poll_seconds} 秒</div></div>
  </div>
  <p class="{'safe' if status['pnl_reconciliation_ok'] else 'warning'}">单笔已实现盈亏合计：{'已与账户累计盈亏核对' if status['pnl_reconciliation_ok'] else '账本待核对'}</p>
  <p>最后心跳：{escape(str(status['last_heartbeat']))}　最后错误：{escape(status['last_error'] or '无')}</p>
  <h2>持仓中</h2>
  <table><thead><tr><th>市场</th><th>方向</th><th>份数</th><th>含费平均成本</th><th>占用金额</th><th>盈亏</th><th>状态</th><th>结束时间（上海）</th></tr></thead><tbody>{position_rows}</tbody></table>
  <h2>已结束</h2>
  <p class="section-note">这里逐笔列出已经卖出的数量或已经结算的仓位；同一市场部分卖出后，也可能同时仍有持仓。</p>
  <table><thead><tr><th>结束时间</th><th>市场</th><th>方向</th><th>结束方式</th><th>份数</th><th>对应成本</th><th>净回款</th><th>费用</th><th>单笔盈亏</th></tr></thead><tbody>{closed_rows}</tbody></table>
  <h2>最近动作</h2>
  <table><thead><tr><th>时间</th><th>类型</th><th>方向</th><th>状态</th><th>原因</th><th>份数</th><th>结束时间（上海）</th></tr></thead><tbody>{ledger_rows}</tbody></table>
</body>
</html>
"""
    _atomic_text(runtime_dir / "status.html", html)


def run_daemon(
    store: PaperStore,
    *,
    runtime_dir: Path,
    poll_seconds: int,
    getter=public_get_json,
    sleeper=time.sleep,
    clock=time.time,
    max_cycles: int | None = None,
) -> None:
    completed_cycles = 0
    while max_cycles is None or completed_cycles < max_cycles:
        observed_at = int(clock())
        try:
            cycle = run_cycle(store, getter=getter, observed_at=observed_at)
            settlements = run_settlement_cycle(
                store,
                getter=getter,
                observed_at=observed_at,
            )
            store.set_runtime_state(
                "last_cycle_summary",
                json.dumps(
                    {"cycle": cycle, "settlements": settlements},
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                ),
            )
            store.set_runtime_state("last_error", "")
        except Exception as exc:
            if store.is_initialized():
                store.set_runtime_state(
                    "last_error",
                    f"{type(exc).__name__}: {exc}",
                )
        if store.is_initialized():
            render_status_files(
                store,
                runtime_dir,
                poll_seconds,
                getter=getter,
            )
        completed_cycles += 1
        if max_cycles is not None and completed_cycles >= max_cycles:
            break
        sleeper(poll_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="macau.weather fixed-ratio paper tracker")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="run one read-only paper cycle")
    mode.add_argument("--status", action="store_true", help="print the latest paper status")
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=DEFAULT_RUNTIME_DIR,
        help="runtime database and status directory",
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
            DEFAULT_POLL_SECONDS,
            getter=public_get_json,
        )
        print(json.dumps(store.status(), ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    lock_file = acquire_process_lock(runtime_dir)
    try:
        if args.once:
            observed_at = int(time.time())
            cycle = run_cycle(store, observed_at=observed_at)
            settlements = run_settlement_cycle(store, observed_at=observed_at)
            render_status_files(
                store,
                runtime_dir,
                DEFAULT_POLL_SECONDS,
                getter=public_get_json,
            )
            print(
                json.dumps(
                    {"cycle": cycle, "settlements": settlements},
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )
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
