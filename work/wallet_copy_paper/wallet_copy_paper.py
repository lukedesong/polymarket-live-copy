#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import html
import json
import os
import sqlite3
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class AccountSpec:
    account_id: str
    username: str
    wallet: str
    starting_cash: Decimal


# The account count, wallet set, and cash are user-specified experiment inputs.
ACCOUNTS = (
    AccountSpec(
        "russell",
        "russell110320",
        "0x118689b24aead1d6e9507b8068d056b2ec4f051b",
        Decimal("100"),
    ),
    AccountSpec(
        "zorro",
        "ZorroDeLaVega",
        "0xaae9b2c5ad90e82b5068c7f8a4b491997633d661",
        Decimal("100"),
    ),
    AccountSpec(
        "sabsab",
        "sabsabinxz",
        "0xd3ecb2aee0d65622da559ff356b00e8c2e626603",
        Decimal("100"),
    ),
)


class ReadOnlyViolation(RuntimeError):
    pass


class DataUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class TradeRow:
    wallet: str
    transaction_hash: str
    asset: str
    condition_id: str
    side: str
    size: Decimal
    price: Decimal
    timestamp: int
    title: str
    outcome: str
    slug: str
    raw: Mapping[str, Any]


@dataclass(frozen=True)
class SourceAction:
    identity: str
    wallet: str
    transaction_hash: str
    asset: str
    condition_id: str
    side: str
    source_size: Decimal
    source_price: Decimal
    timestamp: int
    title: str
    outcome: str
    slug: str
    raw_rows: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class Book:
    asset: str
    condition_id: str
    timestamp: int
    book_hash: str
    min_order_size: Decimal
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True)
class MarketParams:
    condition_id: str
    min_order_size: Decimal
    fee_rate: Decimal
    fee_exponent: int


@dataclass(frozen=True)
class FillLeg:
    price: Decimal
    quantity: Decimal
    fee: Decimal


@dataclass(frozen=True)
class Decision:
    result: str
    side: str
    quantity: Decimal
    gross: Decimal
    fee: Decimal
    cash_delta: Decimal
    legs: tuple[FillLeg, ...]


def decimal_value(value: object, *, field: str, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"invalid_{field}:{value}") from error
    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        raise ValueError(f"invalid_{field}:{value}")
    return parsed


def parse_epoch(value: object) -> int:
    try:
        parsed = int(str(value))
    except ValueError as error:
        raise ValueError(f"invalid_timestamp:{value}") from error
    if parsed < 0:
        raise ValueError(f"invalid_timestamp:{value}")
    # Empirical API formats: Data API emits epoch seconds and CLOB books emit
    # epoch milliseconds. Preserve raw values elsewhere and normalize here.
    if parsed >= 1_000_000_000_000:
        parsed //= 1_000
    return parsed


def _allowed_path(host: str, path: str) -> bool:
    if host == "data-api.polymarket.com":
        return path == "/trades"
    if host == "gamma-api.polymarket.com":
        return path == "/markets"
    if host == "clob.polymarket.com":
        return path == "/book" or path.startswith("/clob-markets/")
    return False


def validate_public_request(method: str, url: str) -> None:
    parsed = urllib.parse.urlsplit(url)
    if method != "GET" or parsed.scheme != "https" or not parsed.hostname:
        raise ReadOnlyViolation(f"blocked_request:{method}:{url}")
    if parsed.username or parsed.password or not _allowed_path(parsed.hostname, parsed.path):
        raise ReadOnlyViolation(f"blocked_request:{method}:{url}")


def _default_transport(url: str) -> Any:
    validate_public_request("GET", url)
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"User-Agent": "wallet-copy-paper/1.0"},
    )
    # Operational estimate for a read-only paper monitor, not a trading rule.
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        raise DataUnavailable(f"public_get_failed:{url}:{error}") from error


class PublicClient:
    def __init__(self, transport: Callable[[str], Any] | None = None):
        self.transport = transport or _default_transport

    def _get(self, url: str) -> Any:
        validate_public_request("GET", url)
        return self.transport(url)

    def trades(self, wallet: str, *, limit: int = 100, offset: int = 0) -> list[TradeRow]:
        query = urllib.parse.urlencode(
            {
                "user": wallet,
                "takerOnly": "false",
                # The official endpoint's documented default page size.
                "limit": limit,
                "offset": offset,
            }
        )
        payload = self._get(f"https://data-api.polymarket.com/trades?{query}")
        if not isinstance(payload, list):
            raise DataUnavailable("trades_payload_not_list")
        return [self._trade_row(item) for item in payload]

    def book(self, asset: str) -> Book:
        query = urllib.parse.urlencode({"token_id": asset})
        payload = self._get(f"https://clob.polymarket.com/book?{query}")
        if not isinstance(payload, Mapping):
            raise DataUnavailable("book_payload_not_object")
        bids = tuple(
            sorted(
                (self._book_level(item) for item in payload.get("bids", [])),
                key=lambda level: level.price,
                reverse=True,
            )
        )
        asks = tuple(
            sorted(
                (self._book_level(item) for item in payload.get("asks", [])),
                key=lambda level: level.price,
            )
        )
        return Book(
            asset=str(payload.get("asset_id") or asset),
            condition_id=str(payload.get("market") or ""),
            timestamp=parse_epoch(payload.get("timestamp")),
            book_hash=str(payload.get("hash") or ""),
            min_order_size=decimal_value(
                payload.get("min_order_size"), field="min_order_size", positive=True
            ),
            bids=bids,
            asks=asks,
        )

    def market_params(self, condition_id: str) -> MarketParams:
        encoded = urllib.parse.quote(condition_id, safe="")
        payload = self._get(f"https://clob.polymarket.com/clob-markets/{encoded}")
        if not isinstance(payload, Mapping):
            raise DataUnavailable("market_params_payload_not_object")
        fee = payload.get("fd")
        if not isinstance(fee, Mapping) or fee.get("to") is not True:
            raise DataUnavailable("official_taker_fee_curve_missing")
        try:
            exponent = int(str(fee.get("e")))
        except ValueError as error:
            raise DataUnavailable("invalid_fee_exponent") from error
        if exponent < 0:
            raise DataUnavailable("invalid_fee_exponent")
        return MarketParams(
            condition_id=condition_id,
            min_order_size=decimal_value(
                payload.get("mos"), field="market_min_order_size", positive=True
            ),
            fee_rate=decimal_value(fee.get("r"), field="fee_rate"),
            fee_exponent=exponent,
        )

    @staticmethod
    def _book_level(payload: Mapping[str, Any]) -> BookLevel:
        return BookLevel(
            price=decimal_value(payload.get("price"), field="book_price", positive=True),
            quantity=decimal_value(payload.get("size"), field="book_size", positive=True),
        )

    @staticmethod
    def _trade_row(payload: Mapping[str, Any]) -> TradeRow:
        side = str(payload.get("side") or "").upper()
        if side not in {"BUY", "SELL"}:
            raise DataUnavailable(f"invalid_trade_side:{side}")
        return TradeRow(
            wallet=str(payload.get("proxyWallet") or "").lower(),
            transaction_hash=str(payload.get("transactionHash") or "").lower(),
            asset=str(payload.get("asset") or ""),
            condition_id=str(payload.get("conditionId") or ""),
            side=side,
            size=decimal_value(payload.get("size"), field="trade_size", positive=True),
            price=decimal_value(payload.get("price"), field="trade_price", positive=True),
            timestamp=parse_epoch(payload.get("timestamp")),
            title=str(payload.get("title") or ""),
            outcome=str(payload.get("outcome") or ""),
            slug=str(payload.get("slug") or ""),
            raw=dict(payload),
        )


def source_identity(row: TradeRow) -> str:
    raw = "|".join(
        (row.wallet.lower(), row.transaction_hash.lower(), row.asset, row.side)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def group_trade_rows(rows: Iterable[TradeRow]) -> list[SourceAction]:
    grouped: dict[tuple[str, str, str, str], list[TradeRow]] = {}
    for item in rows:
        key = (
            item.wallet.lower(),
            item.transaction_hash.lower(),
            item.asset,
            item.side,
        )
        grouped.setdefault(key, []).append(item)
    actions: list[SourceAction] = []
    for items in grouped.values():
        first = items[0]
        total_size = sum((item.size for item in items), Decimal("0"))
        weighted_value = sum((item.size * item.price for item in items), Decimal("0"))
        actions.append(
            SourceAction(
                identity=source_identity(first),
                wallet=first.wallet.lower(),
                transaction_hash=first.transaction_hash.lower(),
                asset=first.asset,
                condition_id=first.condition_id,
                side=first.side,
                source_size=total_size,
                source_price=weighted_value / total_size,
                timestamp=max(item.timestamp for item in items),
                title=first.title,
                outcome=first.outcome,
                slug=first.slug,
                raw_rows=tuple(dict(item.raw) for item in items),
            )
        )
    return sorted(actions, key=lambda item: (item.timestamp, item.identity))


FEE_QUANTUM = Decimal("0.00001")  # official fee precision


def fee_for_leg(
    *,
    quantity: Decimal,
    price: Decimal,
    fee_rate: Decimal,
    exponent: int,
) -> Decimal:
    if fee_rate == 0:
        return Decimal("0")
    raw = quantity * fee_rate * (price * (Decimal("1") - price)) ** exponent
    return raw.quantize(FEE_QUANTUM, rounding=ROUND_HALF_UP)


def _walk(
    levels: Sequence[BookLevel],
    quantity: Decimal,
    *,
    fee_rate: Decimal,
    exponent: int,
) -> tuple[tuple[FillLeg, ...], Decimal, Decimal] | None:
    remaining = quantity
    legs: list[FillLeg] = []
    gross = Decimal("0")
    fee = Decimal("0")
    for level in levels:
        take = min(remaining, level.quantity)
        if take <= 0:
            continue
        leg_fee = fee_for_leg(
            quantity=take,
            price=level.price,
            fee_rate=fee_rate,
            exponent=exponent,
        )
        legs.append(FillLeg(level.price, take, leg_fee))
        gross += level.price * take
        fee += leg_fee
        remaining -= take
        if remaining == 0:
            return tuple(legs), gross, fee
    return None


def _skip(side: str, reason: str) -> Decision:
    return Decision(
        result=reason,
        side=side,
        quantity=Decimal("0"),
        gross=Decimal("0"),
        fee=Decimal("0"),
        cash_delta=Decimal("0"),
        legs=(),
    )


def decide(
    action: SourceAction,
    book: Book,
    params: MarketParams,
    *,
    cash: Decimal,
    position: Decimal,
) -> Decision:
    if action.asset != book.asset or action.condition_id != book.condition_id:
        return _skip(action.side, "SKIP_BOOK_IDENTITY_MISMATCH")
    if action.condition_id != params.condition_id:
        return _skip(action.side, "SKIP_PARAMS_IDENTITY_MISMATCH")
    if book.timestamp < action.timestamp:
        return _skip(action.side, "SKIP_STALE_BOOK")
    if book.min_order_size != params.min_order_size:
        return _skip(action.side, "SKIP_MINIMUM_MISMATCH")
    quantity = book.min_order_size
    if action.side == "SELL":
        if position == 0:
            return _skip(action.side, "SKIP_NO_POSITION")
        if position < quantity:
            return _skip(action.side, "SKIP_BELOW_MINIMUM")
        walked = _walk(
            book.bids,
            quantity,
            fee_rate=params.fee_rate,
            exponent=params.fee_exponent,
        )
        if walked is None:
            return _skip(action.side, "SKIP_INSUFFICIENT_DEPTH")
        legs, gross, fee = walked
        return Decision("FILLED", action.side, quantity, gross, fee, gross - fee, legs)
    walked = _walk(
        book.asks,
        quantity,
        fee_rate=params.fee_rate,
        exponent=params.fee_exponent,
    )
    if walked is None:
        return _skip(action.side, "SKIP_INSUFFICIENT_DEPTH")
    legs, gross, fee = walked
    cost = gross + fee
    if cost > cash:
        return _skip(action.side, "SKIP_INSUFFICIENT_CASH")
    return Decision("FILLED", action.side, quantity, gross, fee, -cost, legs)


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  wallet TEXT NOT NULL UNIQUE,
  starting_cash_text TEXT NOT NULL,
  cash_text TEXT NOT NULL,
  realized_pnl_text TEXT NOT NULL DEFAULT '0'
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_actions (
  identity TEXT PRIMARY KEY,
  account_id TEXT NOT NULL REFERENCES accounts(account_id),
  transaction_hash TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  condition_id TEXT NOT NULL,
  side TEXT NOT NULL,
  source_size_text TEXT NOT NULL,
  source_price_text TEXT NOT NULL,
  source_timestamp INTEGER NOT NULL,
  title TEXT NOT NULL,
  outcome TEXT NOT NULL,
  slug TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  result TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  book_json TEXT,
  params_json TEXT
);
CREATE TABLE IF NOT EXISTS fills (
  fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_identity TEXT NOT NULL UNIQUE REFERENCES source_actions(identity),
  account_id TEXT NOT NULL REFERENCES accounts(account_id),
  side TEXT NOT NULL,
  quantity_text TEXT NOT NULL,
  gross_text TEXT NOT NULL,
  fee_text TEXT NOT NULL,
  cash_delta_text TEXT NOT NULL,
  average_price_text TEXT NOT NULL,
  realized_pnl_text TEXT NOT NULL,
  legs_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lots (
  lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id TEXT NOT NULL REFERENCES accounts(account_id),
  asset_id TEXT NOT NULL,
  condition_id TEXT NOT NULL,
  title TEXT NOT NULL,
  outcome TEXT NOT NULL,
  slug TEXT NOT NULL,
  quantity_open_text TEXT NOT NULL,
  unit_cost_text TEXT NOT NULL,
  source_identity TEXT NOT NULL REFERENCES source_actions(identity)
);
CREATE TABLE IF NOT EXISTS marks (
  account_id TEXT NOT NULL REFERENCES accounts(account_id),
  asset_id TEXT NOT NULL,
  quantity_text TEXT NOT NULL,
  executable_value_text TEXT NOT NULL,
  unliquidated_quantity_text TEXT NOT NULL,
  marked_at INTEGER NOT NULL,
  book_hash TEXT NOT NULL,
  PRIMARY KEY(account_id, asset_id)
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


class PaperStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def initialize(self) -> None:
        self.connection.executescript(SCHEMA)
        with self.connection:
            for account in ACCOUNTS:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO accounts(
                      account_id,username,wallet,starting_cash_text,cash_text,realized_pnl_text
                    ) VALUES(?,?,?,?,?,?)
                    """,
                    (
                        account.account_id,
                        account.username,
                        account.wallet.lower(),
                        str(account.starting_cash),
                        str(account.starting_cash),
                        "0",
                    ),
                )

    def close(self) -> None:
        self.connection.close()

    def cash(self, account_id: str) -> Decimal:
        row = self.connection.execute(
            "SELECT cash_text FROM accounts WHERE account_id=?", (account_id,)
        ).fetchone()
        if row is None:
            raise KeyError(account_id)
        return Decimal(row["cash_text"])

    def position(self, account_id: str, asset: str) -> Decimal:
        rows = self.connection.execute(
            "SELECT quantity_open_text FROM lots WHERE account_id=? AND asset_id=?",
            (account_id, asset),
        )
        return sum((Decimal(row[0]) for row in rows), Decimal("0"))

    def has_source(self, identity: str) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM source_actions WHERE identity=?", (identity,)
        ).fetchone() is not None

    def seed(self, account_id: str, actions: Iterable[SourceAction], *, observed_at: int) -> None:
        with self.connection:
            for source in actions:
                self._insert_source(
                    account_id,
                    source,
                    observed_at=observed_at,
                    result="SEEDED_HISTORY",
                    book=None,
                    params=None,
                )

    def _insert_source(
        self,
        account_id: str,
        source: SourceAction,
        *,
        observed_at: int,
        result: str,
        book: Book | None,
        params: MarketParams | None,
    ) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO source_actions(
              identity,account_id,transaction_hash,asset_id,condition_id,side,
              source_size_text,source_price_text,source_timestamp,title,outcome,slug,
              observed_at,result,raw_json,book_json,params_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                source.identity,
                account_id,
                source.transaction_hash,
                source.asset,
                source.condition_id,
                source.side,
                str(source.source_size),
                str(source.source_price),
                source.timestamp,
                source.title,
                source.outcome,
                source.slug,
                observed_at,
                result,
                _json(source.raw_rows),
                _json(asdict(book)) if book else None,
                _json(asdict(params)) if params else None,
            ),
        )
        return cursor.rowcount == 1

    def apply(
        self,
        account_id: str,
        source: SourceAction,
        book: Book,
        params: MarketParams,
        decision: Decision,
        *,
        observed_at: int,
    ) -> bool:
        with self.connection:
            if not self._insert_source(
                account_id,
                source,
                observed_at=observed_at,
                result=decision.result,
                book=book,
                params=params,
            ):
                return False
            if decision.result != "FILLED":
                return True
            before_cash = self.cash(account_id)
            after_cash = before_cash + decision.cash_delta
            if after_cash < 0:
                raise ValueError("negative_paper_cash")
            realized = Decimal("0")
            if decision.side == "BUY":
                unit_cost = (-decision.cash_delta) / decision.quantity
                self.connection.execute(
                    """
                    INSERT INTO lots(
                      account_id,asset_id,condition_id,title,outcome,slug,
                      quantity_open_text,unit_cost_text,source_identity
                    ) VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        account_id,
                        source.asset,
                        source.condition_id,
                        source.title,
                        source.outcome,
                        source.slug,
                        str(decision.quantity),
                        str(unit_cost),
                        source.identity,
                    ),
                )
            else:
                realized = decision.cash_delta - self._consume_fifo(
                    account_id, source.asset, decision.quantity
                )
            self.connection.execute(
                """
                UPDATE accounts
                SET cash_text=?, realized_pnl_text=CAST(realized_pnl_text AS TEXT)
                WHERE account_id=?
                """,
                (str(after_cash), account_id),
            )
            if realized:
                total_realized = self.realized_pnl(account_id) + realized
                self.connection.execute(
                    "UPDATE accounts SET realized_pnl_text=? WHERE account_id=?",
                    (str(total_realized), account_id),
                )
            self.connection.execute(
                """
                INSERT INTO fills(
                  source_identity,account_id,side,quantity_text,gross_text,fee_text,
                  cash_delta_text,average_price_text,realized_pnl_text,legs_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    source.identity,
                    account_id,
                    decision.side,
                    str(decision.quantity),
                    str(decision.gross),
                    str(decision.fee),
                    str(decision.cash_delta),
                    str(decision.gross / decision.quantity),
                    str(realized),
                    _json([asdict(leg) for leg in decision.legs]),
                ),
            )
            return True

    def _consume_fifo(self, account_id: str, asset: str, quantity: Decimal) -> Decimal:
        remaining = quantity
        removed_cost = Decimal("0")
        rows = self.connection.execute(
            """
            SELECT lot_id,quantity_open_text,unit_cost_text FROM lots
            WHERE account_id=? AND asset_id=? AND CAST(quantity_open_text AS REAL)>0
            ORDER BY lot_id
            """,
            (account_id, asset),
        ).fetchall()
        for row in rows:
            open_quantity = Decimal(row["quantity_open_text"])
            take = min(remaining, open_quantity)
            removed_cost += take * Decimal(row["unit_cost_text"])
            next_quantity = open_quantity - take
            self.connection.execute(
                "UPDATE lots SET quantity_open_text=? WHERE lot_id=?",
                (str(next_quantity), row["lot_id"]),
            )
            remaining -= take
            if remaining == 0:
                return removed_cost
        raise ValueError("paper_position_underflow")

    def realized_pnl(self, account_id: str) -> Decimal:
        row = self.connection.execute(
            "SELECT realized_pnl_text FROM accounts WHERE account_id=?", (account_id,)
        ).fetchone()
        return Decimal(row[0]) if row else Decimal("0")

    def paper_fill_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM fills").fetchone()[0])

    def account_summaries(self) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for account in ACCOUNTS:
            source_actions = int(
                self.connection.execute(
                    """SELECT COUNT(*) FROM source_actions
                    WHERE account_id=? AND result!='SEEDED_HISTORY'""",
                    (account.account_id,),
                ).fetchone()[0]
            )
            fills = int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM fills WHERE account_id=?", (account.account_id,)
                ).fetchone()[0]
            )
            positions = []
            for row in self.connection.execute(
                """
                SELECT asset_id,condition_id,title,outcome,slug,
                       SUM(CAST(quantity_open_text AS REAL)) AS quantity
                FROM lots WHERE account_id=?
                GROUP BY asset_id,condition_id,title,outcome,slug
                HAVING quantity>0
                ORDER BY title,outcome
                """,
                (account.account_id,),
            ):
                positions.append(
                    {
                        "asset": row["asset_id"],
                        "condition_id": row["condition_id"],
                        "title": row["title"],
                        "outcome": row["outcome"],
                        "slug": row["slug"],
                        "quantity": str(Decimal(str(row["quantity"]))),
                    }
                )
            mark_value = sum(
                (
                    Decimal(row[0])
                    for row in self.connection.execute(
                        "SELECT executable_value_text FROM marks WHERE account_id=?",
                        (account.account_id,),
                    )
                ),
                Decimal("0"),
            )
            cash = self.cash(account.account_id)
            summaries.append(
                {
                    "account_id": account.account_id,
                    "username": account.username,
                    "wallet": account.wallet,
                    "starting_cash": str(account.starting_cash),
                    "cash": str(cash),
                    "executable_position_value": str(mark_value),
                    "executable_equity": str(cash + mark_value),
                    "paper_pnl": str(cash + mark_value - account.starting_cash),
                    "realized_pnl": str(self.realized_pnl(account.account_id)),
                    "source_actions": source_actions,
                    "paper_fills": fills,
                    "positions": positions,
                }
            )
        return summaries

    def ledger_rows(self) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT s.account_id,s.observed_at,s.source_timestamp,s.transaction_hash,
                   s.title,s.outcome,s.side AS source_side,s.source_size_text,
                   s.source_price_text,s.result,s.asset_id,s.condition_id,
                   f.quantity_text,f.gross_text,f.fee_text,f.cash_delta_text,
                   f.average_price_text,f.realized_pnl_text
            FROM source_actions s LEFT JOIN fills f ON f.source_identity=s.identity
            WHERE s.result!='SEEDED_HISTORY'
            ORDER BY s.observed_at,s.identity
            """
        ).fetchall()


def build_status(store: PaperStore, *, heartbeat: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "paper_only": True,
        "real_order_submitted": False,
        "heartbeat": dict(heartbeat),
        "accounts": store.account_summaries(),
    }


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_reports(
    store: PaperStore,
    output_dir: Path | str,
    *,
    heartbeat: Mapping[str, Any],
) -> dict[str, Any]:
    root = Path(output_dir)
    status = build_status(store, heartbeat=heartbeat)
    _atomic_text(root / "status.json", _json(status) + "\n")

    rows = store.ledger_rows()
    fieldnames = [
        "account_id",
        "observed_at",
        "source_timestamp",
        "transaction_hash",
        "title",
        "outcome",
        "source_side",
        "source_size_text",
        "source_price_text",
        "result",
        "asset_id",
        "condition_id",
        "quantity_text",
        "gross_text",
        "fee_text",
        "cash_delta_text",
        "average_price_text",
        "realized_pnl_text",
    ]
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        prefix=".ledger.",
        dir=root,
        delete=False,
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row[name] for name in fieldnames})
        handle.flush()
        os.fsync(handle.fileno())
        temporary_csv = handle.name
    os.replace(temporary_csv, root / "ledger.csv")

    cards = []
    for account in status["accounts"]:
        positions = "".join(
            f"<li>{html.escape(item['title'])} — {html.escape(item['outcome'])}: "
            f"{html.escape(item['quantity'])} shares</li>"
            for item in account["positions"]
        ) or "<li>暂无持仓</li>"
        cards.append(
            "<section><h2>"
            + html.escape(account["username"])
            + "</h2><p>现金 $"
            + html.escape(account["cash"])
            + " · 可执行权益 $"
            + html.escape(account["executable_equity"])
            + " · 纸面盈亏 $"
            + html.escape(account["paper_pnl"])
            + "</p><p>源动作 "
            + str(account["source_actions"])
            + " · 纸面成交 "
            + str(account["paper_fills"])
            + "</p><ul>"
            + positions
            + "</ul></section>"
        )
    page = """<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<meta http-equiv="refresh" content="10"><title>三钱包纸面跟单</title>
<style>body{font:16px system-ui;margin:32px;background:#f5f5f7;color:#111}main{max-width:960px;margin:auto}section{background:white;padding:18px;margin:14px 0;border-radius:14px}code{background:#eee;padding:2px 5px}</style>
<main><h1>三钱包纸面跟单</h1><p><code>paper_only: true</code> · <code>real_order_submitted: false</code></p>
<p>开放持仓使用最近一次可执行买盘估值；结算前不是最终输赢。</p>""" + "".join(cards) + "</main></html>"
    _atomic_text(root / "status.html", page)
    return status
