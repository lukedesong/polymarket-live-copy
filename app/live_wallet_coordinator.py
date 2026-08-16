"""Shared physical-wallet invariants for independently audited live sleeves.

The strategy ledgers remain the source of truth for per-sleeve positions and
PnL.  The coordinator owns wallet-wide cash constraints and the immutable
allocation receipt needed when multiple sleeves hold one condition whose
physical wallet balance can be redeemed only once.  Network submission remains
outside this module; each strategy ledger applies only its frozen allocation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Sequence


ZERO = Decimal("0")

_SHARED_REDEMPTION_TRANSITIONS: dict[str, frozenset[str]] = {
    "READY": frozenset(
        {
            "SUBMIT_STARTED",
            "LOSS_DISTRIBUTING",
            "PLATFORM_DISTRIBUTING",
        }
    ),
    "NOT_SUBMITTED_RETRYABLE": frozenset({"SUBMIT_STARTED"}),
    "SUBMIT_STARTED": frozenset(
        {
            "SUBMITTED_UNRECONCILED",
            "UNKNOWN_SUBMISSION",
            "NOT_SUBMITTED_RETRYABLE",
        }
    ),
    "SUBMITTED_UNRECONCILED": frozenset(
        {"PENDING", "UNKNOWN_SUBMISSION", "DISTRIBUTING", "ERROR"}
    ),
    "PENDING": frozenset(
        {"PENDING", "UNKNOWN_SUBMISSION", "DISTRIBUTING", "ERROR"}
    ),
    "UNKNOWN_SUBMISSION": frozenset(
        {
            "UNKNOWN_SUBMISSION",
            "PENDING",
            "DISTRIBUTING",
            "PLATFORM_DISTRIBUTING",
            "ERROR",
        }
    ),
    "DISTRIBUTING": frozenset({"REDEEMED"}),
    "PLATFORM_DISTRIBUTING": frozenset(
        {"REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"}
    ),
    "LOSS_DISTRIBUTING": frozenset({"LOSS_RESOLVED_NO_PAYOUT"}),
    "REDEEMED": frozenset(),
    "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED": frozenset(),
    "LOSS_RESOLVED_NO_PAYOUT": frozenset(),
    "ERROR": frozenset(),
}

_SHARED_DISTRIBUTION_STATES = frozenset(
    {"DISTRIBUTING", "PLATFORM_DISTRIBUTING", "LOSS_DISTRIBUTING"}
)


class SharedWalletCoordinatorError(RuntimeError):
    """Raised when one physical wallet no longer satisfies its invariants."""


@dataclass(frozen=True)
class SleeveSpec:
    profile_key: str
    ledger_path: Path
    role: str

    def normalized(self) -> "SleeveSpec":
        profile = str(self.profile_key).strip()
        role = str(self.role).strip().upper()
        path = Path(self.ledger_path).expanduser().resolve()
        if not profile:
            raise SharedWalletCoordinatorError("EMPTY_PROFILE_KEY")
        if role not in {"RESIDUAL", "RESERVED"}:
            raise SharedWalletCoordinatorError("INVALID_SLEEVE_ROLE")
        return SleeveSpec(profile, path, role)


@dataclass(frozen=True)
class AuthenticatedAccountCashSnapshot:
    """The only cash authority for a new shared-account BUY.

    Strategy ledgers remain position/PnL attribution records.  They are never
    individual wallets and their cash balances never constrain a new order.
    """

    authenticated_collateral_usd: Decimal
    expected_accounting_cash_low_usd: Decimal
    expected_accounting_cash_high_usd: Decimal
    active_buy_reservations_usd: Decimal
    active_sell_reservation_count: int
    active_redemption_reservation_count: int
    redeemed_cash_credit_quarantine_usd: Decimal
    permanent_redeemed_cash_credit_block_usd: Decimal
    available_for_new_buy_usd: Decimal
    unallocated_account_cash_usd: Decimal
    state: str

    def as_dict(self) -> dict[str, str]:
        return {key: str(value) for key, value in asdict(self).items()}


def _decimal(value: Any, error: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise SharedWalletCoordinatorError(error) from exc
    if not result.is_finite() or result < ZERO:
        raise SharedWalletCoordinatorError(error)
    return result


def _signed_decimal(value: Any, error: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise SharedWalletCoordinatorError(error) from exc
    if not result.is_finite():
        raise SharedWalletCoordinatorError(error)
    return result


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_shared_redemption_transition(current: str, target: str) -> None:
    current_state = str(current).strip().upper()
    target_state = str(target).strip().upper()
    allowed = _SHARED_REDEMPTION_TRANSITIONS.get(current_state)
    if allowed is None or target_state not in allowed:
        raise SharedWalletCoordinatorError(
            "INVALID_SHARED_REDEMPTION_STATE_TRANSITION:"
            f"{current_state}->{target_state}"
        )


@contextmanager
def _read_ledger(path: Path) -> Iterator[sqlite3.Connection]:
    if not path.is_file():
        raise SharedWalletCoordinatorError(f"LEDGER_NOT_FOUND:{path}")
    uri = f"file:{path}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 10000")
    try:
        yield connection
    finally:
        connection.close()


def _ledger_integrity(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA integrity_check").fetchall()
    values = [str(row[0]) for row in rows]
    if values != ["ok"]:
        raise SharedWalletCoordinatorError(
            "LEDGER_INTEGRITY_FAILED:" + "|".join(values)
        )


def _ledger_cash(connection: sqlite3.Connection) -> Decimal:
    row = connection.execute(
        "SELECT cash_usd FROM account_state WHERE singleton = 1"
    ).fetchone()
    if row is None:
        raise SharedWalletCoordinatorError("LEDGER_ACCOUNT_STATE_MISSING")
    return _signed_decimal(row["cash_usd"], "INVALID_LEDGER_CASH")


def _active_buy_reservation(connection: sqlite3.Connection) -> Decimal:
    rows = connection.execute(
        """
        SELECT cash_reserved_usd
        FROM order_reservations
        WHERE side = 'BUY' AND active = 1
        """
    ).fetchall()
    return sum(
        (_decimal(row["cash_reserved_usd"], "INVALID_BUY_RESERVATION") for row in rows),
        ZERO,
    )


def _active_sell_reservation_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM order_reservations
        WHERE side = 'SELL' AND active = 1
        """
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _active_redemption_reservation_count(connection: sqlite3.Connection) -> int:
    """Count redemptions whose wallet-side cash effect is not ledger-final yet."""

    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM redemption_receipts
        WHERE state IN (
            'SUBMIT_STARTED',
            'SUBMITTED_UNRECONCILED',
            'PENDING',
            'UNKNOWN_SUBMISSION'
        )
        """
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _pending_redeemed_cash_credit_quarantine(
    connection: sqlite3.Connection,
) -> Decimal:
    """Return confirmed payouts not yet proven in authenticated CLOB cash."""

    try:
        rows = connection.execute(
            """
            SELECT quarantine.payout_usd
            FROM redeemed_cash_credit_quarantines AS quarantine
            LEFT JOIN redeemed_cash_credit_quarantine_verifications AS verification
              ON verification.condition_id = quarantine.condition_id
            LEFT JOIN redeemed_cash_credit_quarantine_voids AS void
              ON void.condition_id = quarantine.condition_id
            LEFT JOIN redeemed_cash_credit_permanent_blocks AS permanent
              ON permanent.condition_id = quarantine.condition_id
            WHERE verification.condition_id IS NULL
              AND void.condition_id IS NULL
              AND permanent.condition_id IS NULL
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            raise SharedWalletCoordinatorError(
                "MISSING_REDEEMED_CASH_CREDIT_QUARANTINE_SCHEMA"
            ) from exc
        raise
    return sum(
        (
            _decimal(
                row["payout_usd"], "INVALID_REDEEMED_CASH_CREDIT_QUARANTINE"
            )
            for row in rows
        ),
        ZERO,
    )


def _permanent_redeemed_cash_credit_block(
    connection: sqlite3.Connection,
) -> Decimal:
    """Return chain-proven false redemption credits excluded indefinitely."""

    try:
        rows = connection.execute(
            """
            SELECT payout_usd
            FROM redeemed_cash_credit_permanent_blocks
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc).lower():
            raise SharedWalletCoordinatorError(
                "MISSING_REDEEMED_CASH_CREDIT_PERMANENT_BLOCK_SCHEMA"
            ) from exc
        raise
    return sum(
        (
            _decimal(
                row["payout_usd"],
                "INVALID_REDEEMED_CASH_CREDIT_PERMANENT_BLOCK",
            )
            for row in rows
        ),
        ZERO,
    )


def _ledger_ownership(
    connection: sqlite3.Connection,
) -> tuple[set[str], set[str]]:
    """Return current token and decision-unit ownership from one frozen ledger."""

    token_rows = connection.execute(
        """
        SELECT token_id
        FROM positions
        WHERE CAST(quantity AS REAL) > 0
        UNION
        SELECT token_id
        FROM order_reservations
        WHERE side = 'BUY' AND active = 1
        """
    ).fetchall()
    tokens = {str(row["token_id"]) for row in token_rows if str(row["token_id"]).strip()}
    condition_rows = connection.execute(
        """
        SELECT condition_id
        FROM positions
        WHERE CAST(quantity AS REAL) > 0 AND condition_id <> ''
        UNION
        SELECT condition_id
        FROM order_reservations
        WHERE side = 'BUY' AND active = 1 AND condition_id <> ''
        """
    ).fetchall()
    conditions = {
        str(row["condition_id"]).lower()
        for row in condition_rows
        if str(row["condition_id"]).strip()
    }
    for row in connection.execute(
        """
        SELECT condition_id, primary_token_id, secondary_token_id
        FROM condition_mappings
        """
    ):
        if str(row["primary_token_id"]) in tokens or str(row["secondary_token_id"]) in tokens:
            conditions.add(str(row["condition_id"]).lower())
    return tokens, conditions


def _assert_disjoint_sleeve_ownership(
    ownership: dict[str, tuple[set[str], set[str]]],
) -> None:
    """Reject an initial or additive migration with ambiguous sell ownership."""

    token_owners: dict[str, str] = {}
    condition_owners: dict[str, str] = {}
    for profile in sorted(ownership):
        tokens, conditions = ownership[profile]
        for token in sorted(tokens):
            prior = token_owners.setdefault(token, profile)
            if prior != profile:
                raise SharedWalletCoordinatorError(
                    f"CROSS_SLEEVE_TOKEN_OWNERSHIP:{token}:{prior}:{profile}"
                )
        for condition in sorted(conditions):
            prior = condition_owners.setdefault(condition, profile)
            if prior != profile:
                raise SharedWalletCoordinatorError(
                    "CROSS_SLEEVE_CONDITION_OWNERSHIP:"
                    f"{condition}:{prior}:{profile}"
                )


class SharedWalletCoordinator:
    """Read-through coordinator for multiple ledgers sharing one wallet."""

    def __init__(self, path: Path):
        self.path = Path(path).expanduser().resolve()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS migration_receipt (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    receipt_json TEXT NOT NULL,
                    receipt_hash TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS migration_history (
                    generation INTEGER PRIMARY KEY,
                    receipt_json TEXT NOT NULL,
                    receipt_hash TEXT NOT NULL UNIQUE
                );

                CREATE TABLE IF NOT EXISTS sleeves (
                    profile_key TEXT PRIMARY KEY,
                    ledger_path TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL CHECK(role IN ('RESIDUAL', 'RESERVED')),
                    ledger_sha256 TEXT NOT NULL,
                    raw_cash_at_migration_usd TEXT NOT NULL,
                    cash_adjustment_usd TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wallet_contract (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    submission_lock_path TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shared_condition_redemptions (
                    condition_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    primary_token_id TEXT NOT NULL,
                    secondary_token_id TEXT NOT NULL,
                    winner_token_id TEXT NOT NULL,
                    primary_quantity TEXT NOT NULL,
                    secondary_quantity TEXT NOT NULL,
                    expected_payout_usd TEXT NOT NULL,
                    inventory_hash TEXT NOT NULL,
                    transaction_id TEXT,
                    transaction_hash TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shared_condition_allocations (
                    condition_id TEXT NOT NULL,
                    profile_key TEXT NOT NULL,
                    ledger_path TEXT NOT NULL,
                    primary_quantity TEXT NOT NULL,
                    primary_cost_basis_usd TEXT NOT NULL,
                    secondary_quantity TEXT NOT NULL,
                    secondary_cost_basis_usd TEXT NOT NULL,
                    payout_usd TEXT NOT NULL,
                    apply_state TEXT NOT NULL,
                    applied_at_ms INTEGER,
                    PRIMARY KEY(condition_id, profile_key),
                    FOREIGN KEY(condition_id)
                        REFERENCES shared_condition_redemptions(condition_id)
                );

                CREATE TABLE IF NOT EXISTS shared_condition_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    condition_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY(condition_id)
                        REFERENCES shared_condition_redemptions(condition_id)
                );
                CREATE INDEX IF NOT EXISTS idx_shared_condition_transitions
                    ON shared_condition_transitions(condition_id, id DESC);

                """
            )

    def authenticated_account_cash_snapshot(
        self, *, authenticated_collateral_usd: Decimal
    ) -> AuthenticatedAccountCashSnapshot:
        """Read physical account cash less only active authenticated BUY holds."""

        physical = _decimal(
            authenticated_collateral_usd, "INVALID_AUTHENTICATED_COLLATERAL"
        )
        accounting_cash_before_quarantine = ZERO
        reservations = ZERO
        sell_reservation_count = 0
        redemption_reservation_count = 0
        pending_redeemed_cash_credit = ZERO
        permanent_redeemed_cash_credit_block = ZERO
        for row in self._sleeves():
            with _read_ledger(Path(str(row["ledger_path"]))) as ledger:
                accounting_cash_before_quarantine += (
                    _ledger_cash(ledger)
                    - _decimal(
                        row["cash_adjustment_usd"], "INVALID_CASH_ADJUSTMENT"
                    )
                )
                reservations += _active_buy_reservation(ledger)
                sell_reservation_count += _active_sell_reservation_count(ledger)
                redemption_reservation_count += _active_redemption_reservation_count(
                    ledger
                )
                pending_redeemed_cash_credit += (
                    _pending_redeemed_cash_credit_quarantine(ledger)
                )
                permanent_redeemed_cash_credit_block += (
                    _permanent_redeemed_cash_credit_block(ledger)
                )
        redeemed_cash_credit_quarantine = (
            pending_redeemed_cash_credit
            if physical < accounting_cash_before_quarantine
            else ZERO
        )
        expected_high = (
            accounting_cash_before_quarantine
            - redeemed_cash_credit_quarantine
            - permanent_redeemed_cash_credit_block
        )
        expected_low = expected_high - reservations
        available = max(physical - reservations, ZERO)
        if permanent_redeemed_cash_credit_block > ZERO:
            state = "PERMANENTLY_BLOCKED_REDEEMED_CASH_CREDIT"
        elif redeemed_cash_credit_quarantine > ZERO:
            state = "QUARANTINED_REDEEMED_CASH_CREDIT"
        elif reservations > ZERO:
            state = "ACTIVE_BUY_RESERVATIONS"
        elif sell_reservation_count > 0:
            state = "ACTIVE_SELL_RESERVATIONS"
        elif redemption_reservation_count > 0:
            state = "ACTIVE_REDEMPTIONS"
        else:
            state = "NO_ACTIVE_CASH_HOLDS"
        return AuthenticatedAccountCashSnapshot(
            authenticated_collateral_usd=physical,
            expected_accounting_cash_low_usd=expected_low,
            expected_accounting_cash_high_usd=expected_high,
            active_buy_reservations_usd=reservations,
            active_sell_reservation_count=sell_reservation_count,
            active_redemption_reservation_count=redemption_reservation_count,
            redeemed_cash_credit_quarantine_usd=redeemed_cash_credit_quarantine,
            permanent_redeemed_cash_credit_block_usd=(
                permanent_redeemed_cash_credit_block
            ),
            available_for_new_buy_usd=available,
            unallocated_account_cash_usd=max(physical - expected_high, ZERO),
            state=state,
        )

    def observe_authenticated_account_cash(
        self,
        *,
        authenticated_collateral_usd: Decimal,
        observed_at_ms: int,
    ) -> AuthenticatedAccountCashSnapshot:
        """Reconcile cash-credit evidence, then read the one wallet snapshot."""

        self.reconcile_redeemed_cash_credit_quarantines(
            authenticated_collateral_usd=authenticated_collateral_usd,
            observed_at_ms=observed_at_ms,
        )
        return self.authenticated_account_cash_snapshot(
            authenticated_collateral_usd=authenticated_collateral_usd,
        )

    @staticmethod
    def _normalize_sleeves(sleeves: Sequence[SleeveSpec]) -> tuple[SleeveSpec, ...]:
        normalized = tuple(sleeve.normalized() for sleeve in sleeves)
        if len(normalized) < 2:
            raise SharedWalletCoordinatorError("MULTIPLE_SLEEVES_REQUIRED")
        profiles = [sleeve.profile_key for sleeve in normalized]
        paths = [str(sleeve.ledger_path) for sleeve in normalized]
        if len(set(profiles)) != len(profiles):
            raise SharedWalletCoordinatorError("DUPLICATE_PROFILE_KEY")
        if len(set(paths)) != len(paths):
            raise SharedWalletCoordinatorError("DUPLICATE_LEDGER_PATH")
        residual = [sleeve for sleeve in normalized if sleeve.role == "RESIDUAL"]
        if len(residual) != 1:
            raise SharedWalletCoordinatorError("EXACTLY_ONE_RESIDUAL_SLEEVE_REQUIRED")
        return normalized

    def initialize_from_frozen_ledgers(
        self,
        *,
        sleeves: Sequence[SleeveSpec],
        authenticated_collateral_usd: Decimal,
        funder_address: str,
        observed_at_ms: int,
        fresh_start: bool = False,
    ) -> dict[str, Any]:
        """Create one immutable migration receipt from stopped/frozen ledgers."""

        normalized = self._normalize_sleeves(sleeves)
        physical = _decimal(
            authenticated_collateral_usd, "INVALID_AUTHENTICATED_COLLATERAL"
        )
        funder = str(funder_address).strip().lower()
        if not funder.startswith("0x") or len(funder) != 42:
            raise SharedWalletCoordinatorError("INVALID_FUNDER_ADDRESS")
        try:
            int(funder[2:], 16)
            observed = int(observed_at_ms)
        except (TypeError, ValueError) as exc:
            raise SharedWalletCoordinatorError("INVALID_MIGRATION_IDENTITY") from exc
        if observed < 0:
            raise SharedWalletCoordinatorError("INVALID_MIGRATION_TIME")

        raw_cash: dict[str, Decimal] = {}
        hashes: dict[str, str] = {}
        ownership: dict[str, tuple[set[str], set[str]]] = {}
        for sleeve in normalized:
            with _read_ledger(sleeve.ledger_path) as ledger:
                _ledger_integrity(ledger)
                active = ledger.execute(
                    "SELECT COUNT(*) FROM order_reservations WHERE active = 1"
                ).fetchone()
                if active is not None and int(active[0]) != 0:
                    raise SharedWalletCoordinatorError(
                        f"ACTIVE_BUY_RESERVATION:{sleeve.profile_key}"
                    )
                if _active_redemption_reservation_count(ledger) != 0:
                    raise SharedWalletCoordinatorError(
                        f"ACTIVE_REDEMPTION_RESERVATION:{sleeve.profile_key}"
                    )
                if fresh_start:
                    existing_tables = {
                        str(row[0])
                        for row in ledger.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                    }
                    for table in (
                        "action_receipts",
                        "action_targets",
                        "action_transitions",
                        "submission_attempts",
                        "order_reservations",
                        "positions",
                        "redemption_receipts",
                        "redemption_transitions",
                        "manual_trade_receipts",
                    ):
                        if table not in existing_tables:
                            continue
                        count = ledger.execute(
                            f'SELECT COUNT(*) FROM "{table}"'
                        ).fetchone()
                        if count is not None and int(count[0]) != 0:
                            raise SharedWalletCoordinatorError(
                                "FRESH_START_LEDGER_NOT_EMPTY:"
                                f"{sleeve.profile_key}:{table}"
                            )
                raw_cash[sleeve.profile_key] = _ledger_cash(ledger)
                ownership[sleeve.profile_key] = _ledger_ownership(ledger)
            hashes[sleeve.profile_key] = _sha256(sleeve.ledger_path)
        _assert_disjoint_sleeve_ownership(ownership)

        raw_total = sum(raw_cash.values(), ZERO)
        if fresh_start:
            overlap = raw_total
            adjustments = dict(raw_cash)
            adjusted_total = ZERO
        else:
            overlap = max(raw_total - physical, ZERO)
            residual = next(s for s in normalized if s.role == "RESIDUAL")
            if overlap > raw_cash[residual.profile_key]:
                raise SharedWalletCoordinatorError(
                    "OVERLAP_EXCEEDS_RESIDUAL_LEDGER_CASH"
                )
            adjustments = {sleeve.profile_key: ZERO for sleeve in normalized}
            adjustments[residual.profile_key] = overlap
            adjusted_total = sum(
                raw_cash[sleeve.profile_key] - adjustments[sleeve.profile_key]
                for sleeve in normalized
            )

        receipt_without_hash: dict[str, Any] = {
            "schema_version": 2,
            "generation": 1,
            "parent_migration_receipt_hash": None,
            "observed_at_ms": observed,
            "funder_address": funder,
            "authenticated_collateral_usd": str(physical),
            "raw_ledger_cash_total_usd": str(raw_total),
            "legacy_cash_overlap_usd": str(overlap),
            "adjusted_ledger_cash_total_usd": str(adjusted_total),
            "cash_adjustments_usd": {
                key: str(value) for key, value in sorted(adjustments.items())
            },
            "sleeves": [
                {
                    "profile_key": sleeve.profile_key,
                    "ledger_path": str(sleeve.ledger_path),
                    "role": sleeve.role,
                    "ledger_sha256": hashes[sleeve.profile_key],
                    "raw_cash_at_migration_usd": str(raw_cash[sleeve.profile_key]),
                }
                for sleeve in sorted(normalized, key=lambda item: item.profile_key)
            ],
        }
        if fresh_start:
            receipt_without_hash.update(
                {
                    "fresh_start": True,
                    "cash_authority": "OFFICIAL_AUTHENTICATED_COLLATERAL_ONLY",
                }
            )
        receipt_hash = hashlib.sha256(
            _canonical_json(receipt_without_hash).encode("utf-8")
        ).hexdigest()
        receipt = {**receipt_without_hash, "migration_receipt_hash": receipt_hash}

        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT receipt_json, receipt_hash FROM migration_receipt WHERE singleton = 1"
            ).fetchone()
            if existing is not None:
                stored = json.loads(str(existing["receipt_json"]))
                if str(existing["receipt_hash"]) != str(
                    stored.get("migration_receipt_hash", "")
                ):
                    raise SharedWalletCoordinatorError("MIGRATION_RECEIPT_HASH_MISMATCH")
                configured = {
                    row["profile_key"]: (row["ledger_path"], row["role"])
                    for row in connection.execute(
                        "SELECT profile_key, ledger_path, role FROM sleeves"
                    )
                }
                requested = {
                    sleeve.profile_key: (str(sleeve.ledger_path), sleeve.role)
                    for sleeve in normalized
                }
                if configured != requested or stored.get("funder_address") != funder:
                    raise SharedWalletCoordinatorError(
                        "COORDINATOR_ALREADY_INITIALIZED_WITH_DIFFERENT_IDENTITY"
                    )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO migration_history(
                        generation, receipt_json, receipt_hash
                    ) VALUES(?, ?, ?)
                    """,
                    (
                        int(stored.get("generation", 1)),
                        _canonical_json(stored),
                        str(existing["receipt_hash"]),
                    ),
                )
                return stored
            for sleeve in normalized:
                connection.execute(
                    """
                    INSERT INTO sleeves(
                        profile_key, ledger_path, role, ledger_sha256,
                        raw_cash_at_migration_usd, cash_adjustment_usd
                    ) VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sleeve.profile_key,
                        str(sleeve.ledger_path),
                        sleeve.role,
                        hashes[sleeve.profile_key],
                        str(raw_cash[sleeve.profile_key]),
                        str(adjustments[sleeve.profile_key]),
                    ),
                )
            connection.execute(
                """
                INSERT INTO migration_receipt(singleton, receipt_json, receipt_hash)
                VALUES(1, ?, ?)
                """,
                (_canonical_json(receipt), receipt_hash),
            )
            connection.execute(
                """
                INSERT INTO migration_history(generation, receipt_json, receipt_hash)
                VALUES(1, ?, ?)
                """,
                (_canonical_json(receipt), receipt_hash),
            )
        return receipt

    def extend_from_frozen_ledgers(
        self,
        *,
        sleeves: Sequence[SleeveSpec],
        authenticated_collateral_usd: Decimal,
        funder_address: str,
        observed_at_ms: int,
    ) -> dict[str, Any]:
        """Append new sleeves while all strategy ledgers are stopped and frozen.

        The current receipt remains in ``migration_history`` and the new
        receipt commits to it by hash.  Existing profile paths and roles are
        immutable; this transition can only add RESERVED sleeves.  Recomputing
        the one residual adjustment from the current physical collateral keeps
        the N-sleeve cash equation exact without rewriting a strategy ledger.
        """

        normalized = self._normalize_sleeves(sleeves)
        physical = _decimal(
            authenticated_collateral_usd, "INVALID_AUTHENTICATED_COLLATERAL"
        )
        funder = str(funder_address).strip().lower()
        if not funder.startswith("0x") or len(funder) != 42:
            raise SharedWalletCoordinatorError("INVALID_FUNDER_ADDRESS")
        try:
            int(funder[2:], 16)
            observed = int(observed_at_ms)
        except (TypeError, ValueError) as exc:
            raise SharedWalletCoordinatorError("INVALID_MIGRATION_IDENTITY") from exc
        if observed < 0:
            raise SharedWalletCoordinatorError("INVALID_MIGRATION_TIME")

        current = self.migration_receipt()
        if str(current.get("funder_address", "")) != funder:
            raise SharedWalletCoordinatorError("FUNDER_ADDRESS_CHANGED")
        if observed <= int(current.get("observed_at_ms", -1)):
            raise SharedWalletCoordinatorError("MIGRATION_TIME_NOT_FORWARD")

        existing = {
            str(row["profile_key"]): (str(row["ledger_path"]), str(row["role"]))
            for row in self._sleeves()
        }
        requested = {
            sleeve.profile_key: (str(sleeve.ledger_path), sleeve.role)
            for sleeve in normalized
        }
        for profile, identity in existing.items():
            if requested.get(profile) != identity:
                raise SharedWalletCoordinatorError(
                    f"EXISTING_SLEEVE_CHANGED:{profile}"
                )
        if set(requested) == set(existing):
            return current
        for sleeve in normalized:
            if sleeve.profile_key not in existing and sleeve.role != "RESERVED":
                raise SharedWalletCoordinatorError(
                    f"NEW_SLEEVE_MUST_BE_RESERVED:{sleeve.profile_key}"
                )

        raw_cash: dict[str, Decimal] = {}
        hashes: dict[str, str] = {}
        ownership: dict[str, tuple[set[str], set[str]]] = {}
        for sleeve in normalized:
            with _read_ledger(sleeve.ledger_path) as ledger:
                _ledger_integrity(ledger)
                active = ledger.execute(
                    "SELECT COUNT(*) FROM order_reservations WHERE active = 1"
                ).fetchone()
                if active is not None and int(active[0]) != 0:
                    raise SharedWalletCoordinatorError(
                        f"ACTIVE_BUY_RESERVATION:{sleeve.profile_key}"
                    )
                if _active_redemption_reservation_count(ledger) != 0:
                    raise SharedWalletCoordinatorError(
                        f"ACTIVE_REDEMPTION_RESERVATION:{sleeve.profile_key}"
                    )
                raw_cash[sleeve.profile_key] = _ledger_cash(ledger)
                ownership[sleeve.profile_key] = _ledger_ownership(ledger)
            hashes[sleeve.profile_key] = _sha256(sleeve.ledger_path)
        _assert_disjoint_sleeve_ownership(ownership)

        raw_total = sum(raw_cash.values(), ZERO)
        overlap = max(raw_total - physical, ZERO)
        residual = next(s for s in normalized if s.role == "RESIDUAL")
        if overlap > raw_cash[residual.profile_key]:
            raise SharedWalletCoordinatorError("OVERLAP_EXCEEDS_RESIDUAL_LEDGER_CASH")
        adjustments = {sleeve.profile_key: ZERO for sleeve in normalized}
        adjustments[residual.profile_key] = overlap
        adjusted_total = sum(
            raw_cash[sleeve.profile_key] - adjustments[sleeve.profile_key]
            for sleeve in normalized
        )
        generation = int(current.get("generation", 1)) + 1
        receipt_without_hash: dict[str, Any] = {
            "schema_version": 2,
            "generation": generation,
            "parent_migration_receipt_hash": current["migration_receipt_hash"],
            "observed_at_ms": observed,
            "funder_address": funder,
            "authenticated_collateral_usd": str(physical),
            "raw_ledger_cash_total_usd": str(raw_total),
            "legacy_cash_overlap_usd": str(overlap),
            "adjusted_ledger_cash_total_usd": str(adjusted_total),
            "cash_adjustments_usd": {
                key: str(value) for key, value in sorted(adjustments.items())
            },
            "sleeves": [
                {
                    "profile_key": sleeve.profile_key,
                    "ledger_path": str(sleeve.ledger_path),
                    "role": sleeve.role,
                    "ledger_sha256": hashes[sleeve.profile_key],
                    "raw_cash_at_migration_usd": str(raw_cash[sleeve.profile_key]),
                }
                for sleeve in sorted(normalized, key=lambda item: item.profile_key)
            ],
        }
        receipt_hash = hashlib.sha256(
            _canonical_json(receipt_without_hash).encode("utf-8")
        ).hexdigest()
        receipt = {**receipt_without_hash, "migration_receipt_hash": receipt_hash}

        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            latest = connection.execute(
                "SELECT receipt_hash FROM migration_receipt WHERE singleton = 1"
            ).fetchone()
            if latest is None or str(latest["receipt_hash"]) != str(
                current["migration_receipt_hash"]
            ):
                raise SharedWalletCoordinatorError("CONCURRENT_MIGRATION_DETECTED")
            connection.execute("DELETE FROM sleeves")
            for sleeve in normalized:
                connection.execute(
                    """
                    INSERT INTO sleeves(
                        profile_key, ledger_path, role, ledger_sha256,
                        raw_cash_at_migration_usd, cash_adjustment_usd
                    ) VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sleeve.profile_key,
                        str(sleeve.ledger_path),
                        sleeve.role,
                        hashes[sleeve.profile_key],
                        str(raw_cash[sleeve.profile_key]),
                        str(adjustments[sleeve.profile_key]),
                    ),
                )
            connection.execute(
                """
                UPDATE migration_receipt
                SET receipt_json = ?, receipt_hash = ?
                WHERE singleton = 1
                """,
                (_canonical_json(receipt), receipt_hash),
            )
            connection.execute(
                """
                INSERT INTO migration_history(generation, receipt_json, receipt_hash)
                VALUES(?, ?, ?)
                """,
                (generation, _canonical_json(receipt), receipt_hash),
            )
        return receipt

    def migration_receipt(self) -> dict[str, Any]:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT receipt_json, receipt_hash FROM migration_receipt WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise SharedWalletCoordinatorError("COORDINATOR_NOT_INITIALIZED")
        receipt = json.loads(str(row["receipt_json"]))
        expected = str(receipt.pop("migration_receipt_hash", ""))
        actual = hashlib.sha256(_canonical_json(receipt).encode("utf-8")).hexdigest()
        receipt["migration_receipt_hash"] = expected
        if expected != actual or expected != str(row["receipt_hash"]):
            raise SharedWalletCoordinatorError("MIGRATION_RECEIPT_HASH_MISMATCH")
        return receipt

    def receipt_history(self) -> list[dict[str, Any]]:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT generation, receipt_json, receipt_hash
                FROM migration_history ORDER BY generation
                """
            ).fetchall()
        history: list[dict[str, Any]] = []
        for row in rows:
            receipt = json.loads(str(row["receipt_json"]))
            expected = str(receipt.pop("migration_receipt_hash", ""))
            actual = hashlib.sha256(
                _canonical_json(receipt).encode("utf-8")
            ).hexdigest()
            receipt["migration_receipt_hash"] = expected
            if expected != actual or expected != str(row["receipt_hash"]):
                raise SharedWalletCoordinatorError(
                    f"MIGRATION_HISTORY_HASH_MISMATCH:{row['generation']}"
                )
            history.append(receipt)
        return history

    def _sleeves(self) -> list[sqlite3.Row]:
        self.migration_receipt()
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT profile_key, ledger_path, role, ledger_sha256,
                       raw_cash_at_migration_usd, cash_adjustment_usd
                FROM sleeves ORDER BY profile_key
                """
            ).fetchall()

    def registered_sleeve(self, profile_key: str) -> dict[str, str]:
        profile = str(profile_key).strip()
        for row in self._sleeves():
            if str(row["profile_key"]) == profile:
                return {key: str(row[key]) for key in row.keys()}
        raise SharedWalletCoordinatorError(f"UNREGISTERED_PROFILE:{profile}")

    def registered_sleeves(self) -> list[dict[str, str]]:
        """Expose frozen sleeve identities for wallet-wide safety maintenance."""

        return [
            {key: str(row[key]) for key in row.keys()}
            for row in self._sleeves()
        ]

    def lock_submission_path(self, path: Path) -> Path:
        """Freeze one process-shared order/redemption lock for every sleeve."""

        candidate = Path(path).expanduser()
        if not candidate.is_absolute():
            raise SharedWalletCoordinatorError(
                "SHARED_WALLET_SUBMISSION_LOCK_PATH_NOT_ABSOLUTE"
            )
        normalized = candidate.resolve()
        self.initialize()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT submission_lock_path
                FROM wallet_contract
                WHERE singleton = 1
                """
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO wallet_contract(singleton, submission_lock_path)
                    VALUES(1, ?)
                    """,
                    (str(normalized),),
                )
            elif Path(str(row["submission_lock_path"])).resolve() != normalized:
                raise SharedWalletCoordinatorError(
                    "SHARED_WALLET_SUBMISSION_LOCK_PATH_MISMATCH:"
                    f"expected={row['submission_lock_path']}:actual={normalized}"
                )
        return normalized

    def latest_authenticated_collateral_observation(self) -> dict[str, str] | None:
        """Return the newest authenticated cash sample across all sleeves.

        Every registered service observes the same physical wallet.  Keeping
        samples in the isolated runtime tables preserves their provenance,
        while selecting the newest sample here prevents one sleeve's status
        page from continuing to use a pre-fill value observed by another.
        """

        observations: list[dict[str, str]] = []
        for row in self._sleeves():
            profile = str(row["profile_key"])
            with _read_ledger(Path(str(row["ledger_path"]))) as ledger:
                values = {
                    str(item["key"]): str(item["value"])
                    for item in ledger.execute(
                        """
                        SELECT key, value
                        FROM runtime_state
                        WHERE key IN (
                            'last_authenticated_collateral_usd',
                            'last_authenticated_collateral_at_ms',
                            'last_authenticated_collateral_transition_id'
                        )
                        """
                    )
                }
            if (
                "last_authenticated_collateral_usd" not in values
                or "last_authenticated_collateral_at_ms" not in values
            ):
                continue
            physical = _decimal(
                values["last_authenticated_collateral_usd"],
                "INVALID_AUTHENTICATED_COLLATERAL_OBSERVATION",
            )
            try:
                observed_at_ms = int(values["last_authenticated_collateral_at_ms"])
                transition_id = int(
                    values.get("last_authenticated_collateral_transition_id", "0")
                )
            except ValueError as exc:
                raise SharedWalletCoordinatorError(
                    "INVALID_AUTHENTICATED_COLLATERAL_OBSERVATION"
                ) from exc
            if observed_at_ms < 0 or transition_id < 0:
                raise SharedWalletCoordinatorError(
                    "INVALID_AUTHENTICATED_COLLATERAL_OBSERVATION"
                )
            observations.append(
                {
                    "profile_key": profile,
                    "authenticated_collateral_usd": str(physical),
                    "observed_at_ms": str(observed_at_ms),
                    "cash_mutation_transition_id": str(transition_id),
                }
            )
        if not observations:
            return None
        return max(
            observations,
            key=lambda item: (int(item["observed_at_ms"]), item["profile_key"]),
        )

    def reconcile_redeemed_cash_credit_quarantines(
        self,
        *,
        authenticated_collateral_usd: Decimal,
        observed_at_ms: int,
    ) -> dict[str, str | int]:
        """Append proof when a CLOB cash sample covers all held payouts.

        A relayer confirmation establishes the redemption outcome, but the
        CLOB collateral balance can lag it.  A quarantine is therefore only
        released after the authenticated physical balance covers the raw
        coordinated ledger cash.  This method appends verification receipts;
        it never rewrites a redemption, account, position, or prior receipt.
        """

        physical = _decimal(
            authenticated_collateral_usd, "INVALID_AUTHENTICATED_COLLATERAL"
        )
        if int(observed_at_ms) < 0:
            raise SharedWalletCoordinatorError(
                "INVALID_AUTHENTICATED_COLLATERAL_OBSERVATION"
            )
        sleeves = self._sleeves()
        raw_high = ZERO
        pending_count = 0
        pending_payout = ZERO
        for sleeve in sleeves:
            adjustment = _decimal(
                sleeve["cash_adjustment_usd"], "INVALID_CASH_ADJUSTMENT"
            )
            with _read_ledger(Path(str(sleeve["ledger_path"]))) as ledger:
                # Full SQLite integrity scans run in the hourly server-health
                # audit.  This path executes alongside a new BUY and must not
                # turn one action into a scan of every sleeve ledger.
                permanent = _permanent_redeemed_cash_credit_block(ledger)
                adjusted = _ledger_cash(ledger) - adjustment - permanent
                pending = _pending_redeemed_cash_credit_quarantine(ledger)
                rows = ledger.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM redeemed_cash_credit_quarantines AS quarantine
                    LEFT JOIN redeemed_cash_credit_quarantine_verifications AS verification
                      ON verification.condition_id = quarantine.condition_id
                    LEFT JOIN redeemed_cash_credit_quarantine_voids AS void
                      ON void.condition_id = quarantine.condition_id
                    LEFT JOIN redeemed_cash_credit_permanent_blocks AS permanent
                      ON permanent.condition_id = quarantine.condition_id
                    WHERE verification.condition_id IS NULL
                      AND void.condition_id IS NULL
                      AND permanent.condition_id IS NULL
                    """
                ).fetchone()
            raw_high += adjusted
            pending_payout += pending
            pending_count += 0 if rows is None else int(rows["count"])
        if pending_count == 0:
            return {
                "state": "NO_PENDING_REDEEMED_CASH_CREDIT",
                "pending_receipt_count": 0,
                "pending_payout_usd": "0",
                "verified_receipt_count": 0,
            }
        if physical < raw_high:
            return {
                "state": "PENDING_PHYSICAL_CASH_CREDIT",
                "pending_receipt_count": pending_count,
                "pending_payout_usd": str(pending_payout),
                "verified_receipt_count": 0,
            }

        verified = 0
        for sleeve in sleeves:
            ledger_path = Path(str(sleeve["ledger_path"]))
            connection = sqlite3.connect(ledger_path, timeout=10)
            try:
                connection.execute("PRAGMA busy_timeout = 10000")
                connection.execute("BEGIN IMMEDIATE")
                rows = connection.execute(
                    """
                    SELECT quarantine.condition_id, quarantine.payout_usd
                    FROM redeemed_cash_credit_quarantines AS quarantine
                    LEFT JOIN redeemed_cash_credit_quarantine_verifications AS verification
                      ON verification.condition_id = quarantine.condition_id
                    LEFT JOIN redeemed_cash_credit_quarantine_voids AS void
                      ON void.condition_id = quarantine.condition_id
                    LEFT JOIN redeemed_cash_credit_permanent_blocks AS permanent
                      ON permanent.condition_id = quarantine.condition_id
                    WHERE verification.condition_id IS NULL
                      AND void.condition_id IS NULL
                      AND permanent.condition_id IS NULL
                    ORDER BY quarantine.condition_id
                    """
                ).fetchall()
                for condition_id, payout_usd in rows:
                    connection.execute(
                        """
                        INSERT INTO redeemed_cash_credit_quarantine_verifications(
                            condition_id, authenticated_collateral_usd,
                            verified_at_ms, details_json
                        ) VALUES(?, ?, ?, ?)
                        """,
                        (
                            str(condition_id),
                            str(physical),
                            int(observed_at_ms),
                            _canonical_json(
                                {
                                    "reason": "AUTHENTICATED_CLOB_COLLATERAL_COVERS_RAW_COORDINATED_CASH",
                                    "payout_usd": str(payout_usd),
                                }
                            ),
                        ),
                    )
                    verified += 1
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
        return {
            "state": "PHYSICAL_CASH_CREDIT_VERIFIED",
            "pending_receipt_count": pending_count,
            "pending_payout_usd": str(pending_payout),
            "verified_receipt_count": verified,
        }

    @staticmethod
    def _append_shared_condition_transition(
        connection: sqlite3.Connection,
        *,
        condition_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO shared_condition_transitions(
                condition_id, state, reason, created_at_ms, details_json
            ) VALUES(?, ?, ?, ?, ?)
            """,
            (
                str(condition_id).lower(),
                str(state),
                str(reason),
                int(created_at_ms),
                _canonical_json(details or {}),
            ),
        )

    def shared_condition_ids(self) -> set[str]:
        """Return conditions exposed in more than one live sleeve.

        Exposure includes inventory, an active BUY reservation, or a local
        redemption receipt.  Counting reservations is essential: otherwise a
        peer sleeve can redeem the physical condition while another sleeve's
        BUY is between reservation and reconciliation.
        """

        owners: dict[str, set[str]] = {}
        for row in self._sleeves():
            profile = str(row["profile_key"])
            with _read_ledger(Path(str(row["ledger_path"]))) as ledger:
                condition_rows = ledger.execute(
                    """
                    SELECT condition_id
                    FROM positions
                    WHERE CAST(quantity AS REAL) > 0 AND condition_id <> ''
                    UNION
                    SELECT c.condition_id
                    FROM condition_mappings AS c
                    LEFT JOIN positions AS p1
                      ON p1.token_id = c.primary_token_id
                    LEFT JOIN positions AS p2
                      ON p2.token_id = c.secondary_token_id
                    WHERE CAST(COALESCE(p1.quantity, '0') AS REAL) > 0
                       OR CAST(COALESCE(p2.quantity, '0') AS REAL) > 0
                    UNION
                    SELECT condition_id
                    FROM order_reservations
                    WHERE active = 1 AND side = 'BUY' AND condition_id <> ''
                    UNION
                    SELECT c.condition_id
                    FROM condition_mappings AS c
                    JOIN order_reservations AS r
                      ON r.token_id = c.primary_token_id
                      OR r.token_id = c.secondary_token_id
                    WHERE r.active = 1 AND r.side = 'BUY'
                    UNION
                    SELECT condition_id
                    FROM redemption_receipts
                    WHERE condition_id <> ''
                    """
                ).fetchall()
            for condition_row in condition_rows:
                condition = str(condition_row["condition_id"]).strip().lower()
                if condition:
                    owners.setdefault(condition, set()).add(profile)
        return {
            condition
            for condition, profiles in owners.items()
            if len(profiles) > 1
        }

    def _shared_condition_inventory(self, condition_id: str) -> dict[str, Any]:
        condition = str(condition_id).strip().lower()
        if not condition:
            raise SharedWalletCoordinatorError("INVALID_SHARED_CONDITION_ID")
        allocations: list[dict[str, Any]] = []
        mapping: tuple[str, str] | None = None
        active_order_reservations = 0
        active_local_redemptions = 0
        exposed_profiles: set[str] = set()
        handoff_states = {
            "NOT_SUBMITTED_RETRYABLE",
            "BLOCK_PRE_SUBMISSION_REVALIDATION",
            "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
        }
        for sleeve in self._sleeves():
            profile = str(sleeve["profile_key"])
            ledger_path = Path(str(sleeve["ledger_path"]))
            with _read_ledger(ledger_path) as ledger:
                direct = ledger.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM positions
                    WHERE condition_id = ? AND CAST(quantity AS REAL) > 0
                    """,
                    (condition,),
                ).fetchone()
                mapped = ledger.execute(
                    """
                    SELECT condition_id, primary_token_id, secondary_token_id
                    FROM condition_mappings WHERE condition_id = ?
                    """,
                    (condition,),
                ).fetchone()
                if mapped is None:
                    direct_reservation = ledger.execute(
                        """
                        SELECT COUNT(*) AS count FROM order_reservations
                        WHERE active = 1 AND side = 'BUY' AND condition_id = ?
                        """,
                        (condition,),
                    ).fetchone()
                    direct_redemption = ledger.execute(
                        "SELECT COUNT(*) AS count FROM redemption_receipts WHERE condition_id = ?",
                        (condition,),
                    ).fetchone()
                    if (
                        (direct is not None and int(direct["count"]) > 0)
                        or (
                            direct_reservation is not None
                            and int(direct_reservation["count"]) > 0
                        )
                        or (
                            direct_redemption is not None
                            and int(direct_redemption["count"]) > 0
                        )
                    ):
                        raise SharedWalletCoordinatorError(
                            f"MISSING_SHARED_CONDITION_MAPPING:{profile}:{condition}"
                        )
                    continue
                current_mapping = (
                    str(mapped["primary_token_id"]),
                    str(mapped["secondary_token_id"]),
                )
                if mapping is None:
                    mapping = current_mapping
                elif mapping != current_mapping:
                    raise SharedWalletCoordinatorError(
                        f"SHARED_CONDITION_MAPPING_MISMATCH:{condition}"
                    )
                inventory = ledger.execute(
                    """
                    SELECT
                        COALESCE(p1.quantity, '0') AS primary_quantity,
                        COALESCE(p1.cost_basis_usd, '0') AS primary_cost_basis_usd,
                        COALESCE(p2.quantity, '0') AS secondary_quantity,
                        COALESCE(p2.cost_basis_usd, '0') AS secondary_cost_basis_usd
                    FROM condition_mappings AS c
                    LEFT JOIN positions AS p1 ON p1.token_id = c.primary_token_id
                    LEFT JOIN positions AS p2 ON p2.token_id = c.secondary_token_id
                    WHERE c.condition_id = ?
                    """,
                    (condition,),
                ).fetchone()
                if inventory is None:
                    continue
                primary_quantity = _decimal(
                    inventory["primary_quantity"],
                    "INVALID_SHARED_PRIMARY_QUANTITY",
                )
                secondary_quantity = _decimal(
                    inventory["secondary_quantity"],
                    "INVALID_SHARED_SECONDARY_QUANTITY",
                )
                active = ledger.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM order_reservations
                    WHERE active = 1
                      AND (
                        condition_id = ?
                        OR token_id IN (?, ?)
                      )
                    """,
                    (condition, current_mapping[0], current_mapping[1]),
                ).fetchone()
                active_count = 0 if active is None else int(active["count"])
                active_order_reservations += active_count
                local_redemption = ledger.execute(
                    """
                    SELECT state, transaction_id, transaction_hash
                    FROM redemption_receipts WHERE condition_id = ?
                    """,
                    (condition,),
                ).fetchone()
                if local_redemption is not None:
                    state = str(local_redemption["state"])
                    has_transaction = bool(
                        str(local_redemption["transaction_id"] or "").strip()
                        or str(local_redemption["transaction_hash"] or "").strip()
                    )
                    if has_transaction or state not in handoff_states:
                        active_local_redemptions += 1
                if (
                    primary_quantity > ZERO
                    or secondary_quantity > ZERO
                    or active_count > 0
                    or local_redemption is not None
                ):
                    exposed_profiles.add(profile)
                if primary_quantity == ZERO and secondary_quantity == ZERO:
                    continue
                primary_cost = _decimal(
                    inventory["primary_cost_basis_usd"],
                    "INVALID_SHARED_PRIMARY_COST",
                )
                secondary_cost = _decimal(
                    inventory["secondary_cost_basis_usd"],
                    "INVALID_SHARED_SECONDARY_COST",
                )
                allocations.append(
                    {
                        "profile_key": profile,
                        "ledger_path": str(ledger_path.resolve()),
                        "primary_quantity": primary_quantity,
                        "primary_cost_basis_usd": primary_cost,
                        "secondary_quantity": secondary_quantity,
                        "secondary_cost_basis_usd": secondary_cost,
                    }
                )
        if mapping is None or len(exposed_profiles) < 2:
            raise SharedWalletCoordinatorError(
                f"SHARED_CONDITION_REQUIRES_MULTIPLE_OWNERS:{condition}"
            )
        if (
            len(allocations) < 2
            and active_order_reservations == 0
            and active_local_redemptions == 0
        ):
            raise SharedWalletCoordinatorError(
                f"SHARED_CONDITION_REQUIRES_MULTIPLE_OWNERS:{condition}"
            )
        allocations.sort(key=lambda item: item["profile_key"])
        primary_quantity = sum(
            (item["primary_quantity"] for item in allocations), ZERO
        )
        secondary_quantity = sum(
            (item["secondary_quantity"] for item in allocations), ZERO
        )
        hash_payload = {
            "condition_id": condition,
            "primary_token_id": mapping[0],
            "secondary_token_id": mapping[1],
            "allocations": [
                {
                    key: str(value)
                    for key, value in allocation.items()
                    if key != "ledger_path"
                }
                | {"ledger_path": str(allocation["ledger_path"])}
                for allocation in allocations
            ],
        }
        inventory_hash = hashlib.sha256(
            _canonical_json(hash_payload).encode("utf-8")
        ).hexdigest()
        return {
            "condition_id": condition,
            "primary_token_id": mapping[0],
            "secondary_token_id": mapping[1],
            "primary_quantity": primary_quantity,
            "secondary_quantity": secondary_quantity,
            "allocations": allocations,
            "inventory_hash": inventory_hash,
            "active_order_reservation_count": active_order_reservations,
            "active_local_redemption_count": active_local_redemptions,
            "exposed_profile_keys": sorted(exposed_profiles),
        }

    def shared_condition_inventory(self, condition_id: str) -> dict[str, Any]:
        """Expose the exact read-through inventory for wallet orchestration."""

        return self._shared_condition_inventory(condition_id)

    def freeze_shared_condition_redemption(
        self,
        *,
        condition_id: str,
        winner_token_id: str,
        created_at_ms: int,
    ) -> dict[str, Any]:
        """Freeze exact sleeve attribution before any wallet redemption call."""

        condition = str(condition_id).strip().lower()
        existing = self.shared_redemption_receipt(condition)
        if existing is not None:
            if str(existing["winner_token_id"]) != str(winner_token_id):
                raise SharedWalletCoordinatorError(
                    "SHARED_REDEMPTION_WINNER_CHANGED"
                )
            return existing
        inventory = self._shared_condition_inventory(condition)
        if inventory["active_order_reservation_count"] != 0:
            raise SharedWalletCoordinatorError(
                "ACTIVE_SHARED_CONDITION_ORDER_RESERVATION"
            )
        if inventory["active_local_redemption_count"] != 0:
            raise SharedWalletCoordinatorError(
                "ACTIVE_LOCAL_REDEMPTION_BLOCKS_SHARED_HANDOFF"
            )
        winner = str(winner_token_id).strip()
        token_ids = {
            str(inventory["primary_token_id"]),
            str(inventory["secondary_token_id"]),
        }
        if winner not in token_ids:
            raise SharedWalletCoordinatorError(
                "SHARED_REDEMPTION_WINNER_TOKEN_MISMATCH"
            )
        payout_key = (
            "primary_quantity"
            if winner == str(inventory["primary_token_id"])
            else "secondary_quantity"
        )
        expected_payout = Decimal(str(inventory[payout_key]))
        observed = int(created_at_ms)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            concurrent = connection.execute(
                "SELECT * FROM shared_condition_redemptions WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if concurrent is not None:
                return {key: str(concurrent[key]) for key in concurrent.keys()}
            connection.execute(
                """
                INSERT INTO shared_condition_redemptions(
                    condition_id, state, primary_token_id, secondary_token_id,
                    winner_token_id, primary_quantity, secondary_quantity,
                    expected_payout_usd, inventory_hash, transaction_id,
                    transaction_hash, created_at_ms, updated_at_ms
                ) VALUES(?, 'READY', ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    condition,
                    str(inventory["primary_token_id"]),
                    str(inventory["secondary_token_id"]),
                    winner,
                    str(inventory["primary_quantity"]),
                    str(inventory["secondary_quantity"]),
                    str(expected_payout),
                    str(inventory["inventory_hash"]),
                    observed,
                    observed,
                ),
            )
            for allocation in inventory["allocations"]:
                payout = Decimal(str(allocation[payout_key]))
                connection.execute(
                    """
                    INSERT INTO shared_condition_allocations(
                        condition_id, profile_key, ledger_path,
                        primary_quantity, primary_cost_basis_usd,
                        secondary_quantity, secondary_cost_basis_usd,
                        payout_usd, apply_state, applied_at_ms
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', NULL)
                    """,
                    (
                        condition,
                        str(allocation["profile_key"]),
                        str(allocation["ledger_path"]),
                        str(allocation["primary_quantity"]),
                        str(allocation["primary_cost_basis_usd"]),
                        str(allocation["secondary_quantity"]),
                        str(allocation["secondary_cost_basis_usd"]),
                        str(payout),
                    ),
                )
            self._append_shared_condition_transition(
                connection,
                condition_id=condition,
                state="READY",
                reason="EXACT_MULTI_SLEEVE_INVENTORY_FROZEN",
                created_at_ms=observed,
                details={
                    "inventory_hash": inventory["inventory_hash"],
                    "owner_profile_keys": [
                        allocation["profile_key"]
                        for allocation in inventory["allocations"]
                    ],
                },
            )
        frozen = self.shared_redemption_receipt(condition)
        if frozen is None:
            raise SharedWalletCoordinatorError(
                "SHARED_REDEMPTION_RECEIPT_NOT_PERSISTED"
            )
        return frozen

    def shared_redemption_receipt(self, condition_id: str) -> dict[str, str] | None:
        self.initialize()
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM shared_condition_redemptions WHERE condition_id = ?",
                (str(condition_id).lower(),),
            ).fetchone()
        if row is None:
            return None
        return {
            key: "" if row[key] is None else str(row[key])
            for key in row.keys()
        }

    def shared_redemption_receipts(self) -> list[dict[str, str]]:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM shared_condition_redemptions ORDER BY condition_id"
            ).fetchall()
        return [
            {
                key: "" if row[key] is None else str(row[key])
                for key in row.keys()
            }
            for row in rows
        ]

    def shared_managed_condition_ids(self) -> set[str]:
        return {
            str(row["condition_id"]).lower()
            for row in self.shared_redemption_receipts()
        } | self.shared_condition_ids()

    def shared_condition_allocations(
        self, condition_id: str
    ) -> list[dict[str, str]]:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM shared_condition_allocations
                WHERE condition_id = ? ORDER BY profile_key
                """,
                (str(condition_id).lower(),),
            ).fetchall()
        return [
            {
                key: "" if row[key] is None else str(row[key])
                for key in row.keys()
            }
            for row in rows
        ]

    def verify_shared_condition_inventory(self, condition_id: str) -> dict[str, Any]:
        receipt = self.shared_redemption_receipt(condition_id)
        if receipt is None:
            raise SharedWalletCoordinatorError("MISSING_SHARED_REDEMPTION_RECEIPT")
        inventory = self._shared_condition_inventory(condition_id)
        if str(inventory["inventory_hash"]) != str(receipt["inventory_hash"]):
            raise SharedWalletCoordinatorError("SHARED_CONDITION_INVENTORY_CHANGED")
        if inventory["active_order_reservation_count"] != 0:
            raise SharedWalletCoordinatorError(
                "ACTIVE_SHARED_CONDITION_ORDER_RESERVATION"
            )
        return inventory

    def start_shared_redemption_submission(
        self, *, condition_id: str, created_at_ms: int
    ) -> bool:
        condition = str(condition_id).lower()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state, transaction_id, transaction_hash FROM shared_condition_redemptions WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if row is None:
                raise SharedWalletCoordinatorError("MISSING_SHARED_REDEMPTION_RECEIPT")
            if str(row["state"]) not in {"READY", "NOT_SUBMITTED_RETRYABLE"}:
                return False
            if row["transaction_id"] is not None or row["transaction_hash"] is not None:
                raise SharedWalletCoordinatorError(
                    "SHARED_RETRYABLE_REDEMPTION_HAS_TRANSACTION_EVIDENCE"
                )
            connection.execute(
                """
                UPDATE shared_condition_redemptions
                SET state = 'SUBMIT_STARTED', updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (int(created_at_ms), condition),
            )
            self._append_shared_condition_transition(
                connection,
                condition_id=condition,
                state="SUBMIT_STARTED",
                reason="",
                created_at_ms=created_at_ms,
            )
        return True

    def mark_shared_redemption_submission(
        self,
        *,
        condition_id: str,
        transaction_id: str,
        transaction_hash: str | None,
        created_at_ms: int,
    ) -> None:
        transaction = str(transaction_id).strip()
        if not transaction:
            raise SharedWalletCoordinatorError(
                "MISSING_SHARED_REDEMPTION_TRANSACTION_ID"
            )
        self.mark_shared_redemption_state(
            condition_id=condition_id,
            state="SUBMITTED_UNRECONCILED",
            reason="",
            created_at_ms=created_at_ms,
            transaction_id=transaction,
            transaction_hash=transaction_hash,
        )

    def mark_shared_redemption_state(
        self,
        *,
        condition_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        details: dict[str, Any] | None = None,
        transaction_id: str | None = None,
        transaction_hash: str | None = None,
    ) -> None:
        condition = str(condition_id).lower()
        target_state = str(state).strip().upper()
        incoming_transaction_id = str(transaction_id or "").strip()
        incoming_transaction_hash = str(transaction_hash or "").strip()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT state, transaction_id, transaction_hash
                FROM shared_condition_redemptions WHERE condition_id = ?
                """,
                (condition,),
            ).fetchone()
            if row is None:
                raise SharedWalletCoordinatorError("MISSING_SHARED_REDEMPTION_RECEIPT")
            current_state = str(row["state"]).strip().upper()
            _validate_shared_redemption_transition(current_state, target_state)
            existing_transaction_id = str(row["transaction_id"] or "").strip()
            existing_transaction_hash = str(row["transaction_hash"] or "").strip()
            if (
                existing_transaction_id
                and incoming_transaction_id
                and existing_transaction_id != incoming_transaction_id
            ):
                raise SharedWalletCoordinatorError(
                    "SHARED_REDEMPTION_TRANSACTION_ID_CHANGED"
                )
            if (
                existing_transaction_hash
                and incoming_transaction_hash
                and existing_transaction_hash != incoming_transaction_hash
            ):
                raise SharedWalletCoordinatorError(
                    "SHARED_REDEMPTION_TRANSACTION_HASH_CHANGED"
                )
            next_transaction_id = (
                incoming_transaction_id or existing_transaction_id or None
            )
            next_transaction_hash = (
                incoming_transaction_hash or existing_transaction_hash or None
            )
            connection.execute(
                """
                UPDATE shared_condition_redemptions
                SET state = ?,
                    transaction_id = ?,
                    transaction_hash = ?,
                    updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (
                    target_state,
                    next_transaction_id,
                    next_transaction_hash,
                    int(created_at_ms),
                    condition,
                ),
            )
            self._append_shared_condition_transition(
                connection,
                condition_id=condition,
                state=target_state,
                reason=reason,
                created_at_ms=created_at_ms,
                details=details,
            )

    def mark_shared_allocation_applied(
        self,
        *,
        condition_id: str,
        profile_key: str,
        created_at_ms: int,
    ) -> None:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                "SELECT state FROM shared_condition_redemptions WHERE condition_id = ?",
                (str(condition_id).lower(),),
            ).fetchone()
            if receipt is None:
                raise SharedWalletCoordinatorError(
                    "MISSING_SHARED_REDEMPTION_RECEIPT"
                )
            receipt_state = str(receipt["state"]).strip().upper()
            if receipt_state not in _SHARED_DISTRIBUTION_STATES:
                raise SharedWalletCoordinatorError(
                    "SHARED_ALLOCATION_APPLY_OUTSIDE_DISTRIBUTION:"
                    + receipt_state
                )
            allocation = connection.execute(
                """
                SELECT apply_state FROM shared_condition_allocations
                WHERE condition_id = ? AND profile_key = ?
                """,
                (str(condition_id).lower(), str(profile_key)),
            ).fetchone()
            if allocation is None:
                raise SharedWalletCoordinatorError(
                    "SHARED_REDEMPTION_ALLOCATION_NOT_FOUND"
                )
            if str(allocation["apply_state"]).upper() == "APPLIED":
                return
            cursor = connection.execute(
                """
                UPDATE shared_condition_allocations
                SET apply_state = 'APPLIED', applied_at_ms = COALESCE(applied_at_ms, ?)
                WHERE condition_id = ? AND profile_key = ?
                """,
                (
                    int(created_at_ms),
                    str(condition_id).lower(),
                    str(profile_key),
                ),
            )
            if cursor.rowcount != 1:
                raise SharedWalletCoordinatorError(
                    "SHARED_REDEMPTION_ALLOCATION_NOT_FOUND"
                )

    def complete_shared_redemption_distribution(
        self,
        *,
        condition_id: str,
        terminal_state: str,
        created_at_ms: int,
        details: dict[str, Any] | None = None,
    ) -> bool:
        condition = str(condition_id).lower()
        target_state = str(terminal_state).strip().upper()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            receipt = connection.execute(
                "SELECT state FROM shared_condition_redemptions WHERE condition_id = ?",
                (condition,),
            ).fetchone()
            if receipt is None:
                raise SharedWalletCoordinatorError(
                    "MISSING_SHARED_REDEMPTION_RECEIPT"
                )
            current_state = str(receipt["state"]).strip().upper()
            _validate_shared_redemption_transition(current_state, target_state)
            pending = connection.execute(
                """
                SELECT COUNT(*) AS count FROM shared_condition_allocations
                WHERE condition_id = ? AND apply_state <> 'APPLIED'
                """,
                (condition,),
            ).fetchone()
            if pending is None or int(pending["count"]) != 0:
                return False
            connection.execute(
                """
                UPDATE shared_condition_redemptions
                SET state = ?, updated_at_ms = ?
                WHERE condition_id = ?
                """,
                (target_state, int(created_at_ms), condition),
            )
            self._append_shared_condition_transition(
                connection,
                condition_id=condition,
                state=target_state,
                reason="",
                created_at_ms=created_at_ms,
                details=details,
            )
        return True

    def buy_collision(
        self,
        *,
        profile_key: str,
        token_id: str,
        condition_id: str,
    ) -> dict[str, str]:
        profile = str(profile_key).strip()
        token = str(token_id).strip()
        condition = str(condition_id).strip().lower()
        if not token:
            raise SharedWalletCoordinatorError("INVALID_COLLISION_TOKEN")
        for row in self._sleeves():
            owner = str(row["profile_key"])
            if owner == profile:
                continue
            with _read_ledger(Path(str(row["ledger_path"]))) as ledger:
                canonical_mapping = None
                if condition:
                    canonical_mapping = ledger.execute(
                        """
                        SELECT primary_token_id, secondary_token_id
                        FROM condition_mappings WHERE condition_id = ?
                        """,
                        (condition,),
                    ).fetchone()
                token_owner = ledger.execute(
                    """
                    SELECT token_id, condition_id FROM positions
                    WHERE token_id = ? AND CAST(quantity AS REAL) > 0
                    UNION ALL
                    SELECT token_id, condition_id FROM order_reservations
                    WHERE token_id = ? AND side = 'BUY' AND active = 1
                    LIMIT 1
                    """,
                    (token, token),
                ).fetchone()
                if token_owner is not None:
                    owner_condition = str(
                        token_owner["condition_id"] or ""
                    ).strip().lower()
                    if not owner_condition:
                        mapped_owner = ledger.execute(
                            """
                            SELECT condition_id FROM condition_mappings
                            WHERE primary_token_id = ? OR secondary_token_id = ?
                            LIMIT 1
                            """,
                            (token, token),
                        ).fetchone()
                        owner_condition = (
                            ""
                            if mapped_owner is None
                            else str(mapped_owner["condition_id"]).strip().lower()
                        )
                    if condition and owner_condition and owner_condition != condition:
                        return {
                            "state": "BLOCK_TOKEN_CONDITION_MAPPING_MISMATCH",
                            "owner_profile_key": owner,
                            "token_id": token,
                            "condition_id": condition,
                            "owner_condition_id": owner_condition,
                        }
                    if condition and owner_condition == condition:
                        if canonical_mapping is None:
                            return {
                                "state": "BLOCK_SHARED_CONDITION_MAPPING_UNAVAILABLE",
                                "owner_profile_key": owner,
                                "token_id": token,
                                "condition_id": condition,
                            }
                        if token not in {
                            str(canonical_mapping["primary_token_id"]),
                            str(canonical_mapping["secondary_token_id"]),
                        }:
                            return {
                                "state": "BLOCK_SHARED_CONDITION_TOKEN_NOT_IN_CANONICAL_PAIR",
                                "owner_profile_key": owner,
                                "token_id": token,
                                "condition_id": condition,
                            }
                        return {
                            "state": "CLEAR_SHARED_CONDITION",
                            "owner_profile_key": owner,
                            "token_id": token,
                            "condition_id": condition,
                        }
                    return {
                        "state": "BLOCK_CROSS_SLEEVE_TOKEN_OWNERSHIP",
                        "owner_profile_key": owner,
                        "token_id": token,
                        "condition_id": condition,
                    }
                if condition:
                    direct_condition_owner = ledger.execute(
                        """
                        SELECT condition_id
                        FROM positions
                        WHERE condition_id = ? AND CAST(quantity AS REAL) > 0
                        UNION ALL
                        SELECT condition_id
                        FROM order_reservations
                        WHERE condition_id = ? AND side = 'BUY' AND active = 1
                        LIMIT 1
                        """,
                        (condition, condition),
                    ).fetchone()
                    if direct_condition_owner is not None:
                        if canonical_mapping is None:
                            return {
                                "state": "BLOCK_SHARED_CONDITION_MAPPING_UNAVAILABLE",
                                "owner_profile_key": owner,
                                "token_id": token,
                                "condition_id": condition,
                            }
                        if token not in {
                            str(canonical_mapping["primary_token_id"]),
                            str(canonical_mapping["secondary_token_id"]),
                        }:
                            return {
                                "state": "BLOCK_SHARED_CONDITION_TOKEN_NOT_IN_CANONICAL_PAIR",
                                "owner_profile_key": owner,
                                "token_id": token,
                                "condition_id": condition,
                            }
                        return {
                            "state": "CLEAR_SHARED_CONDITION",
                            "owner_profile_key": owner,
                            "token_id": token,
                            "condition_id": condition,
                        }
                    condition_owner = ledger.execute(
                        """
                        SELECT c.condition_id
                        FROM condition_mappings AS c
                        LEFT JOIN positions AS p1
                          ON p1.token_id = c.primary_token_id
                        LEFT JOIN positions AS p2
                          ON p2.token_id = c.secondary_token_id
                        WHERE c.condition_id = ?
                          AND (
                            CAST(COALESCE(p1.quantity, '0') AS REAL) > 0
                            OR CAST(COALESCE(p2.quantity, '0') AS REAL) > 0
                          )
                        LIMIT 1
                        """,
                        (condition,),
                    ).fetchone()
                    if condition_owner is not None:
                        if canonical_mapping is None or token not in {
                            str(canonical_mapping["primary_token_id"]),
                            str(canonical_mapping["secondary_token_id"]),
                        }:
                            return {
                                "state": "BLOCK_SHARED_CONDITION_TOKEN_NOT_IN_CANONICAL_PAIR",
                                "owner_profile_key": owner,
                                "token_id": token,
                                "condition_id": condition,
                            }
                        return {
                            "state": "CLEAR_SHARED_CONDITION",
                            "owner_profile_key": owner,
                            "token_id": token,
                            "condition_id": condition,
                        }
        return {
            "state": "CLEAR",
            "owner_profile_key": profile,
            "token_id": token,
            "condition_id": condition,
        }

    def status(self, authenticated_collateral_usd: Decimal) -> dict[str, Any]:
        receipt = self.migration_receipt()
        rows = self._sleeves()
        snapshot = self.authenticated_account_cash_snapshot(
            authenticated_collateral_usd=authenticated_collateral_usd
        ).as_dict()
        return {
            "state": snapshot["state"],
            "migration_receipt_hash": receipt["migration_receipt_hash"],
            "authenticated_account": snapshot,
            "registered_profiles": [str(row["profile_key"]) for row in rows],
        }
