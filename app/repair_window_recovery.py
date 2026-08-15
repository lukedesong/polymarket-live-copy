"""Explicit, receipt-bound delayed recovery for an internal repair window.

This module is intentionally inert until an operator activates one canonical
manifest.  It never discovers generic historical gaps and never changes the
normal forward-copy price contract.
"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Callable, Mapping

from cd90_live_copy import (
    LiveConfigurationError,
    LiveStore,
    SourceAction,
    _fail_closed_stale_causal_target,
    _is_retryable_external_error,
    canonical_hash,
    execute_source_action,
    now_ms,
)


ZERO = Decimal("0")
INTERNAL_GAP_REASON = "PRE_REPAIR_INTERNAL_UNPRICED_GAP_NO_ACTION_TIME_CLOB"
INTERNAL_GAP_PRICING = "PRE_REPAIR_INTERNAL_UNPRICED_NO_ACTION_TIME_CLOB"
POLICY_KIND = "ZERO_ADDITIONAL_LOSS_AFTER_OFFICIAL_FEE"
TERMINAL_RECOVERY_STATES = {
    "IMPORTED_MANUAL_TERMINAL",
    "NO_ORDER_COVERED_BY_CUMULATIVE_SURPLUS",
    "FILLED",
    "PARTIAL_TERMINAL_PREFIX_PRESERVED",
    "SUPERSEDED_BY_LATER_SOURCE_ACTION",
    "EXTERNAL_UNFILLABLE",
}
SUBMISSION_SIDE_EFFECT_BLOCKED_STATE = (
    "BLOCKED_UNRESOLVED_SUBMISSION_SIDE_EFFECT"
)
BLOCKED_RECOVERY_STATES = {
    "BLOCKED_INTERNAL_STALE_CAUSAL_TARGET",
    SUBMISSION_SIDE_EFFECT_BLOCKED_STATE,
}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def fee_adjusted_unit_value(
    *, side: str, price: Decimal, fee_rate: Decimal, fee_exponent: int
) -> Decimal:
    """Return all-in BUY cost or net SELL proceeds for one outcome share."""

    normalized_side = str(side).upper()
    p = Decimal(str(price))
    rate = Decimal(str(fee_rate))
    try:
        exponent = int(fee_exponent)
    except (TypeError, ValueError) as exc:
        raise LiveConfigurationError("INVALID_RECOVERY_FEE_EXPONENT") from exc
    if (
        normalized_side not in {"BUY", "SELL"}
        or p <= ZERO
        or p >= Decimal("1")
        or rate < ZERO
        or exponent < 0
        or Decimal(str(fee_exponent)) != Decimal(exponent)
    ):
        raise LiveConfigurationError("INVALID_RECOVERY_FEE_INPUT")
    fee = rate * (p * (Decimal("1") - p)) ** exponent
    return p + fee if normalized_side == "BUY" else p - fee


class _FrozenLosslessExecution:
    """Expose one already-frozen guarded snapshot to the existing submitter."""

    def __init__(self, base: Any, snapshot: Mapping[str, Any]):
        self.base = base
        self._snapshot = dict(snapshot)
        self._used = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.base, name)

    def snapshot(self, *, token_id: str, side: str) -> dict[str, Any]:
        if self._used:
            raise LiveConfigurationError("RECOVERY_FROZEN_SNAPSHOT_REUSED")
        self._used = True
        expected_token = str(
            self._snapshot.get("repair_window_recovery", {}).get("token_id") or ""
        )
        expected_side = str(
            self._snapshot.get("repair_window_recovery", {}).get("side") or ""
        ).upper()
        if str(token_id) != expected_token or str(side).upper() != expected_side:
            raise LiveConfigurationError("RECOVERY_FROZEN_SNAPSHOT_IDENTITY_MISMATCH")
        return dict(self._snapshot)


class RepairWindowRecoveryManager:
    def __init__(self, store: LiveStore):
        self.store = store
        self.initialize()

    def initialize(self) -> None:
        self.store.initialize()
        with self.store.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS repair_recovery_manifests(
                    manifest_hash TEXT PRIMARY KEY,
                    profile_key TEXT NOT NULL,
                    gap_receipt_id INTEGER NOT NULL,
                    policy_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    activated_at_ms INTEGER NOT NULL,
                    last_processed_head INTEGER,
                    manifest_json TEXT NOT NULL,
                    UNIQUE(profile_key, gap_receipt_id)
                );
                CREATE TABLE IF NOT EXISTS repair_recovery_actions(
                    manifest_hash TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    source_unit_price TEXT NOT NULL,
                    last_evaluated_head INTEGER,
                    last_snapshot_json TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(manifest_hash, action_id),
                    FOREIGN KEY(manifest_hash)
                        REFERENCES repair_recovery_manifests(manifest_hash)
                );
                CREATE TABLE IF NOT EXISTS repair_recovery_transitions(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    manifest_hash TEXT NOT NULL,
                    action_id TEXT,
                    state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_repair_recovery_action_state
                    ON repair_recovery_actions(manifest_hash, state, updated_at_ms);
                """
            )

    def _manifest_row(self, *, active_only: bool = False):
        where = "WHERE state = 'ACTIVE'" if active_only else ""
        with self.store.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM repair_recovery_manifests
                {where}
                ORDER BY activated_at_ms DESC, manifest_hash DESC
                """
            ).fetchall()
        if not rows:
            return None
        if active_only and len(rows) != 1:
            raise LiveConfigurationError("MULTIPLE_ACTIVE_RECOVERY_MANIFESTS")
        return rows[0]

    @staticmethod
    def _manifest_receipt(row: Any) -> dict[str, Any]:
        return {
            "manifest_hash": str(row["manifest_hash"]),
            "profile_key": str(row["profile_key"]),
            "gap_receipt_id": int(row["gap_receipt_id"]),
            "policy_hash": str(row["policy_hash"]),
            "state": str(row["state"]),
            "activated_at_ms": int(row["activated_at_ms"]),
            "last_processed_head": (
                None
                if row["last_processed_head"] is None
                else int(row["last_processed_head"])
            ),
            "manifest": json.loads(str(row["manifest_json"])),
        }

    def active_manifest(self) -> dict[str, Any] | None:
        row = self._manifest_row(active_only=True)
        return None if row is None else self._manifest_receipt(row)

    def claimed_action_ids(self) -> set[str]:
        active = self.active_manifest()
        if active is None:
            return set()
        with self.store.connect() as connection:
            rows = connection.execute(
                """
                SELECT action_id FROM repair_recovery_actions
                WHERE manifest_hash = ?
                """,
                (active["manifest_hash"],),
            ).fetchall()
        return {str(row["action_id"]) for row in rows}

    @staticmethod
    def _causal_key(source: SourceAction) -> tuple[int, int, int, str, str, str, str]:
        return (
            int(source.block_number),
            int(source.log_index),
            int(source.source_timestamp),
            str(source.transaction_hash).lower(),
            str(source.token_id),
            str(source.side).upper(),
            str(source.order_hash).lower(),
        )

    def claim_new_causal_dependents(self, *, claimed_at_ms: int) -> list[str]:
        """Claim only immutable descendants blocked by an active repair action.

        A source action discovered while the repair chain is unresolved must
        not escape into the ordinary current-book retry queue after a restart.
        The latest transition has to prove the exact predecessor relation; no
        generic same-token history is swept into this manifest.
        """

        active = self.active_manifest()
        if active is None:
            return []
        manifest_hash = str(active["manifest_hash"])
        claimed = self.claimed_action_ids()
        newly_claimed: list[str] = []
        while True:
            with self.store.connect() as connection:
                rows = connection.execute(
                    """
                    SELECT a.*,
                           latest.status AS latest_status,
                           latest.reason AS latest_reason,
                           latest.details_json AS latest_details_json
                    FROM action_receipts AS a
                    JOIN action_transitions AS latest
                      ON latest.id = (
                          SELECT id FROM action_transitions
                          WHERE action_id = a.action_id
                          ORDER BY id DESC LIMIT 1
                      )
                    WHERE latest.status = 'PENDING_CAUSAL_ORDER'
                      AND latest.reason = 'PRIOR_SAME_TOKEN_ACTION_NOT_TERMINAL'
                    ORDER BY a.block_number, a.source_log_index,
                             a.source_timestamp, a.transaction_hash,
                             a.token_id, a.side, a.order_hash, a.action_id
                    """
                ).fetchall()
            candidates: list[tuple[SourceAction, str]] = []
            for row in rows:
                action_id = str(row["action_id"])
                if action_id in claimed:
                    continue
                details = json.loads(str(row["latest_details_json"] or "{}"))
                prior_action_id = str(
                    (details.get("prior_action") or {}).get("action_id") or ""
                )
                if prior_action_id not in claimed:
                    continue
                source = self.store._source_from_receipt(row)
                prior = self._source(prior_action_id)
                attempts, reservations = self._attempt_and_reservation_counts(action_id)
                if (
                    attempts != 0
                    or reservations != 0
                    or source.token_id != prior.token_id
                    or self._causal_key(source) <= self._causal_key(prior)
                    or source.source_quantity <= ZERO
                    or source.source_notional <= ZERO
                ):
                    raise LiveConfigurationError(
                        "RECOVERY_DEPENDENT_ACTION_CAUSAL_MISMATCH"
                    )
                candidates.append((source, prior_action_id))
            if not candidates:
                break
            with self.store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                for source, prior_action_id in candidates:
                    connection.execute(
                        """
                        INSERT INTO repair_recovery_actions(
                            manifest_hash, action_id, action_kind, state,
                            source_unit_price, last_evaluated_head,
                            last_snapshot_json, created_at_ms, updated_at_ms
                        ) VALUES(?, ?, 'CAUSAL_DEPENDENT_ACTION', 'AUTHORIZED',
                                 ?, NULL, NULL, ?, ?)
                        ON CONFLICT(manifest_hash, action_id) DO NOTHING
                        """,
                        (
                            manifest_hash,
                            source.action_id,
                            str(source.source_notional / source.source_quantity),
                            int(claimed_at_ms),
                            int(claimed_at_ms),
                        ),
                    )
                    if connection.execute("SELECT changes()").fetchone()[0] != 1:
                        continue
                    connection.execute(
                        """
                        INSERT INTO repair_recovery_transitions(
                            manifest_hash, action_id, state, reason,
                            created_at_ms, details_json
                        ) VALUES(?, ?, 'AUTHORIZED',
                                 'IMMUTABLE_CAUSAL_DESCENDANT_CLAIMED', ?, ?)
                        """,
                        (
                            manifest_hash,
                            source.action_id,
                            int(claimed_at_ms),
                            _json({"prior_action_id": prior_action_id}),
                        ),
                    )
                    claimed.add(source.action_id)
                    newly_claimed.append(source.action_id)
        return newly_claimed

    def _source(self, action_id: str) -> SourceAction:
        with self.store.connect() as connection:
            row = connection.execute(
                "SELECT * FROM action_receipts WHERE action_id = ?",
                (str(action_id),),
            ).fetchone()
        if row is None:
            raise LiveConfigurationError("RECOVERY_ACTION_RECEIPT_MISSING")
        return self.store._source_from_receipt(row)

    def _attempt_and_reservation_counts(self, action_id: str) -> tuple[int, int]:
        with self.store.connect() as connection:
            attempts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM submission_attempts WHERE action_id = ?",
                    (str(action_id),),
                ).fetchone()[0]
            )
            active = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM order_reservations
                    WHERE action_id = ? AND active = 1
                    """,
                    (str(action_id),),
                ).fetchone()[0]
            )
        return attempts, active

    def activate_manifest(
        self, *, manifest: Mapping[str, Any], activated_at_ms: int
    ) -> dict[str, Any]:
        payload = json.loads(_json(dict(manifest)))
        if int(payload.get("version", 0)) != 1:
            raise LiveConfigurationError("UNSUPPORTED_RECOVERY_MANIFEST_VERSION")
        profile_key = str(payload.get("profile_key") or "")
        if not profile_key or self.store.config("profile_key") != profile_key:
            raise LiveConfigurationError("RECOVERY_PROFILE_MISMATCH")
        policy = payload.get("policy")
        if not isinstance(policy, dict) or policy.get("kind") != POLICY_KIND:
            raise LiveConfigurationError("RECOVERY_POLICY_MISMATCH")
        manifest_hash = canonical_hash(payload)
        policy_hash = canonical_hash(policy)
        with self.store.connect() as connection:
            existing = connection.execute(
                "SELECT * FROM repair_recovery_manifests WHERE manifest_hash = ?",
                (manifest_hash,),
            ).fetchone()
        if existing is not None:
            return self._manifest_receipt(existing)
        if self.active_manifest() is not None:
            raise LiveConfigurationError("ACTIVE_RECOVERY_MANIFEST_CONFLICT")

        gap_id = int(payload.get("gap_receipt_id", -1))
        with self.store.connect() as connection:
            gap = connection.execute(
                "SELECT * FROM runtime_gap_receipts WHERE id = ?", (gap_id,)
            ).fetchone()
        if gap is None:
            raise LiveConfigurationError("RECOVERY_GAP_RECEIPT_MISSING")
        expected_gap = payload.get("expected_gap")
        if not isinstance(expected_gap, dict):
            raise LiveConfigurationError("RECOVERY_EXPECTED_GAP_MISSING")
        if (
            str(gap["reason"]) != INTERNAL_GAP_REASON
            or str(expected_gap.get("reason")) != INTERNAL_GAP_REASON
        ):
            raise LiveConfigurationError("RECOVERY_GAP_REASON_MISMATCH")
        if (
            str(gap["pricing_status"]) != INTERNAL_GAP_PRICING
            or str(expected_gap.get("pricing_status")) != INTERNAL_GAP_PRICING
        ):
            raise LiveConfigurationError("RECOVERY_GAP_PRICING_MISMATCH")
        for key in ("previous_processed_block", "resume_head"):
            if int(gap[key]) != int(expected_gap.get(key, -1)):
                raise LiveConfigurationError("RECOVERY_GAP_BOUNDARY_MISMATCH")
        gap_action_ids = [str(value) for value in payload.get("gap_action_ids", [])]
        recorded_gap_ids = [
            str(value)
            for value in json.loads(str(gap["details_json"])).get("action_ids", [])
        ]
        if gap_action_ids != recorded_gap_ids or not gap_action_ids:
            raise LiveConfigurationError("RECOVERY_GAP_ACTION_SET_MISMATCH")
        manual_ids = {
            str(value) for value in payload.get("manual_completed_action_ids", [])
        }
        manual_evidence = payload.get("manual_completion_evidence", {})
        if not isinstance(manual_evidence, dict):
            raise LiveConfigurationError(
                "RECOVERY_MANUAL_COMPLETION_EVIDENCE_INVALID"
            )
        if set(str(key) for key in manual_evidence) != manual_ids:
            raise LiveConfigurationError(
                "RECOVERY_MANUAL_COMPLETION_EVIDENCE_MISSING"
            )
        dependent_ids = [
            str(value) for value in payload.get("causal_dependent_action_ids", [])
        ]
        if not manual_ids.issubset(set(gap_action_ids)):
            raise LiveConfigurationError("RECOVERY_MANUAL_ACTION_NOT_IN_GAP")
        if set(dependent_ids) & set(gap_action_ids):
            raise LiveConfigurationError("RECOVERY_DEPENDENT_ACTION_DUPLICATES_GAP")

        action_rows: list[tuple[SourceAction, str, str]] = []
        causal_predecessors = set(gap_action_ids)
        for action_id in [*gap_action_ids, *dependent_ids]:
            source = self._source(action_id)
            latest = self.store.latest_transition(source)
            attempts, active = self._attempt_and_reservation_counts(action_id)
            unresolved_submission_side_effect = (
                self.store.action_has_unresolved_submission_side_effect(action_id)
            )
            if active:
                raise LiveConfigurationError("RECOVERY_ACTION_HAS_ACTIVE_RESERVATION")
            if action_id in manual_ids:
                if (
                    attempts < 1
                    or unresolved_submission_side_effect
                    or latest is None
                    or latest["terminal_status"]
                    not in {"FILLED", "PARTIAL", "EXTERNAL_UNFILLABLE"}
                ):
                    raise LiveConfigurationError(
                        "RECOVERY_MANUAL_TERMINAL_EVIDENCE_MISMATCH"
                    )
                expected_manual = manual_evidence.get(action_id)
                if not isinstance(expected_manual, dict):
                    raise LiveConfigurationError(
                        "RECOVERY_MANUAL_COMPLETION_EVIDENCE_INVALID"
                    )
                with self.store.connect() as connection:
                    gap_transition = connection.execute(
                        """
                        SELECT id FROM action_transitions
                        WHERE action_id = ?
                          AND status = 'ERROR_INTERNAL'
                          AND reason = ?
                        ORDER BY id LIMIT 1
                        """,
                        (action_id, INTERNAL_GAP_REASON),
                    ).fetchone()
                    attempt = connection.execute(
                        """
                        SELECT attempt_id, order_id
                        FROM submission_attempts
                        WHERE action_id = ?
                        ORDER BY attempt_number DESC, created_at_ms DESC
                        LIMIT 1
                        """,
                        (action_id,),
                    ).fetchone()
                if gap_transition is None or attempt is None:
                    raise LiveConfigurationError(
                        "RECOVERY_MANUAL_TERMINAL_EVIDENCE_MISMATCH"
                    )
                latest_details = dict(latest.get("details") or {})
                receipt_evidence = latest_details.get("receipt_evidence") or []
                if not isinstance(receipt_evidence, list):
                    raise LiveConfigurationError(
                        "RECOVERY_MANUAL_TERMINAL_EVIDENCE_MISMATCH"
                    )
                observed_manual = {
                    "terminal_status": str(latest["terminal_status"]),
                    "terminal_reason": str(latest["reason"]),
                    "attempt_id": str(attempt["attempt_id"]),
                    "order_id": (
                        None if attempt["order_id"] is None else str(attempt["order_id"])
                    ),
                    "requested_quantity": str(
                        latest_details.get("planned_requested_quantity") or ""
                    ),
                    "matched_quantity": str(
                        latest_details.get("matched_quantity") or ""
                    ),
                    "matched_notional_usd": str(
                        latest_details.get("matched_notional_usd") or ""
                    ),
                    "fee_usd": str(latest_details.get("fee_usd") or ""),
                    "transaction_hashes": sorted(
                        str(item.get("transaction_hash") or "")
                        for item in receipt_evidence
                        if isinstance(item, Mapping)
                        and str(item.get("transaction_hash") or "")
                    ),
                }
                if canonical_hash(expected_manual) != canonical_hash(observed_manual):
                    raise LiveConfigurationError(
                        "RECOVERY_MANUAL_COMPLETION_EVIDENCE_MISMATCH"
                    )
                initial_state = "IMPORTED_MANUAL_TERMINAL"
                action_kind = "GAP_MANUAL_COMPLETED"
            elif action_id in gap_action_ids:
                if (
                    source.block_number <= int(gap["previous_processed_block"])
                    or source.block_number > int(gap["resume_head"])
                    or attempts != 0
                    or latest is None
                    or latest["terminal_status"] != "ERROR_INTERNAL"
                    or latest["reason"] != INTERNAL_GAP_REASON
                ):
                    raise LiveConfigurationError("RECOVERY_GAP_ACTION_STATE_MISMATCH")
                initial_state = "AUTHORIZED"
                action_kind = "GAP_ACTION"
            else:
                if attempts != 0 or latest is None:
                    raise LiveConfigurationError(
                        "RECOVERY_DEPENDENT_ACTION_STATE_MISMATCH"
                    )
                prior_action = str(
                    latest.get("details", {})
                    .get("prior_action", {})
                    .get("action_id", "")
                )
                if (
                    latest["terminal_status"] != "PENDING_CAUSAL_ORDER"
                    or latest["reason"] != "PRIOR_SAME_TOKEN_ACTION_NOT_TERMINAL"
                    or prior_action not in causal_predecessors
                ):
                    raise LiveConfigurationError(
                        "RECOVERY_DEPENDENT_ACTION_CAUSAL_MISMATCH"
                    )
                initial_state = "AUTHORIZED"
                action_kind = "CAUSAL_DEPENDENT_ACTION"
                causal_predecessors.add(action_id)
            if source.source_quantity <= ZERO or source.source_notional <= ZERO:
                raise LiveConfigurationError("RECOVERY_SOURCE_PRICE_INPUT_INVALID")
            action_rows.append((source, action_kind, initial_state))

        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO repair_recovery_manifests(
                    manifest_hash, profile_key, gap_receipt_id, policy_hash,
                    state, activated_at_ms, last_processed_head, manifest_json
                ) VALUES(?, ?, ?, ?, 'ACTIVE', ?, NULL, ?)
                """,
                (
                    manifest_hash,
                    profile_key,
                    gap_id,
                    policy_hash,
                    int(activated_at_ms),
                    _json(payload),
                ),
            )
            for source, action_kind, initial_state in action_rows:
                connection.execute(
                    """
                    INSERT INTO repair_recovery_actions(
                        manifest_hash, action_id, action_kind, state,
                        source_unit_price, last_evaluated_head,
                        last_snapshot_json, created_at_ms, updated_at_ms
                    ) VALUES(?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                    """,
                    (
                        manifest_hash,
                        source.action_id,
                        action_kind,
                        initial_state,
                        str(source.source_notional / source.source_quantity),
                        int(activated_at_ms),
                        int(activated_at_ms),
                    ),
                )
            connection.execute(
                """
                INSERT INTO repair_recovery_transitions(
                    manifest_hash, action_id, state, reason,
                    created_at_ms, details_json
                ) VALUES(?, NULL, 'ACTIVE', 'OPERATOR_MANIFEST_ACTIVATED', ?, ?)
                """,
                (
                    manifest_hash,
                    int(activated_at_ms),
                    _json(
                        {
                            "gap_action_count": len(gap_action_ids),
                            "manual_completed_count": len(manual_ids),
                            "causal_dependent_count": len(dependent_ids),
                            "policy_hash": policy_hash,
                        }
                    ),
                ),
            )
        return self.active_manifest()  # type: ignore[return-value]

    def action_receipt(self, action_id: str) -> dict[str, Any]:
        active = self.active_manifest()
        if active is None:
            row = self._manifest_row(active_only=False)
            if row is None:
                raise LiveConfigurationError("RECOVERY_MANIFEST_MISSING")
            manifest_hash = str(row["manifest_hash"])
        else:
            manifest_hash = active["manifest_hash"]
        with self.store.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM repair_recovery_actions
                WHERE manifest_hash = ? AND action_id = ?
                """,
                (manifest_hash, str(action_id)),
            ).fetchone()
        if row is None:
            raise LiveConfigurationError("RECOVERY_ACTION_NOT_CLAIMED")
        return {
            "manifest_hash": str(row["manifest_hash"]),
            "action_id": str(row["action_id"]),
            "action_kind": str(row["action_kind"]),
            "state": str(row["state"]),
            "source_unit_price": str(row["source_unit_price"]),
            "last_evaluated_head": (
                None
                if row["last_evaluated_head"] is None
                else int(row["last_evaluated_head"])
            ),
            "last_snapshot": (
                None
                if row["last_snapshot_json"] is None
                else json.loads(str(row["last_snapshot_json"]))
            ),
        }

    def status_payload(self) -> dict[str, Any]:
        row = self._manifest_row(active_only=False)
        if row is None:
            return {
                "state": "NO_MANIFEST",
                "claimed_action_count": 0,
                "terminal_action_count": 0,
                "blocked_action_count": 0,
                "pending_action_count": 0,
                "actions": [],
            }
        manifest = self._manifest_receipt(row)
        with self.store.connect() as connection:
            actions = connection.execute(
                """
                SELECT action_id, action_kind, state, source_unit_price,
                       last_evaluated_head, last_snapshot_json, updated_at_ms
                FROM repair_recovery_actions
                WHERE manifest_hash = ?
                ORDER BY created_at_ms, action_id
                """,
                (manifest["manifest_hash"],),
            ).fetchall()
        serialized = [
            {
                "action_id": str(action["action_id"]),
                "action_kind": str(action["action_kind"]),
                "state": str(action["state"]),
                "source_unit_price": str(action["source_unit_price"]),
                "last_evaluated_head": (
                    None
                    if action["last_evaluated_head"] is None
                    else int(action["last_evaluated_head"])
                ),
                "last_snapshot": (
                    None
                    if action["last_snapshot_json"] is None
                    else json.loads(str(action["last_snapshot_json"]))
                ),
                "updated_at_ms": int(action["updated_at_ms"]),
            }
            for action in actions
        ]
        terminal = sum(
            1 for action in serialized if action["state"] in TERMINAL_RECOVERY_STATES
        )
        blocked = sum(
            1 for action in serialized if action["state"] in BLOCKED_RECOVERY_STATES
        )
        return {
            "state": manifest["state"],
            "manifest_hash": manifest["manifest_hash"],
            "policy_hash": manifest["policy_hash"],
            "gap_receipt_id": manifest["gap_receipt_id"],
            "last_processed_head": manifest["last_processed_head"],
            "claimed_action_count": len(serialized),
            "terminal_action_count": terminal,
            "blocked_action_count": blocked,
            "pending_action_count": len(serialized) - terminal - blocked,
            "actions": serialized,
        }

    @staticmethod
    def _write_action_state(
        connection: Any,
        *,
        manifest_hash: str,
        action_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        processable_head: int,
        snapshot: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE repair_recovery_actions
            SET state = ?, last_evaluated_head = ?, last_snapshot_json = ?,
                updated_at_ms = ?
            WHERE manifest_hash = ? AND action_id = ?
            """,
            (
                str(state),
                int(processable_head),
                None if snapshot is None else _json(dict(snapshot)),
                int(created_at_ms),
                str(manifest_hash),
                str(action_id),
            ),
        )
        connection.execute(
            """
            INSERT INTO repair_recovery_transitions(
                manifest_hash, action_id, state, reason,
                created_at_ms, details_json
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                str(manifest_hash),
                str(action_id),
                str(state),
                str(reason),
                int(created_at_ms),
                _json(dict(details or {})),
            ),
        )

    def _set_action_state(
        self,
        *,
        manifest_hash: str,
        action_id: str,
        state: str,
        reason: str,
        created_at_ms: int,
        processable_head: int,
        snapshot: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._write_action_state(
                connection,
                manifest_hash=manifest_hash,
                action_id=action_id,
                state=state,
                reason=reason,
                created_at_ms=created_at_ms,
                processable_head=processable_head,
                snapshot=snapshot,
                details=details,
            )

    @staticmethod
    def _discard_adapter_snapshot(execution: Any, token_id: str) -> None:
        options = getattr(execution, "_options_by_token", None)
        if isinstance(options, dict):
            options.pop(str(token_id), None)

    def _required_quantity(self, source: SourceAction) -> Decimal:
        existing = self.store.action_target(source.action_id)
        if existing is not None:
            return Decimal(str(existing["remaining_quantity"]))
        scale = self.store.fixed_share_scale_for_source_block(source.block_number)
        proportional = source.source_quantity * scale
        if self.store.config("minimum_size_policy") != "UPSCALE_TO_CURRENT_MARKET_MINIMUM":
            return proportional
        prior = Decimal(
            str(
                self.store.frozen_causal_target_prefix_before(source)[
                    "scaled_open_target"
                ]
            )
        )
        available = self.store.available_position_quantity(source.token_id)
        surplus = max(available - prior, ZERO)
        if source.side == "BUY":
            return max(proportional - surplus, ZERO)
        return min(available, proportional + surplus)

    def _finalize_below_minimum_without_order(
        self,
        *,
        source: SourceAction,
        required_quantity: Decimal,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Close an exchange-minimum constraint without creating exposure."""

        reason = "RECOVERY_QUANTITY_BELOW_MARKET_MINIMUM"
        target = self.store.action_target(source.action_id)
        if target is None:
            proportional = (
                source.source_quantity
                * self.store.fixed_share_scale_for_source_block(source.block_number)
            )
            target = self.store.ensure_action_target(
                source=source,
                proportional_quantity=proportional,
                target_quantity=proportional,
                state="EXTERNAL_UNFILLABLE",
                reason=reason,
                updated_at_ms=now_ms(),
            )
        filled = Decimal(str(target["cumulative_filled_quantity"]))
        terminal_status = "PARTIAL" if filled > ZERO else "EXTERNAL_UNFILLABLE"
        self.store.set_action_target_state(
            source=source,
            state=terminal_status,
            reason=reason,
            updated_at_ms=now_ms(),
        )
        self.store.append_transition(
            source=source,
            status=terminal_status,
            reason=reason,
            details={
                "repair_window_recovery": dict(evidence),
                "required_quantity": str(required_quantity),
                "new_order_submitted": False,
                "historical_action_replayed": False,
            },
        )
        return {"terminal_status": terminal_status, "reason": reason}

    def _finalize_officially_closed_without_order(
        self,
        *,
        source: SourceAction,
        lifecycle: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Close a repair action when official metadata forbids new orders."""

        reason = "OFFICIAL_MARKET_CLOSED_BEFORE_RETRY"
        target = self.store.action_target(source.action_id)
        if target is None:
            proportional = (
                source.source_quantity
                * self.store.fixed_share_scale_for_source_block(source.block_number)
            )
            self.store.ensure_action_target(
                source=source,
                proportional_quantity=proportional,
                target_quantity=proportional,
                state="EXTERNAL_UNFILLABLE",
                reason=reason,
                updated_at_ms=now_ms(),
            )
        terminal_at_ms = now_ms()
        self.store.set_action_target_state(
            source=source,
            state="EXTERNAL_UNFILLABLE",
            reason=reason,
            updated_at_ms=terminal_at_ms,
        )
        self.store.append_transition(
            source=source,
            status="EXTERNAL_UNFILLABLE",
            reason=reason,
            created_at_ms=terminal_at_ms,
            details={
                "official_market_lifecycle": dict(lifecycle),
                "new_order_submitted": False,
                "historical_action_replayed": False,
            },
        )
        return {"terminal_status": "EXTERNAL_UNFILLABLE", "reason": reason}

    def _guarded_snapshot(
        self,
        *,
        source: SourceAction,
        execution: Any,
        required_quantity: Decimal,
    ) -> tuple[dict[str, Any], dict[str, Any], Decimal]:
        snapshot = dict(execution.snapshot(token_id=source.token_id, side=source.side))
        try:
            rate = Decimal(str(snapshot["fee_bps"])) / Decimal("10000")
            exponent_decimal = Decimal(str(snapshot.get("fee_exponent", "1")))
            exponent = int(exponent_decimal)
            minimum = Decimal(str(snapshot["minimum_order_size"]))
        except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
            self._discard_adapter_snapshot(execution, source.token_id)
            raise LiveConfigurationError("INVALID_RECOVERY_BOOK_CONSTRAINTS") from exc
        if exponent_decimal != Decimal(exponent) or minimum <= ZERO:
            self._discard_adapter_snapshot(execution, source.token_id)
            raise LiveConfigurationError("INVALID_RECOVERY_BOOK_CONSTRAINTS")
        raw_book = snapshot.get("raw_book")
        if not isinstance(raw_book, Mapping):
            self._discard_adapter_snapshot(execution, source.token_id)
            raise LiveConfigurationError("RECOVERY_RAW_BOOK_MISSING")
        key = "asks" if source.side == "BUY" else "bids"
        raw_levels = raw_book.get(key)
        if not isinstance(raw_levels, list):
            self._discard_adapter_snapshot(execution, source.token_id)
            raise LiveConfigurationError("RECOVERY_BOOK_LEVELS_MISSING")
        levels: list[tuple[Decimal, Decimal, Decimal]] = []
        source_price = source.source_notional / source.source_quantity
        for raw in raw_levels:
            if not isinstance(raw, Mapping):
                continue
            try:
                price = Decimal(str(raw["price"]))
                quantity = Decimal(str(raw["size"]))
            except (KeyError, InvalidOperation, TypeError, ValueError):
                continue
            if price <= ZERO or quantity <= ZERO:
                continue
            adjusted = fee_adjusted_unit_value(
                side=source.side,
                price=price,
                fee_rate=rate,
                fee_exponent=exponent,
            )
            eligible = adjusted <= source_price if source.side == "BUY" else adjusted >= source_price
            if eligible:
                levels.append((price, quantity, adjusted))
        requested = required_quantity
        # A delayed repair may never turn the exchange minimum into extra
        # exposure.  If the exact current effect is below the immutable market
        # minimum, preserve it as externally unfillable and submit nothing.
        if requested < minimum:
            requested = ZERO
        depth = sum((quantity for _price, quantity, _adjusted in levels), ZERO)
        limit = None
        adjusted_at_limit = None
        if levels:
            limit = (
                max(price for price, _quantity, _adjusted in levels)
                if source.side == "BUY"
                else min(price for price, _quantity, _adjusted in levels)
            )
            adjusted_at_limit = fee_adjusted_unit_value(
                side=source.side,
                price=limit,
                fee_rate=rate,
                fee_exponent=exponent,
            )
        evidence = {
            "token_id": source.token_id,
            "side": source.side,
            "source_unit_price": str(source_price),
            "required_quantity": str(required_quantity),
            "requested_quantity_after_minimum": str(requested),
            "minimum_order_size": str(minimum),
            "eligible_depth": str(depth),
            "lossless_limit_price": None if limit is None else str(limit),
            "fee_adjusted_unit_value": (
                None if adjusted_at_limit is None else str(adjusted_at_limit)
            ),
            "fee_rate": str(rate),
            "fee_exponent": str(exponent),
            "snapshot_hash": canonical_hash(snapshot),
            "contract": "ZERO_ADDITIONAL_LOSS_AFTER_OFFICIAL_FEE",
        }
        if limit is not None:
            snapshot.update(
                {
                    "best_price": str(limit),
                    "visible_best_level_size": str(depth),
                    "repair_window_recovery": dict(evidence),
                }
            )
        return snapshot, evidence, requested

    def _sync_from_execution_state(
        self, *, manifest_hash: str, source: SourceAction, processable_head: int
    ) -> str | None:
        latest = self.store.latest_transition(source)
        current = self.action_receipt(source.action_id)
        mapping = {
            "FILLED": "FILLED",
            "PARTIAL": "PARTIAL_TERMINAL_PREFIX_PRESERVED",
            "SKIPPED": "NO_ORDER_COVERED_BY_CUMULATIVE_SURPLUS",
            "SUPERSEDED_UNFILLED": "SUPERSEDED_BY_LATER_SOURCE_ACTION",
            "EXTERNAL_UNFILLABLE": "EXTERNAL_UNFILLABLE",
            "SUBMITTED_UNRECONCILED": "SUBMITTED_UNRECONCILED",
            "UNKNOWN_SUBMISSION": "UNKNOWN_SUBMISSION",
            "PARTIAL_PENDING": "PARTIAL_PENDING",
            "PENDING_LIQUIDITY": "PENDING_EXTERNAL_LIQUIDITY",
            "PENDING_EXTERNAL_RETRY": "PENDING_EXTERNAL_LIQUIDITY",
        }
        recovery_state = (
            None
            if latest is None
            else mapping.get(str(latest["terminal_status"]))
        )
        if (
            self.store.action_has_unresolved_submission_side_effect(
                source.action_id
            )
            and (
                current["state"] in TERMINAL_RECOVERY_STATES
                or recovery_state in TERMINAL_RECOVERY_STATES
            )
        ):
            recovery_state = SUBMISSION_SIDE_EFFECT_BLOCKED_STATE
            if current["state"] != recovery_state:
                self._set_action_state(
                    manifest_hash=manifest_hash,
                    action_id=source.action_id,
                    state=recovery_state,
                    reason="UNRESOLVED_SUBMISSION_SIDE_EFFECT",
                    created_at_ms=now_ms(),
                    processable_head=processable_head,
                    snapshot=current["last_snapshot"],
                    details={
                        "execution_terminal_status": (
                            None if latest is None else latest["terminal_status"]
                        ),
                        "submission_side_effect_present": True,
                        "new_order_submitted_by_recovery": False,
                        "current_book_read": False,
                    },
                )
            return recovery_state
        if latest is None:
            return None
        if recovery_state is None:
            return None
        if current["state"] != recovery_state:
            details: dict[str, Any] = {
                "execution_terminal_status": latest["terminal_status"]
            }
            if recovery_state == "PARTIAL_TERMINAL_PREFIX_PRESERVED":
                target = self.store.action_target(source.action_id)
                if target is None:
                    raise LiveConfigurationError(
                        "PARTIAL_RECOVERY_TARGET_MISSING"
                    )
                copied_prefix = Decimal(
                    str(target["cumulative_filled_quantity"])
                )
                unfilled_remainder = Decimal(
                    str(target["remaining_quantity"])
                )
                if copied_prefix <= ZERO or unfilled_remainder <= ZERO:
                    raise LiveConfigurationError(
                        "INVALID_PARTIAL_RECOVERY_TARGET"
                    )
                details.update(
                    {
                        "action_target_quantity": str(
                            target["target_quantity"]
                        ),
                        "copied_prefix_quantity": str(copied_prefix),
                        "unfilled_remainder_quantity": str(
                            unfilled_remainder
                        ),
                        "new_order_submitted": False,
                    }
                )
            self._set_action_state(
                manifest_hash=manifest_hash,
                action_id=source.action_id,
                state=recovery_state,
                reason=str(latest["reason"] or "EXECUTION_STATE_SYNC"),
                created_at_ms=now_ms(),
                processable_head=processable_head,
                snapshot=current["last_snapshot"],
                details=details,
            )
        return recovery_state

    def run_cycle(
        self,
        *,
        processable_head: int,
        execution: Any,
        wallet_lock_path: Path | None,
        coordinator: Any | None,
        profile_key: str,
        exclude_action_ids: set[str] | None = None,
        market_lifecycle_resolver: Callable[[SourceAction], Any] | None = None,
    ) -> dict[str, Any]:
        active = self.active_manifest()
        if active is None:
            return {"state": "NO_ACTIVE_MANIFEST"}
        if str(profile_key) != active["profile_key"]:
            raise LiveConfigurationError("RECOVERY_RUNTIME_PROFILE_MISMATCH")
        self.claim_new_causal_dependents(claimed_at_ms=now_ms())
        head = int(processable_head)
        if active["last_processed_head"] is not None and head <= int(
            active["last_processed_head"]
        ):
            return {"state": "ALREADY_EVALUATED_HEAD", "processable_head": head}
        manifest_hash = active["manifest_hash"]
        excluded = {str(action_id) for action_id in (exclude_action_ids or set())}
        blocked_token_causes: dict[str, dict[str, Any]] = {}
        for action_id in sorted(excluded):
            excluded_source = self._source(action_id)
            blocked_token_causes.setdefault(
                excluded_source.token_id,
                {
                    "action_id": excluded_source.action_id,
                    "excluded_current_intake": True,
                },
            )
        with self.store.connect() as connection:
            rows = connection.execute(
                """
                SELECT r.*, a.block_number, a.source_log_index,
                       a.source_timestamp, a.transaction_hash, a.token_id,
                       a.side, a.order_hash
                FROM repair_recovery_actions AS r
                JOIN action_receipts AS a ON a.action_id = r.action_id
                WHERE r.manifest_hash = ?
                ORDER BY a.block_number, a.source_log_index,
                         a.source_timestamp, a.transaction_hash,
                         a.token_id, a.side, a.order_hash, a.action_id
                """,
                (manifest_hash,),
            ).fetchall()
        decisions: list[dict[str, Any]] = []
        submitted = False
        for row in rows:
            action_id = str(row["action_id"])
            if action_id in excluded:
                continue
            state = str(row["state"])
            source = self._source(action_id)
            if state == "BLOCKED_INTERNAL_STALE_CAUSAL_TARGET":
                continue
            if state in TERMINAL_RECOVERY_STATES:
                if not self.store.action_has_unresolved_submission_side_effect(
                    action_id
                ):
                    continue
                synced = self._sync_from_execution_state(
                    manifest_hash=manifest_hash,
                    source=source,
                    processable_head=head,
                )
                if synced != SUBMISSION_SIDE_EFFECT_BLOCKED_STATE:
                    raise LiveConfigurationError(
                        "SUBMISSION_SIDE_EFFECT_RECOVERY_GATE_MISMATCH"
                    )
                blocked_token_causes.setdefault(
                    source.token_id,
                    {
                        "action_id": source.action_id,
                        "unresolved_submission_side_effect": True,
                    },
                )
                decisions.append(
                    {
                        "action_id": action_id,
                        "state": "CODE_REPAIR_REQUIRED",
                        "recovery_state": SUBMISSION_SIDE_EFFECT_BLOCKED_STATE,
                        "reason": "UNRESOLVED_SUBMISSION_SIDE_EFFECT",
                    }
                )
                continue
            prior_nonterminal = self.store.prior_nonterminal_same_token_action(
                source
            )
            blocked_cause = prior_nonterminal or blocked_token_causes.get(
                source.token_id
            )
            if blocked_cause is not None:
                blocked_token_causes.setdefault(
                    source.token_id, dict(blocked_cause)
                )
                reason = "PRIOR_SAME_TOKEN_ACTION_NOT_TERMINAL"
                self._set_action_state(
                    manifest_hash=manifest_hash,
                    action_id=action_id,
                    state=state,
                    reason=reason,
                    created_at_ms=now_ms(),
                    processable_head=head,
                    snapshot=row["last_snapshot_json"]
                    and json.loads(str(row["last_snapshot_json"])),
                    details={"prior_action": dict(blocked_cause)},
                )
                decisions.append(
                    {
                        "action_id": action_id,
                        "state": state,
                        "reason": reason,
                        "prior_action": dict(blocked_cause),
                    }
                )
                continue
            synced = self._sync_from_execution_state(
                manifest_hash=manifest_hash,
                source=source,
                processable_head=head,
            )
            if synced == SUBMISSION_SIDE_EFFECT_BLOCKED_STATE:
                blocked_token_causes.setdefault(
                    source.token_id,
                    {
                        "action_id": source.action_id,
                        "unresolved_submission_side_effect": True,
                    },
                )
                decisions.append(
                    {
                        "action_id": action_id,
                        "state": "CODE_REPAIR_REQUIRED",
                        "recovery_state": SUBMISSION_SIDE_EFFECT_BLOCKED_STATE,
                        "reason": "UNRESOLVED_SUBMISSION_SIDE_EFFECT",
                    }
                )
                continue
            if synced in TERMINAL_RECOVERY_STATES:
                if synced == "PARTIAL_TERMINAL_PREFIX_PRESERVED":
                    decisions.append(
                        {
                            "action_id": action_id,
                            "state": synced,
                            "reason": "PARTIAL_PREFIX_ALREADY_COPIED",
                        }
                    )
                continue
            if synced in {"SUBMITTED_UNRECONCILED", "UNKNOWN_SUBMISSION"}:
                blocked_token_causes.setdefault(
                    source.token_id,
                    {"action_id": source.action_id, "submission_in_flight": True},
                )
                decisions.append({"action_id": action_id, "state": synced})
                continue
            if source.token_id in blocked_token_causes or submitted:
                continue
            existing_target = self.store.action_target(source.action_id)
            if existing_target is not None:
                latest = self.store.latest_transition(source)
                already_stale = (
                    latest is not None
                    and latest["terminal_status"] == "ERROR_INTERNAL"
                    and latest["reason"] == "INTERNAL_STALE_CAUSAL_TARGET"
                )
                stale_result = (
                    {
                        "terminal_status": "ERROR_INTERNAL",
                        "reason": "INTERNAL_STALE_CAUSAL_TARGET",
                    }
                    if already_stale
                    else _fail_closed_stale_causal_target(
                        store=self.store,
                        source=source,
                        existing_target=existing_target,
                    )
                )
                if stale_result is not None:
                    core_transition = self.store.latest_transition(source)
                    core_transition_id = self.store.latest_transition_id(source)
                    if (
                        core_transition is None
                        or core_transition_id is None
                        or core_transition["terminal_status"] != "ERROR_INTERNAL"
                        or core_transition["reason"]
                        != "INTERNAL_STALE_CAUSAL_TARGET"
                    ):
                        raise LiveConfigurationError(
                            "STALE_CAUSAL_TARGET_CORE_EVIDENCE_MISSING"
                        )
                    recovery_state = "BLOCKED_INTERNAL_STALE_CAUSAL_TARGET"
                    core_details = dict(core_transition.get("details") or {})
                    recovery_details = {
                        "core_error_transition_id": int(core_transition_id),
                        "core_error_details_hash": canonical_hash(core_details),
                        "core_error_reason": str(core_transition["reason"]),
                        "core_error_terminal_status": str(
                            core_transition["terminal_status"]
                        ),
                        "new_order_submitted": False,
                        "current_book_read": False,
                    }
                    self._set_action_state(
                        manifest_hash=manifest_hash,
                        action_id=action_id,
                        state=recovery_state,
                        reason="INTERNAL_STALE_CAUSAL_TARGET",
                        created_at_ms=now_ms(),
                        processable_head=head,
                        snapshot=None,
                        details=recovery_details,
                    )
                    blocked_token_causes.setdefault(
                        source.token_id,
                        {
                            "action_id": source.action_id,
                            "internal_stale_causal_target": True,
                        },
                    )
                    decisions.append(
                        {
                            "action_id": action_id,
                            "state": "CODE_REPAIR_REQUIRED",
                            "recovery_state": recovery_state,
                            "reason": "INTERNAL_STALE_CAUSAL_TARGET",
                            **recovery_details,
                        }
                    )
                    continue
            required = self._required_quantity(source)
            if required == ZERO or (
                source.side == "SELL"
                and self.store.available_position_quantity(source.token_id) == ZERO
            ):
                self.store.append_transition(
                    source=source,
                    status="OBSERVED",
                    reason="OPERATOR_AUTHORIZED_REPAIR_WINDOW_RECOVERY",
                    details={"manifest_hash": manifest_hash},
                )
                result = execute_source_action(
                    store=self.store,
                    source=source,
                    execution=execution,
                    live_enabled=True,
                    wallet_lock_path=wallet_lock_path,
                    coordinator=coordinator,
                    profile_key=profile_key,
                )
                mapped = (
                    "NO_ORDER_COVERED_BY_CUMULATIVE_SURPLUS"
                    if result["terminal_status"] == "SKIPPED"
                    else "EXTERNAL_UNFILLABLE"
                    if result["terminal_status"] == "EXTERNAL_UNFILLABLE"
                    else str(result["terminal_status"])
                )
                self._set_action_state(
                    manifest_hash=manifest_hash,
                    action_id=action_id,
                    state=mapped,
                    reason=str(result.get("reason") or "CURRENT_EFFECT_ZERO"),
                    created_at_ms=now_ms(),
                    processable_head=head,
                    details={"required_quantity": str(required)},
                )
                decisions.append({"action_id": action_id, "state": mapped})
                continue
            if market_lifecycle_resolver is not None:
                try:
                    lifecycle_decision = market_lifecycle_resolver(source)
                except Exception as exc:
                    if not _is_retryable_external_error(exc):
                        raise
                else:
                    raw_metadata = getattr(lifecycle_decision, "metadata", None)
                    if not isinstance(raw_metadata, Mapping):
                        raise LiveConfigurationError(
                            "INVALID_REPAIR_MARKET_LIFECYCLE_METADATA"
                        )
                    lifecycle = dict(raw_metadata)
                    if (
                        lifecycle.get("closed") is True
                        and lifecycle.get("accepting_orders") is False
                    ):
                        result = self._finalize_officially_closed_without_order(
                            source=source,
                            lifecycle=lifecycle,
                        )
                        self._set_action_state(
                            manifest_hash=manifest_hash,
                            action_id=action_id,
                            state="EXTERNAL_UNFILLABLE",
                            reason=str(result["reason"]),
                            created_at_ms=now_ms(),
                            processable_head=head,
                            details={
                                "official_market_lifecycle": lifecycle,
                                "new_order_submitted": False,
                                "historical_action_replayed": False,
                            },
                        )
                        decisions.append(
                            {
                                "action_id": action_id,
                                "state": "EXTERNAL_UNFILLABLE",
                                "reason": str(result["reason"]),
                                "official_market_lifecycle": lifecycle,
                                "new_order_submitted": False,
                            }
                        )
                        blocked_token_causes.setdefault(
                            source.token_id,
                            {
                                "action_id": source.action_id,
                                "terminalized": True,
                            },
                        )
                        continue
            try:
                frozen_snapshot, evidence, requested = self._guarded_snapshot(
                    source=source,
                    execution=execution,
                    required_quantity=required,
                )
            except Exception:
                self._discard_adapter_snapshot(execution, source.token_id)
                raise
            if requested == ZERO:
                self._discard_adapter_snapshot(execution, source.token_id)
                state = "EXTERNAL_UNFILLABLE"
                reason = "RECOVERY_QUANTITY_BELOW_MARKET_MINIMUM"
            elif evidence["lossless_limit_price"] is None:
                self._discard_adapter_snapshot(execution, source.token_id)
                state = "PENDING_PRICE"
                reason = "NO_FEE_ADJUSTED_LOSSLESS_BOOK_LEVEL"
            else:
                state = "CURRENT_EFFECT_RECONSTRUCTED"
                reason = "FEE_ADJUSTED_LOSSLESS_BOOK_FROZEN"
            self._set_action_state(
                manifest_hash=manifest_hash,
                action_id=action_id,
                state=state,
                reason=reason,
                created_at_ms=now_ms(),
                processable_head=head,
                snapshot=evidence,
                details=evidence,
            )
            if state == "EXTERNAL_UNFILLABLE" and requested == ZERO:
                result = self._finalize_below_minimum_without_order(
                    source=source,
                    required_quantity=required,
                    evidence=evidence,
                )
                decisions.append(
                    {
                        "action_id": action_id,
                        "state": "EXTERNAL_UNFILLABLE",
                        "reason": str(result["reason"]),
                        **evidence,
                    }
                )
                blocked_token_causes.setdefault(
                    source.token_id,
                    {"action_id": source.action_id, "terminalized": True},
                )
                continue
            if state != "CURRENT_EFFECT_RECONSTRUCTED":
                blocked_token_causes.setdefault(
                    source.token_id,
                    {"action_id": source.action_id, "recovery_pending": True},
                )
                decisions.append(
                    {
                        "action_id": action_id,
                        "state": state,
                        "reason": reason,
                        **evidence,
                    }
                )
                continue
            self.store.append_transition(
                source=source,
                status="OBSERVED",
                reason="OPERATOR_AUTHORIZED_REPAIR_WINDOW_LOSSLESS_RECOVERY",
                details={
                    "manifest_hash": manifest_hash,
                    "policy_hash": active["policy_hash"],
                    "lossless_snapshot": evidence,
                    "historical_action_replayed": True,
                },
            )
            result = execute_source_action(
                store=self.store,
                source=source,
                execution=_FrozenLosslessExecution(execution, frozen_snapshot),
                live_enabled=True,
                wallet_lock_path=wallet_lock_path,
                coordinator=coordinator,
                profile_key=profile_key,
            )
            mapped = str(result["terminal_status"])
            if mapped == "PENDING_LIQUIDITY":
                mapped = "PENDING_EXTERNAL_LIQUIDITY"
            self._set_action_state(
                manifest_hash=manifest_hash,
                action_id=action_id,
                state=mapped,
                reason=str(result.get("reason") or "EXECUTION_RESULT"),
                created_at_ms=now_ms(),
                processable_head=head,
                snapshot=evidence,
                details={"execution_result": result, "snapshot": evidence},
            )
            decisions.append({"action_id": action_id, "state": mapped})
            submitted = mapped in {
                "SUBMIT_STARTED",
                "SUBMITTED_UNRECONCILED",
                "UNKNOWN_SUBMISSION",
            }
            blocked_token_causes.setdefault(
                source.token_id,
                {"action_id": source.action_id, "recovery_processed": True},
            )
        with self.store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            completion_rows = connection.execute(
                """
                SELECT action_id, state, last_snapshot_json
                FROM repair_recovery_actions
                WHERE manifest_hash = ?
                ORDER BY action_id
                """,
                (manifest_hash,),
            ).fetchall()
            decision_action_ids = {
                str(decision.get("action_id") or "") for decision in decisions
            }
            for completion_row in completion_rows:
                completion_action_id = str(completion_row["action_id"])
                if str(completion_row["state"]) not in TERMINAL_RECOVERY_STATES:
                    continue
                if not self.store.action_has_unresolved_submission_side_effect(
                    completion_action_id,
                    connection=connection,
                ):
                    continue
                if str(completion_row["state"]) != SUBMISSION_SIDE_EFFECT_BLOCKED_STATE:
                    latest_row = connection.execute(
                        """
                        SELECT status FROM action_transitions
                        WHERE action_id = ? ORDER BY id DESC LIMIT 1
                        """,
                        (completion_action_id,),
                    ).fetchone()
                    self._write_action_state(
                        connection,
                        manifest_hash=manifest_hash,
                        action_id=completion_action_id,
                        state=SUBMISSION_SIDE_EFFECT_BLOCKED_STATE,
                        reason="UNRESOLVED_SUBMISSION_SIDE_EFFECT",
                        created_at_ms=now_ms(),
                        processable_head=head,
                        snapshot=(
                            None
                            if completion_row["last_snapshot_json"] is None
                            else json.loads(
                                str(completion_row["last_snapshot_json"])
                            )
                        ),
                        details={
                            "execution_terminal_status": (
                                None
                                if latest_row is None
                                else str(latest_row["status"])
                            ),
                            "submission_side_effect_present": True,
                            "new_order_submitted_by_recovery": False,
                            "current_book_read": False,
                        },
                    )
                if completion_action_id not in decision_action_ids:
                    decisions.append(
                        {
                            "action_id": completion_action_id,
                            "state": "CODE_REPAIR_REQUIRED",
                            "recovery_state": SUBMISSION_SIDE_EFFECT_BLOCKED_STATE,
                            "reason": "UNRESOLVED_SUBMISSION_SIDE_EFFECT",
                        }
                    )
                    decision_action_ids.add(completion_action_id)
            connection.execute(
                """
                UPDATE repair_recovery_manifests
                SET last_processed_head = ? WHERE manifest_hash = ?
                """,
                (head, manifest_hash),
            )
            terminal_states = tuple(sorted(TERMINAL_RECOVERY_STATES))
            remaining = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM repair_recovery_actions
                    WHERE manifest_hash = ?
                      AND state NOT IN ({','.join('?' for _ in terminal_states)})
                    """,
                    (manifest_hash, *terminal_states),
                ).fetchone()[0]
            )
            blocked_states = tuple(sorted(BLOCKED_RECOVERY_STATES))
            blocked_action_ids = [
                str(blocked["action_id"])
                for blocked in connection.execute(
                    f"""
                    SELECT action_id FROM repair_recovery_actions
                    WHERE manifest_hash = ?
                      AND state IN ({','.join('?' for _ in blocked_states)})
                    ORDER BY action_id
                    """,
                    (manifest_hash, *blocked_states),
                ).fetchall()
            ]
            if remaining == 0:
                connection.execute(
                    """
                    UPDATE repair_recovery_manifests
                    SET state = 'COMPLETED' WHERE manifest_hash = ?
                    """,
                    (manifest_hash,),
                )
        if blocked_action_ids:
            blocked_summary = {
                "blocked_action_count": len(blocked_action_ids),
                "blocked_action_ids": blocked_action_ids,
            }
            if (
                len(decisions) == 1
                and decisions[0].get("state") == "CODE_REPAIR_REQUIRED"
            ):
                return {
                    **decisions[0],
                    **blocked_summary,
                    "processable_head": head,
                }
            return {
                "state": "CODE_REPAIR_REQUIRED",
                "processable_head": head,
                **blocked_summary,
                "decision_count": len(decisions),
                "decisions": decisions,
            }
        if len(decisions) == 1:
            return {**decisions[0], "processable_head": head}
        if any(
            decision.get("state") == "CODE_REPAIR_REQUIRED"
            for decision in decisions
        ):
            return {
                "state": "CODE_REPAIR_REQUIRED",
                "processable_head": head,
                "decision_count": len(decisions),
                "decisions": decisions,
            }
        return {
            "state": "CYCLE",
            "processable_head": head,
            "decision_count": len(decisions),
            "decisions": decisions,
        }


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="repair-window-recovery")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--activate", action="store_true", required=True)
    args = parser.parse_args(argv)
    payload = json.loads(args.manifest.read_text())
    receipt = RepairWindowRecoveryManager(LiveStore(args.database)).activate_manifest(
        manifest=payload,
        activated_at_ms=now_ms(),
    )
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
