#!/usr/bin/env python3
"""Hourly, server-native health audit for every cash-live wallet sleeve.

This intentionally does not make trading decisions or submit orders.  The
cash-live services remain under their own systemd ``Restart=on-failure``
policies; this audit records an unhealthy running process instead of inventing
a time-based restart rule or touching an order.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sqlite3
import stat
import subprocess
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_RUNTIME_DIR = MODULE_DIR / "server_health_runtime"
DEFAULT_CD90_RUNTIME_DIR = Path("/srv/polymarket-live/runtime/cd90_live")
DEFAULT_TENNIS_RUNTIME_DIR = Path("/srv/polymarket-live/runtime/tennis_live")
DEFAULT_COORDINATOR_PATH = Path(
    "/srv/polymarket-live/runtime/shared_wallet/coordinator.sqlite3"
)
CURRENT_VERSION_INDEX = Path("/opt/polymarket-live/CURRENT_REPAIR_VERSION.json")

HEALTH_TIMER_UNIT = "com.luke.polymarket.live-health.timer"
SUCCESS_CYCLE_OUTCOMES = frozenset(
    {
        "SUCCESS",
        "SUCCESS_REDEMPTION_MAINTENANCE_PENDING",
        "EXTERNAL_HEAD_RETRY_PENDING",
        "EXTERNAL_WS_RECONNECTING",
    }
)


def _as_text(value: Any) -> str:
    return str(value).strip().lower()


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _as_nonnegative_decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    try:
        return result if result >= 0 else None
    except InvalidOperation:
        return None


def cd90_status_issues(payload: dict[str, Any]) -> list[str]:
    """Return deterministic, local-CD90 status invariant failures.

    There is deliberately no arbitrary heartbeat-age or cursor-gap threshold:
    those measurements are emitted in the report, while malformed or
    impossible state is an internal error immediately.
    """

    issues: list[str] = []
    if payload.get("mode") != "CASH_LIVE":
        issues.append("CD90_MODE_MISMATCH")
    if payload.get("paper_only") is not False:
        issues.append("CD90_PAPER_ONLY_FLAG_INVALID")
    if payload.get("real_order_submission_enabled") is not True:
        issues.append("CD90_ORDER_AUTHORIZATION_FLAG_INVALID")

    # Preserve the cumulative handoff count as immutable history, but degrade
    # current health only while an exact handoff action is still unresolved.
    # Older status producers do not emit the current field, so retain their
    # fail-closed cumulative behavior across a release boundary.
    handoff_count_field = (
        "unresolved_lossless_handoff_action_count"
        if "unresolved_lossless_handoff_action_count" in payload
        else "lossless_handoff_failure_action_count"
    )
    if handoff_count_field in payload:
        lossless_handoff_failures = _as_int(
            payload.get(handoff_count_field)
        )
        if lossless_handoff_failures is None or lossless_handoff_failures < 0:
            issues.append("CD90_LOSSLESS_HANDOFF_FAILURE_COUNT_INVALID")
        elif lossless_handoff_failures:
            issues.append(
                f"CD90_LOSSLESS_HANDOFF_FAILURE:{lossless_handoff_failures}"
            )

    account = payload.get("account")
    if not isinstance(account, dict) or _as_nonnegative_decimal(
        account.get("available_cash_usd")
    ) is None:
        issues.append("CD90_AVAILABLE_CASH_INVALID")

    runtime = payload.get("runtime")
    if not isinstance(runtime, dict):
        issues.append("CD90_RUNTIME_STATE_INVALID")
        return issues

    if _as_text(runtime.get("ws_subscription_active")) != "true":
        issues.append("CD90_WS_SUBSCRIPTION_INACTIVE")

    receipt_count = _as_int(payload.get("action_receipt_count"))
    terminal_counts = payload.get("action_terminal_counts")
    if receipt_count is None or receipt_count < 0 or not isinstance(terminal_counts, dict):
        issues.append("CD90_ACTION_RECEIPT_COUNT_INVALID")
    else:
        terminal_values = [_as_int(value) for value in terminal_counts.values()]
        if any(value is None or value < 0 for value in terminal_values):
            issues.append("CD90_ACTION_TERMINAL_COUNT_INVALID")
        elif sum(value for value in terminal_values if value is not None) != receipt_count:
            issues.append("CD90_ACTION_RECEIPT_NONCONSERVATION")
        else:
            fidelity = payload.get("action_fidelity")
            if isinstance(fidelity, dict):
                # Current profile-eligible internal states are checked from
                # the fidelity summary below.  The cumulative terminal table
                # also contains immutable pre-fidelity/non-followable errors;
                # only unresolved submission side effects remain globally
                # unsafe at this boundary.
                unsafe_states = {"SUBMIT_STARTED", "UNKNOWN_SUBMISSION"}
                unresolved = sum(
                    int(value)
                    for status, value in terminal_counts.items()
                    if str(status).upper() in unsafe_states
                )
            else:
                # Backward-compatible validation for a pre-fidelity status
                # snapshot during a release boundary.
                completed_states = {"FILLED", "PARTIAL", "SKIPPED", "ERROR"}
                unresolved = sum(
                    int(value)
                    for status, value in terminal_counts.items()
                    if str(status).upper() not in completed_states
                )
            if unresolved:
                issues.append(f"CD90_UNRESOLVED_ACTIONS:{unresolved}")

    fidelity = payload.get("action_fidelity")
    if isinstance(fidelity, dict):
        if fidelity.get("conservation_passed") is not True:
            issues.append("CD90_ACTION_FIDELITY_NONCONSERVATION")
        for field, issue_label in (
            ("internal_error", "CD90_ACTION_FIDELITY_INTERNAL_ERRORS"),
            ("missing_target", "CD90_ACTION_FIDELITY_MISSING_TARGETS"),
            ("unclassified_target", "CD90_ACTION_FIDELITY_UNCLASSIFIED_TARGETS"),
        ):
            value = _as_int(fidelity.get(field))
            if value is None or value < 0:
                issues.append(f"{issue_label}_INVALID")
            elif value:
                issues.append(f"{issue_label}:{value}")
        mismatch_field = "retryable_target_terminal_transition_mismatch"
        if mismatch_field in fidelity:
            mismatch_count = _as_int(fidelity.get(mismatch_field))
            if mismatch_count is None or mismatch_count < 0:
                issues.append(
                    "CD90_ACTION_FIDELITY_TARGET_TRANSITION_MISMATCHES_INVALID"
                )
            elif mismatch_count:
                issues.append(
                    "CD90_ACTION_FIDELITY_TARGET_TRANSITION_MISMATCHES:"
                    f"{mismatch_count}"
                )

    current_head = _as_int(runtime.get("current_head"))
    last_processed = _as_int(runtime.get("last_processed_block"))
    if (
        current_head is None
        or last_processed is None
        or current_head < 0
        or last_processed < 0
        or last_processed > current_head
    ):
        issues.append("CD90_CHAIN_CURSOR_INVALID")
    last_cycle_outcome = _as_text(runtime.get("last_cycle_outcome"))
    external_retry_pending = (
        (
            last_cycle_outcome == "external_head_retry_pending"
            and _as_text(runtime.get("external_head_incident_active")) == "true"
        )
        or last_cycle_outcome == "external_ws_reconnecting"
    )
    if last_cycle_outcome != "success" and not external_retry_pending:
        issues.append("CD90_LAST_CYCLE_NOT_SUCCESS")

    redemption = payload.get("automatic_redemption")
    if not isinstance(redemption, dict) or redemption.get("enabled") is not True:
        issues.append("CD90_AUTOMATIC_REDEMPTION_DISABLED")
    elif _as_text(redemption.get("worker_state")) not in {
        "ready",
        "idle",
        "running",
        "waiting_for_resolution",
        "pending_resolution",
        "no_redeemable_position",
    }:
        issues.append("CD90_AUTOMATIC_REDEMPTION_WORKER_INVALID")
    else:
        redemption_counts = redemption.get("terminal_counts", {})
        if not isinstance(redemption_counts, dict):
            issues.append("CD90_REDEMPTION_TERMINAL_COUNT_INVALID")
        else:
            unresolved_redemptions = 0
            for state, raw_count in redemption_counts.items():
                count = _as_int(raw_count)
                if count is None or count < 0:
                    issues.append("CD90_REDEMPTION_TERMINAL_COUNT_INVALID")
                    unresolved_redemptions = 0
                    break
                normalized_state = str(state).upper()
                if normalized_state in {"UNKNOWN_SUBMISSION", "ERROR"} or (
                    normalized_state.startswith("BLOCK_")
                    and normalized_state
                    not in {
                        "BLOCK_PRE_SUBMISSION_REVALIDATION",
                        "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
                    }
                ):
                    unresolved_redemptions += count
            if unresolved_redemptions:
                issues.append(f"CD90_UNRESOLVED_REDEMPTIONS:{unresolved_redemptions}")
    return issues


def _profile_issue_prefix(profile_key: str) -> str:
    normalized = "".join(
        character if character.isalnum() else "_"
        for character in str(profile_key).strip().upper()
    ).strip("_")
    return normalized or "UNKNOWN_PROFILE"


def live_status_issues(
    payload: dict[str, Any], *, profile_key: str
) -> list[str]:
    """Apply one invariant contract to any live sleeve status payload."""

    prefix = _profile_issue_prefix(profile_key)
    issues = [
        issue.replace("CD90_", f"{prefix}_", 1)
        for issue in cd90_status_issues(payload)
    ]
    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        current_head = _as_int(runtime.get("current_head"))
        last_processed = _as_int(runtime.get("last_processed_block"))
        if (
            current_head is not None
            and last_processed is not None
            and current_head >= last_processed >= 0
        ):
            lag = current_head - last_processed
            # The execution core intentionally waits for one successor block
            # before freezing a source block.  Therefore only zero or one is a
            # valid formula-derived cursor lag; two or more means backlog.
            if lag not in {0, 1}:
                issues.append(f"{prefix}_CHAIN_CURSOR_LAG_INVALID:{lag}")
    liquidity_retry = payload.get("liquidity_retry")
    if not isinstance(liquidity_retry, dict):
        issues.append(f"{prefix}_LIQUIDITY_RETRY_POLICY_MISSING")
    else:
        if liquidity_retry.get("policy_id") != "LIQUIDITY_ONLY_RETRY_V2":
            issues.append(f"{prefix}_LIQUIDITY_RETRY_POLICY_INVALID")
        effective_after_block = _as_int(
            liquidity_retry.get("effective_after_block")
        )
        if effective_after_block is None or effective_after_block < 0:
            issues.append(f"{prefix}_LIQUIDITY_RETRY_WATERLINE_INVALID")
        if liquidity_retry.get("deadline_ms") is not None:
            issues.append(f"{prefix}_LIQUIDITY_RETRY_DEADLINE_PRESENT")
        for field, label in (
            (
                "unknown_repost_violation_count",
                f"{prefix}_LIQUIDITY_RETRY_UNKNOWN_REPOSTS",
            ),
            (
                "target_conservation_violation_count",
                f"{prefix}_LIQUIDITY_RETRY_TARGET_NONCONSERVATION",
            ),
        ):
            count = _as_int(liquidity_retry.get(field))
            if count is None or count < 0:
                issues.append(f"{label}_INVALID")
            elif count:
                issues.append(f"{label}:{count}")
    return issues


def cd90_external_limitations(payload: dict[str, Any]) -> list[str]:
    """Quantify immutable external gaps without calling them ledger corruption."""

    limitations: list[str] = []
    unpriced_gap_actions = _as_int(payload.get("unpriced_gap_action_count"))
    if unpriced_gap_actions is None or unpriced_gap_actions < 0:
        limitations.append("CD90_UNPRICED_GAP_ACTION_COUNT_INVALID")
    elif unpriced_gap_actions:
        limitations.append(f"CD90_UNPRICED_GAP_ACTIONS:{unpriced_gap_actions}")
    fidelity = payload.get("action_fidelity")
    if isinstance(fidelity, dict):
        for field, label in (
            ("pending", "CD90_PENDING_ACTION_TARGETS"),
            ("external_or_causal_unfilled", "CD90_EXTERNAL_OR_CAUSAL_UNFILLED"),
            ("metadata_pending", "CD90_PENDING_ACTION_METADATA"),
        ):
            value = _as_int(fidelity.get(field))
            if value is None or value < 0:
                limitations.append(f"{label}_INVALID")
            elif value:
                limitations.append(f"{label}:{value}")
    return limitations


def live_external_limitations(
    payload: dict[str, Any], *, profile_key: str,
    current_version_counts: dict[str, int] | None = None,
) -> list[str]:
    prefix = _profile_issue_prefix(profile_key)
    if current_version_counts is None:
        limitations = [
            issue.replace("CD90_", f"{prefix}_", 1)
            for issue in cd90_external_limitations(payload)
        ]
    else:
        limitations = []
        for field, label in (
            ("pending", f"{prefix}_PENDING_ACTION_TARGETS"),
            (
                "external_or_causal_unfilled",
                f"{prefix}_EXTERNAL_OR_CAUSAL_UNFILLED",
            ),
        ):
            count = int(current_version_counts.get(field, 0))
            if count:
                limitations.append(f"{label}:{count}")
    runtime = payload.get("runtime")
    if isinstance(runtime, dict) and _as_text(
        runtime.get("external_head_incident_active")
    ) == "true":
        count = _as_int(runtime.get("external_head_incident_occurrence_count"))
        limitations.append(
            f"{prefix}_ACTIVE_EXTERNAL_HEAD_INCIDENTS:"
            f"{count if count is not None else 'INVALID'}"
        )
    liquidity_retry = payload.get("liquidity_retry")
    if isinstance(liquidity_retry, dict):
        pending_actions = liquidity_retry.get("pending_actions")
        if isinstance(pending_actions, list):
            for row in pending_actions:
                if isinstance(row, dict):
                    limitations.append(
                        f"{prefix}_LIQUIDITY_RETRY_PENDING:"
                        + json.dumps(row, sort_keys=True, separators=(",", ":"))
                    )
    return limitations


def external_redemption_maintenance_limitation(
    *,
    runtime: dict[str, Any],
    error_audit: dict[str, Any],
    profile_key: str,
) -> str | None:
    """Classify only the exact externally evidenced maintenance outcome."""

    if _as_text(runtime.get("last_cycle_outcome")) != (
        "success_redemption_maintenance_pending"
    ):
        return None
    if _as_int(error_audit.get("internal_event_count")) != 0:
        return None
    if _as_int(error_audit.get("code_repair_event_count")) != 0:
        return None
    if str(error_audit.get("latest_category") or "").upper() != (
        "EXTERNAL_REDEMPTION_MAINTENANCE"
    ):
        return None
    if str(error_audit.get("state") or "").upper() != "ERRORS_OBSERVED":
        return None
    if (_as_int(error_audit.get("event_count")) or 0) < 1:
        return None
    if (_as_int(error_audit.get("external_event_count")) or 0) < 1:
        return None
    category_counts = error_audit.get("category_counts")
    if not isinstance(category_counts, dict) or (
        _as_int(category_counts.get("EXTERNAL_REDEMPTION_MAINTENANCE")) or 0
    ) < 1:
        return None
    return (
        f"{_profile_issue_prefix(profile_key)}_"
        "EXTERNAL_REDEMPTION_MAINTENANCE_PENDING"
    )


def exact_external_redemption_cycle_retry(
    *, payload: dict[str, Any], error_audit: dict[str, Any]
) -> bool:
    redemption = payload.get("automatic_redemption")
    if not isinstance(redemption, dict) or _as_text(
        redemption.get("worker_state")
    ) != "external_retry":
        return False
    if _as_int(error_audit.get("internal_event_count")) != 0:
        return False
    if _as_int(error_audit.get("code_repair_event_count")) != 0:
        return False
    if str(error_audit.get("state") or "").upper() != "ERRORS_OBSERVED":
        return False
    if str(error_audit.get("latest_category") or "").upper() != (
        "EXTERNAL_REDEMPTION_CYCLE"
    ):
        return False
    category_counts = error_audit.get("category_counts")
    return isinstance(category_counts, dict) and (
        _as_int(category_counts.get("EXTERNAL_REDEMPTION_CYCLE")) or 0
    ) > 0


def profile_registry_issues(
    *,
    expected_profiles: set[str],
    monitored_profiles: set[str],
    residual_profiles: set[str] | None = None,
) -> list[str]:
    """Require live-copy monitoring to cover every non-residual coordinator sleeve.

    A residual sleeve is shared-wallet leftover accounting, not a copy
    executor. It may stay registered in the coordinator without a live-profile
    or systemd unit after that wallet's follower is deleted.
    """

    residual_profiles = set(residual_profiles or ())
    expected_for_coverage = expected_profiles - (
        residual_profiles - monitored_profiles
    )
    issues: list[str] = []
    missing = sorted(expected_for_coverage - monitored_profiles)
    extra = sorted(monitored_profiles - expected_profiles)
    if missing:
        issues.append("UNMONITORED_COORDINATOR_PROFILES:" + ",".join(missing))
    if extra:
        issues.append("MONITORED_UNREGISTERED_PROFILES:" + ",".join(extra))
    return issues


def parse_live_profile_specs(values: list[str]) -> dict[str, dict[str, Any]]:
    """Parse N-wallet primary and optional hot-standby service specs."""

    profiles: dict[str, dict[str, Any]] = {}
    for raw in values:
        parts = str(raw).split("=", 3)
        if len(parts) not in {3, 4} or not all(part.strip() for part in parts[:3]):
            raise ValueError("INVALID_LIVE_PROFILE_SPEC")
        profile_key, unit, runtime_dir = (part.strip() for part in parts[:3])
        hot_standby_unit = (
            None if len(parts) == 3 or not parts[3].strip() else parts[3].strip()
        )
        if profile_key in profiles:
            raise ValueError(f"DUPLICATE_LIVE_PROFILE_SPEC:{profile_key}")
        runtime = Path(runtime_dir).expanduser()
        if not runtime.is_absolute():
            raise ValueError(f"LIVE_PROFILE_RUNTIME_NOT_ABSOLUTE:{profile_key}")
        profiles[profile_key] = {
            "unit": unit,
            "runtime_dir": runtime.resolve(),
            "hot_standby_unit": hot_standby_unit,
        }
    if not profiles:
        raise ValueError("EMPTY_LIVE_PROFILE_REGISTRY")
    return profiles


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return loaded if isinstance(loaded, dict) else None


def _run(command: list[str]) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 127, "", str(exc)
    return completed.returncode, completed.stdout.strip(), completed.stderr.strip()


def _unit_state(unit: str) -> dict[str, str]:
    code, stdout, stderr = _run(
        [
            "systemctl",
            "show",
            unit,
            "--property=ActiveState,SubState,Result,ExecMainStatus,NRestarts,UnitFileState",
            "--no-page",
        ]
    )
    values: dict[str, str] = {"unit": unit, "command_exit_code": str(code)}
    for line in stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    if stderr:
        values["stderr"] = stderr
    return values


def _failed_polymarket_units(expected_units: set[str] | None = None) -> list[str]:
    code, stdout, _ = _run(["systemctl", "--failed", "--no-legend", "--plain", "--no-page"])
    # `systemctl --failed` exits nonzero when it finds no failed units on some
    # releases, so the parsed unit list rather than the exit code is decisive.
    del code
    failed: list[str] = []
    for line in stdout.splitlines():
        fields = line.split()
        if (
            fields
            and fields[0].startswith("com.luke.polymarket.")
            and (expected_units is None or fields[0] in expected_units)
        ):
            failed.append(fields[0])
    return sorted(set(failed))


def _sqlite_integrity(path: Path) -> dict[str, str]:
    if not path.exists():
        return {"state": "MISSING", "path": str(path)}
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10) as connection:
            row = connection.execute("PRAGMA integrity_check").fetchone()
        value = str(row[0]) if row else "NO_RESULT"
    except sqlite3.Error as exc:
        return {"state": "ERROR", "path": str(path), "detail": str(exc)}
    return {"state": "OK" if value.lower() == "ok" else "FAILED", "path": str(path), "detail": value}


def release_error_audit_baseline_from_runtime(
    runtime: dict[str, Any] | None,
) -> int | None:
    """Return the current in-process continuity boundary when one exists."""

    if not isinstance(runtime, dict):
        return None
    continuity_markers = (
        _as_int(runtime.get("operator_planned_resume_started_at_ms")),
        _as_int(runtime.get("hot_standby_joined_at_ms")),
        _as_int(runtime.get("ws_subscription_started_at_ms")),
    )
    known_markers = [marker for marker in continuity_markers if marker is not None and marker >= 0]
    return max(known_markers) if known_markers else None


def _release_error_baseline_ms(
    *, runtime: dict[str, Any] | None, ledger_path: Path,
    version_cutover_ms: int | None = None,
) -> int | None:
    """Use the running session marker, or the immutable first-launch receipt.

    A newly registered forward-only sleeve may not have an operator-resume
    timestamp yet.  Its signed launch receipt is the authoritative lower bound
    for the first live session and prevents the health audit from silently
    disabling itself just because no restart has occurred.
    """

    runtime_baseline = release_error_audit_baseline_from_runtime(runtime)
    if runtime_baseline is not None:
        return max(runtime_baseline, version_cutover_ms or 0)
    try:
        with sqlite3.connect(
            f"file:{ledger_path}?mode=ro", uri=True, timeout=10
        ) as connection:
            row = connection.execute(
                "SELECT value FROM config "
                "WHERE key = 'profile_launch_receipt_json'"
            ).fetchone()
        if row is None:
            return None
        receipt = json.loads(str(row[0]))
    except (json.JSONDecodeError, sqlite3.Error):
        return None
    launch_baseline = _as_int(receipt.get("observed_at_ms"))
    launch_result = (
        launch_baseline
        if launch_baseline is not None and launch_baseline >= 0
        else None
    )
    if launch_result is None:
        return version_cutover_ms
    return max(launch_result, version_cutover_ms or 0)


def current_release_cutover_ms(
    index_path: Path = CURRENT_VERSION_INDEX,
) -> int | None:
    """Return the canonical committed version boundary in epoch milliseconds."""

    try:
        index = json.loads(index_path.read_text(encoding="utf-8"))
        timestamp_value = index.get("cutover_at_utc")
        if not timestamp_value:
            receipt = json.loads(
                Path(index["source_commit_receipt"]).read_text(encoding="utf-8")
            )
            timestamp_value = receipt["committed_at_utc"]
        timestamp = str(timestamp_value).replace("Z", "+00:00")
        return int(datetime.fromisoformat(timestamp).timestamp() * 1000)
    except (KeyError, OSError, ValueError, json.JSONDecodeError):
        return None


def current_version_action_counts(
    ledger_path: Path, *, version_cutover_ms: int
) -> dict[str, int]:
    """Count only actions first discovered after the current version cutover."""

    counts = {
        "observed": 0,
        "unresolved": 0,
        "pending": 0,
        "external_or_causal_unfilled": 0,
        "internal_error": 0,
    }
    unresolved_states = {
        "SUBMIT_STARTED", "SUBMITTED_UNRECONCILED", "UNKNOWN_SUBMISSION"
    }
    external_states = {
        "SKIPPED", "EXTERNAL_UNFILLABLE", "EXPIRED_RETRY_WINDOW",
        "SUPERSEDED_UNFILLED",
    }
    try:
        with sqlite3.connect(
            f"file:{ledger_path}?mode=ro", uri=True, timeout=10
        ) as connection:
            rows = connection.execute(
                """
                SELECT target.state, target.target_quantity,
                       target.cumulative_filled_quantity
                FROM action_targets AS target
                JOIN action_receipts AS action
                  ON action.action_id = target.action_id
                WHERE action.discovered_at_ms >= ?
                """,
                (int(version_cutover_ms),),
            ).fetchall()
    except sqlite3.Error:
        return {**counts, "internal_error": 1}
    counts["observed"] = len(rows)
    for state_raw, target_raw, filled_raw in rows:
        state = str(state_raw).upper()
        if state in unresolved_states:
            counts["unresolved"] += 1
        elif state.startswith("PENDING_"):
            counts["pending"] += 1
        elif state in external_states:
            try:
                incomplete = Decimal(str(filled_raw)) < Decimal(str(target_raw))
            except InvalidOperation:
                counts["internal_error"] += 1
            else:
                counts["external_or_causal_unfilled"] += int(incomplete)
        elif state not in {"FILLED", "PARTIAL"}:
            counts["internal_error"] += 1
    return counts


def recovered_internal_before_success(
    *,
    internal_event_count: int,
    code_repair_event_count: int,
    latest_internal_occurred_at_ms: int,
    last_successful_cycle_at_ms: int,
    last_cycle_outcome: str,
) -> bool:
    """True when a post-release internal error is already behind a later success.

    Matches the closed-loop release pre-stop recovery rule: a latched internal
    event must not keep the fleet INTERNAL after a later successful cycle,
    unless it still carries CODE_REPAIR_REQUIRED.
    """

    return (
        internal_event_count > 0
        and code_repair_event_count == 0
        and str(last_cycle_outcome or "").strip().upper() in SUCCESS_CYCLE_OUTCOMES
        and 0 < int(latest_internal_occurred_at_ms or 0) < int(
            last_successful_cycle_at_ms or 0
        )
    )


def release_runtime_error_audit(
    path: Path,
    *,
    release_started_at_ms: int | None,
) -> dict[str, Any]:
    """Latch every immutable runtime error written during this release session."""

    baseline = _as_int(release_started_at_ms)
    empty = {
        "state": "INVALID_BASELINE",
        "release_started_at_ms": release_started_at_ms,
        "event_count": 0,
        "internal_event_count": 0,
        "external_event_count": 0,
        "code_repair_event_count": 0,
        "latest_category": "",
        "latest_internal_occurred_at_ms": 0,
        "last_successful_cycle_at_ms": 0,
        "last_cycle_outcome": "",
        "category_counts": {},
    }
    if baseline is None or baseline < 0:
        return empty
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10) as connection:
            rows = connection.execute(
                """
                SELECT id, category, message, details_json, occurred_at_ms
                FROM runtime_errors
                WHERE occurred_at_ms >= ?
                ORDER BY id
                """,
                (baseline,),
            ).fetchall()
            pre_cutover_action_ids = {
                str(row[0])
                for row in connection.execute(
                    "SELECT action_id FROM action_receipts "
                    "WHERE discovered_at_ms < ?",
                    (baseline,),
                ).fetchall()
            }
            try:
                cycle_rows = {
                    str(row[0]): str(row[1] or "")
                    for row in connection.execute(
                        "SELECT key, value FROM runtime_state "
                        "WHERE key IN ("
                        "'last_successful_cycle_at_ms','last_cycle_outcome'"
                        ")"
                    ).fetchall()
                }
            except sqlite3.Error:
                cycle_rows = {}
    except sqlite3.Error as exc:
        return {
            "state": "READ_ERROR",
            "release_started_at_ms": baseline,
            "event_count": 0,
            "internal_event_count": 0,
            "external_event_count": 0,
            "code_repair_event_count": 0,
            "latest_category": "",
            "latest_internal_occurred_at_ms": 0,
            "last_successful_cycle_at_ms": 0,
            "last_cycle_outcome": "",
            "category_counts": {},
            "detail": f"{type(exc).__name__}:{exc}",
        }
    filtered_rows = []
    for row in rows:
        try:
            details = json.loads(str(row[3] or "{}"))
        except json.JSONDecodeError:
            details = {}
        action_id = str(details.get("action_id") or "")
        if action_id and action_id in pre_cutover_action_ids:
            continue
        filtered_rows.append(row)
    rows = filtered_rows
    category_counts: dict[str, int] = {}
    code_repair_event_count = 0
    safety_gate_event_count = 0
    latest_internal_occurred_at_ms = 0
    for _row_id, category, message, details_json, occurred_at_ms in rows:
        normalized_category = str(category)
        category_counts[normalized_category] = (
            category_counts.get(normalized_category, 0) + 1
        )
        evidence_text = "\n".join(
            (normalized_category, str(message or ""), str(details_json or ""))
        )
        if "CODE_REPAIR_REQUIRED" in evidence_text.upper():
            code_repair_event_count += 1
        if (
            normalized_category == "ACCOUNT_CASH_RECONCILIATION"
            and str(message) == "BLOCK_ACTIVE_WALLET_RESERVATIONS"
        ):
            safety_gate_event_count += 1
        if not normalized_category.upper().startswith("EXTERNAL_"):
            occurred = _as_int(occurred_at_ms) or 0
            if occurred > latest_internal_occurred_at_ms:
                latest_internal_occurred_at_ms = occurred
    external = sum(
        count
        for category, count in category_counts.items()
        if category.upper().startswith("EXTERNAL_")
    )
    total = sum(category_counts.values())
    internal = total - external - safety_gate_event_count
    last_successful_cycle_at_ms = _as_int(
        cycle_rows.get("last_successful_cycle_at_ms")
    ) or 0
    last_cycle_outcome = str(cycle_rows.get("last_cycle_outcome") or "").upper()
    return {
        "state": "OK" if total == 0 else "ERRORS_OBSERVED",
        "release_started_at_ms": baseline,
        "event_count": total,
        "internal_event_count": internal,
        "external_event_count": external,
        "safety_gate_event_count": safety_gate_event_count,
        "code_repair_event_count": code_repair_event_count,
        "latest_category": "" if not rows else str(rows[-1][1]),
        "latest_internal_occurred_at_ms": latest_internal_occurred_at_ms,
        "last_successful_cycle_at_ms": last_successful_cycle_at_ms,
        "last_cycle_outcome": last_cycle_outcome,
        "category_counts": category_counts,
    }


def _atomic_write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(body, encoding="utf-8")
    os.replace(temporary, path)


def _shanghai_timestamp(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, ZoneInfo("Asia/Shanghai")).strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )


def _render_markdown(payload: dict[str, Any]) -> str:
    service_rows = "\n".join(
        "| {unit} | {active} | {sub} | {result} |".format(
            unit=row["unit"],
            active=row.get("ActiveState", "unknown"),
            sub=row.get("SubState", "unknown"),
            result=row.get("Result", "unknown"),
        )
        for row in payload["services"]
    )
    profile_rows = "\n".join(
        "| {profile} | {status} | {sqlite} | {receipts} | {alerts} | {errors} | {issues} | {external} |".format(
            profile=profile,
            status=row["status_state"],
            sqlite=row["sqlite_integrity"]["state"],
            receipts=row["action_receipt_count"],
            alerts=row["unacknowledged_non_netflix_alert_count"],
            errors="{}/{}".format(
                row["release_runtime_error_audit"]["internal_event_count"],
                row["release_runtime_error_audit"]["external_event_count"],
            ),
            issues=", ".join(row["status_issues"]) or "none",
            external=", ".join(row["external_limitations"]) or "none",
        )
        for profile, row in sorted(payload["profiles"].items())
    )
    coordinator_issues = ", ".join(payload["coordinator"]["issues"]) or "none"
    lock_issues = ", ".join(payload["runtime_lock_contract"]["issues"]) or "none"
    failed = ", ".join(payload["failed_polymarket_units"]) or "none"
    return """# Cash-live server health heartbeat

- Generated: {generated_at_shanghai} (empirical value; server clock)
- Overall state: {overall_state} (formula-derived from all registered live sleeves, coordinator, SQLite and systemd checks)
- Monitored/registered profiles: {monitored}/{registered} (formula-derived counts)
- Coordinator state: {coordinator_state}; issues: {coordinator_issues}
- Runtime lock contract: {lock_state}; issues: {lock_issues}
- Failed Polymarket units: {failed} (empirical systemd query)
- Disk available: {disk_available_bytes} bytes (empirical; no unverified low-disk threshold is applied)
- Live-health timer active state: {timer_active_state} (empirical systemd query)

## Services

| Unit | Active | Substate | Result |
| --- | --- | --- | --- |
{service_rows}

## Live sleeves

| Profile | Status | SQLite | Source actions | Non-Netflix alerts | Release errors I/E | Internal issues | External limitations |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
{profile_rows}

This heartbeat checks server health and reports observable faults. It does not submit, cancel, redeem, or modify a real order.
""".format(
        generated_at_shanghai=payload["generated_at_shanghai"],
        overall_state=payload["overall_state"],
        monitored=payload["monitored_profile_count"],
        registered=payload["coordinator"]["registered_profile_count"],
        coordinator_state=payload["coordinator"]["state"],
        coordinator_issues=coordinator_issues,
        lock_state=payload["runtime_lock_contract"]["state"],
        lock_issues=lock_issues,
        failed=failed,
        disk_available_bytes=payload["disk_available_bytes"],
        timer_active_state=payload["health_timer"].get("ActiveState", "unknown"),
        service_rows=service_rows or "| none | unknown | unknown | unknown |",
        profile_rows=profile_rows or "| none | missing | missing | 0 | 0 | 0/0 | missing | missing |",
    )


def _render_html(markdown: str) -> str:
    return (
        "<!doctype html><meta charset=\"utf-8\"><title>Polymarket server health</title>\n"
        "<style>body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;"
        "margin:32px;color:#172033}pre{white-space:pre-wrap;background:#f6f8fa;"
        "padding:16px;border-radius:8px}</style>\n"
        "<h1>Polymarket server health heartbeat</h1><pre>"
        + html.escape(markdown)
        + "</pre>"
    )


def _canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _receipt_hash_valid(receipt: dict[str, Any], stored_hash: str) -> bool:
    import hashlib

    body = dict(receipt)
    embedded = str(body.pop("migration_receipt_hash", ""))
    actual = hashlib.sha256(_canonical_json(body).encode("utf-8")).hexdigest()
    return bool(embedded) and embedded == actual == str(stored_hash)


def coordinator_health(
    *,
    coordinator_path: Path,
    monitored_profiles: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Read-only validation of the current N-sleeve coordinator receipt chain."""

    integrity = _sqlite_integrity(coordinator_path)
    issues: list[str] = []
    if integrity["state"] != "OK":
        issues.append("COORDINATOR_SQLITE_" + integrity["state"])
        return {
            "state": "INVALID",
            "path": str(coordinator_path),
            "sqlite_integrity": integrity,
            "registered_profiles": [],
            "registered_profile_count": 0,
            "issues": issues,
        }
    try:
        with sqlite3.connect(
            f"file:{coordinator_path}?mode=ro", uri=True, timeout=10
        ) as connection:
            connection.row_factory = sqlite3.Row
            current_row = connection.execute(
                "SELECT receipt_json, receipt_hash FROM migration_receipt WHERE singleton = 1"
            ).fetchone()
            sleeve_rows = connection.execute(
                "SELECT profile_key, ledger_path, role FROM sleeves ORDER BY profile_key"
            ).fetchall()
            contract_exists = connection.execute(
                """
                SELECT COUNT(*) FROM sqlite_master
                WHERE type = 'table' AND name = 'wallet_contract'
                """
            ).fetchone()[0]
            contract_row = (
                connection.execute(
                    """
                    SELECT submission_lock_path
                    FROM wallet_contract
                    WHERE singleton = 1
                    """
                ).fetchone()
                if contract_exists
                else None
            )
            history_exists = connection.execute(
                """
                SELECT COUNT(*) FROM sqlite_master
                WHERE type = 'table' AND name = 'migration_history'
                """
            ).fetchone()[0]
            history_rows = (
                connection.execute(
                    """
                    SELECT generation, receipt_json, receipt_hash
                    FROM migration_history ORDER BY generation
                    """
                ).fetchall()
                if history_exists
                else []
            )
    except (sqlite3.Error, TypeError, ValueError, json.JSONDecodeError) as exc:
        issues.append(f"COORDINATOR_READ_ERROR:{type(exc).__name__}")
        return {
            "state": "INVALID",
            "path": str(coordinator_path),
            "sqlite_integrity": integrity,
            "registered_profiles": [],
            "registered_profile_count": 0,
            "issues": issues,
        }
    if current_row is None:
        issues.append("COORDINATOR_CURRENT_RECEIPT_MISSING")
        current: dict[str, Any] = {}
    else:
        try:
            current = json.loads(str(current_row["receipt_json"]))
        except json.JSONDecodeError:
            current = {}
            issues.append("COORDINATOR_CURRENT_RECEIPT_MALFORMED")
        else:
            if not _receipt_hash_valid(current, str(current_row["receipt_hash"])):
                issues.append("COORDINATOR_CURRENT_RECEIPT_HASH_MISMATCH")

    registered = {str(row["profile_key"]) for row in sleeve_rows}
    residual = {
        str(row["profile_key"])
        for row in sleeve_rows
        if str(row["role"]) == "RESIDUAL"
    }
    submission_lock_path = (
        None if contract_row is None else str(contract_row["submission_lock_path"])
    )
    if submission_lock_path is None:
        issues.append("COORDINATOR_SUBMISSION_LOCK_CONTRACT_MISSING")
    elif not Path(submission_lock_path).is_absolute():
        issues.append("COORDINATOR_SUBMISSION_LOCK_PATH_NOT_ABSOLUTE")
    issues.extend(
        profile_registry_issues(
            expected_profiles=registered,
            monitored_profiles=set(monitored_profiles),
            residual_profiles=residual,
        )
    )
    for row in sleeve_rows:
        profile = str(row["profile_key"])
        monitored = monitored_profiles.get(profile)
        if monitored is None:
            continue
        expected_ledger = Path(monitored["runtime_dir"]).resolve() / "live.sqlite3"
        if Path(str(row["ledger_path"])).resolve() != expected_ledger:
            issues.append(f"COORDINATOR_LEDGER_PATH_MISMATCH:{profile}")

    previous_hash: str | None = None
    expected_generation = 1
    for row in history_rows:
        try:
            receipt = json.loads(str(row["receipt_json"]))
        except json.JSONDecodeError:
            issues.append(f"COORDINATOR_HISTORY_MALFORMED:{row['generation']}")
            continue
        generation = _as_int(row["generation"])
        if generation != expected_generation:
            issues.append("COORDINATOR_HISTORY_GENERATION_GAP")
        if not _receipt_hash_valid(receipt, str(row["receipt_hash"])):
            issues.append(f"COORDINATOR_HISTORY_HASH_MISMATCH:{row['generation']}")
        parent = receipt.get("parent_migration_receipt_hash")
        if expected_generation == 1:
            if parent not in {None, ""}:
                issues.append("COORDINATOR_INITIAL_PARENT_NOT_EMPTY")
        elif str(parent) != str(previous_hash):
            issues.append(f"COORDINATOR_HISTORY_PARENT_MISMATCH:{row['generation']}")
        previous_hash = str(row["receipt_hash"])
        expected_generation += 1
    if not history_rows:
        issues.append("COORDINATOR_HISTORY_MISSING")
    elif current and previous_hash != str(current.get("migration_receipt_hash", "")):
        issues.append("COORDINATOR_CURRENT_NOT_HISTORY_TIP")
    return {
        "state": "OK" if not issues else "INVALID",
        "path": str(coordinator_path),
        "sqlite_integrity": integrity,
        "migration_receipt_hash": current.get("migration_receipt_hash"),
        "generation": current.get("generation"),
        "registered_profiles": sorted(registered),
        "registered_profile_count": len(registered),
        "submission_lock_path": submission_lock_path,
        "issues": issues,
    }


def _runtime_lock_contract(
    *,
    runtime_root: Path,
    monitored_profiles: dict[str, dict[str, Any]],
    submission_lock_path: str | None,
    expected_root_uid: int,
    expected_root_gid: int,
    expected_lock_uid: int,
    expected_lock_gid: int,
) -> dict[str, Any]:
    """Verify that pathname replacement cannot split a live flock."""

    root = Path(runtime_root)
    issues: list[str] = []
    try:
        root_status = root.lstat()
    except FileNotFoundError:
        root_status = None
    if (
        root_status is None
        or not stat.S_ISDIR(root_status.st_mode)
        or root.is_symlink()
        or root_status.st_uid != int(expected_root_uid)
        or root_status.st_gid != int(expected_root_gid)
        or stat.S_IMODE(root_status.st_mode) & 0o022
    ):
        issues.append("RUNTIME_ROOT_METADATA_MISMATCH")

    expected_shared = root / "authenticated-wallet.lock"
    if not submission_lock_path or Path(submission_lock_path) != expected_shared:
        issues.append("COORDINATOR_SUBMISSION_LOCK_PATH_MISMATCH")

    locks: list[tuple[str, str | None, Path]] = [
        ("SHARED_WALLET", None, expected_shared)
    ]
    for profile_key, spec in monitored_profiles.items():
        runtime_dir = Path(spec["runtime_dir"])
        if runtime_dir.parent != root:
            issues.append(f"PROFILE_RUNTIME_PARENT_MISMATCH:{profile_key}")
        locks.append(
            (
                "PROFILE_RUNTIME",
                profile_key,
                runtime_dir.with_name(f"{runtime_dir.name}.lock"),
            )
        )
    for label, profile_key, lock in locks:
        suffix = "" if profile_key is None else f":{profile_key}"
        try:
            status = lock.lstat()
        except FileNotFoundError:
            issues.append(f"{label}_LOCK_MISSING{suffix}")
            continue
        mode = stat.S_IMODE(status.st_mode)
        if (
            not stat.S_ISREG(status.st_mode)
            or lock.is_symlink()
            or status.st_nlink != 1
            or status.st_uid != int(expected_lock_uid)
            or status.st_gid != int(expected_lock_gid)
            or mode != 0o600
        ):
            issues.append(f"{label}_LOCK_METADATA_MISMATCH{suffix}")
    return {
        "state": "OK" if not issues else "INVALID",
        "runtime_root": str(root),
        "issues": issues,
    }


def build_payload(
    *,
    cd90_runtime_dir: Path,
    tennis_runtime_dir: Path,
    coordinator_path: Path,
    live_profiles: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    generated_epoch = time.time()
    version_cutover_ms = current_release_cutover_ms()
    if live_profiles is None:
        live_profiles = {
            "cd90": {
                "runtime_dir": cd90_runtime_dir,
                "unit": "com.luke.polymarket.cd90-live.service",
                "hot_standby_unit": (
                    "com.luke.polymarket.cd90-live-hot-standby.service"
                ),
            },
            "tennis_atp_wta_mainline": {
                "runtime_dir": tennis_runtime_dir,
                "unit": "com.luke.polymarket.tennis-live.service",
                "hot_standby_unit": (
                    "com.luke.polymarket.tennis-live-hot-standby.service"
                ),
            },
        }
    unit_states: dict[str, dict[str, str]] = {}
    profile_unit_names: dict[str, list[str]] = {}
    for profile_key, spec in live_profiles.items():
        names = [str(spec["unit"])]
        hot_standby_unit = spec.get("hot_standby_unit")
        if hot_standby_unit:
            names.append(str(hot_standby_unit))
        profile_unit_names[profile_key] = names
        for unit in names:
            unit_states[unit] = _unit_state(unit)
    paused_profiles = sorted(
        profile_key
        for profile_key, names in profile_unit_names.items()
        if names
        and all(
            unit_states[unit].get("ActiveState") == "inactive"
            and unit_states[unit].get("UnitFileState") == "disabled"
            for unit in names
        )
    )
    profiles: dict[str, Any] = {}
    for profile_key, spec in live_profiles.items():
        paused = profile_key in paused_profiles
        runtime_dir = Path(spec["runtime_dir"])
        status_path = runtime_dir / "status.json"
        status = _read_json(status_path)
        issues = (
            [f"{_profile_issue_prefix(profile_key)}_STATUS_MISSING_OR_MALFORMED"]
            if status is None
            else live_status_issues(status, profile_key=profile_key)
        )
        prefix = _profile_issue_prefix(profile_key)
        if status is not None:
            issues = [
                issue
                for issue in issues
                if not (
                    issue.startswith(f"{prefix}_UNRESOLVED_ACTIONS:")
                    or issue.startswith(f"{prefix}_ACTION_FIDELITY_")
                )
            ]
        ledger_path = runtime_dir / "live.sqlite3"
        version_counts = (
            {"observed": 0, "unresolved": 0, "pending": 0,
             "external_or_causal_unfilled": 0, "internal_error": 1}
            if version_cutover_ms is None
            else current_version_action_counts(
                ledger_path, version_cutover_ms=version_cutover_ms
            )
        )
        if version_cutover_ms is None:
            issues.append(f"{prefix}_CURRENT_VERSION_CUTOVER_MISSING")
        if version_counts["unresolved"]:
            issues.append(
                f"{prefix}_UNRESOLVED_ACTIONS:{version_counts['unresolved']}"
            )
        if version_counts["internal_error"]:
            issues.append(
                f"{prefix}_CURRENT_VERSION_ACTION_AUDIT_ERRORS:"
                f"{version_counts['internal_error']}"
            )
        external = (
            []
            if status is None
            else live_external_limitations(
                status,
                profile_key=profile_key,
                current_version_counts=version_counts,
            )
        )
        runtime = status.get("runtime") if isinstance(status, dict) else None
        release_started_at_ms = _release_error_baseline_ms(
            runtime=runtime if isinstance(runtime, dict) else None,
            ledger_path=ledger_path,
            version_cutover_ms=version_cutover_ms,
        )
        error_audit = release_runtime_error_audit(
            runtime_dir / "live.sqlite3",
            release_started_at_ms=release_started_at_ms,
        )
        external_maintenance = (
            None
            if not isinstance(runtime, dict)
            else external_redemption_maintenance_limitation(
                runtime=runtime,
                error_audit=error_audit,
                profile_key=profile_key,
            )
        )
        if external_maintenance is not None and not paused:
            issues = [
                issue
                for issue in issues
                if issue != f"{prefix}_LAST_CYCLE_NOT_SUCCESS"
            ]
            external.append(external_maintenance)
        if not paused and status is not None and exact_external_redemption_cycle_retry(
            payload=status, error_audit=error_audit
        ):
            issues = [
                issue
                for issue in issues
                if issue != f"{prefix}_AUTOMATIC_REDEMPTION_WORKER_INVALID"
            ]
        # A missing/malformed status already records the controlling internal
        # failure.  Do not double-count the derivative fact that its release
        # baseline cannot be read; when status is present, either audit failure
        # remains independently actionable.
        if not paused and status is not None and error_audit["state"] in {
            "INVALID_BASELINE",
            "READ_ERROR",
        }:
            issues.append(
                f"{prefix}_RELEASE_ERROR_AUDIT_{error_audit['state']}"
            )
        recovered_internal = recovered_internal_before_success(
            internal_event_count=int(error_audit.get("internal_event_count") or 0),
            code_repair_event_count=int(
                error_audit.get("code_repair_event_count") or 0
            ),
            latest_internal_occurred_at_ms=int(
                error_audit.get("latest_internal_occurred_at_ms") or 0
            ),
            last_successful_cycle_at_ms=int(
                error_audit.get("last_successful_cycle_at_ms") or 0
            ),
            last_cycle_outcome=str(error_audit.get("last_cycle_outcome") or ""),
        )
        if not paused and error_audit["internal_event_count"] and not recovered_internal:
            issues.append(
                f"{prefix}_POST_RELEASE_INTERNAL_ERROR_EVENTS:"
                f"{error_audit['internal_event_count']}"
            )
        if not paused and error_audit["code_repair_event_count"]:
            issues.append(
                f"{prefix}_POST_RELEASE_CODE_REPAIR_EVENTS:"
                f"{error_audit['code_repair_event_count']}"
            )
        if not paused and error_audit["external_event_count"]:
            external.append(
                f"{prefix}_POST_RELEASE_EXTERNAL_ERROR_EVENTS:"
                f"{error_audit['external_event_count']}"
            )
        hot_standby_unit = spec.get("hot_standby_unit")
        hot_standby_state = (
            None
            if not hot_standby_unit
            else unit_states[str(hot_standby_unit)]
        )
        if not paused and hot_standby_state is not None and hot_standby_state.get(
            "ActiveState"
        ) != "active":
            issues.append(f"{prefix}_HOT_STANDBY_INACTIVE")
        unacknowledged_topic_alert_count = 0
        latest_unacknowledged_topic_alert: dict[str, Any] | None = None
        if not paused and profile_key == "wallet_44b0_netflix" and status is not None:
            alert_summary = status.get("source_topic_alerts")
            if not isinstance(alert_summary, dict):
                issues.append(f"{prefix}_SOURCE_TOPIC_ALERTS_MALFORMED")
            else:
                alert_count = _as_int(alert_summary.get("unacknowledged_count"))
                alerts = alert_summary.get("unacknowledged")
                if (
                    alert_count is None
                    or alert_count < 0
                    or not isinstance(alerts, list)
                    or alert_count != len(alerts)
                    or any(not isinstance(alert, dict) for alert in alerts)
                ):
                    issues.append(f"{prefix}_SOURCE_TOPIC_ALERTS_MALFORMED")
                else:
                    unacknowledged_topic_alert_count = alert_count
                    latest_unacknowledged_topic_alert = (
                        dict(alerts[0]) if alerts else None
                    )
        if paused:
            # An intentionally stopped sleeve has no fresh websocket/status
            # contract.  Its immutable ledger and shared-wallet topology are
            # still verified below; only live-process-derived issues are
            # suppressed while both executor units remain disabled/inactive.
            issues = []
            external = []
        profiles[profile_key] = {
            "paused": paused,
            "runtime_dir": str(runtime_dir),
            "unit": str(spec["unit"]),
            "unit_state": unit_states[str(spec["unit"])],
            "hot_standby_unit": (
                None if not hot_standby_unit else str(hot_standby_unit)
            ),
            "hot_standby_state": hot_standby_state,
            "status_path": str(status_path),
            "status_state": "OK" if status is not None else "MISSING",
            "last_cycle_outcome": (
                str(runtime.get("last_cycle_outcome") or "").strip().upper()
                if isinstance(runtime, dict)
                else ""
            ),
            "status_issues": issues,
            "status_issue_count": len(issues),
            "external_limitations": external,
            "external_limitation_count": len(external),
            "release_runtime_error_audit": error_audit,
            "recovered_internal_error_evidence": (
                {
                    "state": "RECOVERED_BEFORE_LAST_SUCCESSFUL_CYCLE",
                    "event_count": int(error_audit.get("internal_event_count") or 0),
                    "latest_occurred_at_ms": int(
                        error_audit.get("latest_internal_occurred_at_ms") or 0
                    ),
                    "last_successful_cycle_at_ms": int(
                        error_audit.get("last_successful_cycle_at_ms") or 0
                    ),
                    "current_outcome": str(
                        error_audit.get("last_cycle_outcome") or ""
                    ),
                }
                if recovered_internal
                else None
            ),
            "current_version_cutover_ms": version_cutover_ms,
            "current_version_action_counts": version_counts,
            "sqlite_integrity": _sqlite_integrity(ledger_path),
            "action_receipt_count": (
                _as_int(status.get("action_receipt_count")) if status else 0
            ),
            "unacknowledged_non_netflix_alert_count": (
                unacknowledged_topic_alert_count
            ),
            "latest_unacknowledged_non_netflix_alert": (
                latest_unacknowledged_topic_alert
            ),
        }
    services = [
        unit_states[unit]
        for profile_key in live_profiles
        for unit in profile_unit_names[profile_key]
    ]
    service_paused_units = sorted(
        unit
        for profile_key in paused_profiles
        for unit in profile_unit_names[profile_key]
    )
    service_inactive_units = [
        row["unit"]
        for row in services
        if row.get("ActiveState") != "active"
        and row["unit"] not in service_paused_units
    ]
    timer = _unit_state(HEALTH_TIMER_UNIT)
    expected_units = {
        str(unit)
        for spec in live_profiles.values()
        for unit in (spec["unit"], spec.get("hot_standby_unit"))
        if unit
    } | {
        "com.luke.polymarket.live-health.service",
        HEALTH_TIMER_UNIT,
    }
    failed_units = _failed_polymarket_units(expected_units)
    disk_available_bytes = os.statvfs(MODULE_DIR).f_bavail * os.statvfs(MODULE_DIR).f_frsize
    coordinator = coordinator_health(
        coordinator_path=coordinator_path,
        monitored_profiles=live_profiles,
    )
    runtime_root = coordinator_path.resolve().parent.parent
    production_root = Path("/srv/polymarket-live/runtime")
    lock_contract = _runtime_lock_contract(
        runtime_root=runtime_root,
        monitored_profiles=live_profiles,
        submission_lock_path=coordinator.get("submission_lock_path"),
        expected_root_uid=(0 if runtime_root == production_root else os.getuid()),
        expected_root_gid=(0 if runtime_root == production_root else os.getgid()),
        expected_lock_uid=os.getuid(),
        expected_lock_gid=os.getgid(),
    )
    overall_state = "OK"
    if (
        any(row["status_issues"] for row in profiles.values())
        or any(
            row["sqlite_integrity"]["state"] != "OK" for row in profiles.values()
        )
        or coordinator["state"] != "OK"
        or lock_contract["state"] != "OK"
        or service_inactive_units
        or timer.get("ActiveState") != "active"
        or failed_units
    ):
        overall_state = "INTERNAL_DEGRADED"
    elif any(row["external_limitations"] for row in profiles.values()):
        overall_state = "EXTERNAL_DEGRADED"
    return {
        "generated_at_epoch_ms": int(generated_epoch * 1000),
        "generated_at_shanghai": _shanghai_timestamp(generated_epoch),
        "overall_state": overall_state,
        "overall_state_provenance_class": "formula_derived",
        "scope": "all_registered_cash_live_sleeves_no_order_action",
        "profiles": profiles,
        "monitored_profile_count": len(live_profiles),
        "monitored_profile_count_provenance_class": "formula_derived",
        "coordinator": coordinator,
        "runtime_lock_contract": lock_contract,
        "services": services,
        "service_expected_count": len(services),
        "service_expected_count_provenance_class": "formula_derived",
        "service_active_count": sum(
            1 for row in services if row.get("ActiveState") == "active"
        ),
        "service_active_count_provenance_class": "formula_derived",
        "service_inactive_units": service_inactive_units,
        "paused_profiles": paused_profiles,
        "service_paused_units": service_paused_units,
        "service_paused_count": len(service_paused_units),
        "service_paused_count_provenance_class": "formula_derived",
        "health_timer": timer,
        "failed_polymarket_units": failed_units,
        "failed_polymarket_unit_count": len(failed_units),
        "failed_polymarket_unit_count_provenance_class": "formula_derived",
        "disk_available_bytes": disk_available_bytes,
        "disk_available_bytes_provenance_class": "empirical",
    }


def write_reports(*, payload: dict[str, Any], runtime_dir: Path) -> None:
    markdown = _render_markdown(payload)
    _atomic_write(runtime_dir / "server_health_status.json", json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    _atomic_write(runtime_dir / "server_health_report.md", markdown)
    _atomic_write(runtime_dir / "server_health_report.html", _render_html(markdown))
    audit_path = runtime_dir / "server_health_audit.jsonl"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    parser.add_argument("--cd90-runtime-dir", type=Path, default=DEFAULT_CD90_RUNTIME_DIR)
    parser.add_argument("--tennis-runtime-dir", type=Path, default=DEFAULT_TENNIS_RUNTIME_DIR)
    parser.add_argument("--coordinator-path", type=Path, default=DEFAULT_COORDINATOR_PATH)
    parser.add_argument(
        "--live-profile",
        action="append",
        default=[],
        metavar="PROFILE=PRIMARY_UNIT=ABSOLUTE_RUNTIME_DIR[=HOT_STANDBY_UNIT]",
    )
    args = parser.parse_args()
    live_profiles = (
        parse_live_profile_specs(args.live_profile) if args.live_profile else None
    )
    payload = build_payload(
        cd90_runtime_dir=args.cd90_runtime_dir.resolve(),
        tennis_runtime_dir=args.tennis_runtime_dir.resolve(),
        coordinator_path=args.coordinator_path.resolve(),
        live_profiles=live_profiles,
    )
    write_reports(payload=payload, runtime_dir=args.runtime_dir.resolve())
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["overall_state"] in {"OK", "EXTERNAL_DEGRADED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
