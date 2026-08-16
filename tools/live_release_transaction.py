#!/opt/polymarket-live/venv/bin/python
"""Fail-closed single-transaction release for the three live-copy profiles.

This controller intentionally uses only the Python standard library.  The
candidate application is never imported into this process before the durable
start boundary; offline candidate work runs in a separate OS sandbox against
staging database copies.
"""

from __future__ import annotations

import base64
import ctypes
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import IntEnum
import fcntl
import grp
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import select
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote


TRUSTED_PATH = "/usr/sbin:/usr/bin:/sbin:/bin"
# Heuristic I/O/scheduler values only; neither participates in a release PASS
# decision. Production deadlines come from systemd's configured timeout.
HASH_READ_CHUNK_BYTES = 1024 * 1024
SCHEDULER_YIELD_SECONDS = 0.05
LOCAL_TEST_MANAGER_TIMEOUT_USEC = 1_000_000
OFFLINE_AUDIT_GUARD = r"""
import sys
_OFFLINE_NETWORK_EVENTS = {
    'socket.bind',
    'socket.connect',
    'socket.getaddrinfo',
    'socket.gethostbyaddr',
    'socket.gethostbyname',
    'socket.gethostbyname_ex',
    'socket.sendto',
}
_OFFLINE_PROCESS_EVENTS = {'os.posix_spawn', 'os.system', 'subprocess.Popen'}
def _offline_audit(event, _args):
    if event in _OFFLINE_NETWORK_EVENTS:
        raise RuntimeError('offline candidate network access denied:' + event)
    if event in _OFFLINE_PROCESS_EVENTS:
        raise RuntimeError('offline candidate process access denied:' + event)
sys.addaudithook(_offline_audit)
"""
PROFILE_KEYS = (
    "cd90",
    "zockdo_full_wallet",
    "wallet_9506_full_wallet",
)
PRIMARY_UNITS = (
    "com.luke.polymarket.cd90-live.service",
    "com.luke.polymarket.zockdo-live.service",
    "com.luke.polymarket.wallet-9506-live.service",
)
STANDBY_UNITS = (
    "com.luke.polymarket.cd90-live-hot-standby.service",
    "com.luke.polymarket.zockdo-live-hot-standby.service",
    "com.luke.polymarket.wallet-9506-live-hot-standby.service",
)
EXECUTOR_UNITS = tuple(
    unit for pair in zip(PRIMARY_UNITS, STANDBY_UNITS, strict=True) for unit in pair
)
HEALTH_UNIT = "com.luke.polymarket.live-health.service"
HEALTH_TIMER = "com.luke.polymarket.live-health.timer"
ALL_STOP_UNITS = (*EXECUTOR_UNITS, HEALTH_TIMER, HEALTH_UNIT)


@dataclass(frozen=True)
class ProfileSpec:
    key: str
    runtime_name: str
    env_name: str
    primary_unit: str
    standby_unit: str
    app_script: str
    change_suffix: str


PROFILE_SPECS = (
    ProfileSpec(
        "cd90",
        "cd90_live",
        "cd90-live.env",
        PRIMARY_UNITS[0],
        STANDBY_UNITS[0],
        "cd90_live_copy.py",
        "cd90",
    ),
    ProfileSpec(
        "zockdo_full_wallet",
        "zockdo_live",
        "zockdo-live.env",
        PRIMARY_UNITS[1],
        STANDBY_UNITS[1],
        "zockdo_live_copy.py",
        "zockdo",
    ),
    ProfileSpec(
        "wallet_9506_full_wallet",
        "wallet_9506_live",
        "wallet-9506-live.env",
        PRIMARY_UNITS[2],
        STANDBY_UNITS[2],
        "wallet9506_live_copy.py",
        "wallet9506",
    ),
)
COORDINATOR_RELATIVE = Path("shared_wallet/coordinator.sqlite3")
WALLET_LOCK_RELATIVE = Path("authenticated-wallet.lock")
HEALTH_BRIDGE = "polymarket-server-health"
REQUIRED_ASSETS = frozenset(
    {
        "CANDIDATE_TEST_RECEIPT.json",
        "app/cd90_live_copy.py",
        "app/cd90_live_sizing.py",
        "app/live_action_fidelity.py",
        "app/live_copy_profiles.py",
        "app/live_wallet_coordinator.py",
        "app/repair_window_recovery.py",
        "app/server_health_heartbeat.py",
        "ops/polymarket-deadman-alerter.py",
        "app/live_chain_client.py",
        "app/zockdo_live_copy.py",
        "app/wallet9506_live_copy.py",
        *(f"systemd/{unit}" for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER)),
        "systemd/com.luke.polymarket.deadman-alerter.service",
        "systemd/com.luke.polymarket.deadman-alerter.timer",
        f"systemd/{HEALTH_BRIDGE}",
        "tools/assert_no_authenticated_open_orders.py",
        "tools/deploy_three_wallet_core_hotfix_release.sh",
        "tools/live_release_transaction.py",
        "tools/verify_repair_version_authority.py",
        "tests/test_cd90_live_copy.py",
        "tests/test_zockdo_live_copy.py",
        "tests/test_wallet9506_live_copy.py",
        "tests/test_execution_latency.py",
        "tests/test_bounded_retry_health.py",
        "tests/test_bounded_retry_release.py",
        "tests/test_deadman_alerter.py",
    }
)

ATTEMPT_TERMINALS = frozenset(
    {"FILLED", "PARTIAL_FILLED", "NO_FILL", "REJECTED_INTERNAL"}
)
LOCAL_REDEMPTION_TERMINALS = frozenset(
    {
        "REDEEMED",
        "REDEEMED_EXTERNAL_VERIFIED",
        "REDEEMED_OFFICIAL_PAYOUT_CORRECTED",
        "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED",
        "REDEEMED_OFFICIAL_ACTIVITY_VERIFIED",
        "REDEEMED_SHARED_WALLET",
        "REDEEMED_SHARED_PLATFORM_SETTLEMENT",
        "LOSS_RESOLVED_NO_PAYOUT",
        "LOSS_RESOLVED_OFFICIAL_ACTIVITY_CORRECTED",
        "LOSS_RESOLVED_SHARED_WALLET",
    }
)
LOCAL_REDEMPTION_PRE_SUBMIT = frozenset(
    {"NOT_SUBMITTED_RETRYABLE"}
)
LOCAL_REDEMPTION_NO_SUBMISSION_BLOCKS = frozenset(
    {"BLOCK_PRE_SUBMISSION_REVALIDATION", "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY"}
)
COORDINATOR_TERMINALS = frozenset(
    {
        "REDEEMED",
        "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED",
        "LOSS_RESOLVED_NO_PAYOUT",
    }
)
COORDINATOR_PRE_SUBMIT = frozenset({"READY", "NOT_SUBMITTED_RETRYABLE"})
REPAIR_ACTION_TERMINALS = frozenset(
    {
        "IMPORTED_MANUAL_TERMINAL",
        "NO_ORDER_COVERED_BY_CUMULATIVE_SURPLUS",
        "FILLED",
        "PARTIAL_TERMINAL_PREFIX_PRESERVED",
        "SUPERSEDED_BY_LATER_SOURCE_ACTION",
        "EXTERNAL_UNFILLABLE",
    }
)
REPAIR_ACTION_SAFE_PENDING = frozenset(
    {
        "AUTHORIZED",
        "PENDING_PRICE",
        "PENDING_EXTERNAL_LIQUIDITY",
        "PARTIAL_PENDING",
    }
)
REPAIR_TRANSITION_ALLOWLIST = frozenset(
    {
        *REPAIR_ACTION_TERMINALS,
        *REPAIR_ACTION_SAFE_PENDING,
        "CURRENT_EFFECT_RECONSTRUCTED",
        "SUBMITTED_UNRECONCILED",
        "UNKNOWN_SUBMISSION",
    }
)
SUCCESS_OUTCOMES = frozenset(
    {"SUCCESS", "SUCCESS_REDEMPTION_MAINTENANCE_PENDING"}
)
HEALTH_EXTERNAL_RETRY_OUTCOMES = frozenset(
    {"EXTERNAL_HEAD_RETRY_PENDING", "EXTERNAL_WS_RECONNECTING"}
)
EXTERNAL_RUNTIME_CATEGORIES = frozenset(
    {
        "EXTERNAL_ACCOUNT_CASH_RECONCILIATION",
        "EXTERNAL_AUTHENTICATED_COLLATERAL",
        "EXTERNAL_CHAIN_GAP_SCAN",
        "EXTERNAL_HEAD_INCIDENT_RECOVERED",
        "EXTERNAL_HEAD_INCIDENT_STARTED",
        "EXTERNAL_OFFICIAL_REDEEM_EVIDENCE",
        "EXTERNAL_OFFICIAL_REDEEM_PAYOUT_MISMATCH",
        "EXTERNAL_OFFICIAL_REDEEM_PAYOUT_PROOF",
        "EXTERNAL_OFFICIAL_REDEEM_TRANSACTION_IDENTITY",
        "EXTERNAL_OFFICIAL_REDEMPTION_ACTIVITY",
        "EXTERNAL_OFFICIAL_ASSOCIATED_TRADE_RECONCILIATION",
        "EXTERNAL_ONCHAIN_FILL_RECONCILIATION",
        "EXTERNAL_ONCHAIN_ORDER_HASH_RECONCILIATION",
        "EXTERNAL_ORDER_RECONCILIATION",
        "EXTERNAL_PLATFORM_SETTLEMENT_EVIDENCE",
        "EXTERNAL_REDEMPTION_BALANCE_READ",
        "EXTERNAL_REDEMPTION_CONFIRMATION_READ",
        "EXTERNAL_REDEMPTION_CYCLE",
        "EXTERNAL_REDEMPTION_MAINTENANCE",
        "EXTERNAL_REDEMPTION_PAYOUT_PROOF",
        "EXTERNAL_REDEMPTION_RESOLUTION",
        "EXTERNAL_REDEMPTION_STATUS",
        "EXTERNAL_WS_STREAM",
    }
)
HEALTH_STATUS_EXTERNAL_SUFFIXES = frozenset(
    {
        "UNPRICED_GAP_ACTIONS",
        "PENDING_ACTION_TARGETS",
        "EXTERNAL_OR_CAUSAL_UNFILLED",
        "PENDING_ACTION_METADATA",
        "ACTIVE_EXTERNAL_HEAD_INCIDENTS",
    }
)
ACTION_TRANSITION_ALLOWLIST = frozenset(
    {
        "OBSERVED",
        "PLANNED",
        "SCOPE_ELIGIBLE",
        "FILLED",
        "PARTIAL",
        "SKIPPED",
        "EXTERNAL_UNFILLABLE",
        "SUPERSEDED_UNFILLED",
        "PENDING_CAUSAL_ORDER",
        "PENDING_EXTERNAL_RETRY",
        "PENDING_LIQUIDITY",
        "PENDING_CONFIRMED_ZERO_FILL",
        "PENDING_PRICE_PROTECTION",
        "EXPIRED_RETRY_WINDOW",
        "PENDING_METADATA",
        "PENDING_CAPITAL",
        "PENDING_MINIMUM_UNWIND",
        "PENDING_MINIMUM_REMAINDER",
        "PARTIAL_PENDING",
    }
)
MIGRATION_RUNTIME_KEYS = frozenset(
    {
        "operator_planned_resume_from_block",
        "operator_planned_resume_change_id",
        "operator_planned_resume_reason",
        "operator_planned_resume_started_at_ms",
        "operator_planned_resume_state",
        "forward_only_release_boundary_block",
        "pre_release_unplanned_observed_closed_count",
        "legacy_counterparty_order_receipt_count",
        "operator_pre_repair_forward_recovery_armed",
        "operator_pre_repair_forward_recovery_change_id",
        "operator_pre_repair_forward_recovery_reason",
    }
)
MIGRATION_CONFIG_KEYS = frozenset(
    {
        "shared_wallet_migration_receipt_hash",
        "bounded_retry_policy_id",
        "bounded_retry_effective_after_block",
        "bounded_retry_activation_receipt_hash",
        "bounded_retry_activation_receipt_json",
        "liquidity_retry_policy_id",
        "liquidity_retry_effective_after_block",
        "liquidity_retry_activation_receipt_hash",
        "liquidity_retry_activation_receipt_json",
    }
)
MIGRATION_CONFIG_RECEIPT_KEYS = frozenset(
    {*MIGRATION_CONFIG_KEYS, "operator_planned_resume"}
)


class ContractViolation(RuntimeError):
    """A fail-closed release contract was not proven."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class CommandFailure(ContractViolation):
    def __init__(self, argv: Sequence[str], result: CommandResult):
        super().__init__(
            f"command failed:{result.returncode}:{' '.join(argv)}:{result.stderr.strip()}"
        )
        self.argv = tuple(argv)
        self.result = result


class Runner:
    """Small argv-only subprocess boundary, replaceable in tests."""

    def run(
        self,
        argv: tuple[str, ...] | list[str],
        *,
        input_text: str | None = None,
        check: bool = True,
        timeout_seconds: float | None = None,
        parent_death_signal: int | None = None,
    ) -> CommandResult:
        command = [str(item) for item in argv]
        environment = {
            "PATH": TRUSTED_PATH,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        if timeout_seconds is None and parent_death_signal is None:
            completed = subprocess.run(
                command,
                input=input_text,
                text=True,
                capture_output=True,
                check=False,
                env=environment,
            )
            result = CommandResult(
                completed.returncode, completed.stdout, completed.stderr
            )
        else:
            preexec_fn = None
            if parent_death_signal is not None:
                expected_parent_pid = os.getpid()
                libc = ctypes.CDLL(None, use_errno=True)
                prctl = libc.prctl
                prctl.restype = ctypes.c_int

                def arm_parent_death_signal() -> None:
                    if prctl(1, int(parent_death_signal), 0, 0, 0) != 0:
                        os._exit(126)
                    if os.getppid() != expected_parent_pid:
                        os.kill(os.getpid(), int(parent_death_signal))

                preexec_fn = arm_parent_death_signal
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
                start_new_session=True,
                preexec_fn=preexec_fn,
            )
            try:
                stdout, stderr = process.communicate(
                    input=input_text, timeout=timeout_seconds
                )
            except subprocess.TimeoutExpired as exc:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
                raise ContractViolation(
                    f"command timed out:{' '.join(command)}"
                ) from exc
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except OSError:
                    pass
                try:
                    process.wait()
                except OSError:
                    pass
                raise
            result = CommandResult(process.returncode, stdout, stderr)
        if check and result.returncode:
            raise CommandFailure(tuple(command), result)
        return result


def _sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(HASH_READ_CHUNK_BYTES):
            digest.update(block)
    return digest.hexdigest()


def fsync_directory(path: Path) -> None:
    descriptor = os.open(Path(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_regular_file_and_parent(path: Path) -> None:
    path = Path(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise ContractViolation(f"durable regular file required:{path}")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def _validate_sha256(value: str, *, label: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ContractViolation(f"invalid {label}")
    return value


def _manifest_records(manifest: Path) -> list[tuple[str, str, bytes]]:
    records: list[tuple[str, str, bytes]] = []
    seen: set[str] = set()
    for raw in manifest.read_bytes().splitlines(keepends=True):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ContractViolation("manifest is not UTF-8") from exc
        fields = text.split(maxsplit=1)
        if len(fields) != 2:
            raise ContractViolation("invalid manifest record")
        digest = _validate_sha256(fields[0], label="manifest record digest")
        relative = fields[1].strip().lstrip("*")
        if relative.startswith("./"):
            relative = relative[2:]
        pure = PurePosixPath(relative)
        if (
            not relative
            or pure.is_absolute()
            or "\\" in relative
            or not pure.parts
            or any(part in {"", ".", ".."} for part in pure.parts)
            or relative == "MANIFEST.sha256"
        ):
            raise ContractViolation("invalid manifest path")
        if relative in seen:
            raise ContractViolation("duplicate manifest path")
        seen.add(relative)
        records.append((digest, relative, raw))
    return records


def manifest_payload_digest(manifest: Path) -> str:
    digest = hashlib.sha256()
    for _, relative, raw in _manifest_records(manifest):
        if relative != "CANDIDATE_TEST_RECEIPT.json":
            digest.update(raw)
    return digest.hexdigest()


def verify_manifest(
    release_dir: Path, expected_digest: str, required_assets: Iterable[str]
) -> str:
    release_dir = Path(release_dir)
    manifest = release_dir / "MANIFEST.sha256"
    if not manifest.is_file() or manifest.is_symlink():
        raise ContractViolation("candidate manifest is not a regular file")
    if manifest.stat(follow_symlinks=False).st_nlink != 1:
        raise ContractViolation("candidate manifest hardlink forbidden")
    actual_manifest_digest = sha256_file(manifest)
    if actual_manifest_digest != _validate_sha256(
        expected_digest, label="caller manifest digest"
    ):
        raise ContractViolation("caller manifest digest mismatch")
    records = _manifest_records(manifest)
    listed = {relative for _, relative, _ in records}
    required = set(required_assets)
    missing_required = required - listed
    if missing_required:
        raise ContractViolation(f"required manifest assets missing:{sorted(missing_required)}")
    actual: set[str] = set()
    for path in release_dir.rglob("*"):
        mode = path.lstat().st_mode
        relative = path.relative_to(release_dir).as_posix()
        if stat.S_ISLNK(mode):
            raise ContractViolation(f"candidate symlink forbidden:{relative}")
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ContractViolation(f"candidate non-regular asset:{relative}")
        if path.stat(follow_symlinks=False).st_nlink != 1:
            raise ContractViolation(f"candidate hardlink forbidden:{relative}")
        if relative != "MANIFEST.sha256":
            actual.add(relative)
    if actual != listed:
        raise ContractViolation(
            "candidate manifest is not a closed inventory:"
            f"missing={sorted(listed - actual)}:extra={sorted(actual - listed)}"
        )
    bytecode = [
        path
        for path in actual
        if path.endswith(".pyc") or "__pycache__" in PurePosixPath(path).parts
    ]
    if bytecode:
        raise ContractViolation(f"candidate bytecode forbidden:{bytecode}")
    extra_listed = listed - required
    if extra_listed:
        raise ContractViolation(
            f"manifest asset is outside exact allowlist:{sorted(extra_listed)}"
        )
    expected_by_path = {relative: digest for digest, relative, _ in records}
    for relative, expected in expected_by_path.items():
        if sha256_file(release_dir / relative) != expected:
            raise ContractViolation(f"manifest asset digest mismatch:{relative}")
    return actual_manifest_digest


def verify_candidate_test_receipt(release_dir: Path) -> dict[str, Any]:
    release_dir = Path(release_dir)
    path = release_dir / "CANDIDATE_TEST_RECEIPT.json"
    if not path.is_file() or path.is_symlink():
        raise ContractViolation("candidate test receipt is not regular")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractViolation("candidate test receipt is invalid") from exc
    for key in (
        "test_exit_code",
        "ledger_copy_rehearsal_exit_code",
        "real_order_submissions",
    ):
        if payload.get(key) != 0:
            raise ContractViolation(f"candidate test receipt failed:{key}")
    if re.fullmatch(r"[0-9a-f]{40,64}", str(payload.get("candidate_commit") or "")) is None:
        raise ContractViolation("candidate test receipt commit is invalid")
    expected_payload = manifest_payload_digest(release_dir / "MANIFEST.sha256")
    if payload.get("tested_manifest_payload_sha256") != expected_payload:
        raise ContractViolation("candidate test receipt manifest payload mismatch")
    return payload


def _regular_database(path: Path) -> Path:
    path = Path(path)
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError as exc:
        raise ContractViolation(f"regular database required:{path}") from exc
    if not stat.S_ISREG(mode) or path.is_symlink() or path.resolve() != path.absolute():
        raise ContractViolation(f"regular database required:{path}")
    return path


def _align_sqlite_sidecar_metadata(path: Path) -> None:
    database_status = path.stat(follow_symlinks=False)
    changed = False
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{path}{suffix}")
        try:
            observed = sidecar.lstat()
        except FileNotFoundError:
            continue
        if (
            not stat.S_ISREG(observed.st_mode)
            or sidecar.is_symlink()
            or observed.st_nlink != 1
        ):
            raise ContractViolation(f"unsafe SQLite sidecar:{sidecar}")
        try:
            descriptor = os.open(
                sidecar,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
        except FileNotFoundError:
            # SQLite may remove an empty -wal/-shm after lstat but before
            # open.  There is no metadata left to repair and no live-ledger
            # mutation to reject, so leave the database gate read-only.
            continue
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (
                observed.st_dev,
                observed.st_ino,
            ):
                raise ContractViolation(f"SQLite sidecar changed:{sidecar}")
            if (opened.st_uid, opened.st_gid) != (
                database_status.st_uid,
                database_status.st_gid,
            ):
                os.fchown(
                    descriptor,
                    database_status.st_uid,
                    database_status.st_gid,
                )
                changed = True
            expected_mode = stat.S_IMODE(database_status.st_mode)
            if stat.S_IMODE(opened.st_mode) != expected_mode:
                os.fchmod(descriptor, expected_mode)
                changed = True
            if changed:
                os.fsync(descriptor)
        except OSError as exc:
            raise ContractViolation(
                f"SQLite sidecar metadata repair failed:{sidecar}"
            ) from exc
        finally:
            os.close(descriptor)
    if changed:
        fsync_directory(path.parent)


class _MetadataAlignedReadOnlyConnection(sqlite3.Connection):
    _live_database_path: Path | None = None

    def close(self) -> None:
        database_path = self._live_database_path
        self._live_database_path = None
        try:
            super().close()
        finally:
            if database_path is not None:
                _align_sqlite_sidecar_metadata(database_path)


_ROOT_LIVE_READ_TIMEOUT_USEC = 0


def configure_root_live_read_timeout(timeout_usec: int) -> None:
    global _ROOT_LIVE_READ_TIMEOUT_USEC
    if (
        not isinstance(timeout_usec, int)
        or isinstance(timeout_usec, bool)
        or timeout_usec <= 0
    ):
        raise ContractViolation("root live read timeout is invalid")
    _ROOT_LIVE_READ_TIMEOUT_USEC = timeout_usec


_OWNER_SQLITE_WORKER = r"""
import base64
import json
import pathlib
import sqlite3
import sys
from urllib.parse import quote

path = pathlib.Path(sys.argv[1]).resolve()
timeout_usec = int(sys.argv[2])
timeout_seconds = timeout_usec / 1_000_000
uri = "file:" + quote(str(path), safe="/") + "?mode=ro"

def encode(value):
    if isinstance(value, bytes):
        return {"__sqlite_bytes__": base64.b64encode(value).decode("ascii")}
    return value

try:
    connection = sqlite3.connect(uri, uri=True, timeout=timeout_seconds)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=" + str((timeout_usec + 999) // 1000))
    connection.execute("PRAGMA schema_version").fetchone()
except BaseException as exc:
    print(json.dumps({"ok": False, "error_type": type(exc).__name__, "error": str(exc)}), flush=True)
    raise SystemExit(1)

for line in sys.stdin:
    try:
        request = json.loads(line)
        if request.get("close") is True:
            connection.close()
            print(json.dumps({"ok": True, "closed": True}), flush=True)
            raise SystemExit(0)
        cursor = connection.execute(
            str(request["sql"]), tuple(request.get("parameters") or ())
        )
        columns = [str(item[0]) for item in (cursor.description or ())]
        rows = [[encode(value) for value in row] for row in cursor.fetchall()]
        print(json.dumps({"ok": True, "columns": columns, "rows": rows}, separators=(",", ":")), flush=True)
    except SystemExit:
        raise
    except BaseException as exc:
        print(json.dumps({"ok": False, "error_type": type(exc).__name__, "error": str(exc)}, separators=(",", ":")), flush=True)
"""


class _OwnerIsolatedRow:
    def __init__(self, columns: Sequence[str], values: Sequence[Any]):
        self._columns = tuple(columns)
        self._values = tuple(values)
        self._index = {name: index for index, name in enumerate(self._columns)}

    def __getitem__(self, key: int | str | slice) -> Any:
        if isinstance(key, str):
            return self._values[self._index[key]]
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._columns)


class _OwnerIsolatedCursor:
    def __init__(self, columns: Sequence[str], rows: Sequence[Sequence[Any]]):
        self.description = tuple((column, None, None, None, None, None, None) for column in columns)
        self._rows = [
            _OwnerIsolatedRow(columns, row)
            for row in rows
        ]
        self._offset = 0

    def fetchone(self) -> _OwnerIsolatedRow | None:
        if self._offset >= len(self._rows):
            return None
        row = self._rows[self._offset]
        self._offset += 1
        return row

    def fetchall(self) -> list[_OwnerIsolatedRow]:
        rows = self._rows[self._offset :]
        self._offset = len(self._rows)
        return rows

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row


class _OwnerIsolatedReadOnlyConnection:
    row_factory: Any = None

    def __init__(self, path: Path, uid: int, gid: int, timeout_usec: int):
        self._path = Path(path)
        self._timeout_seconds = timeout_usec / 1_000_000
        self._closed = False
        environment = {
            "PATH": TRUSTED_PATH,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONUTF8": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        try:
            self._process = subprocess.Popen(
                (
                    "/usr/bin/setpriv",
                    f"--reuid={uid}",
                    f"--regid={gid}",
                    "--clear-groups",
                    "--no-new-privs",
                    sys.executable,
                    "-I",
                    "-u",
                    "-c",
                    _OWNER_SQLITE_WORKER,
                    str(self._path),
                    str(timeout_usec),
                ),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
                start_new_session=True,
            )
        except OSError as exc:
            raise ContractViolation(
                f"owner-isolated database reader failed to start:{self._path}"
            ) from exc
        if self._process.stdin is None or self._process.stdout is None:
            self._terminate()
            raise ContractViolation(
                f"owner-isolated database reader pipes missing:{self._path}"
            )

    @staticmethod
    def _decode(value: Any) -> Any:
        if isinstance(value, Mapping) and set(value) == {"__sqlite_bytes__"}:
            return base64.b64decode(str(value["__sqlite_bytes__"]), validate=True)
        return value

    def _terminate(self) -> None:
        process = getattr(self, "_process", None)
        if process is None or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            process.wait(timeout=self._timeout_seconds)
        except (OSError, subprocess.TimeoutExpired):
            pass

    def _request(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if self._closed:
            raise ContractViolation(
                f"owner-isolated database reader is closed:{self._path}"
            )
        assert self._process.stdin is not None
        assert self._process.stdout is not None
        try:
            self._process.stdin.write(
                json.dumps(dict(payload), separators=(",", ":")) + "\n"
            )
            self._process.stdin.flush()
            ready, _, _ = select.select(
                [self._process.stdout], [], [], self._timeout_seconds
            )
            if not ready:
                self._terminate()
                raise ContractViolation(
                    f"owner-isolated database reader timed out:{self._path}"
                )
            line = self._process.stdout.readline()
        except (BrokenPipeError, OSError) as exc:
            self._terminate()
            raise ContractViolation(
                f"owner-isolated database reader transport failed:{self._path}"
            ) from exc
        if not line:
            stderr = ""
            if self._process.stderr is not None:
                stderr = self._process.stderr.read().strip()
            self._terminate()
            raise ContractViolation(
                f"owner-isolated database reader exited:{self._path}:{stderr}"
            )
        try:
            response = json.loads(line)
        except json.JSONDecodeError as exc:
            self._terminate()
            raise ContractViolation(
                f"owner-isolated database reader response invalid:{self._path}"
            ) from exc
        if not isinstance(response, Mapping) or response.get("ok") is not True:
            raise ContractViolation(
                "owner-isolated database read failed:"
                f"{self._path}:{response.get('error_type')}:{response.get('error')}"
            )
        return response

    def execute(
        self, sql: str, parameters: Sequence[Any] = ()
    ) -> _OwnerIsolatedCursor:
        response = self._request(
            {"sql": str(sql), "parameters": list(parameters)}
        )
        columns = response.get("columns") or []
        rows = response.get("rows") or []
        if not isinstance(columns, list) or not isinstance(rows, list):
            raise ContractViolation(
                f"owner-isolated database row payload invalid:{self._path}"
            )
        decoded = [
            [self._decode(value) for value in row]
            for row in rows
            if isinstance(row, list)
        ]
        if len(decoded) != len(rows):
            raise ContractViolation(
                f"owner-isolated database row payload invalid:{self._path}"
            )
        return _OwnerIsolatedCursor([str(item) for item in columns], decoded)

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._request({"close": True})
            self._closed = True
            if self._process.stdin is not None:
                self._process.stdin.close()
            self._process.wait(timeout=self._timeout_seconds)
            if self._process.returncode != 0:
                raise ContractViolation(
                    f"owner-isolated database reader close failed:{self._path}"
                )
        except BaseException:
            self._closed = True
            self._terminate()
            raise
        finally:
            _align_sqlite_sidecar_metadata(self._path)


def _ro_connection(path: Path) -> Any:
    path = _regular_database(path)
    uri = f"file:{quote(str(path), safe='/')}?mode=ro"
    connection: Any | None = None
    try:
        database_status = path.stat(follow_symlinks=False)
        if os.geteuid() == 0 and database_status.st_uid != 0:
            if _ROOT_LIVE_READ_TIMEOUT_USEC <= 0:
                raise ContractViolation(
                    f"root live read timeout is not configured:{path}"
                )
            _align_sqlite_sidecar_metadata(path)
            connection = _OwnerIsolatedReadOnlyConnection(
                path,
                database_status.st_uid,
                database_status.st_gid,
                _ROOT_LIVE_READ_TIMEOUT_USEC,
            )
        else:
            connection = sqlite3.connect(
                uri,
                uri=True,
                factory=_MetadataAlignedReadOnlyConnection,
            )
            connection._live_database_path = path
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA schema_version").fetchone()
        if not isinstance(connection, _OwnerIsolatedReadOnlyConnection):
            _align_sqlite_sidecar_metadata(path)
        return connection
    except sqlite3.Error as exc:
        if connection is not None:
            connection.close()
        raise ContractViolation(f"read-only database open failed:{path}") from exc
    except Exception:
        if connection is not None:
            connection.close()
        raise


def sqlite_scalar_ro(path: Path, query: str, parameters: Sequence[Any] = ()) -> Any:
    connection = _ro_connection(path)
    try:
        row = connection.execute(query, tuple(parameters)).fetchone()
    except sqlite3.Error as exc:
        raise ContractViolation(f"read-only database query failed:{path}") from exc
    finally:
        connection.close()
    if row is None:
        return None
    return row[0]


def _verify_integrity(
    connection: sqlite3.Connection, *, label: str, full: bool = True
) -> None:
    if full:
        integrity = [
            str(row[0]) for row in connection.execute("PRAGMA integrity_check")
        ]
        if integrity != ["ok"]:
            raise ContractViolation(f"{label} integrity failed:{integrity}")
    foreign_keys = list(connection.execute("PRAGMA foreign_key_check"))
    if foreign_keys:
        raise ContractViolation(f"{label} foreign key check failed")


_APPEND_ONLY_ID_TABLES = frozenset(
    {
        "runtime_errors",
        "action_transitions",
        "repair_recovery_transitions",
        "redemption_transitions",
        "shared_condition_transitions",
    }
)

_LOCAL_APPEND_ONLY_BASELINES = (
    ("runtime_errors", "runtime_error_id", "runtime_error_prefix_sha256"),
    (
        "action_transitions",
        "action_transition_id",
        "action_transition_prefix_sha256",
    ),
    (
        "repair_recovery_transitions",
        "repair_transition_id",
        "repair_transition_prefix_sha256",
    ),
    (
        "redemption_transitions",
        "redemption_transition_id",
        "redemption_transition_prefix_sha256",
    ),
)


def verify_append_only_watermark(
    connection: sqlite3.Connection, *, table: str, baseline: int, label: str
) -> int:
    if table not in _APPEND_ONLY_ID_TABLES:
        raise ContractViolation(f"append-only table is not approved:{table}")
    try:
        expected = int(baseline)
        current = int(
            connection.execute(
                f"SELECT COALESCE(MAX(id),0) FROM {table}"
            ).fetchone()[0]
        )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        raise ContractViolation(f"append-only watermark invalid:{label}") from exc
    if expected < 0 or current < expected:
        raise ContractViolation(f"append-only watermark regressed:{label}")
    return current


def _sqlite_digest_value(value: Any) -> list[str]:
    if value is None:
        return ["null", ""]
    if isinstance(value, bytes):
        return ["blob", value.hex()]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, float):
        return ["real", repr(value)]
    return ["text", str(value)]


def append_only_prefix_digest(
    connection: sqlite3.Connection, *, table: str, baseline: int, label: str
) -> str:
    """Hash the complete immutable row prefix through an already-frozen id."""
    maximum = verify_append_only_watermark(
        connection, table=table, baseline=baseline, label=label
    )
    del maximum
    try:
        columns = tuple(
            str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
        )
        if not columns or "id" not in columns:
            raise ContractViolation(f"append-only prefix schema invalid:{label}")
        rows = connection.execute(
            f"SELECT {','.join(columns)} FROM {table} WHERE id<=? ORDER BY id",
            (int(baseline),),
        ).fetchall()
    except sqlite3.Error as exc:
        raise ContractViolation(f"append-only prefix query failed:{label}") from exc
    payload = {
        "table": table,
        "baseline": int(baseline),
        "columns": columns,
        "rows": [
            [_sqlite_digest_value(value) for value in tuple(row)] for row in rows
        ],
    }
    return _sha256_bytes(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    )


_REDEMPTION_RECEIPT_TABLES = frozenset(
    {"redemption_receipts", "shared_condition_redemptions"}
)
_REDEMPTION_CONDITION_TABLES = frozenset(
    {
        "redemption_receipts",
        "shared_condition_redemptions",
        "shared_condition_allocations",
    }
)


def redemption_receipt_condition_ids(
    connection: sqlite3.Connection, *, table: str, label: str
) -> tuple[str, ...]:
    if table not in _REDEMPTION_RECEIPT_TABLES:
        raise ContractViolation(f"redemption receipt table is not approved:{table}")
    try:
        rows = connection.execute(
            f"SELECT lower(condition_id) FROM {table} ORDER BY lower(condition_id)"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ContractViolation(f"redemption receipt prefix query failed:{label}") from exc
    values = tuple(str(row[0] or "").strip().lower() for row in rows)
    if any(not value for value in values) or len(set(values)) != len(values):
        raise ContractViolation(f"redemption receipt condition invalid:{label}")
    return values


def redemption_condition_table_snapshot(
    connection: sqlite3.Connection, *, table: str, label: str
) -> dict[str, Any]:
    if table not in _REDEMPTION_CONDITION_TABLES:
        raise ContractViolation(f"redemption condition table is not approved:{table}")
    try:
        columns = tuple(
            str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
        )
        if "condition_id" not in columns:
            raise ContractViolation(f"redemption condition schema invalid:{label}")
        condition_index = columns.index("condition_id")
        rows = connection.execute(
            f"SELECT {','.join(columns)} FROM {table}"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ContractViolation(f"redemption condition snapshot failed:{label}") from exc
    grouped: dict[str, list[list[list[str]]]] = {}
    for row in rows:
        condition_id = str(row[condition_index] or "").strip().lower()
        if not condition_id:
            raise ContractViolation(f"redemption condition identity invalid:{label}")
        encoded = [_sqlite_digest_value(value) for value in tuple(row)]
        grouped.setdefault(condition_id, []).append(encoded)
    for values in grouped.values():
        values.sort(
            key=lambda value: json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            )
        )
    return {
        "table": table,
        "columns": list(columns),
        "rows": {key: grouped[key] for key in sorted(grouped)},
    }


def _validated_condition_snapshot(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractViolation(f"redemption condition snapshot invalid:{label}")
    table = str(value.get("table") or "")
    columns = value.get("columns")
    rows = value.get("rows")
    if (
        table not in _REDEMPTION_CONDITION_TABLES
        or not isinstance(columns, list)
        or "condition_id" not in columns
        or not all(isinstance(item, str) and item for item in columns)
        or not isinstance(rows, Mapping)
    ):
        raise ContractViolation(f"redemption condition snapshot invalid:{label}")
    normalized_rows: dict[str, Any] = {}
    for raw_condition, raw_values in rows.items():
        condition = str(raw_condition).strip().lower()
        if (
            not condition
            or condition != raw_condition
            or not isinstance(raw_values, list)
            or condition in normalized_rows
        ):
            raise ContractViolation(f"redemption condition snapshot invalid:{label}")
        normalized_rows[condition] = raw_values
    return {
        "table": table,
        "columns": list(columns),
        "rows": {key: normalized_rows[key] for key in sorted(normalized_rows)},
    }


def verify_condition_snapshot_progression(
    connection: sqlite3.Connection,
    *,
    expected: Mapping[str, Any],
    current: Mapping[str, Any],
    transition_table: str,
    transition_baseline: int,
    receipt_table: str,
    label: str,
) -> None:
    expected_snapshot = _validated_condition_snapshot(expected, label=label)
    current_snapshot = _validated_condition_snapshot(current, label=label)
    if (
        expected_snapshot["table"] != current_snapshot["table"]
        or expected_snapshot["columns"] != current_snapshot["columns"]
    ):
        raise ContractViolation(f"redemption condition snapshot schema changed:{label}")
    expected_rows = expected_snapshot["rows"]
    current_rows = current_snapshot["rows"]
    if not set(expected_rows).issubset(current_rows):
        raise ContractViolation(f"redemption receipt condition regressed:{label}")
    changed = {
        condition
        for condition in set(expected_rows) | set(current_rows)
        if expected_rows.get(condition) != current_rows.get(condition)
    }
    try:
        post_baseline_conditions = {
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT lower(condition_id) FROM {transition_table} "
                "WHERE id>?",
                (int(transition_baseline),),
            )
        }
    except sqlite3.Error as exc:
        raise ContractViolation(
            f"redemption condition progression query failed:{label}"
        ) from exc
    if not changed.issubset(post_baseline_conditions):
        changed_without_transition = sorted(changed - post_baseline_conditions)
        raise ContractViolation(
            f"redemption receipt changed without transition:{label}:"
            + ",".join(changed_without_transition)
        )
    for condition in sorted(post_baseline_conditions):
        try:
            transition = connection.execute(
                f"SELECT id,state FROM {transition_table} "
                "WHERE lower(condition_id)=? ORDER BY id DESC LIMIT 1",
                (condition,),
            ).fetchone()
            receipt = connection.execute(
                f"SELECT state FROM {receipt_table} "
                "WHERE lower(condition_id)=?",
                (condition,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise ContractViolation(
                f"redemption condition progression query failed:{label}"
            ) from exc
        if (
            transition is None
            or receipt is None
            or str(receipt[0]) != str(transition[1])
        ):
            raise ContractViolation(
                f"redemption transition has stale receipt:{label}:{condition}"
            )


def _verify_repair_storage(connection: sqlite3.Connection) -> None:
    manifests = {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT manifest_hash,state FROM repair_recovery_manifests"
        )
    }
    if sum(state == "ACTIVE" for state in manifests.values()) > 1:
        raise ContractViolation("repair has multiple active manifests")
    actions_by_manifest: dict[str, list[str]] = {key: [] for key in manifests}
    for row in connection.execute(
        "SELECT manifest_hash,state FROM repair_recovery_actions"
    ):
        manifest_hash = str(row[0])
        if manifest_hash not in manifests:
            raise ContractViolation("repair action has no manifest")
        actions_by_manifest[manifest_hash].append(str(row[1]))
    allowed = REPAIR_ACTION_TERMINALS | REPAIR_ACTION_SAFE_PENDING
    for manifest_hash, manifest_state in manifests.items():
        action_states = actions_by_manifest[manifest_hash]
        if not action_states:
            raise ContractViolation("repair manifest has no actions")
        unknown = set(action_states) - allowed
        if unknown:
            raise ContractViolation(f"repair action state is unsafe:{sorted(unknown)}")
        pending = set(action_states) & REPAIR_ACTION_SAFE_PENDING
        if manifest_state == "COMPLETED":
            if pending:
                raise ContractViolation("repair completed manifest has pending actions")
        elif manifest_state == "ACTIVE":
            if not pending:
                raise ContractViolation("repair active manifest has no pending action")
        else:
            raise ContractViolation(f"repair manifest state is unknown:{manifest_state}")


def verify_repair_acceptance_delta(
    connection: sqlite3.Connection,
    *,
    baseline_transition_id: int,
    expected_manifest_hashes: Sequence[str],
) -> int:
    baseline = int(baseline_transition_id)
    if baseline < 0:
        raise ContractViolation("repair transition baseline is negative")
    actual_manifests = tuple(
        str(row[0])
        for row in connection.execute(
            "SELECT manifest_hash FROM repair_recovery_manifests ORDER BY manifest_hash"
        )
    )
    if actual_manifests != tuple(sorted(str(item) for item in expected_manifest_hashes)):
        raise ContractViolation("repair manifest identity changed")
    _verify_repair_storage(connection)
    transition_rows = connection.execute(
        "SELECT id,manifest_hash,action_id,state "
        "FROM repair_recovery_transitions WHERE id>? ORDER BY id",
        (baseline,),
    ).fetchall()
    valid_action_identities = {
        (str(row[0]), str(row[1]))
        for row in connection.execute(
            "SELECT manifest_hash,action_id FROM repair_recovery_actions"
        )
    }
    for row in transition_rows:
        identity = (str(row[1]), str(row[2]))
        if row[2] is None or identity not in valid_action_identities:
            raise ContractViolation("repair transition identity is orphaned")
    unknown = {str(row[3]) for row in transition_rows} - REPAIR_TRANSITION_ALLOWLIST
    if unknown:
        raise ContractViolation(f"repair transition state is unsafe:{sorted(unknown)}")
    maximum = int(
        connection.execute(
            "SELECT COALESCE(MAX(id),0) FROM repair_recovery_transitions"
        ).fetchone()[0]
    )
    if maximum < baseline:
        raise ContractViolation("repair transition history regressed")
    return maximum


def verify_local_storage(
    path: Path, *, cutover: bool, full_integrity: bool = True
) -> None:
    connection = _ro_connection(path)
    try:
        _verify_integrity(connection, label="local", full=full_integrity)
        reservation_flags = [
            row[0] for row in connection.execute("SELECT active FROM order_reservations")
        ]
        if any(type(value) is not int or value != 0 for value in reservation_flags):
            raise ContractViolation("active or invalid order reservation")
        attempt_states = {
            str(row[0]) for row in connection.execute("SELECT state FROM submission_attempts")
        }
        unknown_attempts = attempt_states - ATTEMPT_TERMINALS
        if unknown_attempts:
            raise ContractViolation(f"unsafe submission attempt state:{sorted(unknown_attempts)}")
        for row in connection.execute(
            "SELECT state,expected_payout_usd,transaction_id,transaction_hash "
            "FROM redemption_receipts"
        ):
            state = str(row[0])
            payout = _canonical_decimal_text(row[1], label="local-redemption-payout")
            transaction_id = str(row[2] or "").strip()
            transaction_hash = str(row[3] or "").strip()
            if state.startswith("LOSS_RESOLVED_") and payout != "0":
                raise ContractViolation("local redemption loss payout is nonzero")
            if state in LOCAL_REDEMPTION_TERMINALS:
                continue
            if state in LOCAL_REDEMPTION_PRE_SUBMIT | LOCAL_REDEMPTION_NO_SUBMISSION_BLOCKS:
                if transaction_id or transaction_hash:
                    raise ContractViolation("pre-submit redemption has transaction identity")
                if cutover and state in LOCAL_REDEMPTION_PRE_SUBMIT:
                    raise ContractViolation("safe storage redemption is not cutover ready")
                continue
            raise ContractViolation(f"unknown local redemption state:{state}")
        _verify_repair_storage(connection)
        if connection.execute(
            "SELECT COUNT(*) FROM runtime_state WHERE value LIKE '%CODE_REPAIR_REQUIRED%'"
        ).fetchone()[0]:
            raise ContractViolation("runtime code repair state is present")
        if cutover:
            observed = connection.execute(
                """
                SELECT COUNT(*) FROM action_transitions AS transition
                WHERE transition.id=(
                    SELECT latest.id FROM action_transitions AS latest
                    WHERE latest.action_id=transition.action_id
                    ORDER BY latest.id DESC LIMIT 1
                ) AND transition.status='OBSERVED'
                """
            ).fetchone()[0]
            if observed:
                raise ContractViolation("unplanned observed action is not cutover ready")
    except sqlite3.Error as exc:
        raise ContractViolation(f"local storage contract query failed:{path}") from exc
    finally:
        connection.close()


def _recoverable_legacy_stable_causal_prefix_action_ids(
    connection: sqlite3.Connection,
) -> list[str]:
    empty_prefix_hash = hashlib.sha256(
        json.dumps(
            {"actions": []},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    rows = connection.execute(
        """
        SELECT action.action_id, action.transaction_hash, action.token_id,
               action.side, action.order_hash, action.source_timestamp,
               action.block_number, action.source_log_index,
               target.state AS target_state, target.reason AS target_reason,
               latest.status AS latest_status, latest.reason AS latest_reason,
               latest.details_json AS latest_details_json,
               planned.id AS planned_transition_id,
               planned.created_at_ms AS planned_created_at_ms,
               planned.details_json AS planned_details_json,
               EXISTS(
                   SELECT 1 FROM order_reservations AS reservation
                   WHERE reservation.action_id=action.action_id
                     AND reservation.active<>0
               ) AS active_reservation,
               EXISTS(
                   SELECT 1 FROM submission_attempts AS attempt
                   WHERE attempt.action_id=action.action_id
                     AND attempt.state NOT IN (
                         'FILLED','PARTIAL_FILLED','NO_FILL','REJECTED_INTERNAL'
                     )
               ) AS unresolved_attempt
        FROM action_receipts AS action
        JOIN action_market_metadata AS metadata
          ON metadata.action_id=action.action_id
        JOIN action_targets AS target
          ON target.action_id=action.action_id
        JOIN action_transitions AS latest
          ON latest.id=(
              SELECT candidate.id FROM action_transitions AS candidate
              WHERE candidate.action_id=action.action_id
              ORDER BY candidate.id DESC LIMIT 1
          )
        LEFT JOIN action_transitions AS planned
          ON planned.id=(
              SELECT candidate.id FROM action_transitions AS candidate
              WHERE candidate.action_id=action.action_id
                AND candidate.status='PLANNED'
              ORDER BY candidate.id ASC LIMIT 1
          )
        WHERE metadata.profile_follow=1
          AND LOWER(action.source_role) IN ('maker','verified_public_wallet')
          AND target.state='ERROR_INTERNAL'
          AND target.reason='INTERNAL_STALE_CAUSAL_TARGET'
          AND latest.status='ERROR_INTERNAL'
          AND latest.reason='INTERNAL_STALE_CAUSAL_TARGET'
        """
    ).fetchall()
    recovered: list[str] = []
    for row in rows:
        if bool(row["active_reservation"]) or bool(row["unresolved_attempt"]):
            continue
        try:
            latest = json.loads(str(row["latest_details_json"]))
            planned = json.loads(str(row["planned_details_json"]))
            cumulative = planned["cumulative_sizing"]
            frozen_scaled = Decimal(str(cumulative["prior_scaled_open_target"]))
        except (
            InvalidOperation,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ):
            continue
        if (
            not isinstance(latest, Mapping)
            or not isinstance(cumulative, Mapping)
            or str(cumulative.get("policy") or "")
            != "UPSCALE_TO_CURRENT_MARKET_MINIMUM"
            or not frozen_scaled.is_finite()
            or frozen_scaled < 0
            or cumulative.get("prior_causal_prefix_count") is not None
            or str(cumulative.get("prior_causal_prefix_hash") or "")
            or latest.get("causal_target_provenance_failure")
            != "FROZEN_CAUSAL_PREFIX_COUNT_INVALID"
            or latest.get("frozen_prior_causal_prefix_count") is not None
            or str(latest.get("frozen_prior_causal_prefix_hash") or "")
            or latest.get("new_order_submitted") is not False
        ):
            continue
        prior_rows = connection.execute(
            """
            SELECT prior.action_id, prior.transaction_hash, prior.token_id,
                   prior.side, prior.order_hash, prior.source_timestamp,
                   prior.block_number, prior.source_log_index,
                   target.proportional_quantity, target.target_quantity,
                   target.cumulative_filled_quantity,
                   target.state AS target_state,
                   target.updated_at_ms AS target_updated_at_ms,
                   prior_latest.id AS latest_transition_id,
                   prior_latest.created_at_ms AS latest_created_at_ms,
                   prior_latest.status AS latest_status
            FROM action_receipts AS prior
            LEFT JOIN action_targets AS target
              ON target.action_id=prior.action_id
            LEFT JOIN action_transitions AS prior_latest
              ON prior_latest.id=(
                  SELECT candidate.id FROM action_transitions AS candidate
                  WHERE candidate.action_id=prior.action_id
                  ORDER BY candidate.id DESC LIMIT 1
              )
            WHERE prior.token_id=?
              AND LOWER(prior.source_role) IN ('maker','verified_public_wallet')
              AND (
                  prior.block_number, prior.source_log_index,
                  prior.source_timestamp, prior.transaction_hash,
                  prior.token_id, prior.side, prior.order_hash,
                  prior.action_id
              ) < (?, ?, ?, ?, ?, ?, ?, ?)
            ORDER BY prior.block_number, prior.source_log_index,
                     prior.source_timestamp, prior.transaction_hash,
                     prior.token_id, prior.side, prior.order_hash,
                     prior.action_id
            """,
            (
                str(row["token_id"]),
                int(row["block_number"]),
                int(row["source_log_index"]),
                int(row["source_timestamp"]),
                str(row["transaction_hash"]).lower(),
                str(row["token_id"]),
                str(row["side"]).upper(),
                str(row["order_hash"]).lower(),
                str(row["action_id"]),
            ),
        ).fetchall()
        terminal_states = {
            "SKIPPED",
            "FILLED",
            "PARTIAL",
            "EXTERNAL_UNFILLABLE",
            "SUPERSEDED_UNFILLED",
        }
        entries: list[dict[str, Any]] = []
        current_scaled = Decimal("0")
        stable = True
        for prior in prior_rows:
            if (
                prior["target_updated_at_ms"] is None
                or prior["latest_transition_id"] is None
                or int(prior["target_updated_at_ms"])
                > int(row["planned_created_at_ms"])
                or int(prior["latest_transition_id"])
                >= int(row["planned_transition_id"])
                or int(prior["latest_created_at_ms"])
                > int(row["planned_created_at_ms"])
                or str(prior["target_state"]) not in terminal_states
                or str(prior["latest_status"])
                != str(prior["target_state"])
            ):
                stable = False
                break
            try:
                proportional = Decimal(str(prior["proportional_quantity"]))
                target_quantity = Decimal(str(prior["target_quantity"]))
                cumulative_filled = Decimal(
                    str(prior["cumulative_filled_quantity"])
                )
            except (InvalidOperation, TypeError, ValueError):
                stable = False
                break
            if (
                not proportional.is_finite()
                or proportional <= 0
                or not target_quantity.is_finite()
                or target_quantity <= 0
                or not cumulative_filled.is_finite()
                or cumulative_filled < 0
            ):
                stable = False
                break
            side = str(prior["side"]).upper()
            if side == "BUY":
                current_scaled += proportional
            elif side == "SELL":
                current_scaled = max(current_scaled - proportional, Decimal("0"))
            else:
                stable = False
                break
            entries.append(
                {
                    "action_id": str(prior["action_id"]),
                    "block_number": int(prior["block_number"]),
                    "source_log_index": int(prior["source_log_index"]),
                    "source_timestamp": int(prior["source_timestamp"]),
                    "transaction_hash": str(prior["transaction_hash"]).lower(),
                    "token_id": str(prior["token_id"]),
                    "side": side,
                    "order_hash": str(prior["order_hash"]).lower(),
                    "proportional_quantity": str(proportional),
                    "target_quantity": str(target_quantity),
                    "cumulative_filled_quantity": str(cumulative_filled),
                    "target_state": str(prior["target_state"]),
                    "latest_status": str(prior["latest_status"]),
                }
            )
        current_hash = hashlib.sha256(
            json.dumps(
                {"actions": entries},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        try:
            failure_frozen_scaled = Decimal(
                str(latest.get("frozen_prior_scaled_open_target"))
            )
            failure_current_scaled = Decimal(
                str(latest.get("current_prior_scaled_open_target"))
            )
        except (InvalidOperation, TypeError, ValueError):
            continue
        if (
            stable
            and current_scaled == frozen_scaled
            and failure_frozen_scaled == frozen_scaled
            and failure_current_scaled == current_scaled
            and latest.get("current_prior_causal_prefix_count") == len(entries)
            and str(
                latest.get("current_prior_causal_prefix_hash") or ""
            ).lower()
            == current_hash
            and (entries or current_hash == empty_prefix_hash)
        ):
            recovered.append(str(row["action_id"]))
    return sorted(recovered)


def _pre_stop_candidate_action_evidence(path: Path) -> dict[str, Any]:
    pending_states = {
        "READY",
        "SUBMIT_STARTED",
        "SUBMITTED_UNRECONCILED",
        "UNKNOWN_SUBMISSION",
        "PARTIAL_PENDING",
        "PENDING_LIQUIDITY",
        "PENDING_MINIMUM_UNWIND",
        "PENDING_MINIMUM_REMAINDER",
        "PENDING_CAPITAL",
        "PENDING_EXTERNAL_RETRY",
        "PENDING_CAUSAL_ORDER",
    }


    external_states = {
        "EXTERNAL_UNFILLABLE",
        "SUPERSEDED_UNFILLED",
        "EXPIRED_RETRY_WINDOW",
    }
    internal_states = {
        "ERROR_INTERNAL",
        "ERROR",
        "SKIPPED",
        "PENDING_INTERNAL_INVARIANT",
    }
    fixed_minimum_reasons = {
        "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
        "PROPORTIONAL_BUY_NOTIONAL_BELOW_MARKETABLE_MINIMUM",
        "PRIOR_MINIMUM_UPSCALE_COVERS_PROPORTIONAL_BUY",
    }
    confirmed_zero_fill_reasons = {
        "OFFICIAL_ONCHAIN_ORDER_HASH_NO_FILL",
        "OFFICIAL_CLOB_ZERO_FILL",
        "OFFICIAL_CLOB_NO_MATCH",
    }
    connection = _ro_connection(path)
    try:
        rows = connection.execute(
            """
            SELECT metadata.action_id,
                   target.action_id IS NOT NULL AS has_target,
                   COALESCE(target.state, latest.status, '') AS state,
                   COALESCE(target.reason, latest.reason, '') AS reason,
                   EXISTS(
                       SELECT 1
                       FROM repair_recovery_actions AS recovery
                       JOIN repair_recovery_manifests AS manifest
                         ON manifest.manifest_hash=recovery.manifest_hash
                       WHERE recovery.action_id=metadata.action_id
                         AND manifest.state='ACTIVE'
                         AND recovery.state IN (
                             'AUTHORIZED','PENDING_PRICE',
                             'CURRENT_EFFECT_RECONSTRUCTED',
                             'PENDING_EXTERNAL_LIQUIDITY','PARTIAL_PENDING',
                             'SUBMIT_STARTED','SUBMITTED_UNRECONCILED',
                             'UNKNOWN_SUBMISSION'
                         )
                   ) AS active_repair
            FROM action_market_metadata AS metadata
            JOIN action_receipts AS action
              ON action.action_id=metadata.action_id
            LEFT JOIN action_targets AS target
              ON target.action_id=metadata.action_id
            LEFT JOIN action_transitions AS latest
              ON latest.id=(
                  SELECT candidate.id FROM action_transitions AS candidate
                  WHERE candidate.action_id=metadata.action_id
                  ORDER BY candidate.id DESC LIMIT 1
              )
            WHERE metadata.profile_follow=1
              AND LOWER(action.source_role) IN ('maker','verified_public_wallet')
            """
        ).fetchall()
        latest_counts = {
            str(row[0]): int(row[1])
            for row in connection.execute(
                """
                SELECT transition.status,COUNT(*)
                FROM action_transitions AS transition
                WHERE transition.id=(
                    SELECT latest.id FROM action_transitions AS latest
                    WHERE latest.action_id=transition.action_id
                    ORDER BY latest.id DESC LIMIT 1
                )
                GROUP BY transition.status
                """
            )
        }
        mismatch = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM action_targets AS target
                JOIN action_receipts AS action
                  ON action.action_id=target.action_id
                JOIN action_transitions AS latest
                  ON latest.id=(
                      SELECT candidate.id FROM action_transitions AS candidate
                      WHERE candidate.action_id=target.action_id
                      ORDER BY candidate.id DESC LIMIT 1
                  )
                WHERE LOWER(action.source_role) IN ('maker','verified_public_wallet')
                  AND target.state IN (
                      'PENDING_LIQUIDITY','PARTIAL_PENDING','PENDING_CAPITAL',
                      'PENDING_CONFIRMED_ZERO_FILL','PENDING_PRICE_PROTECTION',
                      'PENDING_MINIMUM_UNWIND','PENDING_MINIMUM_REMAINDER',
                      'PENDING_EXTERNAL_RETRY'
                  )
                  AND latest.status IN (
                      'EXTERNAL_UNFILLABLE','SUPERSEDED_UNFILLED'
                  )
                """
            ).fetchone()[0]
        )
        recoverable_stable_prefix_action_ids = (
            _recoverable_legacy_stable_causal_prefix_action_ids(connection)
            if any(
                bool(row[1])
                and str(row[2]) == "ERROR_INTERNAL"
                and str(row[3]) == "INTERNAL_STALE_CAUSAL_TARGET"
                for row in rows
            )
            else []
        )
    except sqlite3.Error as exc:
        raise ContractViolation("pre-stop candidate action audit failed") from exc
    finally:
        connection.close()
    eligible = len(rows)
    target_count = 0
    active_repair_missing = 0
    accounted = 0
    internal = 0
    legacy_confirmed_zero_fill = 0
    profile_current_internal_latest = 0
    unclassified = 0
    candidate_missing = 0
    recoverable_stable_prefix_actions = set(
        recoverable_stable_prefix_action_ids
    )
    for (
        raw_action_id,
        raw_has_target,
        raw_state,
        raw_reason,
        raw_active_repair,
    ) in rows:
        action_id = str(raw_action_id)
        has_target = bool(raw_has_target)
        state = str(raw_state)
        reason = str(raw_reason)
        if has_target:
            target_count += 1
        elif state == "OBSERVED":
            unclassified += 1
            candidate_missing += 1
            continue
        elif bool(raw_active_repair):
            active_repair_missing += 1
            accounted += 1
            continue
        if action_id in recoverable_stable_prefix_actions:
            accounted += 1
        elif state in {"FILLED", "PARTIAL"} or state in pending_states or state in external_states:
            accounted += 1
        elif state == "SKIPPED" and reason in (
            fixed_minimum_reasons | confirmed_zero_fill_reasons
        ):
            accounted += 1
            if reason in confirmed_zero_fill_reasons:
                legacy_confirmed_zero_fill += 1
        elif state in internal_states:
            accounted += 1
            internal += 1
            if state in {"ERROR_INTERNAL", "PENDING_INTERNAL_INVARIANT"}:
                profile_current_internal_latest += 1
            if not has_target:
                candidate_missing += 1
        else:
            unclassified += 1
            if not has_target:
                candidate_missing += 1
    unsafe_submissions = sum(
        latest_counts.get(state, 0)
        for state in {
            "SUBMIT_STARTED",
            "SUBMITTED_UNRECONCILED",
            "UNKNOWN_SUBMISSION",
        }
    )
    return {
        "profile_eligible_observed": eligible,
        "frozen_target_count": target_count,
        "active_repair_managed_without_target": active_repair_missing,
        "legacy_missing_target": max(
            0, eligible - target_count - active_repair_missing
        ),
        "missing_target": candidate_missing,
        "accounted": accounted,
        "internal_error": internal,
        "legacy_confirmed_zero_fill_count": legacy_confirmed_zero_fill,
        "unclassified_target": unclassified,
        "retryable_target_terminal_transition_mismatch": mismatch,
        "unsafe_submission_action_count": unsafe_submissions,
        "recoverable_legacy_stable_causal_prefix_action_count": len(
            recoverable_stable_prefix_action_ids
        ),
        "recoverable_legacy_stable_causal_prefix_action_ids": (
            recoverable_stable_prefix_action_ids
        ),
        "historical_nonfollowable_internal_count": max(
            0,
            latest_counts.get("ERROR_INTERNAL", 0)
            + latest_counts.get("PENDING_INTERNAL_INVARIANT", 0)
            - profile_current_internal_latest,
        ),
    }


def _pre_stop_bounded_retry_price_improvement_evidence(
    path: Path,
) -> dict[str, int]:
    """Prove that an old health overage is a cash-bounded BUY improvement.

    This compatibility check is deliberately narrower than action
    conservation: only a terminal BUY whose immutable terminal receipt names
    the established price-improvement reason may have filled shares above its
    target.  It neither changes the ledger nor permits a retry.
    """

    reasons = (
        "OFFICIAL_ONCHAIN_BUY_PRICE_IMPROVEMENT_FILL",
        "OFFICIAL_ASSOCIATED_TRADE_BUY_PRICE_IMPROVEMENT_FILL",
    )
    connection = _ro_connection(path)
    try:
        rows = connection.execute(
            """
            SELECT target.target_quantity, target.cumulative_filled_quantity,
                   target.state, target.reason, receipt.side
            FROM action_targets AS target
            JOIN action_receipts AS receipt ON receipt.action_id=target.action_id
            """
        ).fetchall()
    except sqlite3.Error as exc:
        raise ContractViolation(
            "pre-stop bounded retry price-improvement audit failed"
        ) from exc
    finally:
        connection.close()
    verified = 0
    invalid = 0
    for target, filled, state, reason, side in rows:
        try:
            over_target = Decimal(str(filled)) > Decimal(str(target))
        except (InvalidOperation, TypeError, ValueError):
            invalid += 1
            continue
        if not over_target:
            continue
        if (
            str(side).upper() == "BUY"
            and str(state).upper() == "FILLED"
            and str(reason) in reasons
        ):
            verified += 1
        else:
            invalid += 1
    return {"verified_count": verified, "invalid_count": invalid}


def _pre_stop_redemption_evidence(path: Path) -> dict[str, int]:
    connection = _ro_connection(path)
    try:
        rows = connection.execute(
            "SELECT state,transaction_id,transaction_hash FROM redemption_receipts"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ContractViolation("pre-stop redemption audit failed") from exc
    finally:
        connection.close()
    safe_states = (
        LOCAL_REDEMPTION_TERMINALS
        | LOCAL_REDEMPTION_NO_SUBMISSION_BLOCKS
    )
    legacy_unresolved = 0
    unsafe = 0
    for raw_state, raw_transaction_id, raw_transaction_hash in rows:
        state = str(raw_state)
        transaction_id = str(raw_transaction_id or "").strip()
        transaction_hash = str(raw_transaction_hash or "").strip()
        if state in {"UNKNOWN_SUBMISSION", "ERROR"} or state.startswith("BLOCK_"):
            legacy_unresolved += 1
        if state not in safe_states or (
            state in LOCAL_REDEMPTION_PRE_SUBMIT | LOCAL_REDEMPTION_NO_SUBMISSION_BLOCKS
            and (transaction_id or transaction_hash)
        ):
            unsafe += 1
    return {
        "legacy_unresolved_redemption_count": legacy_unresolved,
        "unsafe_redemption_count": unsafe,
    }


def verify_coordinator_storage(
    path: Path, *, cutover: bool, full_integrity: bool = True
) -> None:
    connection = _ro_connection(path)
    try:
        _verify_integrity(
            connection, label="coordinator", full=full_integrity
        )
        rows = connection.execute(
            "SELECT condition_id,state,expected_payout_usd,transaction_id,transaction_hash "
            "FROM shared_condition_redemptions"
        ).fetchall()
        for row in rows:
            condition_id = str(row[0])
            state = str(row[1])
            payout = _canonical_decimal_text(row[2], label="shared-redemption-payout")
            transaction_id = str(row[3] or "").strip()
            transaction_hash = str(row[4] or "").strip()
            allocation_rows = [
                (str(allocation[0]), allocation[1])
                for allocation in connection.execute(
                    "SELECT apply_state,payout_usd FROM shared_condition_allocations "
                    "WHERE condition_id=?",
                    (condition_id,),
                )
            ]
            if not allocation_rows:
                raise ContractViolation("coordinator redemption has no allocations")
            allocations = [item[0] for item in allocation_rows]
            if state.startswith("LOSS_RESOLVED_") and (
                payout != "0"
                or any(
                    _canonical_decimal_text(
                        item[1], label="shared-loss-allocation-payout"
                    )
                    != "0"
                    for item in allocation_rows
                )
            ):
                raise ContractViolation("coordinator redemption loss payout is nonzero")
            if state in COORDINATOR_TERMINALS:
                if any(item != "APPLIED" for item in allocations):
                    raise ContractViolation("coordinator redemption allocation is not applied")
                continue
            if state in COORDINATOR_PRE_SUBMIT:
                if transaction_id or transaction_hash or any(
                    item != "PENDING" for item in allocations
                ):
                    raise ContractViolation("coordinator pre-submit state is inconsistent")
                if cutover:
                    raise ContractViolation("coordinator safe storage is not cutover ready")
                continue
            raise ContractViolation(f"unknown coordinator redemption state:{state}")
        orphan = connection.execute(
            """
            SELECT COUNT(*) FROM shared_condition_allocations AS allocation
            LEFT JOIN shared_condition_redemptions AS redemption
              ON redemption.condition_id=allocation.condition_id
            WHERE redemption.condition_id IS NULL
               OR allocation.apply_state NOT IN ('PENDING','APPLIED')
            """
        ).fetchone()[0]
        if orphan:
            raise ContractViolation("coordinator allocation is orphaned or unknown")
    except sqlite3.Error as exc:
        raise ContractViolation(f"coordinator storage contract query failed:{path}") from exc
    finally:
        connection.close()


EXPECTED_SLEEVE_ROLES = {
    "cd90": "RESIDUAL",
    "zockdo_full_wallet": "RESERVED",
    "wallet_9506_full_wallet": "RESERVED",
}


def _verified_migration_receipt(raw_json: str, stored_hash: str) -> dict[str, Any]:
    try:
        receipt = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise ContractViolation("migration receipt JSON invalid") from exc
    if not isinstance(receipt, dict):
        raise ContractViolation("migration receipt is not an object")
    claimed = str(receipt.get("migration_receipt_hash") or "")
    unsigned = dict(receipt)
    unsigned.pop("migration_receipt_hash", None)
    actual = _sha256_bytes(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    if claimed != actual or str(stored_hash) != actual:
        raise ContractViolation("migration receipt hash mismatch")
    return receipt


def verify_shared_wallet_topology(
    coordinator_path: Path,
    ledgers: Mapping[str, Path],
    wallet_lock: Path,
    *,
    registered_paths: Mapping[str, Path] | None = None,
    full_integrity: bool = True,
) -> str:
    if set(ledgers) != set(PROFILE_KEYS):
        raise ContractViolation("shared wallet ledger profile set mismatch")
    identities = ledgers if registered_paths is None else registered_paths
    if set(identities) != set(PROFILE_KEYS):
        raise ContractViolation("shared wallet registered profile set mismatch")
    expected_sleeves = {
        key: (str(Path(identities[key])), EXPECTED_SLEEVE_ROLES[key])
        for key in PROFILE_KEYS
    }
    connection = _ro_connection(coordinator_path)
    try:
        _verify_integrity(
            connection, label="coordinator topology", full=full_integrity
        )
        rows = connection.execute(
            "SELECT profile_key,ledger_path,role FROM sleeves ORDER BY profile_key"
        ).fetchall()
        actual_sleeves = {
            str(row[0]): (str(row[1]), str(row[2])) for row in rows
        }
        if actual_sleeves != expected_sleeves:
            raise ContractViolation("coordinator sleeve identity mismatch")
        lock_rows = connection.execute(
            "SELECT singleton,submission_lock_path FROM wallet_contract"
        ).fetchall()
        if len(lock_rows) != 1 or int(lock_rows[0][0]) != 1 or str(lock_rows[0][1]) != str(wallet_lock):
            raise ContractViolation("coordinator wallet lock identity mismatch")
        current_rows = connection.execute(
            "SELECT receipt_json,receipt_hash FROM migration_receipt"
        ).fetchall()
        if len(current_rows) != 1:
            raise ContractViolation("coordinator migration tip is not singular")
        current = _verified_migration_receipt(
            str(current_rows[0][0]), str(current_rows[0][1])
        )
        history_rows = connection.execute(
            "SELECT generation,receipt_json,receipt_hash FROM migration_history "
            "ORDER BY generation"
        ).fetchall()
        if not history_rows:
            raise ContractViolation("coordinator migration history missing")
        previous_hash = ""
        previous_funder = ""
        history: list[dict[str, Any]] = []
        for index, row in enumerate(history_rows, start=1):
            receipt = _verified_migration_receipt(str(row[1]), str(row[2]))
            try:
                generation = int(receipt["generation"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ContractViolation("migration generation invalid") from exc
            if int(row[0]) != generation or generation != index:
                raise ContractViolation("migration generation is not contiguous")
            parent = str(receipt.get("parent_migration_receipt_hash") or "")
            funder = str(receipt.get("funder_address") or "").lower()
            if index == 1:
                if parent:
                    raise ContractViolation("initial migration receipt has parent")
            elif parent != previous_hash or funder != previous_funder:
                raise ContractViolation("migration receipt lineage mismatch")
            previous_hash = str(receipt["migration_receipt_hash"])
            previous_funder = funder
            history.append(receipt)
        tip = str(current["migration_receipt_hash"])
        if current != history[-1] or tip != previous_hash:
            raise ContractViolation("migration current receipt is not history tip")
        receipt_sleeves = {
            str(item.get("profile_key") or ""): (
                str(item.get("ledger_path") or ""),
                str(item.get("role") or ""),
            )
            for item in current.get("sleeves", [])
            if isinstance(item, Mapping)
        }
        if receipt_sleeves != expected_sleeves:
            raise ContractViolation("migration receipt sleeve identity mismatch")
    except sqlite3.Error as exc:
        raise ContractViolation("coordinator topology query failed") from exc
    finally:
        connection.close()
    for profile, ledger in ledgers.items():
        connection = _ro_connection(ledger)
        try:
            config = {
                str(row[0]): str(row[1])
                for row in connection.execute(
                    "SELECT key,value FROM config WHERE key IN "
                    "('profile_key','shared_wallet_migration_receipt_hash')"
                )
            }
        except sqlite3.Error as exc:
            raise ContractViolation(f"local topology query failed:{profile}") from exc
        finally:
            connection.close()
        if config.get("profile_key") != profile:
            raise ContractViolation(f"local profile identity mismatch:{profile}")
        if config.get("shared_wallet_migration_receipt_hash") != tip:
            raise ContractViolation(f"local migration tip mismatch:{profile}")
    return tip


def _health_issue_prefix(profile_key: str) -> str:
    normalized = "".join(
        character if character.isalnum() else "_"
        for character in str(profile_key).strip().upper()
    ).strip("_")
    if not normalized:
        raise ContractViolation("health profile key is empty")
    return normalized


def _health_count(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise ContractViolation(f"health count invalid:{label}")
    try:
        count = int(value)
    except (TypeError, ValueError) as exc:
        raise ContractViolation(f"health count invalid:{label}") from exc
    if count < 0:
        raise ContractViolation(f"health count invalid:{label}")
    return count


def _validated_health_runtime_audit(
    row: Mapping[str, Any], *, profile_key: str
) -> dict[str, Any]:
    raw = row.get("release_runtime_error_audit")
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise ContractViolation(f"health runtime audit malformed:{profile_key}")
    event_count = _health_count(raw.get("event_count", 0), label=f"{profile_key}:events")
    internal_count = _health_count(
        raw.get("internal_event_count", 0), label=f"{profile_key}:internal"
    )
    external_count = _health_count(
        raw.get("external_event_count", 0), label=f"{profile_key}:external"
    )
    code_repair_count = _health_count(
        raw.get("code_repair_event_count", 0), label=f"{profile_key}:code-repair"
    )
    raw_categories = raw.get("category_counts", {})
    if not isinstance(raw_categories, Mapping):
        raise ContractViolation(f"health runtime audit categories malformed:{profile_key}")
    categories: dict[str, int] = {}
    for raw_category, raw_count in raw_categories.items():
        category = str(raw_category)
        count = _health_count(raw_count, label=f"{profile_key}:{category}")
        if not category or count <= 0:
            raise ContractViolation(f"health runtime audit category invalid:{profile_key}")
        categories[category] = count
    if sum(categories.values()) != event_count:
        raise ContractViolation(f"health runtime audit total mismatch:{profile_key}")
    observed_external = sum(
        count for category, count in categories.items() if category.startswith("EXTERNAL_")
    )
    if observed_external != external_count or event_count - observed_external != internal_count:
        raise ContractViolation(f"health runtime audit class mismatch:{profile_key}")
    unknown_external = {
        category
        for category in categories
        if category.startswith("EXTERNAL_") and category not in EXTERNAL_RUNTIME_CATEGORIES
    }
    if unknown_external:
        raise ContractViolation(
            f"health runtime audit external category unknown:{profile_key}:"
            f"{sorted(unknown_external)}"
        )
    latest = str(raw.get("latest_category") or "")
    row_latest = str(row.get("latest_runtime_error_category") or "")
    if row_latest and latest and row_latest != latest:
        raise ContractViolation(f"health runtime audit latest mismatch:{profile_key}")
    latest = latest or row_latest
    if (event_count == 0 and latest) or (event_count > 0 and latest not in categories):
        raise ContractViolation(f"health runtime audit latest invalid:{profile_key}")
    state = str(raw.get("state") or ("OK" if event_count == 0 else "ERRORS_OBSERVED"))
    if state != ("OK" if event_count == 0 else "ERRORS_OBSERVED"):
        raise ContractViolation(f"health runtime audit state mismatch:{profile_key}")
    return {
        "event_count": event_count,
        "internal_count": internal_count,
        "external_count": external_count,
        "code_repair_count": code_repair_count,
        "latest": latest,
        "categories": categories,
    }


def _validate_health_external_limitations(
    row: Mapping[str, Any],
    *,
    profile_key: str,
    outcome: str,
    audit: Mapping[str, Any],
) -> int:
    external_count = _health_count(
        row.get("external_limitation_count", 0),
        label=f"{profile_key}:external-limitations",
    )
    raw_limitations = row.get("external_limitations", [])
    if not isinstance(raw_limitations, list) or any(
        not isinstance(item, str) or not item for item in raw_limitations
    ):
        raise ContractViolation(f"health external limitation malformed:{profile_key}")
    limitations = list(raw_limitations)
    if len(limitations) != len(set(limitations)) or len(limitations) != external_count:
        raise ContractViolation(f"health external limitation count mismatch:{profile_key}")
    prefix = _health_issue_prefix(profile_key)
    maintenance_label = f"{prefix}_EXTERNAL_REDEMPTION_MAINTENANCE_PENDING"
    post_release_prefix = f"{prefix}_POST_RELEASE_EXTERNAL_ERROR_EVENTS:"
    legacy_zero_fill_prefix = (
        f"{prefix}_LEGACY_OFFICIAL_CONFIRMED_ZERO_FILL:"
    )
    post_release_count: int | None = None
    for limitation in limitations:
        status_match = re.fullmatch(
            rf"{re.escape(prefix)}_({'|'.join(sorted(HEALTH_STATUS_EXTERNAL_SUFFIXES))}):([1-9][0-9]*)",
            limitation,
        )
        if status_match is not None:
            continue
        if limitation == maintenance_label:
            continue
        if limitation.startswith(post_release_prefix):
            raw_count = limitation.removeprefix(post_release_prefix)
            if re.fullmatch(r"[1-9][0-9]*", raw_count) is None:
                raise ContractViolation(f"health external limitation invalid:{profile_key}:{limitation}")
            post_release_count = int(raw_count)
            continue
        if limitation.startswith(legacy_zero_fill_prefix):
            raw_count = limitation.removeprefix(legacy_zero_fill_prefix)
            if re.fullmatch(r"[1-9][0-9]*", raw_count) is None:
                raise ContractViolation(
                    f"health external limitation invalid:{profile_key}:"
                    f"{limitation}"
                )
            continue
        raise ContractViolation(f"health external limitation unknown:{profile_key}:{limitation}")
    audit_external = int(audit["external_count"])
    if (post_release_count or 0) != audit_external:
        raise ContractViolation(f"health external runtime count mismatch:{profile_key}")
    if audit_external:
        latest = str(audit["latest"])
        categories = dict(audit["categories"])
        if latest not in EXTERNAL_RUNTIME_CATEGORIES or latest not in categories:
            raise ContractViolation(f"external evidence is not exact:{profile_key}")
    has_maintenance = maintenance_label in limitations
    if outcome == "SUCCESS_REDEMPTION_MAINTENANCE_PENDING":
        if (
            not has_maintenance
            or audit.get("latest") != "EXTERNAL_REDEMPTION_MAINTENANCE"
            or int(dict(audit["categories"]).get("EXTERNAL_REDEMPTION_MAINTENANCE", 0)) <= 0
        ):
            raise ContractViolation(f"external evidence is not exact:{profile_key}")
    elif has_maintenance:
        raise ContractViolation(f"external evidence is not exact:{profile_key}")
    return external_count


def validate_health_payload(
    payload: Mapping[str, Any], *, pre_stop: bool = False
) -> str:
    state = str(payload.get("overall_state") or "")
    if state not in {"OK", "EXTERNAL_DEGRADED"}:
        raise ContractViolation(f"invalid health classification:{state}")
    profiles = payload.get("profiles")
    if not isinstance(profiles, Mapping) or set(profiles) != set(PROFILE_KEYS):
        raise ContractViolation("health profile set mismatch")
    services = payload.get("services")
    if not isinstance(services, list) or len(services) != len(EXECUTOR_UNITS):
        raise ContractViolation("health executor service set mismatch")
    if any(not isinstance(item, Mapping) for item in services):
        raise ContractViolation("health executor service set mismatch")
    service_by_name = {
        str(item.get("unit") or ""): item
        for item in services
        if isinstance(item, Mapping)
    }
    if set(service_by_name) != set(EXECUTOR_UNITS):
        raise ContractViolation("health executor service set mismatch")
    paused_profiles: list[str] = []
    paused_units: list[str] = []
    for spec in PROFILE_SPECS:
        row = profiles[spec.key]
        if not isinstance(row, Mapping) or not isinstance(row.get("paused"), bool):
            raise ContractViolation(f"health profile malformed:{spec.key}")
        unit_states: list[Mapping[str, Any]] = []
        for unit, field in (
            (spec.primary_unit, "unit_state"),
            (spec.standby_unit, "hot_standby_state"),
        ):
            state_row = service_by_name[unit]
            profile_state = row.get(field)
            if (
                not isinstance(profile_state, Mapping)
                or str(profile_state.get("unit") or "") != unit
                or str(profile_state.get("ActiveState") or "")
                != str(state_row.get("ActiveState") or "")
                or str(profile_state.get("UnitFileState") or "")
                != str(state_row.get("UnitFileState") or "")
            ):
                raise ContractViolation(f"health profile unit state mismatch:{spec.key}")
            if str(state_row.get("UnitFileState") or "") not in {
                "enabled",
                "disabled",
            }:
                raise ContractViolation(f"health executor unit file state invalid:{unit}")
            unit_states.append(state_row)
        exact_active = all(
            str(item.get("ActiveState") or "") == "active" for item in unit_states
        )
        exact_paused = all(
            str(item.get("ActiveState") or "") == "inactive"
            and str(item.get("UnitFileState") or "") == "disabled"
            for item in unit_states
        )
        if bool(row["paused"]) != exact_paused or not (exact_active or exact_paused):
            raise ContractViolation(f"health executor pause policy invalid:{spec.key}")
        if exact_paused:
            paused_profiles.append(spec.key)
            paused_units.extend((spec.primary_unit, spec.standby_unit))
    raw_paused_profiles = payload.get("paused_profiles")
    if (
        not isinstance(raw_paused_profiles, list)
        or raw_paused_profiles != sorted(paused_profiles)
    ):
        raise ContractViolation("health paused profile set mismatch")
    raw_paused_units = payload.get("service_paused_units")
    if not isinstance(raw_paused_units, list) or raw_paused_units != sorted(paused_units):
        raise ContractViolation("health paused unit set mismatch")
    if int(payload.get("service_paused_count") or 0) != len(paused_units):
        raise ContractViolation("health paused unit count mismatch")
    if int(payload.get("monitored_profile_count") or -1) != len(PROFILE_KEYS):
        raise ContractViolation("health monitored profile count mismatch")
    if int(payload.get("service_expected_count") or -1) != len(EXECUTOR_UNITS):
        raise ContractViolation("health executor expected count mismatch")
    observed_active_count = sum(
        1
        for item in services
        if str(item.get("ActiveState") or "") == "active"
    )
    if int(payload.get("service_active_count") or -1) != observed_active_count:
        raise ContractViolation("health executor active count mismatch")
    if observed_active_count + len(paused_units) != len(EXECUTOR_UNITS):
        raise ContractViolation("health executor coverage mismatch")
    timer = payload.get("health_timer")
    if (
        not isinstance(timer, Mapping)
        or str(timer.get("unit") or "") != HEALTH_TIMER
    ):
        raise ContractViolation("health timer malformed")
    timer_active = str(timer.get("ActiveState") or "") == "active"
    if not timer_active and not (
        pre_stop
        and str(timer.get("ActiveState") or "") == "inactive"
        and str(timer.get("UnitFileState") or "") == "disabled"
    ):
        raise ContractViolation("health timer inactive")
    if payload.get("service_inactive_units"):
        raise ContractViolation("health has inactive services")
    if int(payload.get("failed_polymarket_unit_count") or 0) != 0:
        raise ContractViolation("health has failed unit")
    coordinator = payload.get("coordinator")
    if not isinstance(coordinator, Mapping) or coordinator.get("state") != "OK":
        raise ContractViolation("health coordinator is not OK")
    lock_contract = payload.get("runtime_lock_contract")
    if (
        not isinstance(lock_contract, Mapping)
        or lock_contract.get("state") != "OK"
        or lock_contract.get("issues") not in ([], ())
    ):
        raise ContractViolation("health runtime lock contract is not OK")

    external_evidence = 0
    for key in PROFILE_KEYS:
        row = profiles[key]
        if not isinstance(row, Mapping):
            raise ContractViolation(f"health profile malformed:{key}")
        if int(row.get("status_issue_count") or 0) != 0:
            raise ContractViolation(f"health status issue:{key}")
        sqlite_integrity = row.get("sqlite_integrity")
        if not isinstance(sqlite_integrity, Mapping) or sqlite_integrity.get("state") != "OK":
            raise ContractViolation(f"health sqlite failure:{key}")
        if bool(row.get("paused")):
            if _health_count(
                row.get("external_limitation_count", 0),
                label=f"{key}:paused-external-limitations",
            ) != 0 or row.get("external_limitations") not in ([], ()):
                raise ContractViolation(f"health paused profile has external evidence:{key}")
            continue
        audit = _validated_health_runtime_audit(row, profile_key=key)
        reported_internal = _health_count(
            row.get("runtime_internal_error_count", 0), label=f"{key}:reported-internal"
        )
        reported_code_repair = _health_count(
            row.get("runtime_code_repair_count", 0), label=f"{key}:reported-code-repair"
        )
        if (
            reported_internal
            or reported_code_repair
            or int(audit["internal_count"])
            or int(audit["code_repair_count"])
        ):
            raise ContractViolation(f"health internal runtime evidence:{key}")
        outcome = str(row.get("last_cycle_outcome") or "")
        profile_external_count = _validate_health_external_limitations(
            row, profile_key=key, outcome=outcome, audit=audit
        )
        if outcome not in SUCCESS_OUTCOMES:
            prefix = _health_issue_prefix(key)
            exact_active_incident = any(
                re.fullmatch(
                    rf"{re.escape(prefix)}_ACTIVE_EXTERNAL_HEAD_INCIDENTS:[1-9][0-9]*",
                    str(item),
                )
                for item in row.get("external_limitations", [])
            )
            if outcome not in HEALTH_EXTERNAL_RETRY_OUTCOMES or not exact_active_incident:
                raise ContractViolation(f"unknown cycle outcome:{key}:{outcome}")
        external_evidence += profile_external_count
    if state == "OK" and external_evidence:
        raise ContractViolation("OK health contains external degradation")
    if state == "EXTERNAL_DEGRADED" and external_evidence <= 0:
        raise ContractViolation("external evidence missing")
    return state


def _canonical_decimal_text(value: Any, *, label: str) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractViolation(f"official redemption decimal invalid:{label}") from exc
    if not number.is_finite() or number < 0:
        raise ContractViolation(f"official redemption decimal invalid:{label}")
    normalized = format(number.normalize(), "f")
    return "0" if normalized in {"-0", ""} else normalized


def normalize_official_redemption_identities(
    rows: Iterable[Mapping[str, Any]], wallet_address: str
) -> dict[str, dict[str, str]]:
    wallet = str(wallet_address).strip().lower()
    if re.fullmatch(r"0x[0-9a-f]{40}", wallet) is None:
        raise ContractViolation("official redemption wallet invalid")
    identities: dict[str, dict[str, str]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping) or str(raw.get("type") or "") != "REDEEM":
            raise ContractViolation("official redemption row type invalid")
        proxy_wallet = str(raw.get("proxyWallet") or "").strip().lower()
        condition_id = str(raw.get("conditionId") or "").strip().lower()
        transaction_hash = str(raw.get("transactionHash") or "").strip().lower()
        if proxy_wallet != wallet:
            raise ContractViolation("official redemption wallet mismatch")
        if re.fullmatch(r"0x[0-9a-f]{64}", condition_id) is None:
            raise ContractViolation("official redemption condition invalid")
        if re.fullmatch(r"0x[0-9a-f]{64}", transaction_hash) is None:
            raise ContractViolation("official redemption transaction invalid")
        timestamp_raw = raw.get("timestamp")
        if isinstance(timestamp_raw, bool):
            raise ContractViolation("official redemption timestamp invalid")
        try:
            timestamp_seconds = int(timestamp_raw)
        except (TypeError, ValueError) as exc:
            raise ContractViolation("official redemption timestamp invalid") from exc
        if timestamp_seconds < 0 or str(timestamp_seconds) != str(timestamp_raw).strip():
            raise ContractViolation("official redemption timestamp invalid")
        identity = {
            "type": "REDEEM",
            "proxy_wallet": proxy_wallet,
            "condition_id": condition_id,
            "transaction_hash": transaction_hash,
            "payout_usd": _canonical_decimal_text(
                raw.get("usdcSize"), label="official-payout"
            ),
            "timestamp_seconds": str(timestamp_seconds),
        }
        encoded = json.dumps(
            identity, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        identities[_sha256_bytes(encoded)] = identity
    return identities


def verify_official_redemption_history(
    baseline_identity_hashes: set[str],
    current_identities: Mapping[str, Mapping[str, str]],
) -> None:
    current_hashes = set(current_identities)
    missing = set(baseline_identity_hashes) - current_hashes
    if missing:
        raise ContractViolation(
            "official redemption history regressed:" + ",".join(sorted(missing))
        )


def official_redemption_identity_digest(identity_hashes: Iterable[str]) -> str:
    validated = sorted(
        _validate_sha256(str(item), label="official redemption identity")
        for item in identity_hashes
    )
    if len(validated) != len(set(validated)):
        raise ContractViolation("official redemption identity set contains duplicates")
    return _sha256_bytes(
        json.dumps(validated, separators=(",", ":")).encode("utf-8")
    )


_OFFICIAL_LOCAL_TERMINAL_STATES = frozenset(
    {
        "REDEEMED",
        "REDEEMED_EXTERNAL_VERIFIED",
        "REDEEMED_OFFICIAL_ACTIVITY_VERIFIED",
        "REDEEMED_OFFICIAL_PAYOUT_CORRECTED",
        "LOSS_RESOLVED_OFFICIAL_ACTIVITY_CORRECTED",
        "LOSS_RESOLVED_NO_PAYOUT",
    }
)
_SHARED_LOCAL_TERMINAL_STATES = frozenset(
    {
        "REDEEMED_SHARED_WALLET",
        "REDEEMED_SHARED_PLATFORM_SETTLEMENT",
        "LOSS_RESOLVED_SHARED_WALLET",
    }
)


def verify_official_redemption_conservation(
    *,
    baseline_identity_hashes: set[str],
    current_identities: Mapping[str, Mapping[str, str]],
    ledgers: Mapping[str, Path],
    coordinator: Path,
    transition_baselines: Mapping[str, Mapping[str, Any]],
    shared_transition_baseline: int,
) -> dict[str, Any]:
    verify_official_redemption_history(
        baseline_identity_hashes, current_identities
    )
    if set(ledgers) != set(PROFILE_KEYS) or set(transition_baselines) != set(
        PROFILE_KEYS
    ):
        raise ContractViolation("official redemption profile set mismatch")
    for profile, ledger in ledgers.items():
        try:
            baseline = int(
                transition_baselines[profile]["redemption_transition_id"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractViolation(
                f"redemption transition baseline invalid:{profile}"
            ) from exc
        connection = _ro_connection(Path(ledger))
        try:
            verify_append_only_watermark(
                connection,
                table="redemption_transitions",
                baseline=baseline,
                label=f"redemption:{profile}",
            )
            for row in connection.execute(
                "SELECT state,expected_payout_usd FROM redemption_receipts "
                "WHERE state LIKE 'LOSS_RESOLVED_%'"
            ):
                if _canonical_decimal_text(
                    row[1], label=f"loss-payout:{profile}"
                ) != "0":
                    raise ContractViolation(
                        f"redemption loss payout is nonzero:{profile}"
                    )
        finally:
            connection.close()
    connection = _ro_connection(coordinator)
    try:
        verify_append_only_watermark(
            connection,
            table="shared_condition_transitions",
            baseline=int(shared_transition_baseline),
            label="shared-redemption",
        )
        for row in connection.execute(
            "SELECT condition_id,expected_payout_usd FROM shared_condition_redemptions "
            "WHERE state LIKE 'LOSS_RESOLVED_%'"
        ):
            condition_id = str(row[0])
            if _canonical_decimal_text(
                row[1], label="shared-loss-payout"
            ) != "0":
                raise ContractViolation("shared redemption loss payout is nonzero")
            allocation_rows = connection.execute(
                "SELECT payout_usd FROM shared_condition_allocations "
                "WHERE condition_id=?",
                (condition_id,),
            ).fetchall()
            if any(
                _canonical_decimal_text(
                    allocation[0], label="shared-loss-allocation-payout"
                )
                != "0"
                for allocation in allocation_rows
            ):
                raise ContractViolation("shared redemption loss payout is nonzero")
    finally:
        connection.close()
    by_key: dict[tuple[str, str, str], list[str]] = {}
    for identity_hash, identity in current_identities.items():
        try:
            key = (
                str(identity["condition_id"]).lower(),
                str(identity["transaction_hash"]).lower(),
                _canonical_decimal_text(
                    identity["payout_usd"], label="identity-payout"
                ),
            )
        except KeyError as exc:
            raise ContractViolation("official redemption identity malformed") from exc
        by_key.setdefault(key, []).append(str(identity_hash))
    if any(len(hashes) != 1 for hashes in by_key.values()):
        raise ContractViolation("official redemption identity is ambiguous")

    def condition_terminal_states(profile: str, condition_id: str) -> set[str]:
        connection = _ro_connection(Path(ledgers[profile]))
        try:
            current = connection.execute(
                "SELECT state FROM redemption_receipts WHERE lower(condition_id)=?",
                (condition_id,),
            ).fetchone()
            placeholders = ",".join("?" for _ in LOCAL_REDEMPTION_TERMINALS)
            rows = connection.execute(
                "SELECT state FROM redemption_transitions "
                f"WHERE lower(condition_id)=? AND state IN ({placeholders})",
                (condition_id, *sorted(LOCAL_REDEMPTION_TERMINALS)),
            ).fetchall()
        finally:
            connection.close()
        states = {str(row[0]) for row in rows}
        if current is not None and str(current[0]) in LOCAL_REDEMPTION_TERMINALS:
            states.add(str(current[0]))
        return states

    def latest_local_owner(
        profile: str, identity: Mapping[str, str]
    ) -> str | None:
        connection = _ro_connection(Path(ledgers[profile]))
        try:
            receipt = connection.execute(
                "SELECT state,expected_payout_usd,transaction_hash "
                "FROM redemption_receipts WHERE lower(condition_id)=?",
                (str(identity["condition_id"]).lower(),),
            ).fetchone()
            if receipt is None:
                return None
            transition = connection.execute(
                "SELECT state FROM redemption_transitions "
                "WHERE lower(condition_id)=? ORDER BY id DESC LIMIT 1",
                (str(identity["condition_id"]).lower(),),
            ).fetchone()
        except sqlite3.Error as exc:
            raise ContractViolation(
                f"official local redemption query failed:{profile}"
            ) from exc
        finally:
            connection.close()
        if transition is None or str(receipt[0]) != str(transition[0]):
            raise ContractViolation(
                f"official local redemption transition mismatch:{profile}"
            )
        state = str(receipt[0])
        if state == "REDEEMED_SHARED_WALLET":
            return None
        if state not in _OFFICIAL_LOCAL_TERMINAL_STATES:
            return None
        if str(receipt[2] or "").strip().lower() != str(
            identity["transaction_hash"]
        ).lower():
            return None
        if _canonical_decimal_text(
            receipt[1], label=f"local-payout:{profile}"
        ) != _canonical_decimal_text(
            identity["payout_usd"], label="official-payout"
        ):
            return None
        return f"local:{profile}"

    def shared_owner(identity: Mapping[str, str]) -> str | None:
        connection = _ro_connection(coordinator)
        try:
            receipt = connection.execute(
                "SELECT state,expected_payout_usd,transaction_hash,inventory_hash "
                "FROM shared_condition_redemptions WHERE lower(condition_id)=?",
                (str(identity["condition_id"]).lower(),),
            ).fetchone()
            if receipt is None:
                return None
            transition = connection.execute(
                "SELECT state FROM shared_condition_transitions "
                "WHERE lower(condition_id)=? ORDER BY id DESC LIMIT 1",
                (str(identity["condition_id"]).lower(),),
            ).fetchone()
            allocations = connection.execute(
                "SELECT a.profile_key,a.ledger_path,a.payout_usd,a.apply_state,"
                "a.applied_at_ms,s.ledger_path FROM shared_condition_allocations AS a "
                "LEFT JOIN sleeves AS s ON s.profile_key=a.profile_key "
                "WHERE lower(a.condition_id)=? ORDER BY a.profile_key",
                (str(identity["condition_id"]).lower(),),
            ).fetchall()
        except sqlite3.Error as exc:
            raise ContractViolation("official shared redemption query failed") from exc
        finally:
            connection.close()
        if (
            transition is None
            or str(receipt[0]) != "REDEEMED"
            or str(transition[0]) != "REDEEMED"
            or str(receipt[2] or "").strip().lower()
            != str(identity["transaction_hash"]).lower()
            or _canonical_decimal_text(receipt[1], label="shared-payout")
            != _canonical_decimal_text(
                identity["payout_usd"], label="official-payout"
            )
        ):
            return None
        if not allocations or any(
            str(row[0]) not in PROFILE_KEYS
            or str(Path(str(row[1]))) != str(Path(ledgers[str(row[0])]))
            or str(Path(str(row[5]))) != str(Path(ledgers[str(row[0])]))
            or str(row[3]) != "APPLIED"
            or row[4] is None
            for row in allocations
        ):
            return None
        allocated_profiles = {str(row[0]) for row in allocations}
        for profile in allocated_profiles:
            terminal_states = condition_terminal_states(
                profile, str(identity["condition_id"]).lower()
            )
            if terminal_states - _SHARED_LOCAL_TERMINAL_STATES:
                raise ContractViolation(
                    f"shared redemption allocation has direct terminal:{profile}"
                )
        for profile in set(PROFILE_KEYS) - allocated_profiles:
            if condition_terminal_states(
                profile, str(identity["condition_id"]).lower()
            ):
                raise ContractViolation(
                    f"shared redemption owner includes unallocated profile:{profile}"
                )
        allocation_total = sum(
            (Decimal(str(row[2])) for row in allocations), Decimal("0")
        )
        if _canonical_decimal_text(
            allocation_total, label="shared-allocation-total"
        ) != _canonical_decimal_text(receipt[1], label="shared-payout"):
            return None
        inventory_hash = str(receipt[3])
        for profile, _path, payout, _state, _applied, _registered in allocations:
            connection = _ro_connection(Path(ledgers[str(profile)]))
            try:
                local = connection.execute(
                    "SELECT state,expected_payout_usd,transaction_hash "
                    "FROM redemption_receipts WHERE lower(condition_id)=?",
                    (str(identity["condition_id"]).lower(),),
                ).fetchone()
                transition = connection.execute(
                    "SELECT state,details_json FROM redemption_transitions "
                    "WHERE lower(condition_id)=? ORDER BY id DESC LIMIT 1",
                    (str(identity["condition_id"]).lower(),),
                ).fetchone()
            finally:
                connection.close()
            if (
                local is None
                or transition is None
                or str(local[0]) != "REDEEMED_SHARED_WALLET"
                or str(transition[0]) != "REDEEMED_SHARED_WALLET"
                or str(local[2] or "").strip().lower()
                != str(identity["transaction_hash"]).lower()
                or _canonical_decimal_text(
                    local[1], label=f"shared-local-payout:{profile}"
                )
                != _canonical_decimal_text(
                    payout, label=f"shared-allocation-payout:{profile}"
                )
            ):
                return None
            try:
                details = json.loads(str(transition[1]))
            except json.JSONDecodeError:
                return None
            if str(details.get("inventory_hash") or "") != inventory_hash:
                return None
        return "shared"

    def owners(identity: Mapping[str, str]) -> list[str]:
        found = [
            owner
            for profile in PROFILE_KEYS
            if (owner := latest_local_owner(profile, identity)) is not None
        ]
        connection = _ro_connection(coordinator)
        try:
            coordinator_managed = connection.execute(
                "SELECT 1 FROM shared_condition_redemptions "
                "WHERE lower(condition_id)=?",
                (str(identity["condition_id"]).lower(),),
            ).fetchone() is not None
        finally:
            connection.close()
        shared = shared_owner(identity)
        if coordinator_managed:
            if found:
                raise ContractViolation(
                    "shared redemption condition cannot use a local owner"
                )
            return [] if shared is None else [shared]
        terminal_profiles = {
            profile
            for profile in PROFILE_KEYS
            if condition_terminal_states(
                profile, str(identity["condition_id"]).lower()
            )
        }
        if len(terminal_profiles) > 1:
            raise ContractViolation(
                "local redemption condition has multiple terminal profiles"
            )
        if terminal_profiles and any(
            condition_terminal_states(
                profile, str(identity["condition_id"]).lower()
            )
            & _SHARED_LOCAL_TERMINAL_STATES
            for profile in terminal_profiles
        ):
            raise ContractViolation(
                "local redemption condition contains shared terminal history"
            )
        if shared is not None:
            found.append(shared)
        return found

    validated_local_platform_profiles: set[str] = set()

    def local_platform_batches_are_exact(profile: str) -> bool:
        if profile in validated_local_platform_profiles:
            return True
        try:
            baseline = int(
                transition_baselines[profile]["redemption_transition_id"]
            )
        except (KeyError, TypeError, ValueError):
            return False
        connection = _ro_connection(Path(ledgers[profile]))
        try:
            rows = connection.execute(
                "SELECT id,lower(condition_id),created_at_ms,reason,details_json "
                "FROM redemption_transitions WHERE id>? "
                "AND state='REDEEMED_PLATFORM_SETTLEMENT_VERIFIED' ORDER BY id",
                (baseline,),
            ).fetchall()
            receipt_rows = {
                str(row[0]): row[1]
                for row in connection.execute(
                    "SELECT lower(condition_id),expected_payout_usd "
                    "FROM redemption_receipts "
                    "WHERE state='REDEEMED_PLATFORM_SETTLEMENT_VERIFIED'"
                ).fetchall()
            }
        except sqlite3.Error:
            return False
        finally:
            connection.close()
        if not rows:
            return False
        groups: dict[tuple[int, str, str, str, str], Decimal] = {}
        seen_conditions: set[str] = set()
        for row in rows:
            condition = str(row[1])
            if condition in seen_conditions or condition not in receipt_rows:
                return False
            seen_conditions.add(condition)
            if str(row[3]) != (
                "OFFICIAL_RESOLUTION_ZERO_OUTCOME_BALANCES_AND_EXACT_"
                "AUTHENTICATED_CASH_DELTA"
            ):
                return False
            try:
                details = json.loads(str(row[4]))
                if not isinstance(details, Mapping):
                    return False
                payout = Decimal(
                    _canonical_decimal_text(
                        details["payout_usd"],
                        label=f"platform-payout:{profile}:{condition}",
                    )
                )
                ledger_before = Decimal(
                    _canonical_decimal_text(
                        details["ledger_cash_before_usd"],
                        label=f"platform-ledger-cash:{profile}:{condition}",
                    )
                )
                wallet_before = Decimal(
                    _canonical_decimal_text(
                        details["aggregate_wallet_cash_before_usd"],
                        label=f"platform-wallet-cash:{profile}:{condition}",
                    )
                )
                collateral = Decimal(
                    _canonical_decimal_text(
                        details["authenticated_collateral_usd"],
                        label=f"platform-collateral:{profile}:{condition}",
                    )
                )
                delta = Decimal(
                    _canonical_decimal_text(
                        details["exact_reconciliation_delta_usd"],
                        label=f"platform-delta:{profile}:{condition}",
                    )
                )
                written_off = Decimal(
                    _canonical_decimal_text(
                        details["written_off_cost_usd"],
                        label=f"platform-cost:{profile}:{condition}",
                    )
                )
                receipt_payout = Decimal(
                    _canonical_decimal_text(
                        receipt_rows[condition],
                        label=f"platform-receipt:{profile}:{condition}",
                    )
                )
            except (
                ContractViolation,
                InvalidOperation,
                KeyError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
            ):
                return False
            values = (
                payout,
                ledger_before,
                wallet_before,
                collateral,
                delta,
                written_off,
                receipt_payout,
            )
            if (
                any(not value.is_finite() or value < 0 for value in values)
                or payout <= 0
                or delta <= 0
                or receipt_payout != payout
                or collateral - wallet_before != delta
            ):
                return False
            key = (
                int(row[2]),
                str(ledger_before),
                str(wallet_before),
                str(collateral),
                str(delta),
            )
            groups[key] = groups.get(key, Decimal("0")) + payout
        if any(total != Decimal(key[4]) for key, total in groups.items()):
            return False
        validated_local_platform_profiles.add(profile)
        return True

    def nonofficial_terminal_owner(condition_id: str) -> str | None:
        terminal_profiles = {
            profile
            for profile in PROFILE_KEYS
            if condition_terminal_states(profile, condition_id)
        }
        connection = _ro_connection(coordinator)
        try:
            shared_receipt = connection.execute(
                "SELECT state,expected_payout_usd,transaction_id,transaction_hash,inventory_hash "
                "FROM shared_condition_redemptions WHERE lower(condition_id)=?",
                (condition_id,),
            ).fetchone()
            shared_transitions = connection.execute(
                "SELECT state,reason FROM shared_condition_transitions "
                "WHERE lower(condition_id)=? ORDER BY id",
                (condition_id,),
            ).fetchall()
            shared_allocations = connection.execute(
                "SELECT a.profile_key,a.ledger_path,a.payout_usd,a.apply_state,"
                "a.applied_at_ms,s.ledger_path FROM shared_condition_allocations AS a "
                "LEFT JOIN sleeves AS s ON s.profile_key=a.profile_key "
                "WHERE lower(a.condition_id)=? ORDER BY a.profile_key",
                (condition_id,),
            ).fetchall()
        finally:
            connection.close()
        if shared_receipt is None:
            if len(terminal_profiles) > 1:
                if all(
                    condition_terminal_states(profile, condition_id)
                    == {"LOSS_RESOLVED_NO_PAYOUT"}
                    for profile in terminal_profiles
                ):
                    raise ContractViolation(
                        "hashless redemption has multiple local loss owners"
                    )
                raise ContractViolation(
                    "nonofficial redemption has multiple local owners"
                )
            if not terminal_profiles:
                return None
            profile = next(iter(terminal_profiles))
            states = condition_terminal_states(profile, condition_id)
            if states == {"LOSS_RESOLVED_NO_PAYOUT"}:
                terminal_state = "LOSS_RESOLVED_NO_PAYOUT"
                mode = "loss"
            elif states == {"REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"}:
                terminal_state = "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"
                mode = "platform"
            else:
                return None
            connection = _ro_connection(Path(ledgers[profile]))
            try:
                receipt = connection.execute(
                    "SELECT state,expected_payout_usd,transaction_id,transaction_hash "
                    "FROM redemption_receipts WHERE lower(condition_id)=?",
                    (condition_id,),
                ).fetchone()
                transition = connection.execute(
                    "SELECT state,reason,details_json FROM redemption_transitions "
                    "WHERE lower(condition_id)=? ORDER BY id DESC LIMIT 1",
                    (condition_id,),
                ).fetchone()
                mapping = connection.execute(
                    "SELECT primary_token_id,secondary_token_id "
                    "FROM condition_mappings WHERE lower(condition_id)=?",
                    (condition_id,),
                ).fetchone()
                positions = []
                if mapping is not None:
                    positions = connection.execute(
                        "SELECT token_id,quantity,cost_basis_usd FROM positions "
                        "WHERE token_id IN (?,?) ORDER BY token_id",
                        (str(mapping[0]), str(mapping[1])),
                    ).fetchall()
            finally:
                connection.close()
            if (
                receipt is None
                or transition is None
                or str(receipt[0]) != terminal_state
                or str(transition[0]) != terminal_state
                or bool(str(receipt[2] or "").strip())
                or bool(str(receipt[3] or "").strip())
            ):
                return None
            payout = _canonical_decimal_text(
                receipt[1], label=f"nonofficial-local-payout:{profile}"
            )
            if mode == "loss":
                if payout != "0" or mapping is None:
                    return None
                try:
                    details = json.loads(str(transition[2]))
                except json.JSONDecodeError:
                    return None
                if not isinstance(details, Mapping):
                    return None
                position_by_token = {
                    str(row[0]): (
                        _canonical_decimal_text(
                            row[1], label=f"loss-position-quantity:{profile}"
                        ),
                        _canonical_decimal_text(
                            row[2], label=f"loss-position-cost:{profile}"
                        ),
                    )
                    for row in positions
                }
                token_ids = (str(mapping[0]), str(mapping[1]))
                if any(position_by_token.get(token_id, ("0", "0")) != ("0", "0") for token_id in token_ids):
                    return None
                reason = str(transition[1])
                winner = str(details.get("winner_token_id") or "")
                if reason == "":
                    raw_keys = (
                        "primary_local_raw",
                        "secondary_local_raw",
                        "primary_onchain_raw",
                        "secondary_onchain_raw",
                    )
                    if any(
                        type(details.get(key)) is not int
                        or int(details[key]) < 0
                        for key in raw_keys
                    ):
                        return None
                    if (
                        int(details["primary_local_raw"])
                        != int(details["primary_onchain_raw"])
                        or int(details["secondary_local_raw"])
                        != int(details["secondary_onchain_raw"])
                        or winner not in token_ids
                    ):
                        return None
                    winner_raw = (
                        int(details["primary_local_raw"])
                        if winner == token_ids[0]
                        else int(details["secondary_local_raw"])
                    )
                    if winner_raw != 0:
                        return None
                elif reason == "OFFICIAL_PAYOUT_AND_EXACT_ONCHAIN_INVENTORY_PROOF":
                    numerators = details.get("payout_numerators")
                    if (
                        details.get("onchain_inventory_exact") is not True
                        or type(details.get("onchain_winner_balance_raw")) is not int
                        or int(details["onchain_winner_balance_raw"]) != 0
                        or not isinstance(numerators, list)
                        or len(numerators) != 2
                        or any(type(value) is not int for value in numerators)
                        or min(numerators) != 0
                        or max(numerators) <= 0
                        or sum(value > 0 for value in numerators) != 1
                    ):
                        return None
                    expected_winner = token_ids[0] if numerators[0] > 0 else token_ids[1]
                    if winner != expected_winner:
                        return None
                else:
                    return None
                return f"local-loss:{profile}"
            if payout == "0":
                return None
            if not local_platform_batches_are_exact(profile):
                return None
            if str(transition[1]) != (
                "OFFICIAL_RESOLUTION_ZERO_OUTCOME_BALANCES_AND_EXACT_"
                "AUTHENTICATED_CASH_DELTA"
            ):
                return None
            try:
                details = json.loads(str(transition[2]))
            except json.JSONDecodeError:
                return None
            if _canonical_decimal_text(
                details.get("payout_usd"), label=f"platform-local-payout:{profile}"
            ) != payout:
                return None
            return f"local-platform:{profile}"

        shared_state = str(shared_receipt[0])
        if shared_state == "LOSS_RESOLVED_NO_PAYOUT":
            local_state = "LOSS_RESOLVED_SHARED_WALLET"
            mode = "loss"
            incomplete = "shared loss owner incomplete"
        elif shared_state == "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED":
            local_state = "REDEEMED_SHARED_PLATFORM_SETTLEMENT"
            mode = "platform"
            incomplete = "shared platform settlement owner incomplete"
        else:
            return None
        shared_payout = _canonical_decimal_text(
            shared_receipt[1], label=f"shared-nonofficial-{mode}"
        )
        shared_transition = (
            None if not shared_transitions else shared_transitions[-1]
        )
        if (
            shared_transition is None
            or str(shared_transition[0]) != shared_state
            or bool(str(shared_receipt[2] or "").strip())
            or bool(str(shared_receipt[3] or "").strip())
            or not shared_allocations
        ):
            raise ContractViolation(incomplete)
        predecessor = (
            None if len(shared_transitions) < 2 else shared_transitions[-2]
        )
        if mode == "loss":
            valid_predecessor = (
                predecessor is not None
                and str(predecessor[0]) == "LOSS_DISTRIBUTING"
                and str(predecessor[1])
                == "OFFICIAL_ZERO_PAYOUT_WITH_EXACT_AGGREGATE_INVENTORY"
            )
        else:
            valid_predecessor = (
                predecessor is not None
                and str(predecessor[0]) == "PLATFORM_DISTRIBUTING"
                and str(predecessor[1])
                in {
                    "EXACT_AUTHENTICATED_CASH_DELTA_AND_ZERO_OUTCOME_BALANCES",
                    "UNKNOWN_SUBMISSION_RESOLVED_BY_EXACT_CASH_AND_ZERO_BALANCES",
                }
            )
        if not valid_predecessor:
            raise ContractViolation(incomplete)
        if any(
            str(row[0]) not in PROFILE_KEYS
            or str(Path(str(row[1]))) != str(Path(ledgers[str(row[0])]))
            or str(Path(str(row[5]))) != str(Path(ledgers[str(row[0])]))
            or str(row[3]) != "APPLIED"
            or row[4] is None
            for row in shared_allocations
        ):
            raise ContractViolation(incomplete)
        allocation_payouts = [
            _canonical_decimal_text(
                row[2], label=f"shared-nonofficial-{mode}-allocation"
            )
            for row in shared_allocations
        ]
        if mode == "loss":
            if shared_payout != "0" or any(value != "0" for value in allocation_payouts):
                raise ContractViolation(incomplete)
        else:
            if shared_payout == "0" or any(
                value == "0" for value in allocation_payouts
            ):
                raise ContractViolation(incomplete)
            if _canonical_decimal_text(
                sum((Decimal(value) for value in allocation_payouts), Decimal("0")),
                label="shared-platform-allocation-total",
            ) != shared_payout:
                raise ContractViolation(incomplete)
        allocated_profiles = {str(row[0]) for row in shared_allocations}
        if terminal_profiles != allocated_profiles:
            raise ContractViolation(incomplete)
        inventory_hash = str(shared_receipt[4] or "")
        allocations_by_profile = {
            str(row[0]): _canonical_decimal_text(
                row[2], label=f"shared-nonofficial-{mode}-allocation"
            )
            for row in shared_allocations
        }
        for profile in allocated_profiles:
            if condition_terminal_states(profile, condition_id) != {local_state}:
                raise ContractViolation(incomplete)
            connection = _ro_connection(Path(ledgers[profile]))
            try:
                local = connection.execute(
                    "SELECT state,expected_payout_usd,transaction_id,transaction_hash "
                    "FROM redemption_receipts WHERE lower(condition_id)=?",
                    (condition_id,),
                ).fetchone()
                transition = connection.execute(
                    "SELECT state,details_json FROM redemption_transitions "
                    "WHERE lower(condition_id)=? ORDER BY id DESC LIMIT 1",
                    (condition_id,),
                ).fetchone()
            finally:
                connection.close()
            try:
                details = {} if transition is None else json.loads(str(transition[1]))
            except json.JSONDecodeError:
                details = {}
            if (
                local is None
                or transition is None
                or str(local[0]) != local_state
                or str(transition[0]) != local_state
                or _canonical_decimal_text(
                    local[1], label=f"shared-nonofficial-local:{profile}"
                )
                != allocations_by_profile[profile]
                or bool(str(local[2] or "").strip())
                or bool(str(local[3] or "").strip())
                or str(details.get("inventory_hash") or "") != inventory_hash
            ):
                raise ContractViolation(incomplete)
        return f"shared-{mode}"

    added = set(current_identities) - set(baseline_identity_hashes)
    for identity_hash in added:
        matched = owners(current_identities[identity_hash])
        if not matched:
            raise ContractViolation("official redemption identity has no terminal owner")
        if len(matched) != 1:
            raise ContractViolation("official redemption identity has multiple owners")

    new_conditions: set[str] = set()
    for profile, ledger in ledgers.items():
        try:
            baseline = int(
                transition_baselines[profile]["redemption_transition_id"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractViolation(
                f"redemption transition baseline invalid:{profile}"
            ) from exc
        connection = _ro_connection(Path(ledger))
        try:
            placeholders = ",".join("?" for _ in LOCAL_REDEMPTION_TERMINALS)
            rows = connection.execute(
                "SELECT DISTINCT lower(condition_id) FROM redemption_transitions "
                f"WHERE id>? AND state IN ({placeholders})",
                (baseline, *sorted(LOCAL_REDEMPTION_TERMINALS)),
            ).fetchall()
        finally:
            connection.close()
        new_conditions.update(str(row[0]) for row in rows)
    connection = _ro_connection(coordinator)
    try:
        placeholders = ",".join("?" for _ in COORDINATOR_TERMINALS)
        rows = connection.execute(
            "SELECT DISTINCT lower(condition_id) FROM shared_condition_transitions "
            f"WHERE id>? AND state IN ({placeholders})",
            (int(shared_transition_baseline), *sorted(COORDINATOR_TERMINALS)),
        ).fetchall()
    finally:
        connection.close()
    new_conditions.update(str(row[0]) for row in rows)
    for condition_id in new_conditions:
        matches = [
            identity
            for identity in current_identities.values()
            if str(identity["condition_id"]).lower() == condition_id
        ]
        if not matches:
            if nonofficial_terminal_owner(condition_id) is None:
                raise ContractViolation(
                    "new redemption terminal lacks official identity"
                )
            continue
        owned_matches = []
        for identity in matches:
            matched = owners(identity)
            if len(matched) > 1:
                raise ContractViolation(
                    "new redemption terminal has multiple official owners"
                )
            if matched:
                owned_matches.append(identity)
        if len(owned_matches) != 1:
            raise ContractViolation("new redemption terminal lacks official identity")
    return {
        "baseline_identity_count": len(baseline_identity_hashes),
        "current_identity_count": len(current_identities),
        "added_identity_count": len(added),
        "new_terminal_condition_count": len(new_conditions),
    }


@dataclass(frozen=True)
class FileMetadata:
    uid: int
    gid: int
    mode: int

    @classmethod
    def from_path(cls, path: Path) -> "FileMetadata":
        status = Path(path).stat(follow_symlinks=False)
        return cls(status.st_uid, status.st_gid, stat.S_IMODE(status.st_mode))


def atomic_replace_database(source: Path, target: Path, metadata: FileMetadata) -> None:
    source = _regular_database(source)
    target = _regular_database(target)
    assert_no_sqlite_sidecars(source)
    assert_no_sqlite_sidecars(target)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.release-restore.", dir=target.parent
    )
    temporary = Path(temporary_name)
    source_hash = sha256_file(source)
    replaced = False
    try:
        with source.open("rb") as source_handle, os.fdopen(descriptor, "wb", closefd=False) as target_handle:
            shutil.copyfileobj(source_handle, target_handle)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        os.fchown(descriptor, metadata.uid, metadata.gid)
        os.fchmod(descriptor, metadata.mode)
        # Persist ownership and mode on the replacement inode before the
        # durable phase marker can make this restore non-repeatable.
        os.fsync(descriptor)
        os.replace(temporary, target)
        replaced = True
    finally:
        os.close(descriptor)
        if not replaced and temporary.exists():
            temporary.unlink()
    directory_descriptor = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    if sha256_file(target) != source_hash or FileMetadata.from_path(target) != metadata:
        raise ContractViolation(f"database atomic restore verification failed:{target}")
    connection = _ro_connection(target)
    try:
        _verify_integrity(connection, label="restored database")
    finally:
        connection.close()
    try:
        connection = sqlite3.connect(target)
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        connection.close()
    except sqlite3.Error as exc:
        raise ContractViolation(
            f"database atomic restore checkpoint failed:{target}"
        ) from exc
    if row is None or int(row[0]) != 0:
        raise ContractViolation(f"database atomic restore checkpoint busy:{target}")
    if sha256_file(target) != source_hash:
        raise ContractViolation(f"database atomic restore changed bytes:{target}")
    assert_no_sqlite_sidecars(target)


def assert_no_sqlite_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists() or sidecar.is_symlink():
            raise ContractViolation(f"sqlite sidecar remains:{sidecar}")


def physical_copy_database(source: Path, destination: Path) -> str:
    source = _regular_database(source)
    assert_no_sqlite_sidecars(source)
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise ContractViolation(f"physical snapshot destination exists:{destination}")
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        with source.open("rb") as source_handle, os.fdopen(
            descriptor, "wb", closefd=False
        ) as destination_handle:
            shutil.copyfileobj(source_handle, destination_handle)
            destination_handle.flush()
            os.fsync(destination_handle.fileno())
    finally:
        os.close(descriptor)
    directory_descriptor = os.open(
        destination.parent, os.O_RDONLY | os.O_DIRECTORY
    )
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    source_hash = sha256_file(source)
    if sha256_file(destination) != source_hash:
        raise ContractViolation(f"physical snapshot digest mismatch:{source}")
    return source_hash


def atomic_write_json(path: Path, payload: Mapping[str, Any], *, mode: int) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    temporary = path.with_name(f".{path.name}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    os.chmod(path, mode)
    directory_descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
    return sha256_file(path)


def _seal_path_by_descriptor(
    path: Path, *, mode: int, directory: bool, root_owned: bool
) -> None:
    before = path.lstat()
    expected_kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected_kind(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise ContractViolation(f"seal path identity mismatch:{path}")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ContractViolation(f"seal path changed during open:{path}")
        if root_owned:
            os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _canonical_database_payload(path: Path, *, ignore_migration_fields: bool) -> bytes:
    connection = _ro_connection(path)
    try:
        pragma_names = (
            "application_id",
            "auto_vacuum",
            "encoding",
            "page_size",
            "schema_version",
            "user_version",
        )
        # WAL versus rollback-journal is a storage-mode setting, not an
        # economic ledger field.  The physical snapshot receipt still binds it;
        # the migration delta deliberately compares only economic content.
        if not ignore_migration_fields:
            pragma_names = (*pragma_names, "journal_mode")
        pragmas = {
            name: connection.execute(f"PRAGMA {name}").fetchone()[0]
            for name in pragma_names
        }
        schema = [
            list(row)
            for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_autoindex_%' "
                "ORDER BY type,name,tbl_name,sql"
            )
        ]
        payload: list[Any] = [["database_pragmas", pragmas], ["sqlite_master", schema]]
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        for table in tables:
            columns = [
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            ]
            if not columns:
                raise ContractViolation(f"table metadata missing:{table}")
            where = ""
            parameters: tuple[Any, ...] = ()
            if ignore_migration_fields and table == "config":
                placeholders = ",".join("?" for _ in MIGRATION_CONFIG_KEYS)
                where = f" WHERE key NOT IN ({placeholders})"
                parameters = tuple(sorted(MIGRATION_CONFIG_KEYS))
            elif ignore_migration_fields and table == "config_change_receipts":
                placeholders = ",".join(
                    "?" for _ in MIGRATION_CONFIG_RECEIPT_KEYS
                )
                where = f" WHERE config_key NOT IN ({placeholders})"
                parameters = tuple(sorted(MIGRATION_CONFIG_RECEIPT_KEYS))
            elif ignore_migration_fields and table == "runtime_state":
                placeholders = ",".join("?" for _ in MIGRATION_RUNTIME_KEYS)
                where = f" WHERE key NOT IN ({placeholders})"
                parameters = tuple(sorted(MIGRATION_RUNTIME_KEYS))
            selected = ",".join(f'"{column}"' for column in columns)
            rows = [
                list(row)
                for row in connection.execute(
                    f'SELECT {selected} FROM "{table}"{where} ORDER BY {selected}',
                    parameters,
                )
            ]
            payload.append([table, columns, rows])
    finally:
        connection.close()
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def forbidden_ledger_fingerprint(path: Path) -> str:
    return _sha256_bytes(
        _canonical_database_payload(path, ignore_migration_fields=True)
    )


def canonical_database_fingerprint(path: Path) -> str:
    return _sha256_bytes(
        _canonical_database_payload(path, ignore_migration_fields=False)
    )


def verify_offline_migration_delta(before: Path, after: Path) -> None:
    before_fingerprint = forbidden_ledger_fingerprint(before)
    after_fingerprint = forbidden_ledger_fingerprint(after)
    if before_fingerprint != after_fingerprint:
        raise ContractViolation("offline migration changed economic ledger state")


@dataclass(frozen=True)
class DurableReceipt:
    phase: str
    path: Path
    sha256: str
    payload: Mapping[str, Any] | None = None


class ReceiptStore:
    PHASES = (
        "STOP_INTENT",
        "PREPARED",
        "DB_MUTATION_INTENT",
        "START_INTENT",
        "ACCEPTED",
        "COMMITTED",
        "OLD_START_INTENT",
        "FAILED",
    )

    def __init__(self, root: Path):
        self.root = Path(root)

    def path_for(self, phase: str) -> Path:
        if phase not in self.PHASES:
            raise ContractViolation(f"unknown durable receipt phase:{phase}")
        return self.root / f"{phase}.json"

    @property
    def observed_official_path(self) -> Path:
        return self.root / "OFFICIAL_OBSERVED.json"

    def exists(self, phase: str) -> bool:
        path = self.path_for(phase)
        return path.is_file() and not path.is_symlink()

    def read(self, phase: str) -> DurableReceipt:
        path = self.path_for(phase)
        if not path.is_file() or path.is_symlink():
            raise ContractViolation(f"durable receipt missing:{phase}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContractViolation(f"durable receipt invalid:{phase}") from exc
        if not isinstance(payload, dict):
            raise ContractViolation(f"durable receipt invalid:{phase}")
        if payload.get("phase") != phase:
            raise ContractViolation(f"durable receipt phase mismatch:{phase}")
        if not isinstance(payload.get("evidence"), dict):
            raise ContractViolation(f"durable receipt evidence invalid:{phase}")
        previous = payload.get("previous_receipt_sha256")
        if not isinstance(previous, str):
            raise ContractViolation(f"durable receipt predecessor invalid:{phase}")
        return DurableReceipt(phase, path, sha256_file(path), payload)

    def _previous_phase(self, phase: str) -> str | None:
        ordered = self.PHASES[:6]
        if phase == "FAILED" and self.exists("OLD_START_INTENT"):
            return "OLD_START_INTENT"
        if phase in {"OLD_START_INTENT", "FAILED"}:
            for candidate in reversed(ordered):
                if self.exists(candidate):
                    return candidate
            return None
        index = ordered.index(phase)
        return None if index == 0 else ordered[index - 1]

    def validated_phase(self, expected_evidence: Mapping[str, Any]) -> "Phase":
        ordered = self.PHASES[:6]
        seen_gap = False
        previous_sha = ""
        durable = Phase.PREFLIGHT
        phase_values = {
            "STOP_INTENT": Phase.STOP_INTENT,
            "PREPARED": Phase.STOPPED_PREPARED,
            "DB_MUTATION_INTENT": Phase.DB_MUTATION_INTENT,
            "START_INTENT": Phase.START_INTENT,
            "ACCEPTED": Phase.ACCEPTED,
            "COMMITTED": Phase.COMMITTED,
        }
        for phase in ordered:
            path = self.path_for(phase)
            present = path.exists() or path.is_symlink()
            if not present:
                seen_gap = True
                continue
            if seen_gap:
                raise ContractViolation(f"durable receipt chain noncontiguous:{phase}")
            receipt = self.read(phase)
            payload = dict(receipt.payload or {})
            if payload.get("previous_receipt_sha256") != previous_sha:
                raise ContractViolation(f"durable receipt chain mismatch:{phase}")
            evidence = payload.get("evidence")
            for key, expected in expected_evidence.items():
                if evidence.get(key) != expected:
                    raise ContractViolation(f"durable receipt identity mismatch:{phase}:{key}")
            previous_sha = receipt.sha256
            durable = phase_values[phase]
        old_start_path = self.path_for("OLD_START_INTENT")
        if old_start_path.exists() or old_start_path.is_symlink():
            if durable is Phase.PREFLIGHT:
                raise ContractViolation(
                    "durable receipt chain noncontiguous:OLD_START_INTENT"
                )
            old_start = self.read("OLD_START_INTENT")
            old_payload = dict(old_start.payload or {})
            if old_payload.get("previous_receipt_sha256") != previous_sha:
                raise ContractViolation("durable receipt chain mismatch:OLD_START_INTENT")
            old_evidence = old_payload.get("evidence")
            for key, expected in expected_evidence.items():
                if old_evidence.get(key) != expected:
                    raise ContractViolation(
                        f"durable receipt identity mismatch:OLD_START_INTENT:{key}"
                    )
        return durable

    def validated_failure(
        self, expected_evidence: Mapping[str, Any]
    ) -> DurableReceipt:
        receipt = self.read("FAILED")
        payload = dict(receipt.payload or {})
        previous_phase = self._previous_phase("FAILED")
        previous_sha = (
            "" if previous_phase is None else self.read(previous_phase).sha256
        )
        if payload.get("previous_receipt_sha256") != previous_sha:
            raise ContractViolation("durable receipt chain mismatch:FAILED")
        evidence = payload.get("evidence")
        for key, expected in expected_evidence.items():
            if evidence.get(key) != expected:
                raise ContractViolation(
                    f"durable receipt identity mismatch:FAILED:{key}"
                )
        return receipt

    def read_observed_official_identities(
        self, expected_evidence: Mapping[str, Any]
    ) -> set[str]:
        path = self.observed_official_path
        if not path.is_file() or path.is_symlink():
            raise ContractViolation("durable observed identity checkpoint missing")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContractViolation(
                "durable observed identity checkpoint invalid"
            ) from exc
        if not isinstance(payload, Mapping):
            raise ContractViolation("durable observed identity checkpoint invalid")
        for key, expected in expected_evidence.items():
            if payload.get(key) != expected:
                raise ContractViolation(
                    f"durable observed identity checkpoint mismatch:{key}"
                )
        raw = payload.get("observed_official_redemption_identity_hashes")
        if not isinstance(raw, list):
            raise ContractViolation("durable observed identity checkpoint invalid")
        observed = {
            _validate_sha256(str(item), label="durable observed identity checkpoint")
            for item in raw
        }
        if (
            len(observed) != len(raw)
            or payload.get("identity_digest")
            != official_redemption_identity_digest(observed)
        ):
            raise ContractViolation("durable observed identity checkpoint invalid")
        return observed

    def checkpoint_observed_official_identities(
        self,
        expected_evidence: Mapping[str, Any],
        identities: set[str],
    ) -> set[str]:
        validated = {
            _validate_sha256(str(item), label="durable observed identity checkpoint")
            for item in identities
        }
        if len(validated) != len(identities):
            raise ContractViolation("durable observed identity checkpoint duplicated")
        if self.observed_official_path.exists() or self.observed_official_path.is_symlink():
            previous = self.read_observed_official_identities(expected_evidence)
            if not previous.issubset(validated):
                raise ContractViolation("durable observed identity checkpoint regressed")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root_status = self.root.lstat()
        if (
            not stat.S_ISDIR(root_status.st_mode)
            or self.root.is_symlink()
            or root_status.st_uid != os.geteuid()
            or root_status.st_mode & 0o022
        ):
            raise ContractViolation("durable receipt root identity mismatch")
        payload = {
            **dict(expected_evidence),
            "observed_official_redemption_identity_hashes": sorted(validated),
            "identity_digest": official_redemption_identity_digest(validated),
            "written_at_ns": time.time_ns(),
        }
        body = (
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".OFFICIAL_OBSERVED.tmp.", dir=self.root
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o400)
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
        os.replace(temporary, self.observed_official_path)
        directory_descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        return self.read_observed_official_identities(expected_evidence)

    def write(
        self,
        phase: str,
        evidence: Mapping[str, Any],
    ) -> DurableReceipt:
        destination = self.path_for(phase)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root_status = self.root.lstat()
        if (
            not stat.S_ISDIR(root_status.st_mode)
            or self.root.is_symlink()
            or root_status.st_uid != os.geteuid()
            or root_status.st_mode & 0o022
        ):
            raise ContractViolation("durable receipt root identity mismatch")
        if destination.exists() or destination.is_symlink():
            raise ContractViolation(f"durable receipt already exists:{phase}")
        previous_phase = self._previous_phase(phase)
        if previous_phase is None:
            previous_sha = ""
        else:
            try:
                previous_sha = self.read(previous_phase).sha256
            except ContractViolation:
                previous_path = self.path_for(previous_phase)
                if phase != "FAILED" or not previous_path.is_file() or previous_path.is_symlink():
                    raise
                previous_sha = sha256_file(previous_path)
        payload = {
            "phase": phase,
            "evidence": dict(evidence),
            "previous_receipt_sha256": previous_sha,
            "written_at_ns": time.time_ns(),
        }
        body = (
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{phase}.tmp.", dir=self.root
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.close(descriptor)
        os.replace(temporary, destination)
        os.chmod(destination, 0o400)
        directory_descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
        return self.read(phase)


class Phase(IntEnum):
    PREFLIGHT = 0
    STOP_INTENT = 1
    STOPPED_PREPARED = 2
    DB_MUTATION_INTENT = 3
    START_INTENT = 4
    ACCEPTED = 5
    COMMITTED = 6


@dataclass(frozen=True)
class TransactionConfig:
    new_release: Path
    expected_manifest_digest: str
    change_id: str
    snapshot: Path
    current_link: Path = Path("/opt/polymarket-live/current")
    snapshot_root: Path = Path("/var/lib/polymarket-live-release-snapshots")
    runtime_root: Path = Path("/srv/polymarket-live/runtime")
    env_root: Path = Path("/etc/polymarket-live")
    systemd_root: Path = Path("/etc/systemd/system")
    health_status_path: Path = Path(
        "/srv/polymarket-live/runtime/server_health/server_health_status.json"
    )
    venv_python: Path = Path("/opt/polymarket-live/venv/bin/python")
    transaction_lock: Path = Path("/run/lock/polymarket-live-release.lock")
    production: bool = True

    @classmethod
    def from_argv(cls, argv: Sequence[str]) -> "TransactionConfig":
        if len(argv) != 4:
            raise ContractViolation(
                "usage: NEW_RELEASE EXPECTED_MANIFEST_SHA256 CHANGE_ID SNAPSHOT"
            )
        new_release, digest, change_id, snapshot = argv
        if re.fullmatch(r"[A-Za-z0-9._:-]+", change_id) is None:
            raise ContractViolation("invalid change id")
        _validate_sha256(digest, label="caller manifest digest")
        return cls(Path(new_release), digest, change_id, Path(snapshot))


class ReleaseTransaction:
    def __init__(self, config: TransactionConfig, runner: Runner | None = None):
        self.config = config
        self.runner = runner or Runner()
        self.phase = Phase.PREFLIGHT
        self.receipts = ReceiptStore(config.snapshot)
        self._mutation_receipt: DurableReceipt | None = None
        self._start_receipt: DurableReceipt | None = None
        self._accepted_receipt: DurableReceipt | None = None
        self._prestart_lock_held = False
        self._final_lock_held = False
        self.old_release = config.current_link.resolve()
        self.runtimes = {
            spec.key: config.runtime_root / spec.runtime_name for spec in PROFILE_SPECS
        }
        self.coordinator = config.runtime_root / COORDINATOR_RELATIVE
        self.wallet_lock = config.runtime_root / WALLET_LOCK_RELATIVE
        self.migration_stage = Path(f"{config.snapshot}.migration-stage")
        self.env_hashes: dict[str, str] = {}
        self.original_activity: dict[str, str] = {}
        self.original_enablement: dict[str, str] = {}
        self.old_health_state: dict[str, str] = {}
        self.database_metadata: dict[str, FileMetadata] = {}
        self.exact_hashes: dict[str, str] = {}
        self.baselines: dict[str, dict[str, int | str]] = {}
        self.official_redemption_digest = ""
        self.official_redemption_identity_hashes: set[str] = set()
        self.observed_official_redemption_identity_hashes: set[str] = set()
        self._latest_official_identities: dict[str, dict[str, str]] = {}
        self.shared_redemption_transition_baseline = 0
        self.shared_redemption_transition_prefix_sha256 = ""
        self.shared_redemption_receipt_conditions: tuple[str, ...] = ()
        self.shared_redemption_receipt_rows_json = ""
        self.shared_redemption_allocation_rows_json = ""
        self.final_evidence: dict[str, Any] = {}
        self.restart_baselines: dict[str, int] = {}
        self.candidate_start_boundary_ns = 0
        self.candidate_status_mtime_baselines: dict[str, int] = {}
        self._wallet_lock_handle: Any | None = None
        self._profile_lock_handles: dict[str, Any] = {}
        self._transaction_lock_handle: Any | None = None
        self.pre_stop_fingerprints: dict[str, str] = {}
        self.exact_snapshot_receipt_hash = ""
        self.old_artifact_hashes: dict[str, str] = {}
        self.health_baseline_mtime_ns = 0
        self.manager_timeout_usec = (
            0 if config.production else LOCAL_TEST_MANAGER_TIMEOUT_USEC
        )
        self._failure_reason = ""
        self._cleanup_results: dict[str, str] = {}
        self.migration_stage_evidence: dict[str, Any] = {
            "state": "NOT_INSPECTED",
            "path": str(self.migration_stage),
        }
        self._stage_seal_step = "not_started"
        self._recovery_handled = False

    def acquire_transaction_lock(self) -> None:
        if self._transaction_lock_handle is not None:
            return
        if self.config.production and os.geteuid() != 0:
            raise ContractViolation("production controller requires euid 0")
        self.config.transaction_lock.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            self.config.transaction_lock,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        handle = os.fdopen(descriptor, "a+")
        status = os.fstat(handle.fileno())
        expected_uid = 0 if self.config.production else os.getuid()
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != expected_uid
            or stat.S_IMODE(status.st_mode) != 0o600
        ):
            handle.close()
            raise ContractViolation("transaction lock identity mismatch")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise ContractViolation("transaction lock is already held") from exc
        self._transaction_lock_handle = handle

    def release_transaction_lock(self) -> None:
        if self._transaction_lock_handle is None:
            return
        fcntl.flock(self._transaction_lock_handle.fileno(), fcntl.LOCK_UN)
        self._transaction_lock_handle.close()
        self._transaction_lock_handle = None

    def _systemctl(
        self, *arguments: str, check: bool = True
    ) -> CommandResult:
        return self.runner.run(
            ("/usr/bin/sudo", "-n", "/usr/bin/systemctl", *arguments),
            check=check,
        )

    def _manager_timeout_usec(self) -> int:
        if not self.config.production:
            return LOCAL_TEST_MANAGER_TIMEOUT_USEC
        result = self.runner.run(
            (
                "/usr/bin/sudo",
                "-n",
                "/usr/bin/busctl",
                "get-property",
                "org.freedesktop.systemd1",
                "/org/freedesktop/systemd1",
                "org.freedesktop.systemd1.Manager",
                "DefaultTimeoutStartUSec",
            )
        )
        fields = result.stdout.split()
        if len(fields) != 2 or fields[0] != "t" or not fields[1].isdigit():
            raise ContractViolation("systemd manager timeout is invalid")
        timeout = int(fields[1])
        if timeout <= 0:
            raise ContractViolation("systemd manager timeout is nonpositive")
        return timeout

    def _sqlite_timeout_ms(self) -> int:
        timeout_usec = self._manager_timeout_usec()
        return (timeout_usec + 999) // 1000

    def _deadline(self) -> float:
        return time.monotonic() + self._manager_timeout_usec() / 1_000_000

    def _property(self, unit: str, property_name: str) -> str:
        result = self._systemctl(
            "show", f"--property={property_name}", "--value", unit
        )
        return result.stdout.strip()

    def _active(self, unit: str) -> bool:
        return self._systemctl("is-active", "--quiet", unit, check=False).returncode == 0

    def _enabled(self, unit: str) -> str:
        result = self._systemctl("is-enabled", unit, check=False)
        state = result.stdout.strip()
        if not state:
            raise ContractViolation(f"unit enablement unavailable:{unit}")
        return state

    def _live_database(self, spec: ProfileSpec) -> Path:
        return self.runtimes[spec.key] / "live.sqlite3"

    def _env_path(self, spec: ProfileSpec) -> Path:
        return self.config.env_root / spec.env_name

    def _assert_tree_immutable(self, root: Path, *, label: str) -> None:
        if not root.is_dir() or root.is_symlink():
            raise ContractViolation(f"{label} tree is not a regular directory")
        expected_uid = 0 if self.config.production else os.getuid()
        expected_gid = 0 if self.config.production else os.getgid()
        for path in (root, *root.rglob("*")):
            status = path.lstat()
            if stat.S_ISLNK(status.st_mode):
                raise ContractViolation(f"{label} tree contains symlink:{path}")
            if status.st_uid != expected_uid or status.st_gid != expected_gid:
                raise ContractViolation(f"{label} tree ownership mismatch:{path}")
            if status.st_mode & 0o022:
                raise ContractViolation(f"{label} tree is group/world writable:{path}")

    def _verify_existing_manifest(self, root: Path) -> None:
        manifest = root / "MANIFEST.sha256"
        if not manifest.is_file() or manifest.is_symlink():
            raise ContractViolation("old release manifest missing")
        records = _manifest_records(manifest)
        for digest, relative, _ in records:
            path = root / relative
            if not path.is_file() or path.is_symlink() or sha256_file(path) != digest:
                raise ContractViolation(f"old release manifest mismatch:{relative}")
        required = {
            "app/cd90_live_copy.py",
            "tools/assert_no_authenticated_open_orders.py",
        }
        if required - {relative for _, relative, _ in records}:
            raise ContractViolation("old release audit tools are not manifest bound")

    def _static_compile_candidate(self) -> None:
        sources = [self.config.new_release / asset for asset in REQUIRED_ASSETS if asset.endswith(".py")]
        program = """
import ast, pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
for raw in sys.argv[2:]:
    path = pathlib.Path(raw).resolve()
    if root not in path.parents:
        raise SystemExit('candidate static source escaped release')
    source = path.read_text(encoding='utf-8')
    compile(ast.parse(source, filename=str(path)), str(path), 'exec', dont_inherit=True)
"""
        if self.config.production:
            argv = (
                "/usr/bin/sudo",
                "-n",
                "-u",
                "polymarket-live",
                "/usr/bin/env",
                "-i",
                f"PATH={TRUSTED_PATH}",
                "LANG=C.UTF-8",
                "LC_ALL=C.UTF-8",
                "PYTHONUTF8=1",
                "PYTHONDONTWRITEBYTECODE=1",
                str(self.config.venv_python),
                "-I",
                "-c",
                program,
                str(self.config.new_release),
                *(str(path) for path in sources),
            )
            self.runner.run(argv)
        else:
            completed = subprocess.run(
                [sys.executable, "-I", "-c", program, str(self.config.new_release), *(str(path) for path in sources)],
                text=True,
                capture_output=True,
                check=False,
            )
            if completed.returncode:
                raise ContractViolation(completed.stderr.strip())

    def _verify_env_files(self, *, capture: bool) -> None:
        current: dict[str, str] = {}
        for spec in PROFILE_SPECS:
            path = self._env_path(spec)
            try:
                status = path.lstat()
            except FileNotFoundError as exc:
                raise ContractViolation(f"environment file missing:{path}") from exc
            expected_uid = 0 if self.config.production else os.getuid()
            if (
                not stat.S_ISREG(status.st_mode)
                or path.is_symlink()
                or status.st_uid != expected_uid
                or status.st_mode & 0o022
            ):
                raise ContractViolation(f"environment file identity mismatch:{path}")
            current[spec.key] = sha256_file(path)
        if capture:
            self.env_hashes = current
        elif current != self.env_hashes:
            raise ContractViolation("environment file hash drift")

    def _verify_database_file_identity(self) -> None:
        if self.config.production:
            try:
                expected_uid = pwd.getpwnam("polymarket-live").pw_uid
                expected_gid = grp.getgrnam("polymarket-live").gr_gid
            except KeyError as exc:
                raise ContractViolation("polymarket-live account identity missing") from exc
        else:
            expected_uid = os.getuid()
            expected_gid = os.getgid()
        for key, database in self._exact_snapshot_sources().items():
            parent = database.parent
            try:
                parent_status = parent.lstat()
                database_status = database.lstat()
            except FileNotFoundError as exc:
                raise ContractViolation(f"database metadata missing:{key}") from exc
            if (
                not stat.S_ISDIR(parent_status.st_mode)
                or parent.is_symlink()
                or parent.resolve() != parent.absolute()
                or parent_status.st_uid != expected_uid
                or parent_status.st_gid != expected_gid
                or stat.S_IMODE(parent_status.st_mode) != 0o700
            ):
                raise ContractViolation(f"database metadata parent mismatch:{key}")
            mode = stat.S_IMODE(database_status.st_mode)
            if (
                not stat.S_ISREG(database_status.st_mode)
                or database.is_symlink()
                or database.resolve() != database.absolute()
                or database_status.st_nlink != 1
                or database_status.st_uid != expected_uid
                or database_status.st_gid != expected_gid
                or mode != 0o600
            ):
                raise ContractViolation(f"database metadata mismatch:{key}")

    def _verify_database_gates(
        self, *, cutover: bool, full_integrity: bool = True
    ) -> None:
        if self.config.production:
            self._verify_database_file_identity()
        ledgers = {
            spec.key: self._live_database(spec) for spec in PROFILE_SPECS
        }
        for spec in PROFILE_SPECS:
            verify_local_storage(
                ledgers[spec.key],
                cutover=cutover,
                full_integrity=full_integrity,
            )
        verify_coordinator_storage(
            self.coordinator,
            cutover=cutover,
            full_integrity=full_integrity,
        )
        verify_shared_wallet_topology(
            self.coordinator,
            ledgers,
            self.wallet_lock,
            # Coordinator storage above already performs the full scan.
            full_integrity=False,
        )

    def _capture_runtime_baselines(self) -> None:
        integer_keys = (
            "last_processed_block",
            "last_successful_cycle_at_ms",
            "hot_standby_joined_at_ms",
            "hot_standby_primary_runtime_lock_seen_at_ms",
            "hot_standby_last_observed_head",
        )
        text_keys = (
            "operator_planned_resume_change_id",
            "operator_planned_resume_state",
            "operator_planned_resume_from_block",
            "operator_pre_repair_forward_recovery_armed",
        )
        captured: dict[str, dict[str, int | str]] = {}
        for spec in PROFILE_SPECS:
            database = self._live_database(spec)
            values: dict[str, int | str] = {}
            connection = _ro_connection(database)
            try:
                for key in integer_keys:
                    value = connection.execute(
                        "SELECT COALESCE((SELECT value FROM runtime_state "
                        "WHERE key=?),'0')",
                        (key,),
                    ).fetchone()[0]
                    try:
                        values[key] = int(value)
                    except (TypeError, ValueError) as exc:
                        raise ContractViolation(
                            f"invalid runtime baseline:{spec.key}:{key}"
                        ) from exc
                for key in text_keys:
                    values[key] = str(
                        connection.execute(
                            "SELECT COALESCE((SELECT value FROM runtime_state "
                            "WHERE key=?),'')",
                            (key,),
                        ).fetchone()[0]
                        or ""
                    )
                for table, label in (
                    ("runtime_errors", "runtime_error_id"),
                    ("action_transitions", "action_transition_id"),
                    ("repair_recovery_transitions", "repair_transition_id"),
                    ("redemption_transitions", "redemption_transition_id"),
                ):
                    values[label] = int(
                        connection.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}").fetchone()[0]
                    )
                values["repair_manifest_hashes_json"] = json.dumps(
                    [
                        str(row[0])
                        for row in connection.execute(
                            "SELECT manifest_hash FROM repair_recovery_manifests "
                            "ORDER BY manifest_hash"
                        )
                    ],
                    separators=(",", ":"),
                )
                for table, baseline_key, digest_key in _LOCAL_APPEND_ONLY_BASELINES:
                    values[digest_key] = append_only_prefix_digest(
                        connection,
                        table=table,
                        baseline=int(values[baseline_key]),
                        label=f"{table}:{spec.key}",
                    )
                values["redemption_receipt_conditions_json"] = json.dumps(
                    redemption_receipt_condition_ids(
                        connection,
                        table="redemption_receipts",
                        label=f"redemption:{spec.key}",
                    ),
                    separators=(",", ":"),
                )
                values["redemption_receipt_rows_json"] = json.dumps(
                    redemption_condition_table_snapshot(
                        connection,
                        table="redemption_receipts",
                        label=f"redemption:{spec.key}",
                    ),
                    sort_keys=True,
                    separators=(",", ":"),
                )
            finally:
                connection.close()
            captured[spec.key] = values
        coordinator_connection = _ro_connection(self.coordinator)
        try:
            shared_redemption_transition_baseline = int(
                coordinator_connection.execute(
                    "SELECT COALESCE(MAX(id),0) FROM shared_condition_transitions"
                ).fetchone()[0]
            )
            shared_redemption_transition_prefix_sha256 = append_only_prefix_digest(
                coordinator_connection,
                table="shared_condition_transitions",
                baseline=shared_redemption_transition_baseline,
                label="shared-redemption",
            )
            shared_redemption_receipt_conditions = redemption_receipt_condition_ids(
                coordinator_connection,
                table="shared_condition_redemptions",
                label="shared-redemption",
            )
            shared_redemption_receipt_rows_json = json.dumps(
                redemption_condition_table_snapshot(
                    coordinator_connection,
                    table="shared_condition_redemptions",
                    label="shared-redemption",
                ),
                sort_keys=True,
                separators=(",", ":"),
            )
            shared_redemption_allocation_rows_json = json.dumps(
                redemption_condition_table_snapshot(
                    coordinator_connection,
                    table="shared_condition_allocations",
                    label="shared-redemption-allocations",
                ),
                sort_keys=True,
                separators=(",", ":"),
            )
        finally:
            coordinator_connection.close()
        # Do not destroy the last durable in-memory baseline if any read in the
        # replacement capture fails.  Commit the complete snapshot at once.
        self.baselines = captured
        self.shared_redemption_transition_baseline = (
            shared_redemption_transition_baseline
        )
        self.shared_redemption_transition_prefix_sha256 = (
            shared_redemption_transition_prefix_sha256
        )
        self.shared_redemption_receipt_conditions = (
            shared_redemption_receipt_conditions
        )
        self.shared_redemption_receipt_rows_json = (
            shared_redemption_receipt_rows_json
        )
        self.shared_redemption_allocation_rows_json = (
            shared_redemption_allocation_rows_json
        )

    def _verify_redemption_history_prefixes(self) -> None:
        for spec in PROFILE_SPECS:
            baseline = self.baselines.get(spec.key)
            if not isinstance(baseline, Mapping):
                raise ContractViolation(
                    f"redemption history baseline missing:{spec.key}"
                )
            try:
                raw_conditions = json.loads(
                    str(baseline["redemption_receipt_conditions_json"])
                )
                expected_receipt_snapshot = json.loads(
                    str(baseline["redemption_receipt_rows_json"])
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ContractViolation(
                    f"redemption history baseline invalid:{spec.key}"
                ) from exc
            if not isinstance(raw_conditions, list):
                raise ContractViolation(
                    f"redemption receipt condition baseline invalid:{spec.key}"
                )
            expected_conditions = tuple(str(item).lower() for item in raw_conditions)
            if (
                list(expected_conditions) != sorted(expected_conditions)
                or any(not item for item in expected_conditions)
                or len(set(expected_conditions)) != len(expected_conditions)
            ):
                raise ContractViolation(
                    f"redemption receipt condition baseline invalid:{spec.key}"
                )
            connection = _ro_connection(self._live_database(spec))
            try:
                for table, baseline_key, digest_key in _LOCAL_APPEND_ONLY_BASELINES:
                    try:
                        table_baseline = int(baseline[baseline_key])
                        expected_table_digest = _validate_sha256(
                            str(baseline[digest_key]),
                            label=f"{table} prefix:{spec.key}",
                        )
                    except (KeyError, TypeError, ValueError) as exc:
                        raise ContractViolation(
                            f"immutable history baseline invalid:{spec.key}:{table}"
                        ) from exc
                    actual_table_digest = append_only_prefix_digest(
                        connection,
                        table=table,
                        baseline=table_baseline,
                        label=f"{table}:{spec.key}",
                    )
                    if actual_table_digest != expected_table_digest:
                        raise ContractViolation(
                            f"immutable history prefix changed:{spec.key}:{table}"
                        )
                current_conditions = set(
                    redemption_receipt_condition_ids(
                        connection,
                        table="redemption_receipts",
                        label=f"redemption:{spec.key}",
                    )
                )
                current_receipt_snapshot = redemption_condition_table_snapshot(
                    connection,
                    table="redemption_receipts",
                    label=f"redemption:{spec.key}",
                )
                verify_condition_snapshot_progression(
                    connection,
                    expected=expected_receipt_snapshot,
                    current=current_receipt_snapshot,
                    transition_table="redemption_transitions",
                    transition_baseline=int(baseline["redemption_transition_id"]),
                    receipt_table="redemption_receipts",
                    label=f"redemption:{spec.key}",
                )
            finally:
                connection.close()
            if not set(expected_conditions).issubset(current_conditions):
                raise ContractViolation(
                    f"redemption receipt condition regressed:{spec.key}"
                )

        expected_shared_digest = _validate_sha256(
            self.shared_redemption_transition_prefix_sha256,
            label="shared redemption transition prefix",
        )
        expected_shared_conditions = tuple(
            str(item).lower() for item in self.shared_redemption_receipt_conditions
        )
        if (
            list(expected_shared_conditions) != sorted(expected_shared_conditions)
            or any(not item for item in expected_shared_conditions)
            or len(set(expected_shared_conditions)) != len(expected_shared_conditions)
        ):
            raise ContractViolation("shared redemption receipt condition baseline invalid")
        try:
            expected_shared_receipts = json.loads(
                self.shared_redemption_receipt_rows_json
            )
            expected_shared_allocations = json.loads(
                self.shared_redemption_allocation_rows_json
            )
        except json.JSONDecodeError as exc:
            raise ContractViolation("shared redemption snapshot baseline invalid") from exc
        connection = _ro_connection(self.coordinator)
        try:
            actual_shared_digest = append_only_prefix_digest(
                connection,
                table="shared_condition_transitions",
                baseline=self.shared_redemption_transition_baseline,
                label="shared-redemption",
            )
            current_shared_conditions = set(
                redemption_receipt_condition_ids(
                    connection,
                    table="shared_condition_redemptions",
                    label="shared-redemption",
                )
            )
            current_shared_receipts = redemption_condition_table_snapshot(
                connection,
                table="shared_condition_redemptions",
                label="shared-redemption",
            )
            current_shared_allocations = redemption_condition_table_snapshot(
                connection,
                table="shared_condition_allocations",
                label="shared-redemption-allocations",
            )
            verify_condition_snapshot_progression(
                connection,
                expected=expected_shared_receipts,
                current=current_shared_receipts,
                transition_table="shared_condition_transitions",
                transition_baseline=self.shared_redemption_transition_baseline,
                receipt_table="shared_condition_redemptions",
                label="shared-redemption",
            )
            verify_condition_snapshot_progression(
                connection,
                expected=expected_shared_allocations,
                current=current_shared_allocations,
                transition_table="shared_condition_transitions",
                transition_baseline=self.shared_redemption_transition_baseline,
                receipt_table="shared_condition_redemptions",
                label="shared-redemption-allocations",
            )
        finally:
            connection.close()
        if actual_shared_digest != expected_shared_digest:
            raise ContractViolation("shared redemption transition prefix changed")
        if not set(expected_shared_conditions).issubset(current_shared_conditions):
            raise ContractViolation("shared redemption receipt condition regressed")

    def _capture_original_service_state(self) -> None:
        if not self.config.production:
            self.original_activity = {unit: "active" for unit in EXECUTOR_UNITS}
            self.original_enablement = {unit: "enabled" for unit in EXECUTOR_UNITS}
            self.original_enablement[HEALTH_TIMER] = "disabled"
            self.old_health_state = {
                "timer_active": "inactive",
                "service_active": "inactive",
                "timer_enabled": "disabled",
            }
            return
        self.original_activity = {
            unit: self._property(unit, "ActiveState") for unit in EXECUTOR_UNITS
        }
        self.original_enablement = {
            unit: self._property(unit, "UnitFileState")
            for unit in (*EXECUTOR_UNITS, HEALTH_TIMER)
        }
        self._validate_original_executor_policy()
        self.old_health_state = {
            "timer_active": self._property(HEALTH_TIMER, "ActiveState"),
            "service_active": self._property(HEALTH_UNIT, "ActiveState"),
            "timer_enabled": self.original_enablement[HEALTH_TIMER],
        }

    def _validate_original_executor_policy(self) -> None:
        if (
            not self.config.production
            and not self.original_activity
            and not self.original_enablement
        ):
            self.original_activity = {
                unit: "active" for unit in EXECUTOR_UNITS
            }
            self.original_enablement = {
                **{unit: "enabled" for unit in EXECUTOR_UNITS},
                HEALTH_TIMER: "disabled",
            }
        if set(self.original_activity) != set(EXECUTOR_UNITS):
            raise ContractViolation("original executor activity set mismatch")
        if set(self.original_enablement) != {*EXECUTOR_UNITS, HEALTH_TIMER}:
            raise ContractViolation("original executor enablement set mismatch")
        if any(
            self.original_activity[unit] not in {"active", "inactive"}
            for unit in EXECUTOR_UNITS
        ):
            raise ContractViolation("original executor activity state invalid")
        if any(
            self.original_enablement[unit] not in {"enabled", "disabled"}
            for unit in (*EXECUTOR_UNITS, HEALTH_TIMER)
        ):
            raise ContractViolation("original executor enablement state invalid")
        for spec in PROFILE_SPECS:
            activities = tuple(
                self.original_activity[unit]
                for unit in (spec.primary_unit, spec.standby_unit)
            )
            enablement = tuple(
                self.original_enablement[unit]
                for unit in (spec.primary_unit, spec.standby_unit)
            )
            fully_active = activities == ("active", "active")
            fully_paused = activities == ("inactive", "inactive") and enablement == (
                "disabled",
                "disabled",
            )
            if not fully_active and not fully_paused:
                raise ContractViolation(
                    f"executor profile policy is partial or unsafe:{spec.key}:"
                    f"{activities}:{enablement}"
                )

    def _profile_original_mode(self, spec: ProfileSpec) -> str:
        self._validate_original_executor_policy()
        if self.original_activity[spec.primary_unit] == "active":
            return "ACTIVE"
        return "PAUSED"

    def _original_active_executor_units(self) -> tuple[str, ...]:
        self._validate_original_executor_policy()
        return tuple(
            unit
            for unit in EXECUTOR_UNITS
            if self.original_activity[unit] == "active"
        )

    def _change_ids_unused(self) -> None:
        for spec in PROFILE_SPECS:
            expected = f"{self.config.change_id}-{spec.change_suffix}"
            actual = sqlite_scalar_ro(
                self._live_database(spec),
                "SELECT COALESCE((SELECT value FROM runtime_state "
                "WHERE key='operator_planned_resume_change_id'),'')",
            )
            if str(actual) == expected:
                raise ContractViolation(f"operator change id was already used:{expected}")

    def _official_digest_and_order_gate(self) -> str:
        if not self.config.production:
            self._latest_official_identities = {}
            return official_redemption_identity_digest(())
        if self.manager_timeout_usec <= 0:
            self.manager_timeout_usec = self._manager_timeout_usec()
        configure_root_live_read_timeout(self.manager_timeout_usec)
        deadline = time.monotonic() + self.manager_timeout_usec / 1_000_000

        def remaining_seconds() -> float:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ContractViolation("official side-effect gate timed out")
            return remaining

        env_path = self._env_path(PROFILE_SPECS[0])
        audit_tool = self.old_release / "tools/assert_no_authenticated_open_orders.py"
        self.runner.run(
            (
                "/usr/bin/sudo",
                "-n",
                "-u",
                "polymarket-live",
                "/usr/bin/env",
                "-i",
                f"PATH={TRUSTED_PATH}",
                "PYTHONDONTWRITEBYTECODE=1",
                f"AUDIT_RELEASE={self.old_release}",
                "/bin/sh",
                "-ceu",
                'set -a; . "$1"; set +a; cd "$AUDIT_RELEASE/app"; '
                'exec /opt/polymarket-live/venv/bin/python -I "$AUDIT_RELEASE/tools/assert_no_authenticated_open_orders.py"',
                "sh",
                str(env_path),
            ),
            timeout_seconds=remaining_seconds(),
        )
        program = """
import json, os, sys
from pathlib import Path
app = Path(os.environ['AUDIT_RELEASE']) / 'app'
sys.path.insert(0, str(app))
from cd90_live_copy import fetch_official_redemption_activities
wallet = str(os.environ.get('POLYMARKET_FUNDER_ADDRESS') or '').lower()
rows = fetch_official_redemption_activities(wallet)
if not isinstance(rows, list):
    raise SystemExit('official redemption activity response is not a list')
print(json.dumps({'wallet': wallet, 'rows': rows}, sort_keys=True, separators=(',', ':'), default=str))
"""
        result = self.runner.run(
            (
                "/usr/bin/sudo",
                "-n",
                "-u",
                "polymarket-live",
                "/usr/bin/env",
                "-i",
                f"PATH={TRUSTED_PATH}",
                "PYTHONDONTWRITEBYTECODE=1",
                f"AUDIT_RELEASE={self.old_release}",
                "/bin/sh",
                "-ceu",
                'set -a; . "$1"; set +a; cd "$AUDIT_RELEASE/app"; '
                'exec /opt/polymarket-live/venv/bin/python -I -c "$2"',
                "sh",
                str(env_path),
                program,
            ),
            timeout_seconds=remaining_seconds(),
        )
        encoded = result.stdout.strip().splitlines()[-1] if result.stdout.strip() else ""
        try:
            payload = json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise ContractViolation("official redemption activity payload invalid") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("rows"), list):
            raise ContractViolation("official redemption activity payload invalid")
        identities = normalize_official_redemption_identities(
            payload["rows"], str(payload.get("wallet") or "")
        )
        self._latest_official_identities = identities
        return official_redemption_identity_digest(identities)

    def _observed_identity_checkpoint_evidence(self) -> dict[str, str]:
        return {
            "change_id": self.config.change_id,
            "manifest_sha256": self.config.expected_manifest_digest,
            "new_release": str(self.config.new_release),
        }

    def _checkpoint_observed_official_identities(self) -> None:
        persisted = self.receipts.checkpoint_observed_official_identities(
            self._observed_identity_checkpoint_evidence(),
            set(self.observed_official_redemption_identity_hashes),
        )
        if persisted != self.observed_official_redemption_identity_hashes:
            raise ContractViolation("durable observed identity checkpoint mismatch")

    def _conserve_latest_official_redemption_snapshot(
        self, digest: str
    ) -> dict[str, Any]:
        verify_official_redemption_history(
            set(self.observed_official_redemption_identity_hashes),
            self._latest_official_identities,
        )
        # Once the official source has exposed an identity, preserve that
        # monotonic observation even if its local owner has not become durable
        # yet.  FAILED evidence then prevents a later API omission from hiding
        # an already observed wallet side effect.
        self.observed_official_redemption_identity_hashes = set(
            self._latest_official_identities
        )
        self._checkpoint_observed_official_identities()
        evidence = verify_official_redemption_conservation(
            baseline_identity_hashes=set(self.official_redemption_identity_hashes),
            current_identities=self._latest_official_identities,
            ledgers={
                spec.key: self._live_database(spec) for spec in PROFILE_SPECS
            },
            coordinator=self.coordinator,
            transition_baselines=self.baselines,
            shared_transition_baseline=self.shared_redemption_transition_baseline,
        )
        return {
            **evidence,
            "identity_digest": digest,
            "observed_identity_count": len(
                self.observed_official_redemption_identity_hashes
            ),
        }

    def _verify_official_redemption_conservation_current(self) -> dict[str, Any]:
        self._verify_redemption_history_prefixes()
        digest = self._official_digest_and_order_gate()
        return self._conserve_latest_official_redemption_snapshot(digest)

    def _verify_sandbox_capability(self) -> None:
        if not self.config.production:
            return
        program = """
import _socket
from pathlib import Path
for path, write in ((Path('/etc/polymarket-live/cd90-live.env'), False), (Path('/srv/polymarket-live/runtime/probe'), True)):
    try:
        path.write_text('x') if write else path.read_bytes()
    except OSError:
        pass
    else:
        raise SystemExit('sandbox filesystem capability leak')
s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
try:
    try:
        s.connect(('192.0.2.1', 9))
    except OSError:
        pass
    else:
        raise SystemExit('sandbox network capability leak')
finally:
    s.close()
"""
        shell = """
mount --make-rprivate /
mount --bind / /
mount -o remount,bind,ro /
mount -t tmpfs -o mode=000,nosuid,nodev,noexec denied-env /etc/polymarket-live
mount -t tmpfs -o mode=000,nosuid,nodev,noexec denied-runtime /srv/polymarket-live/runtime
mount -t tmpfs -o mode=0700,nosuid,nodev,noexec isolated-tmp /tmp
exec /usr/bin/setpriv --reuid=polymarket-live --regid=polymarket-live --clear-groups --no-new-privs \
 /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 \
 /opt/polymarket-live/venv/bin/python -I -c "$1"
"""
        self.runner.run(
            (
                "/usr/bin/unshare",
                "--mount",
                "--net",
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                "--mount-proc",
                "/bin/sh",
                "-ceu",
                shell,
                "sh",
                program,
            ),
            parent_death_signal=signal.SIGKILL,
        )

    def _verify_old_fleet_identity(self) -> None:
        if not self.config.production:
            return
        self._validate_original_executor_policy()
        expected_python = Path("/opt/polymarket-live/venv/bin/python").resolve()
        for spec in PROFILE_SPECS:
            for unit, mode in (
                (spec.primary_unit, "--run"),
                (spec.standby_unit, "--run-hot-standby"),
            ):
                installed = self.config.systemd_root / unit
                expected = self.old_release / "systemd" / unit
                if (
                    not installed.is_file()
                    or installed.is_symlink()
                    or installed.read_bytes() != expected.read_bytes()
                ):
                    raise ContractViolation(f"installed old unit mismatch:{unit}")
                installed_status = installed.stat(follow_symlinks=False)
                if (
                    installed_status.st_uid != 0
                    or installed_status.st_gid != 0
                    or stat.S_IMODE(installed_status.st_mode) != 0o644
                ):
                    raise ContractViolation(f"installed old unit metadata mismatch:{unit}")
                if self._property(unit, "FragmentPath") != str(installed):
                    raise ContractViolation(f"unit fragment identity mismatch:{unit}")
                if self._property(unit, "DropInPaths"):
                    raise ContractViolation(f"unit drop-in forbidden:{unit}")
                current_activity = self._property(unit, "ActiveState")
                current_enablement = self._property(unit, "UnitFileState")
                if current_activity != self.original_activity[unit]:
                    raise ContractViolation(f"old unit activity drift:{unit}")
                if current_enablement != self.original_enablement[unit]:
                    raise ContractViolation(f"old unit enablement drift:{unit}")
                if self._profile_original_mode(spec) == "PAUSED":
                    if self._property(unit, "MainPID") != "0":
                        raise ContractViolation(f"paused old unit PID is nonzero:{unit}")
                    continue
                pid_text = self._property(unit, "MainPID")
                if not pid_text.isdigit() or int(pid_text) <= 0:
                    raise ContractViolation(f"old unit main pid invalid:{unit}")
                pid = int(pid_text)
                if Path(f"/proc/{pid}/cwd").resolve() != self.old_release / "app":
                    raise ContractViolation(f"old unit cwd mismatch:{unit}")
                if Path(f"/proc/{pid}/exe").resolve() != expected_python:
                    raise ContractViolation(f"old unit executable mismatch:{unit}")
                command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
                expected_command = (
                    f"/opt/polymarket-live/current/app/{spec.app_script} {mode} "
                    f"--runtime-dir {self.runtimes[spec.key]}"
                )
                if expected_command not in command:
                    raise ContractViolation(f"old unit command mismatch:{unit}")
        self.verify_old_health_artifacts(
            unit_root=self.config.systemd_root,
            bridge=Path("/usr/local/sbin") / HEALTH_BRIDGE,
            heartbeat=Path(
                "/usr/local/libexec/polymarket/server_health_heartbeat.py"
            ),
        )

    def _verify_snapshot_space(self, copies: int) -> None:
        database_bytes = sum(
            self._live_database(spec).stat().st_size for spec in PROFILE_SPECS
        ) + self.coordinator.stat().st_size
        required = database_bytes * copies
        available = shutil.disk_usage(self.config.snapshot_root.parent).free
        if available < required:
            raise ContractViolation(
                f"snapshot free space insufficient:available={available}:required={required}"
            )

    def _verify_snapshot_directory_identity(self) -> None:
        expected_uid = 0 if self.config.production else os.getuid()
        expected_gid = 0 if self.config.production else os.getgid()
        for path, mode in (
            (self.config.snapshot_root, 0o711 if self.config.production else 0o755),
            (self.config.snapshot, 0o700),
        ):
            status = path.lstat()
            if (
                not stat.S_ISDIR(status.st_mode)
                or path.is_symlink()
                or status.st_uid != expected_uid
                or status.st_gid != expected_gid
                or stat.S_IMODE(status.st_mode) != mode
            ):
                raise ContractViolation(f"snapshot directory identity mismatch:{path}")

    def _prepare_snapshot_directories(self) -> None:
        if self.config.production:
            self.runner.run(
                (
                    "/usr/bin/sudo",
                    "-n",
                    "/usr/bin/install",
                    "-d",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0711",
                    str(self.config.snapshot_root),
                )
            )
            self.runner.run(
                (
                    "/usr/bin/sudo",
                    "-n",
                    "/usr/bin/install",
                    "-d",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0700",
                    str(self.config.snapshot),
                )
            )
        else:
            self.config.snapshot_root.mkdir(parents=True, exist_ok=True)
            self.config.snapshot.mkdir(mode=0o700, exist_ok=True)
            self.config.snapshot.chmod(0o700)
        self._verify_snapshot_directory_identity()

    def _verify_service_lock_files(self) -> None:
        if not self.config.production:
            return
        self._verify_runtime_lock_parent()
        expected_uid, expected_gid = self._service_account_ids()
        paths = {"shared-wallet": self.wallet_lock}
        for spec in PROFILE_SPECS:
            legacy = self._legacy_profile_lock(spec)
            # The current executor contract uses the root-level candidate lock.
            # Older profiles may still retain a directory-local lock; validate
            # it when present, but do not invent one for profiles onboarded
            # after that legacy path was retired.
            if legacy.exists():
                paths[f"legacy-profile:{spec.key}"] = legacy
            paths[f"candidate-profile:{spec.key}"] = self._candidate_profile_lock(spec)
        for label, path in paths.items():
            try:
                status = path.lstat()
            except FileNotFoundError as exc:
                raise ContractViolation(f"lock metadata missing:{label}") from exc
            mode = stat.S_IMODE(status.st_mode)
            if (
                not stat.S_ISREG(status.st_mode)
                or path.is_symlink()
                or status.st_nlink != 1
                or status.st_uid != expected_uid
                or status.st_gid != expected_gid
                or mode & 0o600 != 0o600
                or mode & 0o022
            ):
                raise ContractViolation(f"lock metadata mismatch:{label}")

    def _service_account_ids(self) -> tuple[int, int]:
        if not self.config.production:
            return os.getuid(), os.getgid()
        uid_text = self.runner.run(
            ("/usr/bin/id", "-u", "polymarket-live")
        ).stdout.strip()
        gid_text = self.runner.run(
            ("/usr/bin/id", "-g", "polymarket-live")
        ).stdout.strip()
        if not uid_text.isdigit() or not gid_text.isdigit():
            raise ContractViolation("service account lock identity is invalid")
        return int(uid_text), int(gid_text)

    def _legacy_profile_lock(self, spec: ProfileSpec) -> Path:
        return self.runtimes[spec.key] / "live.lock"

    def _candidate_profile_lock(self, spec: ProfileSpec) -> Path:
        return self.config.runtime_root / f"{spec.runtime_name}.lock"

    def _verify_runtime_lock_parent(self) -> None:
        try:
            status = self.config.runtime_root.lstat()
        except FileNotFoundError as exc:
            raise ContractViolation("runtime lock parent missing") from exc
        expected_uid = 0 if self.config.production else os.getuid()
        expected_gid = 0 if self.config.production else os.getgid()
        if (
            not stat.S_ISDIR(status.st_mode)
            or self.config.runtime_root.is_symlink()
            or status.st_uid != expected_uid
            or status.st_gid != expected_gid
            or stat.S_IMODE(status.st_mode) & 0o022
        ):
            raise ContractViolation("runtime lock parent metadata mismatch")

    def _prepare_candidate_profile_locks(self) -> None:
        self._verify_runtime_lock_parent()
        expected_uid, expected_gid = self._service_account_ids()
        for spec in PROFILE_SPECS:
            lock = self._candidate_profile_lock(spec)
            try:
                descriptor = os.open(
                    lock,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            except FileExistsError:
                descriptor = -1
            if descriptor >= 0:
                try:
                    os.fchown(descriptor, expected_uid, expected_gid)
                    os.fchmod(descriptor, 0o600)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                fsync_directory(self.config.runtime_root)
            try:
                status = lock.lstat()
            except FileNotFoundError as exc:
                raise ContractViolation(
                    f"candidate profile lock missing:{spec.key}"
                ) from exc
            if (
                not stat.S_ISREG(status.st_mode)
                or lock.is_symlink()
                or status.st_nlink != 1
                or status.st_uid != expected_uid
                or status.st_gid != expected_gid
                or stat.S_IMODE(status.st_mode) != 0o600
            ):
                raise ContractViolation(
                    f"candidate profile lock metadata mismatch:{spec.key}"
                )

    def _acquire_wallet_lock(self, *, final: bool) -> None:
        if final and self._final_lock_held:
            return
        if not final and self._prestart_lock_held:
            return
        if not self.wallet_lock.exists():
            if self.config.production:
                raise ContractViolation("shared wallet lock file missing")
            self.wallet_lock.parent.mkdir(parents=True, exist_ok=True)
            self.wallet_lock.touch()
        status = self.wallet_lock.lstat()
        if not stat.S_ISREG(status.st_mode) or self.wallet_lock.is_symlink():
            raise ContractViolation("shared wallet lock identity mismatch")
        descriptor = os.open(
            self.wallet_lock,
            os.O_RDWR | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
        )
        handle = os.fdopen(descriptor, "a+")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise ContractViolation("shared wallet lock is busy") from exc
        self._wallet_lock_handle = handle
        if final:
            self._final_lock_held = True
        else:
            self._prestart_lock_held = True

    def _release_wallet_lock(self, *, final: bool) -> None:
        held = self._final_lock_held if final else self._prestart_lock_held
        if not held:
            return
        if self._wallet_lock_handle is None:
            raise ContractViolation("shared wallet lock handle missing")
        fcntl.flock(self._wallet_lock_handle.fileno(), fcntl.LOCK_UN)
        self._wallet_lock_handle.close()
        self._wallet_lock_handle = None
        if final:
            self._final_lock_held = False
        else:
            self._prestart_lock_held = False

    def _acquire_profile_locks(self) -> None:
        if self.config.production:
            self._verify_service_lock_files()
        if self._profile_lock_handles:
            expected = {f"candidate:{spec.key}" for spec in PROFILE_SPECS}
            expected.update(
                f"legacy:{spec.key}"
                for spec in PROFILE_SPECS
                if self._legacy_profile_lock(spec).exists()
            )
            if set(self._profile_lock_handles) != expected:
                raise ContractViolation("profile runtime lock set is incomplete")
            return
        acquired: dict[str, Any] = {}
        try:
            for spec in PROFILE_SPECS:
                locks = [("candidate", self._candidate_profile_lock(spec))]
                legacy = self._legacy_profile_lock(spec)
                if legacy.exists():
                    locks.insert(0, ("legacy", legacy))
                for kind, lock in locks:
                    if not lock.exists():
                        if self.config.production:
                            raise ContractViolation(
                                f"profile runtime lock missing:{kind}:{spec.key}"
                            )
                        lock.parent.mkdir(parents=True, exist_ok=True)
                        lock.touch()
                    status = lock.lstat()
                    if (
                        not stat.S_ISREG(status.st_mode)
                        or lock.is_symlink()
                        or status.st_nlink != 1
                    ):
                        raise ContractViolation(
                            f"profile runtime lock identity mismatch:{kind}:{spec.key}"
                        )
                    descriptor = os.open(
                        lock,
                        os.O_RDWR | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
                    )
                    handle = os.fdopen(descriptor, "a+")
                    try:
                        fcntl.flock(
                            handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                        )
                    except BaseException:
                        handle.close()
                        raise
                    acquired[f"{kind}:{spec.key}"] = handle
        except BaseException:
            for handle in acquired.values():
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()
            raise
        self._profile_lock_handles = acquired

    def _release_profile_locks(self) -> None:
        handles, self._profile_lock_handles = self._profile_lock_handles, {}
        errors: list[OSError] = []
        for handle in handles.values():
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError as exc:
                errors.append(exc)
            finally:
                handle.close()
        if errors:
            raise ContractViolation("profile runtime lock release failed") from errors[0]

    def _verify_quiescent(self) -> None:
        if self.config.production:
            self._verify_service_lock_files()
            for unit in (*EXECUTOR_UNITS, HEALTH_UNIT):
                if self._active(unit):
                    raise ContractViolation(f"unit remained active:{unit}")
                if self._property(unit, "MainPID") != "0":
                    raise ContractViolation(f"unit MainPID remained nonzero:{unit}")
                cgroup = self._property(unit, "ControlGroup")
                if cgroup == "/":
                    raise ContractViolation(f"unit cgroup is invalid:{unit}")
                if cgroup:
                    procs = Path("/sys/fs/cgroup") / cgroup.lstrip("/") / "cgroup.procs"
                    if procs.exists() and procs.read_text(encoding="utf-8").strip():
                        raise ContractViolation(f"unit cgroup is not empty:{unit}")
            if self._active(HEALTH_TIMER):
                raise ContractViolation("health timer remained active")
            service_uid = self.runner.run(
                ("/usr/bin/id", "-u", "polymarket-live")
            ).stdout.strip()
            if not service_uid.isdigit():
                raise ContractViolation("service account uid is invalid")
            process_list = self.runner.run(
                ("/usr/bin/ps", "-eo", "uid=,pid=,args=")
            ).stdout
            for line in process_list.splitlines():
                fields = line.split(maxsplit=2)
                if len(fields) >= 2 and fields[0] == service_uid:
                    raise ContractViolation(
                        "service-account orphan process remains"
                    )
        for spec in PROFILE_SPECS:
            for kind, lock in (
                ("legacy", self._legacy_profile_lock(spec)),
                ("candidate", self._candidate_profile_lock(spec)),
            ):
                if kind == "legacy" and not lock.exists():
                    # Directory-local locks predate the root-level profile
                    # lock contract.  A missing retired lock is not evidence
                    # of a live writer; candidate locks remain mandatory.
                    continue
                if not lock.exists():
                    if self.config.production:
                        raise ContractViolation(
                            f"profile runtime lock missing:{kind}:{spec.key}"
                        )
                    lock.parent.mkdir(parents=True, exist_ok=True)
                    lock.touch()
                status = lock.lstat()
                if (
                    not stat.S_ISREG(status.st_mode)
                    or lock.is_symlink()
                    or status.st_nlink != 1
                ):
                    raise ContractViolation(
                        f"profile runtime lock identity mismatch:{kind}:{spec.key}"
                    )
                handle_key = f"{kind}:{spec.key}"
                if handle_key in self._profile_lock_handles:
                    held_status = os.fstat(
                        self._profile_lock_handles[handle_key].fileno()
                    )
                    if (
                        held_status.st_dev != status.st_dev
                        or held_status.st_ino != status.st_ino
                    ):
                        raise ContractViolation(
                            f"profile runtime lock inode changed:{kind}:{spec.key}"
                        )
                    continue
                descriptor = os.open(
                    lock, os.O_RDWR | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
                )
                with os.fdopen(descriptor, "a+") as handle:
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError as exc:
                        raise ContractViolation(
                            f"profile runtime lock busy:{kind}:{spec.key}"
                        ) from exc
                    finally:
                        try:
                            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                        except OSError:
                            pass

    def _database_receipt(self, path: Path) -> dict[str, Any]:
        connection = _ro_connection(path)
        try:
            integrity = [str(row[0]) for row in connection.execute("PRAGMA integrity_check")]
            foreign_keys = [list(row) for row in connection.execute("PRAGMA foreign_key_check")]
            receipt = {
                "sha256": sha256_file(path),
                "integrity_check": integrity,
                "foreign_key_check": foreign_keys,
                "page_size": int(connection.execute("PRAGMA page_size").fetchone()[0]),
                "user_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
                "application_id": int(connection.execute("PRAGMA application_id").fetchone()[0]),
                "schema_version": int(connection.execute("PRAGMA schema_version").fetchone()[0]),
                "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]),
            }
        finally:
            connection.close()
        self._checkpoint_staged_databases({"snapshot": path})
        if sha256_file(path) != receipt["sha256"]:
            raise ContractViolation(f"database snapshot changed during receipt:{path}")
        if receipt["integrity_check"] != ["ok"] or receipt["foreign_key_check"]:
            raise ContractViolation(f"database snapshot receipt failed:{path}")
        return receipt

    def _exact_snapshot_sources(self) -> dict[str, Path]:
        sources = {spec.key: self._live_database(spec) for spec in PROFILE_SPECS}
        sources["coordinator"] = self.coordinator
        return sources

    def _snapshot_database(self, source: Path, destination: Path) -> None:
        physical_copy_database(source, destination)

    def checkpoint_and_verify_stable_databases(
        self, sources: Mapping[str, Path]
    ) -> None:
        for key, source in sources.items():
            source = _regular_database(source)
            expected = self.pre_stop_fingerprints.get(key)
            if expected is None:
                raise ContractViolation(f"pre-stop ledger fingerprint missing:{key}")
            if canonical_database_fingerprint(source) != expected:
                raise ContractViolation(f"ledger changed across stop:{key}")
            try:
                connection = sqlite3.connect(source)
                row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                connection.close()
            except sqlite3.Error as exc:
                raise ContractViolation(f"post-stop checkpoint failed:{key}") from exc
            if row is None or int(row[0]) != 0:
                raise ContractViolation(f"post-stop checkpoint remained busy:{key}")
            if canonical_database_fingerprint(source) != expected:
                raise ContractViolation(f"ledger changed across stop:{key}")
            # The logical read above can recreate empty WAL/SHM sidecars even
            # in query-only mode.  Remove those read artifacts once more, then
            # prove that cleanup did not change the main database bytes.
            stable_main_hash = sha256_file(source)
            try:
                connection = sqlite3.connect(source)
                final_row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                connection.close()
            except sqlite3.Error as exc:
                raise ContractViolation(f"post-stop final checkpoint failed:{key}") from exc
            if final_row is None or int(final_row[0]) != 0:
                raise ContractViolation(f"post-stop final checkpoint remained busy:{key}")
            if sha256_file(source) != stable_main_hash:
                raise ContractViolation(f"post-stop checkpoint changed main database:{key}")
            assert_no_sqlite_sidecars(source)

    def stop_and_prove_quiescent(self) -> None:
        first = self._systemctl("stop", *ALL_STOP_UNITS, check=False)
        try:
            self._verify_quiescent()
            if first.returncode == 0:
                self._acquire_profile_locks()
                return
        except ContractViolation:
            pass
        self._systemctl(
            "kill",
            "--kill-whom=all",
            "--signal=SIGKILL",
            *ALL_STOP_UNITS,
            check=False,
        )
        self._systemctl("stop", *ALL_STOP_UNITS, check=False)
        self._verify_quiescent()
        self._acquire_profile_locks()

    def _stop_intent_evidence(self) -> dict[str, Any]:
        return {
            "transaction_id": self.config.change_id,
            "change_id": self.config.change_id,
            "manifest_sha256": self.config.expected_manifest_digest,
            "new_release": str(self.config.new_release),
            "old_release": str(self.old_release),
            "environment_hashes": dict(self.env_hashes),
            "original_activity": dict(self.original_activity),
            "original_enablement": dict(self.original_enablement),
            "old_health_state": dict(self.old_health_state),
            "post_stop_baselines": self.baselines,
            "official_redemption_digest": self.official_redemption_digest,
            "official_redemption_identity_hashes": sorted(
                self.official_redemption_identity_hashes
            ),
            "observed_official_redemption_identity_hashes": sorted(
                self.observed_official_redemption_identity_hashes
            ),
            "shared_redemption_transition_baseline": (
                self.shared_redemption_transition_baseline
            ),
            "shared_redemption_transition_prefix_sha256": (
                self.shared_redemption_transition_prefix_sha256
            ),
            "shared_redemption_receipt_conditions": list(
                self.shared_redemption_receipt_conditions
            ),
            "shared_redemption_receipt_rows_json": (
                self.shared_redemption_receipt_rows_json
            ),
            "shared_redemption_allocation_rows_json": (
                self.shared_redemption_allocation_rows_json
            ),
            "health_baseline_mtime_ns": self.health_baseline_mtime_ns,
            "manager_timeout_usec": self.manager_timeout_usec,
        }

    def _common_receipt_evidence(self) -> dict[str, Any]:
        return {
            **self._stop_intent_evidence(),
            "exact_snapshot_hashes": dict(self.exact_hashes),
            "database_metadata": {
                key: {"uid": value.uid, "gid": value.gid, "mode": value.mode}
                for key, value in self.database_metadata.items()
            },
            "exact_snapshot_receipt_sha256": self.exact_snapshot_receipt_hash,
            "old_artifact_hashes": dict(self.old_artifact_hashes),
        }

    def verify_old_health_artifacts(
        self, *, unit_root: Path, bridge: Path, heartbeat: Path
    ) -> None:
        expected = {
            Path(unit_root) / HEALTH_UNIT: self.old_release / "systemd" / HEALTH_UNIT,
            Path(unit_root) / HEALTH_TIMER: self.old_release / "systemd" / HEALTH_TIMER,
            Path(bridge): self.old_release / "systemd" / HEALTH_BRIDGE,
            Path(heartbeat): self.old_release / "app/server_health_heartbeat.py",
        }
        for installed, source in expected.items():
            if (
                not installed.is_file()
                or installed.is_symlink()
                or not source.is_file()
                or source.is_symlink()
                or installed.read_bytes() != source.read_bytes()
            ):
                raise ContractViolation(f"old health artifact mismatch:{installed}")
            if self.config.production:
                if installed.name in {HEALTH_UNIT, HEALTH_TIMER}:
                    if self._property(installed.name, "FragmentPath") != str(
                        installed
                    ):
                        raise ContractViolation(
                            f"old health unit fragment mismatch:{installed.name}"
                        )
                    if self._property(installed.name, "DropInPaths"):
                        raise ContractViolation(
                            f"old health unit drop-in forbidden:{installed.name}"
                        )
                status = installed.stat(follow_symlinks=False)
                expected_mode = 0o755 if installed.name == HEALTH_BRIDGE else 0o644
                if (
                    status.st_uid != 0
                    or status.st_gid != 0
                    or stat.S_IMODE(status.st_mode) != expected_mode
                ):
                    raise ContractViolation(
                        f"old health artifact metadata mismatch:{installed}"
                    )

    def _seal_directory(self, path: Path) -> None:
        root_status = path.lstat()
        if not stat.S_ISDIR(root_status.st_mode) or path.is_symlink():
            raise ContractViolation(f"seal root is not a regular directory:{path}")
        if self.config.production:
            # Freeze directory membership first; the candidate service account
            # can no longer add, remove, or replace entries while root seals
            # and verifies their bytes.
            os.chown(path, 0, 0)
            os.chmod(path, 0o500)
        items = list(path.rglob("*"))
        for item in items:
            status = item.lstat()
            mode = status.st_mode
            if stat.S_ISLNK(mode):
                raise ContractViolation(f"seal tree contains symlink:{item}")
            if not stat.S_ISREG(mode) and not stat.S_ISDIR(mode):
                raise ContractViolation(f"seal tree contains special file:{item}")
            if stat.S_ISREG(mode) and status.st_nlink != 1:
                raise ContractViolation(f"seal tree contains hardlink:{item}")
        if self.config.production:
            for item in sorted(items, key=lambda value: len(value.parts), reverse=True):
                os.chown(item, 0, 0, follow_symlinks=False)
                os.chmod(item, 0o500 if item.is_dir() else 0o400)
        else:
            for item in items:
                if stat.S_ISREG(item.lstat().st_mode):
                    item.chmod(0o400)
        for item in (*items, path):
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            if stat.S_ISDIR(item.lstat().st_mode):
                flags |= getattr(os, "O_DIRECTORY", 0)
            descriptor = os.open(item, flags)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _best_effort_failed_stage_evidence(
        self, error: BaseException
    ) -> dict[str, Any]:
        path = self.migration_stage
        inventory: list[dict[str, Any]] = []
        regular_hashes: dict[str, str] = {}
        unsafe: list[str] = []
        pending = [path]
        try:
            while pending:
                directory = pending.pop()
                with os.scandir(directory) as entries:
                    for entry in sorted(entries, key=lambda value: value.name):
                        item = Path(entry.path)
                        relative = item.relative_to(path).as_posix()
                        try:
                            status = item.lstat()
                            if stat.S_ISLNK(status.st_mode):
                                kind = "symlink"
                                unsafe.append(f"{relative}:{kind}")
                            elif stat.S_ISDIR(status.st_mode):
                                kind = "directory"
                                pending.append(item)
                            elif stat.S_ISREG(status.st_mode) and status.st_nlink == 1:
                                kind = "regular"
                                digest = hashlib.sha256()
                                try:
                                    with item.open("rb") as handle:
                                        while block := handle.read(HASH_READ_CHUNK_BYTES):
                                            digest.update(block)
                                    regular_hashes[relative] = digest.hexdigest()
                                except OSError as hash_error:
                                    unsafe.append(
                                        f"{relative}:unreadable:{type(hash_error).__name__}"
                                    )
                            elif stat.S_ISREG(status.st_mode):
                                kind = "hardlink"
                                unsafe.append(f"{relative}:{kind}")
                            else:
                                kind = "special"
                                unsafe.append(f"{relative}:{kind}")
                            inventory.append(
                                {
                                    "path": relative,
                                    "kind": kind,
                                    "size": int(status.st_size),
                                }
                            )
                        except OSError as item_error:
                            unsafe.append(
                                f"{relative}:unreadable:{type(item_error).__name__}"
                            )
        except OSError as scan_error:
            unsafe.append(f".:unreadable:{type(scan_error).__name__}")
        inventory.sort(key=lambda item: str(item["path"]))
        return {
            "path": str(path),
            "observed_phase": self.phase.name,
            "state": "CONTAINMENT_FAILED",
            "failed_step": self._stage_seal_step,
            "inventory_count": len(inventory),
            "regular_file_hashes": regular_hashes,
            "unsafe_entries": sorted(unsafe),
            "inventory_digest": _sha256_bytes(
                json.dumps(
                    inventory, sort_keys=True, separators=(",", ":")
                ).encode()
            ),
            "seal_error": f"{type(error).__name__}:{error}",
        }

    def _verify_no_process_references_migration_stage(self) -> None:
        if not self.config.production:
            return
        proc_root = Path("/proc")
        if not proc_root.is_dir():
            raise ContractViolation("process reference proof unavailable")
        protected: set[tuple[int, int]] = set()
        pending = [self.migration_stage]
        while pending:
            item = pending.pop()
            try:
                status = item.lstat()
            except OSError as exc:
                raise ContractViolation(
                    "migration stage identity unreadable during process proof"
                ) from exc
            protected.add((int(status.st_dev), int(status.st_ino)))
            if stat.S_ISDIR(status.st_mode) and not item.is_symlink():
                try:
                    with os.scandir(item) as entries:
                        pending.extend(Path(entry.path) for entry in entries)
                except OSError as exc:
                    raise ContractViolation(
                        "migration stage tree unreadable during process proof"
                    ) from exc

        references: set[int] = set()

        def check_reference(candidate: Path, pid: int) -> None:
            try:
                status = candidate.stat()
            except FileNotFoundError:
                return
            except PermissionError as exc:
                raise ContractViolation(
                    f"process reference unreadable:{pid}:{candidate.name}"
                ) from exc
            except OSError as exc:
                raise ContractViolation(
                    f"process reference unreadable:{pid}:{candidate.name}"
                ) from exc
            if (int(status.st_dev), int(status.st_ino)) in protected:
                references.add(pid)

        for process in proc_root.iterdir():
            if not process.name.isdigit():
                continue
            pid = int(process.name)
            for name in ("cwd", "root", "exe"):
                check_reference(process / name, pid)
            descriptor_root = process / "fd"
            try:
                descriptors = list(descriptor_root.iterdir())
            except FileNotFoundError:
                descriptors = []
            except PermissionError as exc:
                raise ContractViolation(
                    f"process descriptors unreadable:{pid}"
                ) from exc
            except OSError as exc:
                raise ContractViolation(
                    f"process descriptors unreadable:{pid}"
                ) from exc
            for descriptor in descriptors:
                check_reference(descriptor, pid)
            maps = process / "maps"
            try:
                lines = maps.read_text(encoding="utf-8", errors="replace").splitlines()
            except FileNotFoundError:
                lines = []
            except PermissionError as exc:
                raise ContractViolation(f"process maps unreadable:{pid}") from exc
            except OSError as exc:
                raise ContractViolation(f"process maps unreadable:{pid}") from exc
            for line in lines:
                fields = line.split(maxsplit=5)
                if len(fields) < 5 or not fields[4].isdigit() or ":" not in fields[3]:
                    continue
                major_text, minor_text = fields[3].split(":", 1)
                try:
                    device = os.makedev(int(major_text, 16), int(minor_text, 16))
                    inode = int(fields[4])
                except ValueError:
                    continue
                if (int(device), inode) in protected:
                    references.add(pid)
        if references:
            raise ContractViolation(
                "migration stage process reference remains:"
                + ",".join(str(pid) for pid in sorted(references))
            )

    def seal_partial_migration_stage(self) -> dict[str, Any]:
        self._stage_seal_step = "root_containment"
        try:
            return self._seal_partial_migration_stage_once()
        except BaseException as exc:
            if self.migration_stage_evidence.get("state") != "CONTAINMENT_FAILED":
                self.migration_stage_evidence = self._best_effort_failed_stage_evidence(
                    exc
                )
            if (
                isinstance(exc, ContractViolation)
                and str(exc) == "migration stage containment failed"
            ):
                raise
            raise ContractViolation("migration stage evidence failed") from exc

    def _seal_partial_migration_stage_once(self) -> dict[str, Any]:
        path = self.migration_stage
        base: dict[str, Any] = {
            "path": str(path),
            "observed_phase": self.phase.name,
            "inventory_count": 0,
            "regular_file_hashes": {},
            "unsafe_entries": [],
            "inventory_digest": _sha256_bytes(b"[]"),
            "seal_error": "",
        }
        if not path.exists() and not path.is_symlink():
            evidence = {**base, "state": "ABSENT"}
            self.migration_stage_evidence = evidence
            return evidence
        try:
            self._stage_seal_step = "root_containment"
            root_status = path.lstat()
            if not stat.S_ISDIR(root_status.st_mode) or path.is_symlink():
                raise ContractViolation("migration stage root is not a directory")
            _seal_path_by_descriptor(
                path,
                mode=0o500,
                directory=True,
                root_owned=self.config.production,
            )
            parent_descriptor = os.open(
                path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(parent_descriptor)
            finally:
                os.close(parent_descriptor)
        except BaseException as exc:
            evidence = {
                **base,
                "state": "CONTAINMENT_FAILED",
                "failed_step": self._stage_seal_step,
                "seal_error": f"{type(exc).__name__}:{exc}",
            }
            self.migration_stage_evidence = evidence
            raise ContractViolation("migration stage containment failed") from exc

        inventory: list[dict[str, Any]] = []
        safe_regular: list[tuple[Path, str]] = []
        safe_directories: list[Path] = []
        unsafe: list[str] = []
        pending = [path]
        while pending:
            self._stage_seal_step = "inventory_scan"
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in sorted(entries, key=lambda value: value.name):
                    item = Path(entry.path)
                    relative = item.relative_to(path).as_posix()
                    status = item.lstat()
                    if stat.S_ISLNK(status.st_mode):
                        kind = "symlink"
                        unsafe.append(f"{relative}:{kind}")
                    elif stat.S_ISDIR(status.st_mode):
                        kind = "directory"
                        safe_directories.append(item)
                        pending.append(item)
                    elif stat.S_ISREG(status.st_mode):
                        if status.st_nlink != 1:
                            kind = "hardlink"
                            unsafe.append(f"{relative}:{kind}")
                        else:
                            kind = "regular"
                            safe_regular.append((item, relative))
                    else:
                        kind = "special"
                        unsafe.append(f"{relative}:{kind}")
                    inventory.append(
                        {
                            "path": relative,
                            "kind": kind,
                            "size": int(status.st_size),
                        }
                    )

        regular_hashes: dict[str, str] = {}
        for item, relative in safe_regular:
            self._stage_seal_step = "hash_regular_file"
            _seal_path_by_descriptor(
                item,
                mode=0o400,
                directory=False,
                root_owned=self.config.production,
            )
            regular_hashes[relative] = sha256_file(item)
        for directory in sorted(
            safe_directories, key=lambda value: len(value.parts), reverse=True
        ):
            self._stage_seal_step = "seal_directory"
            _seal_path_by_descriptor(
                directory,
                mode=0o500,
                directory=True,
                root_owned=self.config.production,
            )
        for durable_directory in (path, path.parent):
            self._stage_seal_step = "fsync_directory"
            descriptor = os.open(
                durable_directory,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        inventory.sort(key=lambda item: str(item["path"]))
        inventory_digest = _sha256_bytes(
            json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
        )
        expected_databases = {
            f"{key}.sqlite3" for key in (*PROFILE_KEYS, "coordinator")
        }
        expected_inventory = {*expected_databases, "verified-stage.json"}
        verified = False
        if not unsafe and set(regular_hashes) == expected_inventory:
            self._stage_seal_step = "verify_stage_receipt"
            try:
                receipt = json.loads(
                    (path / "verified-stage.json").read_text(encoding="utf-8")
                )
                recorded = receipt.get("database_hashes")
                verified = isinstance(recorded, Mapping) and {
                    str(key): str(value) for key, value in recorded.items()
                } == {
                    key.removesuffix(".sqlite3"): regular_hashes[key]
                    for key in expected_databases
                }
            except (OSError, json.JSONDecodeError):
                verified = False
        self._stage_seal_step = "process_reference_scan"
        self._verify_no_process_references_migration_stage()
        evidence = {
            **base,
            "state": (
                "SEALED_VERIFIED" if verified else "SEALED_PARTIAL_UNVERIFIED"
            ),
            "inventory_count": len(inventory),
            "regular_file_hashes": regular_hashes,
            "unsafe_entries": sorted(unsafe),
            "inventory_digest": inventory_digest,
        }
        self.migration_stage_evidence = evidence
        self._stage_seal_step = "complete"
        return evidence

    def _verify_migration_stage_inventory(self) -> None:
        expected = {
            f"{key}.sqlite3" for key in (*PROFILE_KEYS, "coordinator")
        }
        actual: set[str] = set()
        for item in self.migration_stage.rglob("*"):
            status = item.lstat()
            mode = status.st_mode
            relative = item.relative_to(self.migration_stage).as_posix()
            if stat.S_ISLNK(mode):
                raise ContractViolation(
                    f"migration stage inventory contains symlink:{relative}"
                )
            if not stat.S_ISREG(mode):
                raise ContractViolation(
                    f"migration stage inventory contains non-file:{relative}"
                )
            if status.st_nlink != 1:
                raise ContractViolation(
                    f"migration stage inventory contains hardlink:{relative}"
                )
            actual.add(relative)
        if actual != expected:
            raise ContractViolation(
                "migration stage inventory mismatch:"
                f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
            )

    def _checkpoint_staged_databases(
        self, databases: Mapping[str, Path]
    ) -> None:
        """Checkpoint stopped/offline DBs after reads that can recreate sidecars."""

        for key, database in databases.items():
            connection: sqlite3.Connection | None = None
            try:
                connection = sqlite3.connect(database)
                row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            except sqlite3.Error as exc:
                raise ContractViolation(
                    f"staged database checkpoint failed:{key}"
                ) from exc
            finally:
                if connection is not None:
                    connection.close()
            if row is None or int(row[0]) != 0:
                raise ContractViolation(f"staged database checkpoint busy:{key}")
            for suffix in ("-wal", "-shm"):
                sidecar = Path(f"{database}{suffix}")
                if not sidecar.exists() and not sidecar.is_symlink():
                    continue
                status = sidecar.lstat()
                if (
                    sidecar.is_symlink()
                    or not stat.S_ISREG(status.st_mode)
                    or (suffix == "-wal" and status.st_size != 0)
                ):
                    raise ContractViolation(
                        f"staged database checkpoint sidecar unsafe:{key}:{suffix}"
                    )
                sidecar.unlink()
            directory_descriptor = os.open(
                database.parent, os.O_RDONLY | os.O_DIRECTORY
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
            assert_no_sqlite_sidecars(database)

    def _finalize_live_database_replacement(
        self, expected_hashes: Mapping[str, str]
    ) -> None:
        databases = self._exact_snapshot_sources()
        if set(databases) != set(expected_hashes):
            raise ContractViolation("live replacement database set mismatch")
        self._checkpoint_staged_databases(
            {f"live-final:{key}": path for key, path in databases.items()}
        )
        for key, database in databases.items():
            if sha256_file(database) != expected_hashes[key]:
                raise ContractViolation(f"live replacement final hash mismatch:{key}")
            metadata = self.database_metadata.get(key)
            if metadata is not None and FileMetadata.from_path(database) != metadata:
                raise ContractViolation(
                    f"live replacement final metadata mismatch:{key}"
                )

    def _disable_autostart(self) -> None:
        if not self.config.production:
            return
        self._systemctl("disable", *EXECUTOR_UNITS, HEALTH_TIMER)
        for unit in (*EXECUTOR_UNITS, HEALTH_TIMER):
            if self._enabled(unit) != "disabled":
                raise ContractViolation(f"unit autostart disable failed:{unit}")

    def _run_candidate_sandbox(self, program: str, *, stage: Path | None) -> None:
        timeout_usec = self.manager_timeout_usec or self._manager_timeout_usec()
        if timeout_usec <= 0:
            raise ContractViolation("candidate sandbox timeout is invalid")
        timeout_seconds = timeout_usec / 1_000_000
        if not self.config.production:
            environment = {
                "CANDIDATE_APP": str(self.config.new_release / "app"),
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            if stage is not None:
                environment.update(
                    {
                        "STAGE": str(stage),
                        **{
                            f"CHANGE_ID_{spec.key.upper()}": (
                                f"{self.config.change_id}-{spec.change_suffix}"
                            )
                            for spec in PROFILE_SPECS
                        },
                    }
                )
            try:
                completed = subprocess.run(
                    [sys.executable, "-I", "-"],
                    input=program,
                    text=True,
                    capture_output=True,
                    check=False,
                    env=environment,
                    timeout=timeout_seconds,
                )
            except subprocess.TimeoutExpired as exc:
                raise ContractViolation("candidate sandbox timed out") from exc
            if completed.returncode:
                raise ContractViolation(f"candidate sandbox failed:{completed.stderr.strip()}")
            return
        if stage is None:
            stage_setup = (
                "mount -t tmpfs -o mode=000,nosuid,nodev,noexec "
                "denied-runtime /srv/polymarket-live/runtime"
            )
            stage_environment = ""
            arguments: tuple[str, ...] = ()
        else:
            stage_setup = (
                'mount -t tmpfs -o mode=000,nosuid,nodev,noexec denied-runtime /srv/polymarket-live/runtime\n'
                'mount --bind "$1" "$1"\n'
                'mount -o remount,bind,rw,nosuid,nodev,noexec "$1"'
            )
            stage_environment = (
                ' STAGE="$1"'
                + "".join(
                    f' CHANGE_ID_{spec.key.upper()}="${index}"'
                    for index, spec in enumerate(PROFILE_SPECS, start=3)
                )
            )
            arguments = (
                str(stage),
                str(self.config.new_release / "app"),
                *(f"{self.config.change_id}-{spec.change_suffix}" for spec in PROFILE_SPECS),
            )
        if stage is None:
            arguments = (str(self.config.new_release / "app"),)
            app_argument = "$1"
        else:
            app_argument = "$2"
        shell = f"""
mount --make-rprivate /
mount --bind / /
mount -o remount,bind,ro /
mount -t tmpfs -o mode=000,nosuid,nodev,noexec denied-env /etc/polymarket-live
{stage_setup}
mount -t tmpfs -o mode=0700,nosuid,nodev,noexec isolated-tmp /tmp
exec /usr/bin/setpriv --reuid=polymarket-live --regid=polymarket-live --clear-groups --no-new-privs \\
 /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 \\
 CANDIDATE_APP="{app_argument}"{stage_environment} /opt/polymarket-live/venv/bin/python -I -
"""
        self.runner.run(
            (
                "/usr/bin/unshare",
                "--mount",
                "--net",
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                "--mount-proc",
                "/bin/sh",
                "-ceu",
                shell,
                "sh",
                *arguments,
            ),
            input_text=program,
            timeout_seconds=timeout_seconds,
            parent_death_signal=signal.SIGKILL,
        )

    def _offline_migration_program(self) -> str:
        profiles = [
            {
                "key": spec.key,
                "database": f"{spec.key}.sqlite3",
                "change_env": f"CHANGE_ID_{spec.key.upper()}",
                # A user-paused profile must stay paused through an unrelated
                # core release.  Its ledger is still staged and verified, but
                # it must not receive a forward-resume marker.
                "resume": self._profile_original_mode(spec) == "ACTIVE",
            }
            for spec in PROFILE_SPECS
        ]
        return OFFLINE_AUDIT_GUARD + f"""
import os, sys, time
from pathlib import Path
app = Path(os.environ['CANDIDATE_APP']).resolve()
sys.path.insert(0, str(app))
from cd90_live_copy import LiveStore, arm_pre_repair_forward_recovery
from live_wallet_coordinator import SharedWalletCoordinator
for module_name in ('cd90_live_copy', 'live_wallet_coordinator'):
    module = sys.modules[module_name]
    if Path(module.__file__).resolve().parent != app:
        raise SystemExit('offline migration import escaped candidate')
stage = Path(os.environ['STAGE']).resolve()
coordinator = SharedWalletCoordinator(stage / 'coordinator.sqlite3')
coordinator.initialize()
current = coordinator.migration_receipt()
history = coordinator.receipt_history()
changed_at_ms = time.time_ns() // 1_000_000
for item in {profiles!r}:
    store = LiveStore(stage / item['database'])
    store.initialize()
    store.migrate_shared_wallet_migration_receipt_hash(
        expected_receipt_hash=current['migration_receipt_hash'],
        receipt_history=history,
        changed_at_ms=changed_at_ms,
    )
    store.ensure_bounded_retry_policy_at_current_cursor(
        activated_at_ms=changed_at_ms,
        change_id=os.environ[item['change_env']],
    )
    store.ensure_liquidity_retry_policy_at_current_cursor(
        activated_at_ms=changed_at_ms,
        change_id=os.environ[item['change_env']],
    )
    if item['resume']:
        arm_pre_repair_forward_recovery(
            store=store,
            change_id=os.environ[item['change_env']],
            reason='single-transaction-cutover-forward-recovery',
            armed_at_ms=changed_at_ms,
        )
print('OFFLINE_STAGE_COMPLETE')
"""

    def _candidate_import_program(self) -> str:
        modules = tuple(
            Path(asset).stem
            for asset in sorted(REQUIRED_ASSETS)
            if asset.startswith("app/") and asset.endswith(".py")
        )
        return OFFLINE_AUDIT_GUARD + f"""
import importlib, os, sys
from pathlib import Path
app = Path(os.environ['CANDIDATE_APP']).resolve()
sys.path.insert(0, str(app))
for name in {modules!r}:
    module = importlib.import_module(name)
    if Path(module.__file__).resolve().parent != app:
        raise SystemExit('candidate import escaped release')
print('CANDIDATE_IMPORT_COMPLETE')
"""

    def _verify_stage_resume(self) -> None:
        for spec in PROFILE_SPECS:
            database = self.migration_stage / f"{spec.key}.sqlite3"
            if self._profile_original_mode(spec) != "ACTIVE":
                # The profile was explicitly paused before this transaction;
                # it is not a recovery candidate and must not be armed.
                continue
            expected_change = f"{self.config.change_id}-{spec.change_suffix}"
            expected = {
                "operator_planned_resume_change_id": expected_change,
                "operator_planned_resume_state": "PENDING",
                "operator_pre_repair_forward_recovery_armed": "true",
            }
            for key, value in expected.items():
                actual = sqlite_scalar_ro(
                    database,
                    "SELECT COALESCE((SELECT value FROM runtime_state WHERE key=?),'')",
                    (key,),
                )
                if str(actual) != value:
                    raise ContractViolation(f"offline stage recovery mismatch:{spec.key}:{key}")
            from_block = sqlite_scalar_ro(
                database,
                "SELECT COALESCE((SELECT value FROM runtime_state "
                "WHERE key='operator_planned_resume_from_block'),'')",
            )
            cursor = sqlite_scalar_ro(
                database,
                "SELECT COALESCE((SELECT value FROM runtime_state "
                "WHERE key='last_processed_block'),'')",
            )
            if str(from_block) != str(cursor):
                raise ContractViolation(f"offline stage resume cursor mismatch:{spec.key}")
            bounded_retry_boundary = sqlite_scalar_ro(
                database,
                "SELECT COALESCE((SELECT value FROM config "
                "WHERE key='bounded_retry_effective_after_block'),'')",
            )
            try:
                bounded_retry_boundary_int = int(str(bounded_retry_boundary))
                cursor_int = int(str(cursor))
            except ValueError as exc:
                raise ContractViolation(
                    f"offline stage bounded retry cursor invalid:{spec.key}"
                ) from exc
            # A failed prior cutover may already have immutably activated the
            # prospective policy.  Never rewrite that receipt.  It is safe
            # only when its waterline is at or behind the copied cursor;
            # legacy terminal targets remain excluded from retry selection.
            if bounded_retry_boundary_int > cursor_int:
                raise ContractViolation(
                    f"offline stage bounded retry cursor ahead:{spec.key}"
                )
            liquidity_retry_boundary = sqlite_scalar_ro(
                database,
                "SELECT COALESCE((SELECT value FROM config "
                "WHERE key='liquidity_retry_effective_after_block'),'')",
            )
            try:
                liquidity_retry_boundary_int = int(str(liquidity_retry_boundary))
            except ValueError as exc:
                raise ContractViolation(
                    f"offline stage liquidity retry cursor invalid:{spec.key}"
                ) from exc
            # The policy boundary is immutable.  A live cursor is expected to
            # move forward after a prior release, so only a future boundary is
            # invalid; requiring equality makes every later release fail.
            if liquidity_retry_boundary_int > cursor_int:
                raise ContractViolation(
                    f"offline stage liquidity retry cursor ahead:{spec.key}"
                )

    def _install_and_verify_candidate(self) -> None:
        if not self.config.production:
            temporary = self.config.current_link.with_name(
                f".{self.config.current_link.name}.candidate"
            )
            temporary.symlink_to(self.config.new_release, target_is_directory=True)
            os.replace(temporary, self.config.current_link)
            fsync_directory(self.config.current_link.parent)
            if self.config.current_link.resolve() != self.config.new_release.resolve():
                raise ContractViolation("candidate current link switch failed")
            return
        for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER):
            installed = self.config.systemd_root / unit
            self.runner.run(
                (
                    "/usr/bin/sudo",
                    "-n",
                    "/usr/bin/install",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0644",
                    str(self.config.new_release / "systemd" / unit),
                    str(installed),
                )
            )
            fsync_regular_file_and_parent(installed)
        health_bridge = Path("/usr/local/sbin") / HEALTH_BRIDGE
        self.runner.run(
            (
                "/usr/bin/sudo",
                "-n",
                "/usr/bin/install",
                "-o",
                "root",
                "-g",
                "root",
                "-m",
                "0755",
                str(self.config.new_release / "systemd" / HEALTH_BRIDGE),
                str(health_bridge),
            )
        )
        fsync_regular_file_and_parent(health_bridge)
        heartbeat = Path(
            "/usr/local/libexec/polymarket/server_health_heartbeat.py"
        )
        self.runner.run(
            (
                "/usr/bin/sudo",
                "-n",
                "/usr/bin/install",
                "-o",
                "root",
                "-g",
                "root",
                "-m",
                "0644",
                str(self.config.new_release / "app/server_health_heartbeat.py"),
                str(heartbeat),
            )
        )
        fsync_regular_file_and_parent(heartbeat)
        temporary = self.config.current_link.with_name(
            f".{self.config.current_link.name}.{self.config.change_id}.tmp"
        )
        if temporary.exists() or temporary.is_symlink():
            raise ContractViolation("candidate current-link temp already exists")
        temporary.symlink_to(self.config.new_release, target_is_directory=True)
        os.replace(temporary, self.config.current_link)
        fsync_directory(self.config.current_link.parent)
        self._systemctl("daemon-reload")
        self._verify_installed_candidate()

    def _verify_installed_candidate(self) -> None:
        if self.config.current_link.resolve() != self.config.new_release.resolve():
            raise ContractViolation("current link does not target candidate")
        if not self.config.production:
            return
        for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER):
            installed = self.config.systemd_root / unit
            candidate = self.config.new_release / "systemd" / unit
            if (
                not installed.is_file()
                or installed.is_symlink()
                or installed.read_bytes() != candidate.read_bytes()
            ):
                raise ContractViolation(f"installed candidate unit mismatch:{unit}")
            status = installed.stat(follow_symlinks=False)
            if self.config.production and (
                status.st_uid != 0
                or status.st_gid != 0
                or stat.S_IMODE(status.st_mode) != 0o644
            ):
                raise ContractViolation(f"installed candidate unit metadata mismatch:{unit}")
            if self._property(unit, "FragmentPath") != str(installed):
                raise ContractViolation(f"candidate unit fragment mismatch:{unit}")
            if self._property(unit, "DropInPaths"):
                raise ContractViolation(f"candidate unit drop-in forbidden:{unit}")
        for installed, candidate in (
            (
                Path("/usr/local/sbin") / HEALTH_BRIDGE,
                self.config.new_release / "systemd" / HEALTH_BRIDGE,
            ),
            (
                Path("/usr/local/libexec/polymarket/server_health_heartbeat.py"),
                self.config.new_release / "app/server_health_heartbeat.py",
            ),
        ):
            if (
                not installed.is_file()
                or installed.is_symlink()
                or installed.read_bytes() != candidate.read_bytes()
            ):
                raise ContractViolation(f"installed candidate asset mismatch:{installed}")
            status = installed.stat(follow_symlinks=False)
            expected_mode = 0o755 if installed.name == HEALTH_BRIDGE else 0o644
            if self.config.production and (
                status.st_uid != 0
                or status.st_gid != 0
                or stat.S_IMODE(status.st_mode) != expected_mode
            ):
                raise ContractViolation(
                    f"installed candidate asset metadata mismatch:{installed}"
                )
        for spec in PROFILE_SPECS:
            for unit in (spec.primary_unit, spec.standby_unit):
                if self._property(unit, "WorkingDirectory") != "/opt/polymarket-live/current/app":
                    raise ContractViolation(f"candidate unit working directory mismatch:{unit}")
                command = self._property(unit, "ExecStart")
                if (
                    "/opt/polymarket-live/venv/bin/python" not in command
                    or f"/opt/polymarket-live/current/app/{spec.app_script}" not in command
                ):
                    raise ContractViolation(f"candidate unit command mismatch:{unit}")
                environment = self._property(unit, "Environment")
                for expected in (
                    f"POLYMARKET_SHARED_WALLET_LOCK_PATH={self.wallet_lock}",
                    f"POLYMARKET_SHARED_WALLET_COORDINATOR_PATH={self.coordinator}",
                ):
                    if expected not in environment:
                        raise ContractViolation(f"candidate unit environment mismatch:{unit}")
                environment_files = self._property(unit, "EnvironmentFiles")
                if str(self._env_path(spec)) not in environment_files:
                    raise ContractViolation(f"candidate unit env file mismatch:{unit}")

    def _verify_primary_lock_owner(self, spec: ProfileSpec, pid: int) -> None:
        if not self.config.production:
            return
        self._verify_lock_owner(
            spec=spec,
            pid=pid,
            lock_path=self._candidate_profile_lock(spec),
            label="primary runtime",
        )

    def _verify_legacy_primary_lock_owner(
        self, spec: ProfileSpec, pid: int
    ) -> None:
        if not self.config.production:
            return
        result = self.runner.run(
            ("/usr/bin/sudo", "-n", "/usr/bin/lslocks", "-n", "-o", "PID,PATH")
        )
        compatible_paths = (
            self._legacy_profile_lock(spec),
            self._candidate_profile_lock(spec),
        )
        owners = {path: [] for path in compatible_paths}
        for line in result.stdout.splitlines():
            fields = line.split(maxsplit=1)
            if len(fields) != 2:
                continue
            for path in compatible_paths:
                if fields[1] == str(path):
                    owners[path].append(fields[0])
        expected = [str(pid)]
        held = [path for path, path_owners in owners.items() if path_owners == expected]
        unused_are_empty = all(
            path_owners == [] or path in held
            for path, path_owners in owners.items()
        )
        if len(held) != 1 or not unused_are_empty:
            raise ContractViolation(
                "compatible old primary runtime lock owner mismatch:"
                f"{spec.key}:"
                + ":".join(
                    f"{path}={path_owners}" for path, path_owners in owners.items()
                )
            )

    def _verify_lock_owner(
        self,
        *,
        spec: ProfileSpec,
        pid: int,
        lock_path: Path,
        label: str,
    ) -> None:
        result = self.runner.run(
            ("/usr/bin/sudo", "-n", "/usr/bin/lslocks", "-n", "-o", "PID,PATH")
        )
        owners = []
        for line in result.stdout.splitlines():
            fields = line.split(maxsplit=1)
            if len(fields) == 2 and fields[1] == str(lock_path):
                owners.append(fields[0])
        if owners != [str(pid)]:
            raise ContractViolation(
                f"{label} lock owner mismatch:{spec.key}:{owners}"
            )

    def _verify_candidate_process(self, unit: str, spec: ProfileSpec) -> int:
        if not self.config.production:
            return os.getpid()
        if not self._active(unit):
            raise ContractViolation(f"candidate unit inactive:{unit}")
        pid_text = self._property(unit, "MainPID")
        if not pid_text.isdigit() or int(pid_text) <= 0:
            raise ContractViolation(f"candidate MainPID invalid:{unit}")
        pid = int(pid_text)
        if Path(f"/proc/{pid}/cwd").resolve() != self.config.new_release / "app":
            raise ContractViolation(f"candidate cwd mismatch:{unit}")
        if Path(f"/proc/{pid}/exe").resolve() != self.config.venv_python.resolve():
            raise ContractViolation(f"candidate executable mismatch:{unit}")
        mode = "--run" if unit == spec.primary_unit else "--run-hot-standby"
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
        expected_command = (
            f"/opt/polymarket-live/current/app/{spec.app_script} {mode} "
            f"--runtime-dir {self.runtimes[spec.key]}"
        )
        if expected_command not in command:
            raise ContractViolation(f"candidate command mismatch:{unit}")
        return pid

    def _runtime_text(self, database: Path, key: str) -> str:
        value = sqlite_scalar_ro(
            database,
            "SELECT COALESCE((SELECT value FROM runtime_state WHERE key=?),'')",
            (key,),
        )
        return str(value or "")

    def _runtime_int(self, database: Path, key: str) -> int:
        value = self._runtime_text(database, key) or "0"
        try:
            return int(value)
        except ValueError as exc:
            raise ContractViolation(f"runtime value is not integer:{key}:{value}") from exc

    def _runtime_snapshot(
        self, database: Path, keys: Sequence[str]
    ) -> dict[str, str]:
        ordered = tuple(dict.fromkeys(str(key) for key in keys))
        if not ordered:
            return {}
        placeholders = ",".join("?" for _ in ordered)
        connection = _ro_connection(database)
        try:
            rows = connection.execute(
                f"SELECT key,value FROM runtime_state WHERE key IN ({placeholders})",
                ordered,
            ).fetchall()
        except sqlite3.Error as exc:
            raise ContractViolation(
                f"runtime snapshot query failed:{database}"
            ) from exc
        finally:
            connection.close()
        found = {str(row[0]): str(row[1] or "") for row in rows}
        return {key: found.get(key, "") for key in ordered}

    def _verify_profile_acceptance(self, spec: ProfileSpec) -> dict[str, Any]:
        database = self._live_database(spec)
        baseline = self.baselines[spec.key]
        primary_pid = self._verify_candidate_process(spec.primary_unit, spec)
        self._verify_candidate_process(spec.standby_unit, spec)
        for unit in (spec.primary_unit, spec.standby_unit):
            if self.config.production:
                restart = self._property(unit, "NRestarts")
                if not restart.isdigit():
                    raise ContractViolation(f"candidate restart count invalid:{unit}")
                current_restart = int(restart)
                if unit not in self.restart_baselines:
                    if current_restart != 0:
                        raise ContractViolation(
                            f"candidate restarted before first acceptance:{unit}"
                        )
                    self.restart_baselines[unit] = 0
                elif current_restart != self.restart_baselines[unit]:
                    raise ContractViolation(f"candidate restart count changed:{unit}")
        runtime = self._runtime_snapshot(
            database,
            (
                "last_successful_cycle_at_ms",
                "last_cycle_outcome",
                "last_processed_block",
                "current_head",
                "external_head_incident_active",
                "hot_standby_joined_at_ms",
                "hot_standby_primary_runtime_lock_seen_at_ms",
                "hot_standby_last_observed_head",
                "operator_planned_resume_change_id",
                "operator_planned_resume_state",
                "operator_planned_resume_from_block",
                "operator_pre_repair_forward_recovery_armed",
                "operator_planned_resume_completed_at_ms",
                "operator_planned_resume_processed_to_block",
            ),
        )

        def runtime_text(key: str) -> str:
            return runtime.get(key, "")

        def runtime_int(key: str) -> int:
            value = runtime_text(key) or "0"
            try:
                return int(value)
            except ValueError as exc:
                raise ContractViolation(
                    f"runtime value is not integer:{key}:{value}"
                ) from exc

        successful = runtime_int("last_successful_cycle_at_ms")
        outcome = runtime_text("last_cycle_outcome")
        cursor = runtime_int("last_processed_block")
        if cursor < int(baseline["last_processed_block"]):
            raise ContractViolation(f"candidate cursor regressed:{spec.key}")
        if outcome in SUCCESS_OUTCOMES:
            if successful <= int(baseline["last_successful_cycle_at_ms"]):
                raise ContractViolation(f"candidate cycle did not advance:{spec.key}")
        elif outcome == "EXTERNAL_HEAD_RETRY_PENDING":
            current_head = runtime_int("current_head")
            incident_active = runtime_text("external_head_incident_active").lower()
            if (
                successful < int(baseline["last_successful_cycle_at_ms"])
                or cursor <= int(baseline["last_processed_block"])
                or current_head < cursor
                or incident_active != "true"
            ):
                raise ContractViolation(
                    f"candidate external head retry evidence invalid:{spec.key}"
                )
        else:
            raise ContractViolation(
                f"candidate cycle outcome unknown:{spec.key}:{outcome}"
            )
        for key in (
            "hot_standby_joined_at_ms",
            "hot_standby_primary_runtime_lock_seen_at_ms",
            "hot_standby_last_observed_head",
        ):
            if runtime_int(key) <= int(baseline[key]):
                raise ContractViolation(f"candidate standby evidence did not advance:{spec.key}:{key}")
        expected_change = f"{self.config.change_id}-{spec.change_suffix}"
        expected_runtime = {
            "operator_planned_resume_change_id": expected_change,
            "operator_planned_resume_state": "COMPLETED",
            "operator_planned_resume_from_block": "",
            "operator_pre_repair_forward_recovery_armed": "false",
        }
        for key, expected in expected_runtime.items():
            if runtime_text(key) != expected:
                raise ContractViolation(f"candidate resume receipt mismatch:{spec.key}:{key}")
        completed_at = runtime_int("operator_planned_resume_completed_at_ms")
        if completed_at < self.candidate_start_boundary_ns // 1_000_000:
            raise ContractViolation(f"candidate resume completion is stale:{spec.key}")
        processed_to = runtime_int("operator_planned_resume_processed_to_block")
        if processed_to < int(baseline["last_processed_block"]) or cursor < processed_to:
            raise ContractViolation(f"candidate resume cursor proof failed:{spec.key}")
        connection = _ro_connection(database)
        try:
            runtime_error_max = verify_append_only_watermark(
                connection,
                table="runtime_errors",
                baseline=int(baseline["runtime_error_id"]),
                label=f"runtime-errors:{spec.key}",
            )
            action_transition_max = verify_append_only_watermark(
                connection,
                table="action_transitions",
                baseline=int(baseline["action_transition_id"]),
                label=f"action-transitions:{spec.key}",
            )
            error_rows = connection.execute(
                "SELECT id,category FROM runtime_errors WHERE id>? ORDER BY id",
                (int(baseline["runtime_error_id"]),),
            ).fetchall()
            categories = [str(row[1]) for row in error_rows]
            unknown_categories = set(categories) - EXTERNAL_RUNTIME_CATEGORIES
            if unknown_categories:
                raise ContractViolation(
                    f"candidate runtime error category unknown:{spec.key}:{sorted(unknown_categories)}"
                )
            if outcome == "SUCCESS_REDEMPTION_MAINTENANCE_PENDING":
                if not categories or categories[-1] != "EXTERNAL_REDEMPTION_MAINTENANCE":
                    raise ContractViolation(f"candidate maintenance evidence missing:{spec.key}")
            action_states = {
                str(row[0])
                for row in connection.execute(
                    "SELECT status FROM action_transitions WHERE id>?",
                    (int(baseline["action_transition_id"]),),
                )
            }
            if action_states - ACTION_TRANSITION_ALLOWLIST:
                raise ContractViolation(
                    f"candidate action transition unknown:{spec.key}:"
                    f"{sorted(action_states - ACTION_TRANSITION_ALLOWLIST)}"
                )
            try:
                repair_manifest_hashes = json.loads(
                    str(baseline["repair_manifest_hashes_json"])
                )
            except (KeyError, TypeError, json.JSONDecodeError) as exc:
                raise ContractViolation(
                    f"repair manifest baseline invalid:{spec.key}"
                ) from exc
            if not isinstance(repair_manifest_hashes, list) or any(
                not isinstance(item, str) for item in repair_manifest_hashes
            ):
                raise ContractViolation(f"repair manifest baseline invalid:{spec.key}")
            repair_max = verify_repair_acceptance_delta(
                connection,
                baseline_transition_id=int(baseline["repair_transition_id"]),
                expected_manifest_hashes=repair_manifest_hashes,
            )
        finally:
            connection.close()
        self._verify_primary_lock_owner(spec, primary_pid)
        return {
            "mode": "ACTIVE",
            "paused": False,
            "primary_pid": primary_pid,
            "outcome": outcome,
            "successful_cycle_at_ms": successful,
            "cursor": cursor,
            "runtime_error_max_id": (
                runtime_error_max
            ),
            "action_transition_max_id": action_transition_max,
            "repair_transition_max_id": repair_max,
            "resume_completed_at_ms": completed_at,
            "resume_processed_to_block": processed_to,
        }

    def _verify_paused_profile_unit_policy(
        self, spec: ProfileSpec, *, label: str
    ) -> dict[str, str]:
        if self._profile_original_mode(spec) != "PAUSED":
            raise ContractViolation(f"{label} profile was not originally paused:{spec.key}")
        evidence: dict[str, str] = {}
        for role, unit in (
            ("primary", spec.primary_unit),
            ("standby", spec.standby_unit),
        ):
            activity = (
                self._property(unit, "ActiveState")
                if self.config.production
                else "inactive"
            )
            enablement = self._enabled(unit) if self.config.production else "disabled"
            if activity != "inactive" or enablement != "disabled":
                raise ContractViolation(f"paused {label} unit policy mismatch:{unit}")
            if self.config.production and self._property(unit, "MainPID") != "0":
                raise ContractViolation(f"paused {label} unit PID is nonzero:{unit}")
            evidence[f"{role}_state"] = activity
            evidence[f"{role}_unit_file_state"] = enablement
        return evidence

    def _verify_paused_profile_acceptance(self, spec: ProfileSpec) -> dict[str, Any]:
        unit_evidence = self._verify_paused_profile_unit_policy(
            spec, label="candidate"
        )
        database = self._live_database(spec)
        baseline = self.baselines.get(spec.key)
        if not isinstance(baseline, Mapping):
            raise ContractViolation(f"paused runtime baseline missing:{spec.key}")
        runtime = self._runtime_snapshot(
            database,
            (
                "operator_planned_resume_change_id",
                "operator_planned_resume_state",
                "operator_planned_resume_from_block",
                "operator_pre_repair_forward_recovery_armed",
                "last_processed_block",
            ),
        )
        resume_change = runtime["operator_planned_resume_change_id"]
        resume_state = runtime["operator_planned_resume_state"]
        resume_from = runtime["operator_planned_resume_from_block"]
        resume_armed = runtime["operator_pre_repair_forward_recovery_armed"]
        try:
            cursor = int(runtime["last_processed_block"] or "0")
        except ValueError as exc:
            raise ContractViolation(
                f"runtime value is not integer:last_processed_block:"
                f"{runtime['last_processed_block']}"
            ) from exc
        if cursor < int(baseline["last_processed_block"]):
            raise ContractViolation(f"paused candidate cursor regressed:{spec.key}")
        for key, actual in (
            ("operator_planned_resume_change_id", resume_change),
            ("operator_planned_resume_state", resume_state),
            ("operator_planned_resume_from_block", resume_from),
            ("operator_pre_repair_forward_recovery_armed", resume_armed),
        ):
            if actual != str(baseline.get(key) or ""):
                raise ContractViolation(
                    f"paused candidate resume evidence drift:{spec.key}:{key}"
                )
        return {
            "mode": "PAUSED",
            "paused": True,
            **unit_evidence,
            "cursor": cursor,
            "resume_change_id": resume_change,
            "resume_state": resume_state,
            "resume_from_block": resume_from,
            "resume_armed": resume_armed == "true",
        }

    def _verify_candidate_acceptance_once(
        self,
        *,
        full_integrity: bool = True,
        official_digest: str | None = None,
    ) -> dict[str, Any]:
        evidence = {
            spec.key: (
                self._verify_profile_acceptance(spec)
                if self._profile_original_mode(spec) == "ACTIVE"
                else self._verify_paused_profile_acceptance(spec)
            )
            for spec in PROFILE_SPECS
        }
        self._verify_database_gates(
            cutover=True, full_integrity=full_integrity
        )
        self._verify_env_files(capture=False)
        verify_manifest(
            self.config.new_release,
            self.config.expected_manifest_digest,
            REQUIRED_ASSETS,
        )
        self._verify_installed_candidate()
        self._verify_redemption_history_prefixes()
        digest = (
            self._official_digest_and_order_gate()
            if official_digest is None
            else official_digest
        )
        evidence["official_redemption_conservation"] = (
            self._conserve_latest_official_redemption_snapshot(digest)
        )
        return evidence

    def _verify_candidate_acceptance_with_owner_isolated_databases(
        self, *, full_integrity: bool
    ) -> dict[str, Any]:
        if self.config.production and not self._final_lock_held:
            raise ContractViolation(
                "candidate acceptance requires the final shared wallet lock"
            )
        # Fetch authenticated official evidence under the wallet lock, then
        # inspect service-owned SQLite through the owner-isolated reader.
        # The running executors are never paused by the release verifier.
        official_digest = self._official_digest_and_order_gate()
        return self._verify_candidate_acceptance_once(
            full_integrity=full_integrity,
            official_digest=official_digest,
        )

    def _capture_candidate_status_mtime_baselines(self) -> None:
        baselines: dict[str, int] = {}
        for spec in PROFILE_SPECS:
            if self._profile_original_mode(spec) != "ACTIVE":
                continue
            path = self.runtimes[spec.key] / "status.json"
            try:
                status = path.lstat()
            except FileNotFoundError:
                baselines[spec.key] = 0
                continue
            if not stat.S_ISREG(status.st_mode):
                raise ContractViolation(
                    f"candidate status path is not regular:{spec.key}"
                )
            baselines[spec.key] = status.st_mtime_ns
        self.candidate_status_mtime_baselines = baselines

    def _verify_candidate_startup_status_once(self) -> dict[str, Any]:
        if self.candidate_start_boundary_ns <= 0:
            raise ContractViolation("candidate start boundary is missing")
        evidence: dict[str, Any] = {}
        boundary_ms = self.candidate_start_boundary_ns // 1_000_000
        for spec in PROFILE_SPECS:
            if self._profile_original_mode(spec) != "ACTIVE":
                continue
            if spec.key not in self.candidate_status_mtime_baselines:
                raise ContractViolation(
                    f"candidate status baseline is missing:{spec.key}"
                )
            path = self.runtimes[spec.key] / "status.json"
            try:
                status = path.lstat()
                if not stat.S_ISREG(status.st_mode):
                    raise ContractViolation(
                        f"candidate status path is not regular:{spec.key}"
                    )
                payload = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError as exc:
                raise ContractViolation(
                    f"candidate status is missing:{spec.key}"
                ) from exc
            except (OSError, json.JSONDecodeError) as exc:
                raise ContractViolation(
                    f"candidate status is unreadable:{spec.key}"
                ) from exc
            generated_at_ms = payload.get("generated_at_ms")
            if (
                not isinstance(generated_at_ms, int)
                or isinstance(generated_at_ms, bool)
                or generated_at_ms < boundary_ms
                or status.st_mtime_ns
                <= self.candidate_status_mtime_baselines[spec.key]
            ):
                raise ContractViolation(
                    f"candidate status is not fresh:{spec.key}"
                )
            evidence[spec.key] = {
                "path": str(path),
                "mtime_ns": status.st_mtime_ns,
                "generated_at_ms": generated_at_ms,
            }
        return evidence

    def _wait_for_candidate_startup_readiness(self, deadline: float) -> None:
        if not self.config.production:
            return
        last_error: ContractViolation | None = None
        while time.monotonic() < deadline:
            try:
                self._verify_candidate_startup_status_once()
                return
            except ContractViolation as exc:
                last_error = exc
                time.sleep(SCHEDULER_YIELD_SECONDS)
        raise ContractViolation(f"candidate startup status timeout:{last_error}")

    def _wait_for_candidate_and_lock(self) -> dict[str, Any]:
        deadline = self._deadline()
        # Do not let the root release verifier become the first process to
        # open a freshly replaced WAL database.  A new primary status file is
        # filesystem-only proof that every active profile has initialized its
        # own database before any root SQLite acceptance read begins.
        self._wait_for_candidate_startup_readiness(deadline)
        last_error: ContractViolation | None = None
        while time.monotonic() < deadline:
            try:
                self._acquire_wallet_lock(final=True)
                return self._verify_candidate_acceptance_with_owner_isolated_databases(
                    full_integrity=False
                )
            except ContractViolation as exc:
                last_error = exc
                if self._final_lock_held:
                    self._release_wallet_lock(final=True)
                # Cadence is only a yield; the pass boundary is evidence plus
                # the manager-derived deadline, never an attempt threshold.
                time.sleep(SCHEDULER_YIELD_SECONDS)
        raise ContractViolation(f"candidate acceptance timeout:{last_error}")

    def _normalized_pre_stop_health_payload(
        self, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        normalized = json.loads(json.dumps(payload))
        if "runtime_lock_contract" not in normalized:
            # The installed N-1 heartbeat predates this payload field.  The
            # release preflight has already verified the runtime root and all
            # legacy/candidate/shared lock files directly before invoking the
            # old health unit, so carry only that verified result across the
            # one pre-stop compatibility boundary.  Candidate health remains
            # strict and must produce its own field after startup.
            normalized["runtime_lock_contract"] = {
                "state": "OK",
                "issues": [],
                "source": "RELEASE_PREFLIGHT_LOCK_VERIFICATION",
            }
        profiles = normalized.get("profiles")
        if not isinstance(profiles, dict):
            return normalized
        legacy_rewritten = False
        policy_rewritten = False
        if self.original_activity or self.original_enablement:
            self._validate_original_executor_policy()
            services = normalized.get("services")
            if not isinstance(services, list) or any(
                not isinstance(item, dict) for item in services
            ):
                return normalized
            service_by_name = {
                str(item.get("unit") or ""): item
                for item in services
                if isinstance(item, dict)
            }
            if set(service_by_name) != set(EXECUTOR_UNITS):
                return normalized
            paused_profile_keys: list[str] = []
            paused_unit_names: list[str] = []
            for spec in PROFILE_SPECS:
                row = profiles.get(spec.key)
                if not isinstance(row, dict):
                    continue
                for unit in (spec.primary_unit, spec.standby_unit):
                    service_by_name[unit].setdefault(
                        "UnitFileState", self.original_enablement[unit]
                    )
                    if (
                        str(service_by_name[unit].get("ActiveState") or "")
                        != self.original_activity[unit]
                        or str(service_by_name[unit].get("UnitFileState") or "")
                        != self.original_enablement[unit]
                    ):
                        raise ContractViolation(
                            f"pre-stop executor policy drift:{unit}"
                        )
                row["unit_state"] = dict(service_by_name[spec.primary_unit])
                row["hot_standby_state"] = dict(
                    service_by_name[spec.standby_unit]
                )
                paused = self._profile_original_mode(spec) == "PAUSED"
                row["paused"] = paused
                if paused:
                    paused_profile_keys.append(spec.key)
                    paused_unit_names.extend(
                        (spec.primary_unit, spec.standby_unit)
                    )
                    row["status_issues"] = []
                    row["status_issue_count"] = 0
                    row["external_limitations"] = []
                    row["external_limitation_count"] = 0
            normalized["paused_profiles"] = sorted(paused_profile_keys)
            normalized["service_paused_units"] = sorted(paused_unit_names)
            normalized["service_paused_count"] = len(paused_unit_names)
            normalized["service_active_count"] = sum(
                1
                for item in services
                if str(item.get("ActiveState") or "") == "active"
            )
            normalized["service_inactive_units"] = [
                str(item.get("unit") or "")
                for item in services
                if str(item.get("ActiveState") or "") != "active"
                and str(item.get("unit") or "") not in paused_unit_names
            ]
            timer = normalized.get("health_timer")
            if isinstance(timer, dict):
                timer.setdefault(
                    "UnitFileState",
                    self.old_health_state.get(
                        "timer_enabled", self.original_enablement[HEALTH_TIMER]
                    ),
                )
            policy_rewritten = True
        for spec in PROFILE_SPECS:
            row = profiles.get(spec.key)
            if not isinstance(row, dict):
                continue
            if row.get("paused") is True:
                continue
            if not row.get("last_cycle_outcome"):
                row["last_cycle_outcome"] = self._runtime_text(
                    self._live_database(spec), "last_cycle_outcome"
                ).upper()
            audit = row.get("release_runtime_error_audit")
            if isinstance(audit, dict):
                event_count = _health_count(
                    audit.get("event_count", 0),
                    label=f"legacy-health-events:{spec.key}",
                )
                if self.config.production or int(
                    audit.get("internal_event_count") or 0
                ) > 0 or (
                    event_count > 0
                    and (
                        "latest_category" not in audit
                        or "code_repair_event_count" not in audit
                    )
                ):
                    legacy_rewritten = True
                    baseline = _health_count(
                        audit.get("release_started_at_ms"),
                        label=f"legacy-health-baseline:{spec.key}",
                    )
                    connection = _ro_connection(self._live_database(spec))
                    try:
                        error_rows = connection.execute(
                            "SELECT occurred_at_ms,category,message,details_json "
                            "FROM runtime_errors "
                            "WHERE occurred_at_ms>=? ORDER BY id",
                            (baseline,),
                        ).fetchall()
                    except sqlite3.Error as exc:
                        raise ContractViolation(
                            f"legacy health audit query failed:{spec.key}"
                        ) from exc
                    finally:
                        connection.close()
                    category_counts: dict[str, int] = {}
                    code_repair = 0
                    for _occurred_at_ms, category, message, details_json in error_rows:
                        normalized_category = str(category)
                        category_counts[normalized_category] = (
                            category_counts.get(normalized_category, 0) + 1
                        )
                        if "CODE_REPAIR_REQUIRED" in "\n".join(
                            (
                                normalized_category,
                                str(message or ""),
                                str(details_json or ""),
                            )
                        ).upper():
                            code_repair += 1
                    external_count = sum(
                        count
                        for category, count in category_counts.items()
                        if category.upper().startswith("EXTERNAL_")
                    )
                    total = sum(category_counts.values())
                    audit = {
                        "state": "OK" if total == 0 else "ERRORS_OBSERVED",
                        "release_started_at_ms": baseline,
                        "event_count": total,
                        "internal_event_count": total - external_count,
                        "external_event_count": external_count,
                        "code_repair_event_count": code_repair,
                        "latest_category": (
                            "" if not error_rows else str(error_rows[-1][1])
                        ),
                        "category_counts": category_counts,
                    }
                    row["release_runtime_error_audit"] = audit
                else:
                    audit.setdefault("code_repair_event_count", 0)
                    if event_count == 0:
                        audit.setdefault("latest_category", "")
            if not isinstance(audit, dict):
                # N-1 health payloads may not have a runtime-error audit,
                # but their immutable action/target evidence is still needed
                # to distinguish old display bugs from an unsafe release.
                audit = {}
                row["release_runtime_error_audit"] = audit
            row.setdefault("external_limitations", [])
            row.setdefault("runtime_internal_error_count", 0)
            row.setdefault("runtime_code_repair_count", 0)
            prefix = _health_issue_prefix(spec.key)
            issues = list(row.get("status_issues") or [])
            external = list(row.get("external_limitations") or [])
            action_issue_prefixes = (
                f"{prefix}_UNRESOLVED_ACTIONS:",
                f"{prefix}_ACTION_FIDELITY_MISSING_TARGETS:",
                f"{prefix}_ACTION_FIDELITY_UNCLASSIFIED_TARGETS:",
            )
            action_internal_issue_prefix = (
                f"{prefix}_ACTION_FIDELITY_INTERNAL_ERRORS:"
            )
            action_nonconservation = (
                f"{prefix}_ACTION_FIDELITY_NONCONSERVATION"
            )
            if action_nonconservation in issues or any(
                str(issue).startswith(
                    (*action_issue_prefixes, action_internal_issue_prefix)
                )
                for issue in issues
            ):
                action_evidence = _pre_stop_candidate_action_evidence(
                    self._live_database(spec)
                )
                recoverable_count = int(
                    action_evidence[
                        "recoverable_legacy_stable_causal_prefix_action_count"
                    ]
                )
                internal_issues = [
                    str(issue)
                    for issue in issues
                    if str(issue).startswith(action_internal_issue_prefix)
                ]
                confirmed_zero_fill_count = int(
                    action_evidence.get(
                        "legacy_confirmed_zero_fill_count", 0
                    )
                )
                compatible_internal_issue = not internal_issues or (
                    (recoverable_count + confirmed_zero_fill_count) > 0
                    and internal_issues
                    == [
                        f"{action_internal_issue_prefix}"
                        f"{recoverable_count + confirmed_zero_fill_count}"
                    ]
                )
                if (
                    action_evidence["profile_eligible_observed"]
                    == action_evidence["accounted"]
                    and action_evidence["internal_error"] == 0
                    and action_evidence["unclassified_target"] == 0
                    and action_evidence["missing_target"] == 0
                    and action_evidence[
                        "retryable_target_terminal_transition_mismatch"
                    ]
                    == 0
                    and action_evidence["unsafe_submission_action_count"] == 0
                    and compatible_internal_issue
                ):
                    issues = [
                        issue
                        for issue in issues
                        if str(issue) != action_nonconservation
                        and not str(issue).startswith(action_issue_prefixes)
                        and not str(issue).startswith(
                            action_internal_issue_prefix
                        )
                    ]
                    if confirmed_zero_fill_count:
                        limitation = (
                            f"{prefix}_LEGACY_OFFICIAL_CONFIRMED_ZERO_FILL:"
                            f"{confirmed_zero_fill_count}"
                        )
                        if limitation not in external:
                            external.append(limitation)
                    row["pre_stop_classified_action_evidence"] = {
                        "profile_eligible_observed": action_evidence[
                            "profile_eligible_observed"
                        ],
                        "accounted": action_evidence["accounted"],
                        "unsafe_submission_action_count": 0,
                        "legacy_missing_target": action_evidence[
                            "legacy_missing_target"
                        ],
                        "historical_nonfollowable_internal_count": action_evidence[
                            "historical_nonfollowable_internal_count"
                        ],
                        "legacy_confirmed_zero_fill_count": (
                            confirmed_zero_fill_count
                        ),
                    }
                    if recoverable_count:
                        recoverable_ids = list(
                            action_evidence[
                                "recoverable_legacy_stable_causal_prefix_action_ids"
                            ]
                        )
                        row[
                            "pre_stop_recoverable_stable_causal_prefix_evidence"
                        ] = {
                            "action_count": recoverable_count,
                            "action_ids": recoverable_ids,
                            "action_ids_sha256": hashlib.sha256(
                                "\n".join(recoverable_ids).encode("utf-8")
                            ).hexdigest(),
                            "unsafe_submission_action_count": 0,
                        }
                    legacy_rewritten = True
            bounded_retry_issue_prefix = (
                f"{prefix}_BOUNDED_RETRY_TARGET_NONCONSERVATION:"
            )
            bounded_retry_issues = [
                str(issue)
                for issue in issues
                if str(issue).startswith(bounded_retry_issue_prefix)
            ]
            if len(bounded_retry_issues) == 1:
                raw_bounded_retry_count = bounded_retry_issues[0].removeprefix(
                    bounded_retry_issue_prefix
                )
                try:
                    bounded_retry_issue_count = _health_count(
                        raw_bounded_retry_count,
                        label=f"legacy-bounded-retry-overage:{spec.key}",
                    )
                except ContractViolation:
                    bounded_retry_issue_count = -1
                if bounded_retry_issue_count > 0:
                    improvement_evidence = (
                        _pre_stop_bounded_retry_price_improvement_evidence(
                            self._live_database(spec)
                        )
                    )
                    if (
                        improvement_evidence["invalid_count"] == 0
                        and improvement_evidence["verified_count"]
                        == bounded_retry_issue_count
                    ):
                        issues = [
                            issue
                            for issue in issues
                            if str(issue) != bounded_retry_issues[0]
                        ]
                        row[
                            "pre_stop_bounded_retry_price_improvement_evidence"
                        ] = {
                            "verified_count": bounded_retry_issue_count,
                            "invalid_count": 0,
                            "source": "IMMUTABLE_ACTION_TARGET_AND_RECEIPT",
                        }
                        legacy_rewritten = True
            redemption_issue_prefix = f"{prefix}_UNRESOLVED_REDEMPTIONS:"
            if any(
                str(issue).startswith(redemption_issue_prefix) for issue in issues
            ):
                redemption_evidence = _pre_stop_redemption_evidence(
                    self._live_database(spec)
                )
                if redemption_evidence["unsafe_redemption_count"] == 0:
                    issues = [
                        issue
                        for issue in issues
                        if not str(issue).startswith(redemption_issue_prefix)
                    ]
                    row["pre_stop_safe_redemption_block_evidence"] = {
                        "safe_block_count": redemption_evidence[
                            "legacy_unresolved_redemption_count"
                        ],
                        "unsafe_redemption_count": 0,
                    }
                    legacy_rewritten = True
            issues = [
                issue
                for issue in issues
                if not str(issue).startswith(
                    (
                        f"{prefix}_POST_RELEASE_INTERNAL_ERROR_EVENTS:",
                        f"{prefix}_POST_RELEASE_CODE_REPAIR_EVENTS:",
                    )
                )
            ]
            external = [
                limitation
                for limitation in external
                if not str(limitation).startswith(
                    f"{prefix}_POST_RELEASE_EXTERNAL_ERROR_EVENTS:"
                )
            ]
            internal_count = int(audit.get("internal_event_count") or 0)
            external_count = int(audit.get("external_event_count") or 0)
            code_repair_count = int(audit.get("code_repair_event_count") or 0)
            if (
                internal_count > 0
                and code_repair_count == 0
                and int(row.get("runtime_internal_error_count") or 0) == 0
                and int(row.get("runtime_code_repair_count") or 0) == 0
            ):
                current_outcome = self._runtime_text(
                    self._live_database(spec), "last_cycle_outcome"
                ).upper()
                successful_at_ms = self._runtime_int(
                    self._live_database(spec), "last_successful_cycle_at_ms"
                )
                connection = _ro_connection(self._live_database(spec))
                try:
                    recovered_rows = connection.execute(
                        "SELECT occurred_at_ms,category,message,details_json "
                        "FROM runtime_errors WHERE occurred_at_ms>=? "
                        "ORDER BY id",
                        (int(audit.get("release_started_at_ms") or 0),),
                    ).fetchall()
                except sqlite3.Error as exc:
                    raise ContractViolation(
                        f"pre-stop recovery audit query failed:{spec.key}"
                    ) from exc
                finally:
                    connection.close()
                internal_rows = [
                    item
                    for item in recovered_rows
                    if not str(item[1]).upper().startswith("EXTERNAL_")
                ]
                latest_internal_at_ms = max(
                    (int(item[0]) for item in internal_rows), default=0
                )
                contains_code_repair = any(
                    "CODE_REPAIR_REQUIRED"
                    in "\n".join(str(value or "") for value in item[1:]).upper()
                    for item in internal_rows
                )
                if (
                    len(internal_rows) == internal_count
                    and not contains_code_repair
                    and current_outcome
                    in (SUCCESS_OUTCOMES | HEALTH_EXTERNAL_RETRY_OUTCOMES)
                    and 0 < latest_internal_at_ms < successful_at_ms
                ):
                    original_audit = dict(audit)
                    external_rows = [
                        item
                        for item in recovered_rows
                        if str(item[1]).upper().startswith("EXTERNAL_")
                    ]
                    external_categories: dict[str, int] = {}
                    for item in external_rows:
                        category = str(item[1])
                        external_categories[category] = (
                            external_categories.get(category, 0) + 1
                        )
                    audit = {
                        "state": "OK" if not external_rows else "ERRORS_OBSERVED",
                        "release_started_at_ms": int(
                            original_audit.get("release_started_at_ms") or 0
                        ),
                        "event_count": len(external_rows),
                        "internal_event_count": 0,
                        "external_event_count": len(external_rows),
                        "code_repair_event_count": 0,
                        "latest_category": (
                            "" if not external_rows else str(external_rows[-1][1])
                        ),
                        "category_counts": external_categories,
                    }
                    row["release_runtime_error_audit"] = audit
                    row["pre_stop_recovered_internal_error_evidence"] = {
                        "state": "RECOVERED_BEFORE_LAST_SUCCESSFUL_CYCLE",
                        "event_count": internal_count,
                        "latest_occurred_at_ms": latest_internal_at_ms,
                        "last_successful_cycle_at_ms": successful_at_ms,
                        "current_outcome": current_outcome,
                        "original_audit": original_audit,
                    }
                    internal_count = 0
                    external_count = len(external_rows)
                    code_repair_count = 0
                    legacy_rewritten = True
            if internal_count:
                issues.append(
                    f"{prefix}_POST_RELEASE_INTERNAL_ERROR_EVENTS:{internal_count}"
                )
            if code_repair_count:
                issues.append(
                    f"{prefix}_POST_RELEASE_CODE_REPAIR_EVENTS:{code_repair_count}"
                )
            if external_count:
                external.append(
                    f"{prefix}_POST_RELEASE_EXTERNAL_ERROR_EVENTS:{external_count}"
                )
            latest_category = str(audit.get("latest_category") or "")
            if (
                row.get("last_cycle_outcome")
                == "SUCCESS_REDEMPTION_MAINTENANCE_PENDING"
                and latest_category == "EXTERNAL_REDEMPTION_MAINTENANCE"
                and internal_count == 0
                and code_repair_count == 0
            ):
                issues = [
                    issue
                    for issue in issues
                    if issue != f"{prefix}_LAST_CYCLE_NOT_SUCCESS"
                ]
                limitation = (
                    f"{prefix}_EXTERNAL_REDEMPTION_MAINTENANCE_PENDING"
                )
                if limitation not in external:
                    external.append(limitation)
            legacy_ready_issue = (
                f"{prefix}_AUTOMATIC_REDEMPTION_WORKER_INVALID"
            )
            if legacy_ready_issue in issues:
                status_path = self.runtimes[spec.key] / "status.json"
                try:
                    if not status_path.is_file() or status_path.is_symlink():
                        raise OSError("status is not a regular file")
                    live_status = json.loads(
                        status_path.read_text(encoding="utf-8")
                    )
                except (OSError, json.JSONDecodeError):
                    live_status = None
                redemption = (
                    live_status.get("automatic_redemption")
                    if isinstance(live_status, Mapping)
                    else None
                )
                if (
                    isinstance(redemption, Mapping)
                    and redemption.get("enabled") is True
                    and str(redemption.get("worker_state") or "").upper()
                    == "READY"
                ):
                    issues = [
                        issue for issue in issues if issue != legacy_ready_issue
                    ]
                    row["pre_stop_ready_redemption_worker_evidence"] = {
                        "enabled": True,
                        "worker_state": "READY",
                    }
                    legacy_rewritten = True
            if (
                latest_category == "EXTERNAL_REDEMPTION_CYCLE"
                and internal_count == 0
                and code_repair_count == 0
            ):
                issues = [
                    issue
                    for issue in issues
                    if issue != f"{prefix}_AUTOMATIC_REDEMPTION_WORKER_INVALID"
                ]
            row["status_issues"] = issues
            row["status_issue_count"] = len(issues)
            row["external_limitations"] = external
            row["external_limitation_count"] = len(external)
        internal_degraded = any(
            (
                (
                    int(row.get("status_issue_count") or 0) > 0
                    or int(row.get("runtime_internal_error_count") or 0) > 0
                    or int(row.get("runtime_code_repair_count") or 0) > 0
                    or int(
                        dict(row.get("release_runtime_error_audit") or {}).get(
                            "internal_event_count", 0
                        )
                        or 0
                    )
                    > 0
                    or int(
                        dict(row.get("release_runtime_error_audit") or {}).get(
                            "code_repair_event_count", 0
                        )
                        or 0
                    )
                    > 0
                )
                and row.get("paused") is not True
            )
            or dict(row.get("sqlite_integrity") or {}).get("state") != "OK"
            for row in profiles.values()
            if isinstance(row, Mapping)
        )
        coordinator = normalized.get("coordinator")
        internal_degraded = internal_degraded or not isinstance(
            coordinator, Mapping
        ) or coordinator.get("state") != "OK"
        internal_degraded = internal_degraded or bool(
            normalized.get("service_inactive_units")
        ) or int(normalized.get("failed_polymarket_unit_count") or 0) != 0
        external_degraded = any(
            int(row.get("external_limitation_count") or 0) > 0
            for row in profiles.values()
            if isinstance(row, Mapping)
        )
        if legacy_rewritten or policy_rewritten:
            normalized["overall_state"] = (
                "INTERNAL_DEGRADED"
                if internal_degraded
                else "EXTERNAL_DEGRADED"
                if external_degraded
                else "OK"
            )
        return normalized

    def _verify_pre_stop_health(self) -> str:
        path = self.config.health_status_path
        previous_mtime = (
            path.stat(follow_symlinks=False).st_mtime_ns
            if path.is_file() and not path.is_symlink()
            else 0
        )
        previous_generated = 0
        if previous_mtime:
            try:
                previous_payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(previous_payload, Mapping):
                    previous_generated = _health_count(
                        previous_payload.get("generated_at_epoch_ms", 0),
                        label="pre-stop-health-generated",
                    )
            except (OSError, json.JSONDecodeError, ContractViolation):
                previous_generated = 0
        result = CommandResult(0, "", "")
        if self.config.production:
            self._systemctl("reset-failed", HEALTH_UNIT, check=False)
            result = self._systemctl("start", HEALTH_UNIT, check=False)
            deadline = self._deadline()
            while time.monotonic() < deadline:
                if path.is_file() and not path.is_symlink():
                    mtime = path.stat(follow_symlinks=False).st_mtime_ns
                    if mtime > previous_mtime:
                        break
                time.sleep(SCHEDULER_YIELD_SECONDS)
            else:
                raise ContractViolation("pre-stop health status is not fresh")
        if not path.is_file() or path.is_symlink():
            raise ContractViolation("pre-stop health status missing")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContractViolation("pre-stop health status JSON invalid") from exc
        if not isinstance(payload, Mapping):
            raise ContractViolation("pre-stop health status malformed")
        if self.config.production:
            generated = _health_count(
                payload.get("generated_at_epoch_ms"),
                label="pre-stop-health-generated",
            )
            if generated <= previous_generated:
                raise ContractViolation("pre-stop health generation did not advance")
        if self.config.production:
            self._acquire_wallet_lock(final=False)
            try:
                normalized = self._normalized_pre_stop_health_payload(payload)
            finally:
                self._release_wallet_lock(final=False)
        else:
            normalized = self._normalized_pre_stop_health_payload(payload)
        classification = validate_health_payload(normalized, pre_stop=True)
        if self.config.production and result.returncode != 0:
            intentional_legacy_degradation = bool(
                normalized.get("paused_profiles")
            ) or self.old_health_state.get("timer_active") != "active" or any(
                isinstance(row, Mapping)
                and (
                    "pre_stop_recovered_internal_error_evidence" in row
                    or "pre_stop_ready_redemption_worker_evidence" in row
                    or "pre_stop_recoverable_stable_causal_prefix_evidence"
                    in row
                    or "pre_stop_bounded_retry_price_improvement_evidence"
                    in row
                )
                for row in dict(normalized.get("profiles") or {}).values()
            )
            if not intentional_legacy_degradation:
                raise ContractViolation("pre-stop health unit failed")
        self.health_baseline_mtime_ns = path.stat(
            follow_symlinks=False
        ).st_mtime_ns
        return classification

    def _fresh_health(self, baseline_mtime_ns: int) -> tuple[str, str, int]:
        if not self.config.production:
            if not self.config.health_status_path.is_file():
                raise ContractViolation("health status missing")
            mtime = self.config.health_status_path.stat().st_mtime_ns
            if mtime <= baseline_mtime_ns:
                raise ContractViolation("health status is not fresh")
            payload = json.loads(self.config.health_status_path.read_text(encoding="utf-8"))
            classification = validate_health_payload(payload)
            return classification, sha256_file(self.config.health_status_path), mtime
        # reset-failed is advisory: a freshly installed timer may not be
        # loaded yet, while the following start is the authoritative gate.
        self._systemctl("reset-failed", HEALTH_UNIT, HEALTH_TIMER, check=False)
        self._systemctl("start", HEALTH_TIMER)
        deadline = self._deadline()
        last_error: ContractViolation | None = None
        while time.monotonic() < deadline:
            result = self._systemctl("start", HEALTH_UNIT, check=False)
            if result.returncode == 0 and self.config.health_status_path.is_file():
                mtime = self.config.health_status_path.stat().st_mtime_ns
                if mtime > baseline_mtime_ns:
                    try:
                        payload = json.loads(
                            self.config.health_status_path.read_text(encoding="utf-8")
                        )
                        classification = validate_health_payload(payload)
                        return classification, sha256_file(self.config.health_status_path), mtime
                    except (json.JSONDecodeError, ContractViolation) as exc:
                        last_error = (
                            exc
                            if isinstance(exc, ContractViolation)
                            else ContractViolation("health status JSON invalid")
                        )
            time.sleep(SCHEDULER_YIELD_SECONDS)
        raise ContractViolation(f"fresh health timeout:{last_error}")

    def _enable_candidate_autostart(self) -> None:
        if not self.config.production:
            return
        self._validate_original_executor_policy()
        for unit in EXECUTOR_UNITS:
            expected = self.original_enablement[unit]
            self._systemctl("enable" if expected == "enabled" else "disable", unit)
        self._systemctl("enable", HEALTH_TIMER)
        self._verify_candidate_executor_policy_restored()

    def _verify_candidate_executor_policy_restored(self) -> None:
        if not self.config.production:
            return
        self._validate_original_executor_policy()
        for unit in EXECUTOR_UNITS:
            if self._enabled(unit) != self.original_enablement[unit]:
                raise ContractViolation(f"candidate unit file policy drift:{unit}")
            expected_activity = self.original_activity[unit]
            actual_activity = self._property(unit, "ActiveState")
            if actual_activity != expected_activity:
                raise ContractViolation(f"candidate activity policy drift:{unit}")
        if self._enabled(HEALTH_TIMER) != "enabled" or not self._active(HEALTH_TIMER):
            raise ContractViolation("candidate health timer policy drift")

    def run(self) -> None:
        prior_handlers = {
            item: signal.getsignal(item)
            for item in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM)
        }
        for item in prior_handlers:
            signal.signal(item, self.handle_signal)
        self.acquire_transaction_lock()
        try:
            if self.config.snapshot.exists() or self.config.snapshot.is_symlink():
                self._recover_existing_transaction()
                return
            self.old_release = self.config.current_link.resolve()
            self.preflight()
            self.stop_snapshot_prepare()
            self.stage_replace()
            self.switch_start()
            self.accept_commit()
        except BaseException as exc:
            for item in prior_handlers:
                signal.signal(item, signal.SIG_IGN)
            if self.phase is not Phase.COMMITTED and not self._recovery_handled:
                self.rollback(
                    "TRANSACTION_FAILURE:"
                    f"{type(exc).__name__}:{str(exc)[:1000]}"
                )
            raise
        finally:
            if self._profile_lock_handles:
                self._release_profile_locks()
            if self._final_lock_held:
                self._release_wallet_lock(final=True)
            if self._prestart_lock_held:
                self._release_wallet_lock(final=False)
            for item, handler in prior_handlers.items():
                signal.signal(item, handler)
            self.release_transaction_lock()

    def _hydrate_stop_evidence(self, evidence: Mapping[str, Any]) -> None:
        expected = {
            "change_id": self.config.change_id,
            "manifest_sha256": self.config.expected_manifest_digest,
            "new_release": str(self.config.new_release),
        }
        for key, value in expected.items():
            if evidence.get(key) != value:
                raise ContractViolation(f"stop evidence identity mismatch:{key}")
        self.old_release = Path(str(evidence.get("old_release") or ""))
        if not self.old_release.is_absolute():
            raise ContractViolation("stop evidence old release path invalid")
        if self.config.production:
            if self.old_release.parent != Path("/opt/polymarket-live/releases"):
                raise ContractViolation("stop evidence old release escaped immutable root")
            self._assert_tree_immutable(self.old_release, label="stop evidence old release")
            self._verify_existing_manifest(self.old_release)
        self.env_hashes = {
            str(key): _validate_sha256(str(value), label=f"environment:{key}")
            for key, value in dict(evidence.get("environment_hashes") or {}).items()
        }
        self.original_enablement = {
            str(key): str(value)
            for key, value in dict(evidence.get("original_enablement") or {}).items()
        }
        self.original_activity = {
            str(key): str(value)
            for key, value in dict(evidence.get("original_activity") or {}).items()
        }
        self.old_health_state = {
            str(key): str(value)
            for key, value in dict(evidence.get("old_health_state") or {}).items()
        }
        self.baselines = {
            str(key): dict(value)
            for key, value in dict(evidence.get("post_stop_baselines") or {}).items()
            if isinstance(value, Mapping)
        }
        self.official_redemption_digest = str(
            evidence.get("official_redemption_digest") or ""
        )
        raw_official_hashes = evidence.get("official_redemption_identity_hashes", [])
        if not isinstance(raw_official_hashes, list):
            raise ContractViolation("stop evidence official identity set invalid")
        self.official_redemption_identity_hashes = {
            _validate_sha256(str(item), label="stop evidence official identity")
            for item in raw_official_hashes
        }
        if len(self.official_redemption_identity_hashes) != len(raw_official_hashes):
            raise ContractViolation("stop evidence official identity set duplicated")
        raw_observed_hashes = evidence.get(
            "observed_official_redemption_identity_hashes", raw_official_hashes
        )
        if not isinstance(raw_observed_hashes, list):
            raise ContractViolation("stop evidence observed identity set invalid")
        self.observed_official_redemption_identity_hashes = {
            _validate_sha256(str(item), label="stop evidence observed identity")
            for item in raw_observed_hashes
        }
        if (
            len(self.observed_official_redemption_identity_hashes)
            != len(raw_observed_hashes)
            or not self.official_redemption_identity_hashes.issubset(
                self.observed_official_redemption_identity_hashes
            )
        ):
            raise ContractViolation("stop evidence observed identity set invalid")
        self.shared_redemption_transition_prefix_sha256 = str(
            evidence.get("shared_redemption_transition_prefix_sha256") or ""
        )
        raw_shared_conditions = evidence.get(
            "shared_redemption_receipt_conditions", []
        )
        if not isinstance(raw_shared_conditions, list):
            raise ContractViolation(
                "stop evidence shared redemption receipt conditions invalid"
            )
        self.shared_redemption_receipt_conditions = tuple(
            str(item).lower() for item in raw_shared_conditions
        )
        self.shared_redemption_receipt_rows_json = str(
            evidence.get("shared_redemption_receipt_rows_json") or ""
        )
        self.shared_redemption_allocation_rows_json = str(
            evidence.get("shared_redemption_allocation_rows_json") or ""
        )
        try:
            self.health_baseline_mtime_ns = int(
                evidence.get("health_baseline_mtime_ns") or 0
            )
            self.manager_timeout_usec = int(
                evidence.get("manager_timeout_usec") or 0
            )
            if self.manager_timeout_usec > 0:
                configure_root_live_read_timeout(self.manager_timeout_usec)
            self.shared_redemption_transition_baseline = int(
                evidence.get("shared_redemption_transition_baseline") or 0
            )
        except (TypeError, ValueError) as exc:
            raise ContractViolation("stop evidence numeric baseline invalid") from exc
        if (
            self.manager_timeout_usec <= 0
            or self.shared_redemption_transition_baseline < 0
        ):
            raise ContractViolation("stop evidence manager timeout invalid")
        if self.config.production:
            if set(self.env_hashes) != set(PROFILE_KEYS):
                raise ContractViolation("stop evidence environment hash set mismatch")
            try:
                self._validate_original_executor_policy()
            except ContractViolation as exc:
                raise ContractViolation("stop evidence executor policy invalid") from exc
            if set(self.baselines) != set(PROFILE_KEYS):
                raise ContractViolation("stop evidence runtime baseline set mismatch")
            _validate_sha256(
                self.official_redemption_digest,
                label="stop evidence official redemption digest",
            )
            _validate_sha256(
                self.shared_redemption_transition_prefix_sha256,
                label="stop evidence shared redemption transition prefix",
            )
            if (
                list(self.shared_redemption_receipt_conditions)
                != sorted(self.shared_redemption_receipt_conditions)
                or any(not item for item in self.shared_redemption_receipt_conditions)
                or len(set(self.shared_redemption_receipt_conditions))
                != len(self.shared_redemption_receipt_conditions)
            ):
                raise ContractViolation(
                    "stop evidence shared redemption receipt conditions invalid"
                )
            try:
                _validated_condition_snapshot(
                    json.loads(self.shared_redemption_receipt_rows_json),
                    label="stop evidence shared redemption receipts",
                )
                _validated_condition_snapshot(
                    json.loads(self.shared_redemption_allocation_rows_json),
                    label="stop evidence shared redemption allocations",
                )
            except json.JSONDecodeError as exc:
                raise ContractViolation(
                    "stop evidence shared redemption snapshot invalid"
                ) from exc
            if (
                official_redemption_identity_digest(
                    self.official_redemption_identity_hashes
                )
                != self.official_redemption_digest
            ):
                raise ContractViolation("stop evidence official digest mismatch")
            for profile, baseline in self.baselines.items():
                try:
                    redemption_transition_id = int(
                        baseline["redemption_transition_id"]
                    )
                    for table, baseline_key, digest_key in _LOCAL_APPEND_ONLY_BASELINES:
                        if int(baseline[baseline_key]) < 0:
                            raise ContractViolation(
                                f"stop evidence immutable baseline invalid:{profile}:{table}"
                            )
                        _validate_sha256(
                            str(baseline[digest_key]),
                            label=f"stop evidence {table} prefix:{profile}",
                        )
                    receipt_conditions = json.loads(
                        str(baseline["redemption_receipt_conditions_json"])
                    )
                    receipt_rows = json.loads(
                        str(baseline["redemption_receipt_rows_json"])
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    raise ContractViolation(
                        f"stop evidence redemption baseline invalid:{profile}"
                    ) from exc
                if redemption_transition_id < 0:
                    raise ContractViolation(
                        f"stop evidence redemption baseline invalid:{profile}"
                    )
                if (
                    not isinstance(receipt_conditions, list)
                    or receipt_conditions != sorted(receipt_conditions)
                    or any(not isinstance(item, str) or not item for item in receipt_conditions)
                    or len(set(receipt_conditions)) != len(receipt_conditions)
                ):
                    raise ContractViolation(
                        f"stop evidence redemption receipt conditions invalid:{profile}"
                    )
                _validated_condition_snapshot(
                    receipt_rows,
                    label=f"stop evidence redemption receipts:{profile}",
                )

    def _hydrate_from_stop_intent(self) -> None:
        receipt = self.receipts.read("STOP_INTENT")
        payload = dict(receipt.payload or {})
        evidence = payload.get("evidence")
        if not isinstance(evidence, dict):
            raise ContractViolation("stop intent evidence invalid")
        self._hydrate_stop_evidence(evidence)

    def _hydrate_from_prepared(self) -> None:
        receipt = self.receipts.read("PREPARED")
        payload = dict(receipt.payload or {})
        evidence = payload.get("evidence")
        if not isinstance(evidence, dict):
            raise ContractViolation("prepared evidence invalid")
        self._hydrate_stop_evidence(evidence)
        raw_hashes = evidence.get("exact_snapshot_hashes")
        raw_metadata = evidence.get("database_metadata")
        if not isinstance(raw_hashes, dict) or not isinstance(raw_metadata, dict):
            raise ContractViolation("prepared database evidence invalid")
        expected_keys = set(self._exact_snapshot_sources())
        if set(raw_hashes) != expected_keys or set(raw_metadata) != expected_keys:
            raise ContractViolation("prepared database evidence set mismatch")
        self.exact_hashes = {
            key: _validate_sha256(str(value), label=f"exact snapshot:{key}")
            for key, value in raw_hashes.items()
        }
        self.database_metadata = {}
        for key, raw in raw_metadata.items():
            if not isinstance(raw, Mapping):
                raise ContractViolation(f"prepared database metadata invalid:{key}")
            try:
                self.database_metadata[key] = FileMetadata(
                    int(raw["uid"]), int(raw["gid"]), int(raw["mode"])
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ContractViolation(
                    f"prepared database metadata invalid:{key}"
                ) from exc
        self.exact_snapshot_receipt_hash = _validate_sha256(
            str(evidence.get("exact_snapshot_receipt_sha256") or ""),
            label="exact snapshot receipt",
        )
        self.old_artifact_hashes = {
            str(key): _validate_sha256(str(value), label=f"old artifact:{key}")
            for key, value in dict(evidence.get("old_artifact_hashes") or {}).items()
        }
        if self.config.production:
            expected_artifacts = {
                *EXECUTOR_UNITS,
                HEALTH_UNIT,
                HEALTH_TIMER,
                HEALTH_BRIDGE,
                "server_health_heartbeat.py",
                "old-release-path",
            }
            if set(self.old_artifact_hashes) != expected_artifacts:
                raise ContractViolation("prepared old artifact set mismatch")
        exact_receipt = self.config.snapshot / "exact-snapshot.json"
        if sha256_file(exact_receipt) != self.exact_snapshot_receipt_hash:
            raise ContractViolation("exact snapshot receipt drift")
        try:
            exact_payload = json.loads(exact_receipt.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContractViolation("exact snapshot receipt invalid") from exc
        database_receipts = exact_payload.get("databases")
        artifact_receipts = exact_payload.get("artifacts")
        if not isinstance(database_receipts, Mapping) or not isinstance(
            artifact_receipts, Mapping
        ):
            raise ContractViolation("exact snapshot receipt structure invalid")
        recorded_hashes = {
            str(key): (
                str(value.get("sha256") or "")
                if isinstance(value, Mapping)
                else str(value)
            )
            for key, value in database_receipts.items()
        }
        if recorded_hashes != self.exact_hashes:
            raise ContractViolation("exact snapshot database receipt mismatch")
        if {str(key): str(value) for key, value in artifact_receipts.items()} != self.old_artifact_hashes:
            raise ContractViolation("exact snapshot artifact receipt mismatch")
        for key, expected_hash in self.exact_hashes.items():
            source = self.config.snapshot / f"exact-{key}.sqlite3"
            if sha256_file(source) != expected_hash:
                raise ContractViolation(f"sealed exact database drift:{key}")

    def _hydrate_latest_observed_official_identities(self) -> None:
        latest_phase: str | None = None
        receipt: DurableReceipt | None = None
        expected_identity = {
            "change_id": self.config.change_id,
            "manifest_sha256": self.config.expected_manifest_digest,
            "new_release": str(self.config.new_release),
        }
        if self.receipts.exists("FAILED"):
            receipt = self.receipts.validated_failure(expected_identity)
            latest_phase = "FAILED"
        elif self.receipts.exists("OLD_START_INTENT"):
            latest_phase = "OLD_START_INTENT"
        else:
            for phase in (
                "COMMITTED",
                "ACCEPTED",
                "START_INTENT",
                "DB_MUTATION_INTENT",
                "PREPARED",
                "STOP_INTENT",
            ):
                if self.receipts.exists(phase):
                    latest_phase = phase
                    break
        if latest_phase is None:
            raise ContractViolation("durable observed identity evidence missing")
        if receipt is None:
            receipt = self.receipts.read(latest_phase)
        payload = dict(receipt.payload or {})
        evidence = payload.get("evidence")
        if not isinstance(evidence, Mapping):
            raise ContractViolation("durable observed identity evidence invalid")
        raw = evidence.get("observed_official_redemption_identity_hashes")
        if not isinstance(raw, list):
            raise ContractViolation("durable observed identity evidence invalid")
        observed = {
            _validate_sha256(str(item), label="durable observed identity")
            for item in raw
        }
        if (
            len(observed) != len(raw)
            or not self.official_redemption_identity_hashes.issubset(observed)
            or not self.observed_official_redemption_identity_hashes.issubset(
                observed
            )
        ):
            raise ContractViolation("durable observed identity evidence regressed")
        checkpoint_path = self.receipts.observed_official_path
        if checkpoint_path.exists() or checkpoint_path.is_symlink():
            checkpoint = self.receipts.read_observed_official_identities(
                expected_identity
            )
            if not observed.issubset(checkpoint) and not checkpoint.issubset(observed):
                raise ContractViolation("durable observed identity evidence diverged")
            observed |= checkpoint
            if not self.official_redemption_identity_hashes.issubset(observed):
                raise ContractViolation("durable observed identity evidence regressed")
        self.observed_official_redemption_identity_hashes = observed

    def _recover_existing_transaction(self) -> None:
        status = self.config.snapshot.lstat()
        expected_uid = 0 if self.config.production else os.getuid()
        if (
            not stat.S_ISDIR(status.st_mode)
            or self.config.snapshot.is_symlink()
            or status.st_uid != expected_uid
            or status.st_mode & 0o022
        ):
            raise ContractViolation("existing transaction evidence root is untrusted")
        self._assert_tree_immutable(
            self.config.snapshot, label="existing transaction evidence"
        )
        phase = self._durable_phase()
        self.phase = phase
        if phase is Phase.STOP_INTENT:
            self._hydrate_from_stop_intent()
            self._hydrate_latest_observed_official_identities()
            try:
                self.rollback("RECOVER_EXISTING_TRANSACTION")
            finally:
                self._recovery_handled = True
            raise ContractViolation(
                "prior release transaction recovered from stop intent; new cutover not started"
            )
        if phase is Phase.COMMITTED:
            if self.migration_stage.exists() or self.migration_stage.is_symlink():
                self._assert_tree_immutable(
                    self.migration_stage, label="existing migration stage"
                )
            self._hydrate_from_prepared()
            self._hydrate_latest_observed_official_identities()
            return
        if phase is Phase.PREFLIGHT:
            raise ContractViolation("snapshot exists without prepared transaction evidence")
        self._hydrate_from_prepared()
        self._hydrate_latest_observed_official_identities()
        if self.receipts.exists("OLD_START_INTENT"):
            try:
                self.rollback("RECOVERED_OLD_START_INTENT")
            finally:
                self._recovery_handled = True
            raise ContractViolation("prior release transaction recovered to old start intent")
        try:
            self.rollback("RECOVER_EXISTING_TRANSACTION")
        finally:
            self._recovery_handled = True
        raise ContractViolation("prior release transaction recovered; new cutover not started")

    def _durable_phase(self) -> Phase:
        expected = {
            "change_id": self.config.change_id,
            "manifest_sha256": self.config.expected_manifest_digest,
            "new_release": str(self.config.new_release),
        }
        try:
            return self.receipts.validated_phase(expected)
        except ContractViolation:
            # Any malformed marker at or beyond the start boundary is treated
            # as if candidate side effects may already exist.
            for phase in ("COMMITTED", "ACCEPTED", "START_INTENT", "OLD_START_INTENT"):
                path = self.receipts.path_for(phase)
                if path.exists() or path.is_symlink():
                    return Phase.START_INTENT
            path = self.receipts.path_for("DB_MUTATION_INTENT")
            if path.exists() or path.is_symlink():
                return Phase.DB_MUTATION_INTENT
            path = self.receipts.path_for("PREPARED")
            if path.exists() or path.is_symlink():
                return Phase.STOPPED_PREPARED
            path = self.receipts.path_for("STOP_INTENT")
            if path.exists() or path.is_symlink():
                return Phase.STOP_INTENT
            raise

    def rollback(self, reason: str) -> None:
        self._failure_reason = str(reason)
        phase = max(self.phase, self._durable_phase())
        if phase is Phase.COMMITTED:
            return
        self._normalize_wallet_lock_for_recovery()
        errors: list[BaseException] = []

        def attempt(name: str, operation: Any) -> bool:
            try:
                operation()
                self._cleanup_results[name] = "PASS"
                return True
            except BaseException as exc:
                self._cleanup_results[name] = f"FAIL:{type(exc).__name__}:{exc}"
                errors.append(exc)
                return False

        if phase < Phase.START_INTENT and self.receipts.exists("OLD_START_INTENT"):
            stopped = attempt("quiesce", self.quiesce)
            stage_sealed = stopped and attempt(
                "seal_partial_migration_stage", self.seal_partial_migration_stage
            )
            if stage_sealed:
                attempt(
                    "recover_old_start_intent", self.restore_old_without_database
                )
            attempt("retain_evidence", self.retain_evidence)
        elif phase >= Phase.START_INTENT:
            stopped = attempt("quiesce", self.quiesce)
            stage_sealed = stopped and attempt(
                    "seal_partial_migration_stage", self.seal_partial_migration_stage
            )
            attempt("disable_candidate", self.disable_candidate)
            restored = stage_sealed and attempt(
                "restore_exact_databases", self.restore_exact_databases
            )
            if restored:
                attempt("restore_old_release", self.restore_old_release)
            attempt("retain_evidence", self.retain_evidence)
        elif phase is Phase.DB_MUTATION_INTENT:
            stopped = attempt("quiesce", self.quiesce)
            stage_sealed = stopped and attempt(
                "seal_partial_migration_stage", self.seal_partial_migration_stage
            )
            restored = stage_sealed and attempt(
                "restore_exact_databases", self.restore_exact_databases
            )
            if restored:
                attempt("restore_old_release", self.restore_old_release)
            attempt("retain_evidence", self.retain_evidence)
        elif phase is Phase.STOPPED_PREPARED:
            stopped = attempt("quiesce", self.quiesce)
            stage_sealed = stopped and attempt(
                "seal_partial_migration_stage", self.seal_partial_migration_stage
            )
            if stage_sealed:
                attempt(
                    "restore_old_without_database", self.restore_old_without_database
                )
            attempt("retain_evidence", self.retain_evidence)
        elif phase is Phase.STOP_INTENT:
            stopped = attempt("quiesce", self.quiesce)
            stage_sealed = stopped and attempt(
                "seal_partial_migration_stage", self.seal_partial_migration_stage
            )
            if stage_sealed:
                attempt(
                    "restore_old_without_database", self.restore_old_without_database
                )
            attempt("retain_evidence", self.retain_evidence)
        else:
            attempt("noop_before_stop", self.noop_before_stop)
        if errors:
            raise ContractViolation(
                "release rollback incomplete:" + ";".join(str(item) for item in errors)
            ) from errors[0]

    def _normalize_wallet_lock_for_recovery(self) -> None:
        """Reuse a held final acceptance lock for pre-start recovery."""

        if self._final_lock_held:
            if self._wallet_lock_handle is None:
                raise ContractViolation("shared wallet final lock handle missing")
            self._final_lock_held = False
            self._prestart_lock_held = True

    def handle_signal(self, signum: int, _frame: Any) -> None:
        raise SystemExit(128 + int(signum))

    # Six explicit responsibilities keep ordering visible without a string
    # dispatch table.  Each phase writes its durable marker before crossing
    # the corresponding irreversible boundary.
    def preflight(self) -> None:
        if self.config.production and os.geteuid() != 0:
            raise ContractViolation("production controller requires euid 0")
        if not self.config.current_link.is_symlink():
            raise ContractViolation("current release pointer is not a symlink")
        if self.config.production:
            expected_prefix = Path("/opt/polymarket-live/releases")
            if self.config.new_release.parent != expected_prefix:
                raise ContractViolation("candidate release path is outside immutable root")
            if self.old_release.parent != expected_prefix:
                raise ContractViolation("old release path is outside immutable root")
            self.manager_timeout_usec = self._manager_timeout_usec()
            configure_root_live_read_timeout(self.manager_timeout_usec)
        controller = (
            self.config.new_release / "tools/live_release_transaction.py"
        ).resolve()
        wrapper = (
            self.config.new_release
            / "tools/deploy_three_wallet_core_hotfix_release.sh"
        ).resolve()
        if Path(__file__).resolve() != controller and self.config.production:
            raise ContractViolation("running controller is not candidate-bound")
        if self.config.production and Path(os.environ.get("RELEASE_WRAPPER", "")).resolve() != wrapper:
            raise ContractViolation("running wrapper is not candidate-bound")
        self._assert_tree_immutable(self.config.new_release, label="candidate")
        verify_manifest(
            self.config.new_release,
            self.config.expected_manifest_digest,
            REQUIRED_ASSETS,
        )
        verify_candidate_test_receipt(self.config.new_release)
        if self.old_release == self.config.new_release.resolve():
            raise ContractViolation("candidate already equals current release")
        self._assert_tree_immutable(self.old_release, label="old release")
        self._verify_existing_manifest(self.old_release)
        self._static_compile_candidate()
        self._verify_env_files(capture=True)
        self._verify_database_gates(cutover=True)
        self._verify_runtime_lock_parent()
        self._prepare_candidate_profile_locks()
        self._verify_service_lock_files()
        self._change_ids_unused()
        self._capture_original_service_state()
        self._verify_old_fleet_identity()
        self._verify_pre_stop_health()
        self._verify_sandbox_capability()
        self._verify_snapshot_space(copies=len(("exact", "stage")))
        if self.config.snapshot.parent != self.config.snapshot_root:
            raise ContractViolation("snapshot is outside root")
        if self.config.snapshot.name != self.config.change_id:
            raise ContractViolation("snapshot name is not bound to change id")
        if self.config.snapshot.exists() or self.config.snapshot.is_symlink():
            raise ContractViolation("snapshot path already exists")
        if self.migration_stage.exists() or self.migration_stage.is_symlink():
            raise ContractViolation("migration stage path already exists")

    def stop_snapshot_prepare(self) -> None:
        self._prepare_snapshot_directories()
        self._verify_service_lock_files()
        self._acquire_wallet_lock(final=False)
        self._verify_env_files(capture=False)
        self.official_redemption_digest = self._official_digest_and_order_gate()
        self.official_redemption_identity_hashes = set(
            self._latest_official_identities
        )
        self.observed_official_redemption_identity_hashes = set(
            self._latest_official_identities
        )
        self._checkpoint_observed_official_identities()
        # SQLite reads run in an owner-isolated subprocess while the shared
        # wallet submission lock freezes new side effects.  The controller
        # never opens service-owned WAL databases as root and never pauses an
        # executor to inspect them.
        def capture_running_boundary() -> None:
            self._verify_database_gates(cutover=True)
            self._capture_runtime_baselines()

        if self.manager_timeout_usec <= 0:
            self.manager_timeout_usec = self._manager_timeout_usec()
        configure_root_live_read_timeout(self.manager_timeout_usec)
        capture_running_boundary()
        self.receipts.write("STOP_INTENT", self._stop_intent_evidence())
        self.phase = Phase.STOP_INTENT
        # The only clean stop boundary in the successful transaction.
        self.stop_and_prove_quiescent()
        self._verify_env_files(capture=False)
        self._verify_database_gates(cutover=True)
        # Byte/logical stability begins only after quiescence.  The earlier
        # STOP_INTENT baselines, not this fingerprint, audit writes that were
        # legitimately finishing while systemd stopped the old fleet.
        self.pre_stop_fingerprints = {
            key: canonical_database_fingerprint(path)
            for key, path in self._exact_snapshot_sources().items()
        }
        self.checkpoint_and_verify_stable_databases(self._exact_snapshot_sources())
        # These two checks deliberately still use the STOP_INTENT baselines.
        # Only after they prove that no transition or external redemption was
        # hidden in the stop interval may the exact stopped state be promoted.
        self._verify_redemption_history_prefixes()
        current_digest = self._official_digest_and_order_gate()
        self._conserve_latest_official_redemption_snapshot(current_digest)
        self.official_redemption_identity_hashes = set(
            self._latest_official_identities
        )
        self.official_redemption_digest = current_digest
        self._capture_runtime_baselines()
        # Baseline capture is read-only but SQLite may recreate empty WAL/SHM
        # files.  Re-prove the frozen fingerprint and remove those artifacts
        # before taking the physical exact snapshots.
        self.checkpoint_and_verify_stable_databases(self._exact_snapshot_sources())

        receipts: dict[str, Any] = {}
        for key, source in self._exact_snapshot_sources().items():
            self.database_metadata[key] = FileMetadata.from_path(source)
            destination = self.config.snapshot / f"exact-{key}.sqlite3"
            self._snapshot_database(source, destination)
            receipt = self._database_receipt(destination)
            receipts[key] = receipt
            self.exact_hashes[key] = str(receipt["sha256"])
        artifact_hashes: dict[str, str] = {}
        if self.config.production:
            for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER):
                source = self.config.systemd_root / unit
                destination = self.config.snapshot / unit
                shutil.copy2(source, destination)
                with destination.open("rb") as handle:
                    os.fsync(handle.fileno())
                artifact_hashes[unit] = sha256_file(destination)
            for source, name in (
                (Path("/usr/local/sbin") / HEALTH_BRIDGE, HEALTH_BRIDGE),
                (
                    Path("/usr/local/libexec/polymarket/server_health_heartbeat.py"),
                    "server_health_heartbeat.py",
                ),
            ):
                destination = self.config.snapshot / name
                shutil.copy2(source, destination)
                with destination.open("rb") as handle:
                    os.fsync(handle.fileno())
                artifact_hashes[name] = sha256_file(destination)
        old_release_path = self.config.snapshot / "old-release-path"
        with old_release_path.open("w", encoding="utf-8") as handle:
            handle.write(f"{self.old_release}\n")
            handle.flush()
            os.fsync(handle.fileno())
        artifact_hashes["old-release-path"] = sha256_file(
            self.config.snapshot / "old-release-path"
        )
        self.old_artifact_hashes = dict(artifact_hashes)
        self.exact_snapshot_receipt_hash = atomic_write_json(
            self.config.snapshot / "exact-snapshot.json",
            {"databases": receipts, "artifacts": artifact_hashes},
            mode=0o600,
        )
        exact_allocated = sum(
            (self.config.snapshot / f"exact-{key}.sqlite3").stat().st_size
            for key in self._exact_snapshot_sources()
        )
        snapshot_free = shutil.disk_usage(self.config.snapshot_root).free
        runtime_free = shutil.disk_usage(self.config.runtime_root).free
        if snapshot_free < exact_allocated or runtime_free < exact_allocated:
            raise ContractViolation(
                "post-stop exact staging/restore workspace is insufficient:"
                f"snapshot_free={snapshot_free}:runtime_free={runtime_free}:"
                f"required={exact_allocated}"
            )
        self._seal_directory(self.config.snapshot)
        self.receipts.write("PREPARED", self._common_receipt_evidence())
        self.phase = Phase.STOPPED_PREPARED
        self._disable_autostart()

    def stage_replace(self) -> None:
        if self.config.production:
            self.runner.run(
                (
                    "/usr/bin/sudo",
                    "-n",
                    "/usr/bin/install",
                    "-d",
                    "-o",
                    "polymarket-live",
                    "-g",
                    "polymarket-live",
                    "-m",
                    "0700",
                    str(self.migration_stage),
                )
            )
        else:
            self.migration_stage.mkdir(mode=0o700)
        for key in self._exact_snapshot_sources():
            source = self.config.snapshot / f"exact-{key}.sqlite3"
            destination = self.migration_stage / f"{key}.sqlite3"
            if self.config.production:
                self.runner.run(
                    (
                        "/usr/bin/sudo",
                        "-n",
                        "/usr/bin/install",
                        "-o",
                        "polymarket-live",
                        "-g",
                        "polymarket-live",
                        "-m",
                        "0600",
                        str(source),
                        str(destination),
                    )
                )
            else:
                shutil.copyfile(source, destination)
                destination.chmod(0o600)
            if sha256_file(destination) != self.exact_hashes[key]:
                raise ContractViolation(f"migration stage copy mismatch:{key}")
        self._run_candidate_sandbox(
            self._offline_migration_program(), stage=self.migration_stage
        )
        self._verify_migration_stage_inventory()
        if self.config.production:
            self._seal_directory(self.migration_stage)
        staged = {
            spec.key: self.migration_stage / f"{spec.key}.sqlite3"
            for spec in PROFILE_SPECS
        }
        for spec in PROFILE_SPECS:
            before = self.config.snapshot / f"exact-{spec.key}.sqlite3"
            after = self.migration_stage / f"{spec.key}.sqlite3"
            verify_offline_migration_delta(before, after)
            verify_local_storage(after, cutover=True)
        coordinator_before = self.config.snapshot / "exact-coordinator.sqlite3"
        coordinator_after = self.migration_stage / "coordinator.sqlite3"
        if _canonical_database_payload(
            coordinator_before, ignore_migration_fields=False
        ) != _canonical_database_payload(
            coordinator_after, ignore_migration_fields=False
        ):
            raise ContractViolation("offline migration changed coordinator ledger")
        verify_coordinator_storage(coordinator_after, cutover=True)
        verify_shared_wallet_topology(
            coordinator_after,
            staged,
            self.wallet_lock,
            registered_paths={
                spec.key: self._live_database(spec) for spec in PROFILE_SPECS
            },
        )
        self._verify_stage_resume()
        self._checkpoint_staged_databases(
            {
                **{f"stage:{key}": value for key, value in staged.items()},
                "stage:coordinator": self.migration_stage / "coordinator.sqlite3",
                **{
                    f"exact:{key}": self.config.snapshot / f"exact-{key}.sqlite3"
                    for key in self._exact_snapshot_sources()
                },
            }
        )
        for key, expected in self.exact_hashes.items():
            if sha256_file(
                self.config.snapshot / f"exact-{key}.sqlite3"
            ) != expected:
                raise ContractViolation(f"exact restore source changed:{key}")
        stage_hashes = {
            key: sha256_file(self.migration_stage / f"{key}.sqlite3")
            for key in self._exact_snapshot_sources()
        }
        atomic_write_json(
            self.migration_stage / "verified-stage.json",
            {"database_hashes": stage_hashes},
            mode=0o600,
        )
        self._seal_directory(self.migration_stage)
        self._mutation_receipt = self.receipts.write(
            "DB_MUTATION_INTENT",
            {**self._common_receipt_evidence(), "staged_database_hashes": stage_hashes},
        )
        self.phase = Phase.DB_MUTATION_INTENT
        for key, target in self._exact_snapshot_sources().items():
            atomic_replace_database(
                self.migration_stage / f"{key}.sqlite3",
                target,
                self.database_metadata[key],
            )
            if sha256_file(target) != stage_hashes[key]:
                raise ContractViolation(f"live database replacement mismatch:{key}")
        self._verify_database_gates(cutover=True)
        self._verify_env_files(capture=False)
        verify_manifest(
            self.config.new_release,
            self.config.expected_manifest_digest,
            REQUIRED_ASSETS,
        )
        self._install_and_verify_candidate()
        self._finalize_live_database_replacement(stage_hashes)

    def switch_start(self) -> None:
        self._verify_env_files(capture=False)
        verify_manifest(
            self.config.new_release,
            self.config.expected_manifest_digest,
            REQUIRED_ASSETS,
        )
        self._verify_installed_candidate()
        # This import runs without credentials, network, or live-runtime access.
        # It remains on the database-restorable side of the durable start edge.
        self._run_candidate_sandbox(
            self._candidate_import_program(), stage=None
        )
        active_units = self._original_active_executor_units()
        if self.config.production and active_units:
            # reset-failed is advisory and systemd rejects a mixed request
            # when even one historical unit alias is absent.  Isolate each
            # active unit so a missing alias cannot block the valid executors.
            for unit in active_units:
                self._systemctl("reset-failed", unit, check=False)
        self._capture_candidate_status_mtime_baselines()
        self.candidate_start_boundary_ns = time.time_ns()
        start_evidence = {
            **self._common_receipt_evidence(),
            "candidate_start_boundary_ns": self.candidate_start_boundary_ns,
            "candidate_status_mtime_baselines": dict(
                self.candidate_status_mtime_baselines
            ),
            "db_mutation_intent_sha256": (
                "" if self._mutation_receipt is None else self._mutation_receipt.sha256
            ),
        }
        self._start_receipt = self.receipts.write("START_INTENT", start_evidence)
        self.phase = Phase.START_INTENT
        self._release_profile_locks()
        self._release_wallet_lock(final=False)
        if self.config.production and active_units:
            self._systemctl("start", *active_units)
        else:
            self.restart_baselines = {unit: 0 for unit in active_units}

    def accept_commit(self) -> None:
        candidate_evidence = self._wait_for_candidate_and_lock()
        first_health = self._fresh_health(self.health_baseline_mtime_ns)
        # Health is evidence, not a replacement for the ledger, process,
        # official-order, installed-file, and environment gates.
        candidate_evidence = self._verify_candidate_acceptance_with_owner_isolated_databases(
            full_integrity=True
        )
        accepted_evidence = {
            **self._common_receipt_evidence(),
            "start_intent_sha256": (
                "" if self._start_receipt is None else self._start_receipt.sha256
            ),
            "candidate_acceptance": candidate_evidence,
            "health_classification": first_health[0],
            "health_status_sha256": first_health[1],
            "health_status_mtime_ns": first_health[2],
        }
        self._accepted_receipt = self.receipts.write("ACCEPTED", accepted_evidence)
        self.phase = Phase.ACCEPTED
        self._enable_candidate_autostart()
        final_health = self._fresh_health(first_health[2])
        self._verify_candidate_executor_policy_restored()
        # The full SQLite integrity pass already completed under the same
        # held wallet lock before ACCEPTED.  Recheck every live side-effect,
        # receipt, process and FK gate here without repeatedly stress-scanning
        # hot WAL databases.
        final_acceptance = self._verify_candidate_acceptance_with_owner_isolated_databases(
            full_integrity=False
        )
        self.final_evidence = {
            **self._common_receipt_evidence(),
            "accepted_sha256": self._accepted_receipt.sha256,
            "candidate_acceptance": final_acceptance,
            "health_classification": final_health[0],
            "health_status_sha256": final_health[1],
            "health_status_mtime_ns": final_health[2],
            "restart_baselines": dict(self.restart_baselines),
        }
        self.receipts.write("COMMITTED", self.final_evidence)
        self.phase = Phase.COMMITTED
        self._release_wallet_lock(final=True)

    def noop_before_stop(self) -> None:
        return

    def quiesce(self) -> None:
        if self.config.production:
            self._systemctl("stop", *ALL_STOP_UNITS, check=False)
            try:
                self._verify_quiescent()
                self._acquire_profile_locks()
                return
            except ContractViolation:
                self._systemctl(
                    "kill",
                    "--kill-whom=all",
                    "--signal=SIGKILL",
                    *ALL_STOP_UNITS,
                    check=False,
                )
                self._systemctl("stop", *ALL_STOP_UNITS, check=False)
        self._verify_quiescent()
        self._acquire_profile_locks()

    def restore_old_without_database(self) -> None:
        if self.receipts.exists("PREPARED"):
            self._restore_old_artifacts_and_start()
            return
        if not self._prestart_lock_held:
            self._acquire_wallet_lock(final=False)
        self._verify_unchanged_old_files_before_start()
        self._verify_env_files(capture=False)
        self._verify_database_gates(cutover=True)
        self._release_profile_locks()
        self._release_wallet_lock(final=False)
        self._start_and_accept_old_fleet()

    def _start_and_accept_old_fleet(self) -> None:
        if not self.config.production:
            return
        try:
            active_units = self._original_active_executor_units()
            if active_units:
                self._systemctl("start", *active_units)
            self._restore_original_enablement_and_health()
            self._verify_old_release_acceptance()
        except BaseException:
            try:
                self.quiesce()
            except BaseException as quiescence_error:
                raise ContractViolation(
                    "old fleet recovery failed and could not be requiesced"
                ) from quiescence_error
            raise

    def restore_exact_databases(self) -> None:
        if not self._prestart_lock_held:
            self._acquire_wallet_lock(final=False)
        targets = self._exact_snapshot_sources()
        self._checkpoint_staged_databases(
            {
                **{f"restore-target:{key}": value for key, value in targets.items()},
                **{
                    f"restore-source:{key}": self.config.snapshot
                    / f"exact-{key}.sqlite3"
                    for key in targets
                },
            }
        )
        for key, target in targets.items():
            source = self.config.snapshot / f"exact-{key}.sqlite3"
            expected = self.exact_hashes.get(key)
            metadata = self.database_metadata.get(key)
            if expected is None or metadata is None:
                raise ContractViolation(f"exact restore evidence missing:{key}")
            if sha256_file(source) != expected:
                raise ContractViolation(f"exact restore source drift:{key}")
            atomic_replace_database(source, target, metadata)
            if sha256_file(target) != expected:
                raise ContractViolation(f"exact database restore mismatch:{key}")
        self._verify_database_gates(cutover=True)

    def restore_old_release(self) -> None:
        self._restore_old_artifacts_and_start()

    def disable_candidate(self) -> None:
        if not self.config.production:
            return
        self._systemctl("disable", *EXECUTOR_UNITS, HEALTH_TIMER, check=False)
        for unit in (*EXECUTOR_UNITS, HEALTH_TIMER):
            if self._enabled(unit) != "disabled":
                raise ContractViolation(f"candidate disable failed:{unit}")

    def retain_evidence(self) -> None:
        if self.receipts.exists("FAILED"):
            return
        evidence = {
            **self._common_receipt_evidence(),
            "failure_reason": self._failure_reason or "UNSPECIFIED_FAILURE",
            "cleanup_results": dict(self._cleanup_results),
            "durable_phase": self._durable_phase().name,
            "migration_stage_evidence": dict(self.migration_stage_evidence),
        }
        self.receipts.write("FAILED", evidence)

    def _restore_original_enablement_and_health(self) -> None:
        if not self.config.production:
            return
        for unit, state in self.original_enablement.items():
            action = "enable" if state == "enabled" else "disable"
            self._systemctl(action, unit)
        if self.old_health_state.get("timer_active") == "active":
            self._systemctl("start", HEALTH_TIMER)
        if self.old_health_state.get("service_active") == "active":
            self._systemctl("start", HEALTH_UNIT)

    def _restore_old_artifacts_and_start(self) -> None:
        if not self._prestart_lock_held:
            self._acquire_wallet_lock(final=False)
        old_path_file = self.config.snapshot / "old-release-path"
        if not old_path_file.is_file() or old_path_file.is_symlink():
            raise ContractViolation("old release path evidence missing")
        expected_old_path_hash = self.old_artifact_hashes.get("old-release-path")
        if (
            expected_old_path_hash is None
            or sha256_file(old_path_file) != expected_old_path_hash
        ):
            raise ContractViolation("old release path evidence drift")
        old_release = Path(old_path_file.read_text(encoding="utf-8").strip())
        if old_release != self.old_release:
            raise ContractViolation("old release path evidence mismatch")
        if self.config.production:
            artifact_targets: dict[str, tuple[Path, int]] = {
                **{
                    unit: (self.config.systemd_root / unit, 0o644)
                    for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER)
                },
                HEALTH_BRIDGE: (Path("/usr/local/sbin") / HEALTH_BRIDGE, 0o755),
                "server_health_heartbeat.py": (
                    Path("/usr/local/libexec/polymarket/server_health_heartbeat.py"),
                    0o644,
                ),
            }
            for name, (target, mode) in artifact_targets.items():
                source = self.config.snapshot / name
                expected = self.old_artifact_hashes.get(name)
                if expected is None or sha256_file(source) != expected:
                    raise ContractViolation(f"old artifact evidence drift:{name}")
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary = target.with_name(f".{target.name}.old-release.tmp")
                shutil.copyfile(source, temporary)
                os.chown(temporary, 0, 0)
                os.chmod(temporary, mode)
                with temporary.open("rb") as handle:
                    os.fsync(handle.fileno())
                os.replace(temporary, target)
                fsync_regular_file_and_parent(target)
            temporary_link = self.config.current_link.with_name(
                f".{self.config.current_link.name}.old-release.tmp"
            )
            if temporary_link.exists() or temporary_link.is_symlink():
                if not temporary_link.is_symlink() or temporary_link.resolve() != self.old_release:
                    raise ContractViolation("old current-link temporary path is unsafe")
                temporary_link.unlink()
            temporary_link.symlink_to(self.old_release, target_is_directory=True)
            os.replace(temporary_link, self.config.current_link)
            fsync_directory(self.config.current_link.parent)
            self._systemctl("daemon-reload")
            self._verify_old_fleet_identity_after_restore()
        elif self.config.current_link.resolve() != self.old_release:
            temporary_link = self.config.current_link.with_name(
                f".{self.config.current_link.name}.old-release.tmp"
            )
            temporary_link.symlink_to(self.old_release, target_is_directory=True)
            os.replace(temporary_link, self.config.current_link)
            fsync_directory(self.config.current_link.parent)
        self._verify_env_files(capture=False)
        self._verify_database_gates(cutover=True)
        if not self.receipts.exists("OLD_START_INTENT"):
            self.receipts.write("OLD_START_INTENT", self._common_receipt_evidence())
        self._release_profile_locks()
        self._release_wallet_lock(final=False)
        self._start_and_accept_old_fleet()

    def _verify_old_fleet_identity_after_restore(self) -> None:
        if self.config.current_link.resolve() != self.old_release:
            raise ContractViolation("old current link restore mismatch")
        for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER):
            source = self.config.snapshot / unit
            installed = self.config.systemd_root / unit
            if (
                installed.read_bytes() != source.read_bytes()
                or self._property(unit, "FragmentPath") != str(installed)
                or self._property(unit, "DropInPaths")
            ):
                raise ContractViolation(f"old installed unit restore mismatch:{unit}")
        self.verify_old_health_artifacts(
            unit_root=self.config.systemd_root,
            bridge=Path("/usr/local/sbin") / HEALTH_BRIDGE,
            heartbeat=Path(
                "/usr/local/libexec/polymarket/server_health_heartbeat.py"
            ),
        )

    def _verify_paused_old_profile_acceptance(self, spec: ProfileSpec) -> None:
        self._verify_paused_profile_unit_policy(spec, label="old")
        database = self._live_database(spec)
        baseline = self.baselines.get(spec.key)
        if not isinstance(baseline, Mapping):
            raise ContractViolation(f"old runtime baseline missing:{spec.key}")
        cursor = self._runtime_int(database, "last_processed_block")
        if cursor < int(baseline["last_processed_block"]):
            raise ContractViolation(f"paused old cursor regressed:{spec.key}")
        for key in (
            "operator_planned_resume_change_id",
            "operator_planned_resume_state",
            "operator_planned_resume_from_block",
            "operator_pre_repair_forward_recovery_armed",
        ):
            if self._runtime_text(database, key) != str(baseline.get(key) or ""):
                raise ContractViolation(
                    f"paused old resume evidence drift:{spec.key}:{key}"
                )
        verify_local_storage(database, cutover=True)

    def _verify_unchanged_old_files_before_start(self) -> None:
        if self.config.current_link.resolve() != self.old_release:
            raise ContractViolation("old current link changed before recovery")
        if not self.config.production:
            return
        for unit in (*EXECUTOR_UNITS, HEALTH_UNIT, HEALTH_TIMER):
            installed = self.config.systemd_root / unit
            expected = self.old_release / "systemd" / unit
            if (
                not installed.is_file()
                or installed.is_symlink()
                or installed.read_bytes() != expected.read_bytes()
                or self._property(unit, "FragmentPath") != str(installed)
                or self._property(unit, "DropInPaths")
            ):
                raise ContractViolation(f"old unit changed before recovery:{unit}")
        self.verify_old_health_artifacts(
            unit_root=self.config.systemd_root,
            bridge=Path("/usr/local/sbin") / HEALTH_BRIDGE,
            heartbeat=Path(
                "/usr/local/libexec/polymarket/server_health_heartbeat.py"
            ),
        )

    def _verify_old_release_acceptance_once(self, official_digest: str) -> None:
        self._verify_database_gates(cutover=True)
        self._verify_env_files(capture=False)
        self._verify_old_fleet_identity_after_restore()
        for spec in PROFILE_SPECS:
            if self._profile_original_mode(spec) == "PAUSED":
                self._verify_paused_old_profile_acceptance(spec)
                continue
            primary_pid = 0
            for unit in (spec.primary_unit, spec.standby_unit):
                if not self._active(unit):
                    raise ContractViolation(f"old executor did not recover:{unit}")
                if self._enabled(unit) != self.original_enablement[unit]:
                    raise ContractViolation(f"old executor enablement drift:{unit}")
                pid_text = self._property(unit, "MainPID")
                if not pid_text.isdigit() or int(pid_text) <= 0:
                    raise ContractViolation(f"old executor PID invalid:{unit}")
                pid = int(pid_text)
                if Path(f"/proc/{pid}/cwd").resolve() != self.old_release / "app":
                    raise ContractViolation(f"old executor cwd mismatch:{unit}")
                restart = self._property(unit, "NRestarts")
                if not restart.isdigit() or int(restart) != 0:
                    raise ContractViolation(f"old executor restarted:{unit}")
                if unit == spec.primary_unit:
                    primary_pid = pid
            database = self._live_database(spec)
            baseline = self.baselines.get(spec.key)
            if not isinstance(baseline, Mapping):
                raise ContractViolation(f"old runtime baseline missing:{spec.key}")
            if self._runtime_int(database, "last_successful_cycle_at_ms") <= int(
                baseline["last_successful_cycle_at_ms"]
            ):
                raise ContractViolation(
                    f"old successful cycle did not advance:{spec.key}"
                )
            if self._runtime_int(database, "last_processed_block") < int(
                baseline["last_processed_block"]
            ):
                raise ContractViolation(f"old cursor regressed:{spec.key}")
            for key in (
                "hot_standby_joined_at_ms",
                "hot_standby_primary_runtime_lock_seen_at_ms",
                "hot_standby_last_observed_head",
            ):
                if self._runtime_int(database, key) <= int(baseline[key]):
                    raise ContractViolation(
                        f"old standby evidence did not advance:{spec.key}:{key}"
                    )
            self._verify_legacy_primary_lock_owner(spec, primary_pid)
        self._verify_redemption_history_prefixes()
        self._conserve_latest_official_redemption_snapshot(official_digest)

    def _verify_old_release_acceptance(self) -> None:
        if not self.config.production:
            self._verify_database_gates(cutover=True)
            self._verify_env_files(capture=False)
            return
        deadline = self._deadline()
        last_error: ContractViolation | None = None
        while time.monotonic() < deadline:
            try:
                self._acquire_wallet_lock(final=False)
                official_digest = self._official_digest_and_order_gate()
                self._verify_old_release_acceptance_once(official_digest)
                self._release_wallet_lock(final=False)
                return
            except ContractViolation as exc:
                last_error = exc
                if self._prestart_lock_held:
                    self._release_wallet_lock(final=False)
                time.sleep(SCHEDULER_YIELD_SECONDS)
        raise ContractViolation(f"old release acceptance timeout:{last_error}")


def main(argv: Sequence[str] | None = None) -> int:
    config = TransactionConfig.from_argv(sys.argv[1:] if argv is None else argv)
    expected_controller = (
        config.new_release / "tools" / "live_release_transaction.py"
    ).resolve()
    if Path(__file__).resolve() != expected_controller:
        raise ContractViolation("controller path is not candidate-bound")
    expected_wrapper = (
        config.new_release
        / "tools"
        / "deploy_three_wallet_core_hotfix_release.sh"
    ).resolve()
    if Path(os.environ.get("RELEASE_WRAPPER", "")).resolve() != expected_wrapper:
        raise ContractViolation("wrapper path is not candidate-bound")
    ReleaseTransaction(config).run()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ContractViolation as exc:
        print(f"LIVE_RELEASE_TRANSACTION_FAILED:{exc}", file=sys.stderr)
        raise SystemExit(1)
