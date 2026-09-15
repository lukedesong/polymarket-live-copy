"""Forward-only real-money follower for Grand-Vista 0x42b79c at 20% source shares."""

from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import cd90_live_copy as core
from cd90_live_copy import LiveStore
from live_copy_profiles import FullWalletEventScope


SOURCE_WALLET = "0x42b79c6507a44be938f181ca6b8d379d1e3d5cd6"
PROFILE_KEY = core.LIVE_PROFILE_WALLET_42B79C_FULL_WALLET
PROFILE_SCOPE = "FULL_WALLET"
USER_AUTHORIZED_ALLOCATION_USD = Decimal("0")
FIXED_SHARE_SCALE = Decimal("0.2")
SCALE_BASIS = "USER_AUTHORIZED_TWENTY_PERCENT_OF_SOURCE_SHARES_NO_SLEEVE_BUDGET"
LEGACY_HALF_SHARE_SCALE = Decimal("0.5")
LEGACY_HALF_SCALE_BASIS = "USER_AUTHORIZED_HALF_OF_SOURCE_SHARES_NO_SLEEVE_BUDGET"
LEGACY_TENTH_SHARE_SCALE = Decimal("0.1")
LEGACY_TENTH_SCALE_BASIS = "USER_AUTHORIZED_TENTH_OF_SOURCE_SHARES_NO_SLEEVE_BUDGET"
LEGACY_SCALE_BASIS = LEGACY_HALF_SCALE_BASIS
TENTH_REBASE_MULTIPLIER = Decimal("0.2")
TWENTY_PERCENT_REBASE_MULTIPLIER = Decimal("2")
DEFAULT_RUNTIME_DIR = Path(__file__).resolve().parent / "wallet_42b79c_live_runtime"


class Wallet42b79cConfigurationError(RuntimeError):
    pass


def build_core_env(env: Mapping[str, str]) -> dict[str, str]:
    values = {str(key): str(value) for key, value in env.items()}
    lock_path = Path(values.get("POLYMARKET_SHARED_WALLET_LOCK_PATH", ""))
    coordinator_path = Path(
        values.get("POLYMARKET_SHARED_WALLET_COORDINATOR_PATH", "")
    )
    if not lock_path.is_absolute():
        raise Wallet42b79cConfigurationError("SHARED_WALLET_LOCK_PATH_NOT_ABSOLUTE")
    if not coordinator_path.is_absolute():
        raise Wallet42b79cConfigurationError(
            "SHARED_WALLET_COORDINATOR_PATH_NOT_ABSOLUTE"
        )
    values.update(
        {
            "CD90_SOURCE_WALLET": SOURCE_WALLET,
            "CD90_ALLOCATION_USD": str(USER_AUTHORIZED_ALLOCATION_USD),
        }
    )
    return values


def _stored_launch_receipt(store: LiveStore) -> dict[str, Any] | None:
    raw = store.config("profile_launch_receipt_json")
    if raw is None:
        return None
    try:
        receipt = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise Wallet42b79cConfigurationError(
            "INVALID_STORED_PROFILE_LAUNCH_RECEIPT"
        ) from exc
    if not isinstance(receipt, dict):
        raise Wallet42b79cConfigurationError("INVALID_STORED_PROFILE_LAUNCH_RECEIPT")
    expected = {
        "profile_key": PROFILE_KEY,
        "source_wallet": SOURCE_WALLET,
        "profile_scope": PROFILE_SCOPE,
        "allocation_usd": str(USER_AUTHORIZED_ALLOCATION_USD),
        "historical_catch_up": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise Wallet42b79cConfigurationError(
                f"STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:{key}"
            )
    _require_authorized_scale_state(store, receipt)
    stored_hash = store.config("profile_launch_receipt_hash")
    unsigned = dict(receipt)
    receipt_hash = unsigned.pop("receipt_hash", None)
    recomputed = core.canonical_hash(unsigned)
    if receipt_hash != recomputed or stored_hash != recomputed:
        raise Wallet42b79cConfigurationError(
            "STORED_PROFILE_LAUNCH_RECEIPT_HASH_MISMATCH"
        )
    return receipt


def _has_scale_rebase(
    store: LiveStore,
    *,
    previous_scale: Decimal,
    new_scale: Decimal,
    multiplier: Decimal,
) -> bool:
    for row in store.scale_rebase_receipts():
        if (
            Decimal(str(row["previous_scale"])) == previous_scale
            and Decimal(str(row["new_scale"])) == new_scale
            and Decimal(str(row["requested_multiplier"])) == multiplier
        ):
            return True
    return False


def _require_authorized_scale_state(
    store: LiveStore, receipt: Mapping[str, Any]
) -> None:
    try:
        receipt_scale = Decimal(str(receipt.get("fixed_share_scale")))
        current_scale = store.fixed_share_scale()
    except Exception as exc:
        raise Wallet42b79cConfigurationError(
            "STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:fixed_share_scale"
        ) from exc
    receipt_basis = str(receipt.get("scale_basis") or "")
    if current_scale != FIXED_SHARE_SCALE:
        raise Wallet42b79cConfigurationError(
            "STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:fixed_share_scale"
        )
    if receipt_scale == FIXED_SHARE_SCALE and receipt_basis == SCALE_BASIS:
        return
    has_tenth = _has_scale_rebase(
        store,
        previous_scale=LEGACY_HALF_SHARE_SCALE,
        new_scale=LEGACY_TENTH_SHARE_SCALE,
        multiplier=TENTH_REBASE_MULTIPLIER,
    )
    has_twenty = _has_scale_rebase(
        store,
        previous_scale=LEGACY_TENTH_SHARE_SCALE,
        new_scale=FIXED_SHARE_SCALE,
        multiplier=TWENTY_PERCENT_REBASE_MULTIPLIER,
    )
    if receipt_scale == LEGACY_TENTH_SHARE_SCALE and has_twenty:
        return
    if receipt_scale == LEGACY_HALF_SHARE_SCALE and has_tenth and has_twenty:
        return
    raise Wallet42b79cConfigurationError(
        "STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:fixed_share_scale"
    )


def apply_tenth_scale_rebase(
    *,
    store: LiveStore,
    change_id: str,
    effective_after_block: int,
    resume_from_block: int,
    requested_at_ms: int,
) -> dict[str, Any]:
    """Cut the locked 0.5 scale to 0.1 for future source actions only."""

    current = store.fixed_share_scale()
    normalized_change_id = str(change_id).strip()
    if current == LEGACY_TENTH_SHARE_SCALE or current == FIXED_SHARE_SCALE:
        for row in store.scale_rebase_receipts():
            if (
                Decimal(str(row["previous_scale"])) == LEGACY_HALF_SHARE_SCALE
                and Decimal(str(row["new_scale"])) == LEGACY_TENTH_SHARE_SCALE
                and Decimal(str(row["requested_multiplier"]))
                == TENTH_REBASE_MULTIPLIER
            ):
                return {**row, "idempotent": True}
        if current == LEGACY_TENTH_SHARE_SCALE:
            raise Wallet42b79cConfigurationError("TENTH_SCALE_ALREADY_ACTIVE")
        raise Wallet42b79cConfigurationError("TENTH_SCALE_SUPERSEDED_BY_TWENTY_PERCENT")
    if current != LEGACY_HALF_SHARE_SCALE:
        raise Wallet42b79cConfigurationError(
            f"TENTH_REBASE_UNSUPPORTED_CURRENT_SCALE:{current}"
        )
    result = store.rebase_fixed_share_scale(
        multiplier=TENTH_REBASE_MULTIPLIER,
        change_id=normalized_change_id,
        effective_after_block=effective_after_block,
        resume_from_block=resume_from_block,
        requested_at_ms=requested_at_ms,
    )
    if Decimal(str(result["new_scale"])) != LEGACY_TENTH_SHARE_SCALE:
        raise Wallet42b79cConfigurationError(
            f"TENTH_REBASE_RESULT_NOT_TENTH:{result['new_scale']}"
        )
    return result


def apply_twenty_percent_scale_rebase(
    *,
    store: LiveStore,
    change_id: str,
    effective_after_block: int,
    resume_from_block: int,
    requested_at_ms: int,
) -> dict[str, Any]:
    """Double the locked 0.1 scale to 0.2 for future source actions only."""

    current = store.fixed_share_scale()
    normalized_change_id = str(change_id).strip()
    if current == FIXED_SHARE_SCALE:
        existing = store.latest_scale_rebase()
        if (
            existing is not None
            and Decimal(str(existing["previous_scale"])) == LEGACY_TENTH_SHARE_SCALE
            and Decimal(str(existing["new_scale"])) == FIXED_SHARE_SCALE
            and Decimal(str(existing["requested_multiplier"]))
            == TWENTY_PERCENT_REBASE_MULTIPLIER
        ):
            return {**existing, "idempotent": True}
        raise Wallet42b79cConfigurationError("TWENTY_PERCENT_SCALE_ALREADY_ACTIVE")
    if current != LEGACY_TENTH_SHARE_SCALE:
        raise Wallet42b79cConfigurationError(
            f"TWENTY_PERCENT_REBASE_UNSUPPORTED_CURRENT_SCALE:{current}"
        )
    result = store.rebase_fixed_share_scale(
        multiplier=TWENTY_PERCENT_REBASE_MULTIPLIER,
        change_id=normalized_change_id,
        effective_after_block=effective_after_block,
        resume_from_block=resume_from_block,
        requested_at_ms=requested_at_ms,
    )
    if Decimal(str(result["new_scale"])) != FIXED_SHARE_SCALE:
        raise Wallet42b79cConfigurationError(
            f"TWENTY_PERCENT_REBASE_RESULT_NOT_TWENTY_PERCENT:{result['new_scale']}"
        )
    return result


def prepare_store(*, store: LiveStore, observed_at_ms: int) -> dict[str, Any]:
    store.lock_config_once("profile_key", PROFILE_KEY)
    store.lock_config_once("profile_scope", PROFILE_SCOPE)
    store.lock_config_once("source_wallet", SOURCE_WALLET)

    stored = _stored_launch_receipt(store)
    if stored is not None:
        stored_scale = Decimal(str(stored["fixed_share_scale"]))
        if stored_scale == FIXED_SHARE_SCALE:
            store.initialize_explicit_fixed_scale_once(
                allocation_usd=USER_AUTHORIZED_ALLOCATION_USD,
                fixed_share_scale=FIXED_SHARE_SCALE,
                scale_basis=SCALE_BASIS,
                observed_at_ms=int(stored["observed_at_ms"]),
            )
        return stored

    scale = store.initialize_explicit_fixed_scale_once(
        allocation_usd=USER_AUTHORIZED_ALLOCATION_USD,
        fixed_share_scale=FIXED_SHARE_SCALE,
        scale_basis=SCALE_BASIS,
        observed_at_ms=int(observed_at_ms),
    )
    receipt = {
        "profile_key": PROFILE_KEY,
        "source_wallet": SOURCE_WALLET,
        "profile_scope": PROFILE_SCOPE,
        "allocation_usd": str(USER_AUTHORIZED_ALLOCATION_USD),
        "fixed_share_scale": str(FIXED_SHARE_SCALE),
        "scale_basis": SCALE_BASIS,
        "historical_catch_up": False,
        "observed_at_ms": int(scale["observed_at_ms"]),
    }
    receipt["receipt_hash"] = core.canonical_hash(receipt)
    store.lock_config_once("profile_launch_receipt_hash", receipt["receipt_hash"])
    store.lock_config_once(
        "profile_launch_receipt_json",
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
    )
    persisted = _stored_launch_receipt(store)
    if persisted is None:
        raise Wallet42b79cConfigurationError("PROFILE_LAUNCH_RECEIPT_NOT_PERSISTED")
    return persisted


def run_service(
    *, runtime_dir: Path, env: Mapping[str, str], hot_standby: bool = False
) -> None:
    values = build_core_env(env)
    store = LiveStore(runtime_dir / "live.sqlite3")
    prepare_store(store=store, observed_at_ms=core.now_ms())
    core.run_live_service(
        runtime_dir=runtime_dir,
        env=values,
        profile_key=PROFILE_KEY,
        action_scope=FullWalletEventScope(core._bounded_public_json),
        hot_standby=hot_standby,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wallet-42b79c-live-copy")
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--run", action="store_true")
    modes.add_argument("--run-hot-standby", action="store_true")
    modes.add_argument("--status", action="store_true")
    modes.add_argument("--preflight", action="store_true")
    modes.add_argument("--establish-forward-watermark", action="store_true")
    modes.add_argument("--apply-tenth-scale-rebase", action="store_true")
    modes.add_argument("--apply-twenty-percent-scale-rebase", action="store_true")
    parser.add_argument("--scale-change-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    runtime_dir = Path(args.runtime_dir).resolve()
    store = LiveStore(runtime_dir / "live.sqlite3")
    if args.status:
        print(
            json.dumps(
                core.write_status_files(store, runtime_dir),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.run or args.run_hot_standby:
        run_service(
            runtime_dir=runtime_dir,
            env=os.environ,
            hot_standby=bool(args.run_hot_standby),
        )
        return 0

    if args.apply_tenth_scale_rebase or args.apply_twenty_percent_scale_rebase:
        if not str(args.scale_change_id or "").strip():
            raise Wallet42b79cConfigurationError("MISSING_SCALE_REBASE_CHANGE_ID")
        core._live_env(os.environ)
        with core._exclusive_runtime_lock(
            core._profile_runtime_lock_path(runtime_dir)
        ):
            last_raw = store.runtime_value("last_processed_block")
            head_raw = store.runtime_value("current_head")
            if last_raw is None or head_raw is None:
                raise Wallet42b79cConfigurationError(
                    "SCALE_REBASE_REQUIRES_CAUGHT_UP_CHAIN_CURSOR"
                )
            last_processed_block = int(last_raw)
            current_head = int(head_raw)
            if current_head < last_processed_block:
                raise Wallet42b79cConfigurationError(
                    "SCALE_REBASE_REQUIRES_CAUGHT_UP_CHAIN_CURSOR"
                )
            apply_rebase = (
                apply_twenty_percent_scale_rebase
                if args.apply_twenty_percent_scale_rebase
                else apply_tenth_scale_rebase
            )
            result = apply_rebase(
                store=store,
                change_id=str(args.scale_change_id),
                effective_after_block=current_head,
                resume_from_block=last_processed_block,
                requested_at_ms=core.now_ms(),
            )
            payload = core.write_status_files(store, runtime_dir)
        label = (
            "twenty_percent_scale_rebase"
            if args.apply_twenty_percent_scale_rebase
            else "tenth_scale_rebase"
        )
        print(
            json.dumps(
                {
                    label: {
                        key: str(value) if isinstance(value, Decimal) else value
                        for key, value in result.items()
                    },
                    "config": payload["config"],
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0

    values = build_core_env(os.environ)
    receipt = prepare_store(store=store, observed_at_ms=core.now_ms())
    if args.establish_forward_watermark:
        with core._exclusive_runtime_lock(
            core._profile_runtime_lock_path(runtime_dir)
        ):
            follower = core.LiveSourceFollower(
                store=store,
                rpc=core.RpcClient(),
                source_wallet=SOURCE_WALLET,
                clock_ms=core.now_ms,
            )
            result = follower.establish_forward_watermark()
            payload = core.write_status_files(store, runtime_dir)
        print(
            json.dumps(
                {"forward_watermark": result, "runtime": payload["runtime"]},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    if args.preflight:
        adapter = core.CLOBExecutionAdapter(
            core.build_authenticated_live_client(values),
            minimum_marketable_buy_notional_usd=Decimal(
                values["CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD"]
            ),
            receipt_reader=core.RpcClient(),
        )
        core._arm_runtime(
            store=store,
            adapter=adapter,
            env=values,
            minimum_size_policy=core.minimum_size_policy_for_profile(PROFILE_KEY),
            source_action_detection_contract=(
                core.source_action_detection_contract_for_profile(PROFILE_KEY)
            ),
        )
        print(
            json.dumps(
                {
                    "preflight": "PASS",
                    "profile_key": PROFILE_KEY,
                    "source_wallet": SOURCE_WALLET,
                    "fixed_share_scale": receipt["fixed_share_scale"],
                    "historical_catch_up": False,
                },
                sort_keys=True,
            )
        )
        return 0
    raise Wallet42b79cConfigurationError("UNHANDLED_CLI_MODE")


if __name__ == "__main__":
    raise SystemExit(main())
