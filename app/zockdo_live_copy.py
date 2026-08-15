"""Forward-only real-money follower for the approved zockdo public wallet."""

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


SOURCE_WALLET = "0xcd741947f7430f96bf1820a0b30d8a0fad3100a1"
PROFILE_KEY = core.LIVE_PROFILE_ZOCKDO_FULL_WALLET
PROFILE_SCOPE = "FULL_WALLET"
USER_AUTHORIZED_ALLOCATION_USD = Decimal("0")
FIXED_SHARE_SCALE = Decimal("0.5")
SCALE_BASIS = "USER_AUTHORIZED_HALF_OF_SOURCE_SHARES_NO_SLEEVE_BUDGET"
DEFAULT_RUNTIME_DIR = Path(__file__).resolve().parent / "zockdo_live_runtime"


class ZockdoConfigurationError(RuntimeError):
    pass


def build_core_env(env: Mapping[str, str]) -> dict[str, str]:
    values = {str(key): str(value) for key, value in env.items()}
    lock_path = Path(values.get("POLYMARKET_SHARED_WALLET_LOCK_PATH", ""))
    coordinator_path = Path(
        values.get("POLYMARKET_SHARED_WALLET_COORDINATOR_PATH", "")
    )
    if not lock_path.is_absolute():
        raise ZockdoConfigurationError("SHARED_WALLET_LOCK_PATH_NOT_ABSOLUTE")
    if not coordinator_path.is_absolute():
        raise ZockdoConfigurationError("SHARED_WALLET_COORDINATOR_PATH_NOT_ABSOLUTE")
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
        raise ZockdoConfigurationError(
            "INVALID_STORED_PROFILE_LAUNCH_RECEIPT"
        ) from exc
    if not isinstance(receipt, dict):
        raise ZockdoConfigurationError("INVALID_STORED_PROFILE_LAUNCH_RECEIPT")
    expected = {
        "profile_key": PROFILE_KEY,
        "source_wallet": SOURCE_WALLET,
        "profile_scope": PROFILE_SCOPE,
        "allocation_usd": str(USER_AUTHORIZED_ALLOCATION_USD),
        "fixed_share_scale": str(FIXED_SHARE_SCALE),
        "scale_basis": SCALE_BASIS,
        "historical_catch_up": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise ZockdoConfigurationError(
                f"STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:{key}"
            )
    stored_hash = store.config("profile_launch_receipt_hash")
    unsigned = dict(receipt)
    receipt_hash = unsigned.pop("receipt_hash", None)
    recomputed = core.canonical_hash(unsigned)
    if receipt_hash != recomputed or stored_hash != recomputed:
        raise ZockdoConfigurationError(
            "STORED_PROFILE_LAUNCH_RECEIPT_HASH_MISMATCH"
        )
    return receipt


def prepare_store(*, store: LiveStore, observed_at_ms: int) -> dict[str, Any]:
    store.lock_config_once("profile_key", PROFILE_KEY)
    store.lock_config_once("profile_scope", PROFILE_SCOPE)
    store.lock_config_once("source_wallet", SOURCE_WALLET)

    stored = _stored_launch_receipt(store)
    if stored is not None:
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
        raise ZockdoConfigurationError("PROFILE_LAUNCH_RECEIPT_NOT_PERSISTED")
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
    parser = argparse.ArgumentParser(prog="zockdo-live-copy")
    parser.add_argument("--runtime-dir", type=Path, default=DEFAULT_RUNTIME_DIR)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--run", action="store_true")
    modes.add_argument("--run-hot-standby", action="store_true")
    modes.add_argument("--status", action="store_true")
    modes.add_argument("--preflight", action="store_true")
    modes.add_argument("--establish-forward-watermark", action="store_true")
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
    raise ZockdoConfigurationError("UNHANDLED_CLI_MODE")


if __name__ == "__main__":
    raise SystemExit(main())
