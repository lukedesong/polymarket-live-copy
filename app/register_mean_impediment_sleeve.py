"""Register mean_impediment_btc5m as a RESERVED sleeve on the shared wallet.

Dry run by default: prints the current sleeve table, the live collateral it
would commit to, and the sleeve set it would write -- and writes nothing.
Pass --apply to actually run extend_from_frozen_ledgers.

Preconditions enforced by the coordinator itself: every strategy ledger must be
stopped, existing profile paths/roles are immutable, and only RESERVED sleeves
may be added.
"""

import argparse
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "/opt/polymarket-live/current/app")

import cd90_live_copy as core
from live_wallet_coordinator import SharedWalletCoordinator, SleeveSpec
import mean_impediment_live_copy as mi

NEW_PROFILE = mi.PROFILE_KEY
NEW_LEDGER = Path("/srv/polymarket-live/runtime/mean_impediment_live/live.sqlite3")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    values = mi.build_core_env(os.environ)
    coordinator = SharedWalletCoordinator(
        Path(os.environ["POLYMARKET_SHARED_WALLET_COORDINATOR_PATH"])
    )

    existing = coordinator.registered_sleeves()
    print("=== 现有 sleeves ===")
    for row in existing:
        print(f"  {row['profile_key']:28} {row['role']:9} "
              f"raw={row['raw_cash_at_migration_usd']:>14} "
              f"adj={row['cash_adjustment_usd']}")

    if any(r["profile_key"] == NEW_PROFILE for r in existing):
        print(f"\n{NEW_PROFILE} 已注册，无需迁移。")
        return 0

    adapter = core.CLOBExecutionAdapter(
        core.build_authenticated_live_client(values),
        minimum_marketable_buy_notional_usd=Decimal(
            values["CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD"]
        ),
        receipt_reader=core.RpcClient(),
    )
    collateral = adapter.collateral_balance_usd()
    funder = str(values["POLYMARKET_FUNDER_ADDRESS"]).strip().lower()
    print(f"\n=== 共享钱包实时抵押品 ===\n  {collateral} USDC   funder={funder}")

    specs = [
        SleeveSpec(r["profile_key"], Path(r["ledger_path"]), r["role"])
        for r in existing
    ] + [SleeveSpec(NEW_PROFILE, NEW_LEDGER, "RESERVED")]

    print(f"\n=== 迁移后将写入 {len(specs)} 个 sleeve ===")
    for s in sorted(specs, key=lambda x: x.profile_key):
        mark = "  <== 新增" if s.profile_key == NEW_PROFILE else ""
        print(f"  {s.profile_key:28} {s.role:9} {s.ledger_path}{mark}")

    if not args.apply:
        print("\n[DRY RUN] 未写入任何内容。确认无误后加 --apply。")
        return 0

    receipt = coordinator.extend_from_frozen_ledgers(
        sleeves=specs,
        authenticated_collateral_usd=collateral,
        funder_address=funder,
        observed_at_ms=core.now_ms(),
    )
    print("\n=== 迁移回执 ===")
    print(json.dumps(receipt, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
