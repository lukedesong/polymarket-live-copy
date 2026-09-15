"""Offline regression check for the bounded public-wallet baseline.

No network, no credentials, no orders.  Exercises all three branches with a
stub page reader against a real LiveStore on a temp sqlite file.
"""
import sys, tempfile
from pathlib import Path

sys.path.insert(0, "/opt/polymarket-live/releases/20260915T192800Z-bounded-public-baseline-v1/app")
import cd90_live_copy as core

PAGE = core.PUBLIC_WALLET_TRADE_PAGE_SIZE
MAXP = core.PUBLIC_WALLET_TRADE_MAX_PAGES
WALLET = "0x129246beaac356e104c397f0985c11b7469be5a6"


def row(i):
    return {
        "row_id": f"row-{i:07d}",
        "transaction_hash": f"0x{i:064x}",
        "token_id": str(1000 + i),
        "side": "BUY",
        "source_quantity": "10",
        "source_price": "0.5",
        "source_timestamp": 1789500000 + i,
        "raw": {"i": i},
    }


def build(profile_key, total_rows, tmp):
    store = core.LiveStore(Path(tmp) / f"{profile_key}-{total_rows}.sqlite3")
    store.lock_config_once("profile_key", profile_key)
    f = core.LiveSourceFollower(
        store=store, rpc=None, source_wallet=WALLET, clock_ms=lambda: 1789500000000
    )
    f.public_get_json = lambda url: []  # must be non-None to enable the path

    def read_page(*, offset):
        start, end = offset, min(offset + PAGE, total_rows)
        return [row(i) for i in range(start, end)] if start < total_rows else []

    f._read_verified_public_wallet_trade_page = read_page
    return f, store


fails = []
with tempfile.TemporaryDirectory() as tmp:
    # 1. huge wallet + opted-in profile -> bounded baseline accepted
    f, store = build(core.LIVE_PROFILE_MEAN_IMPEDIMENT_BTC5M, PAGE * MAXP + 5000, tmp)
    n = f._establish_public_wallet_forward_watermark()
    end = store.runtime_value("public_wallet_forward_watermark_reached_history_end") \
        if hasattr(store, "runtime_value") else None
    ok = n == PAGE * MAXP
    print(f"[1] 巨量钱包 + 已开放 profile -> 基线 {n} 行, 期望 {PAGE*MAXP}  {'PASS' if ok else 'FAIL'}")
    print(f"    reached_history_end = {end!r} (期望 'false')")
    if not ok or str(end) != "false":
        fails.append("case1")

    # 2. huge wallet + every other profile -> unchanged hard failure
    for other in (
        core.LIVE_PROFILE_ZOCKDO_FULL_WALLET,
        core.LIVE_PROFILE_CD90,
        core.LIVE_PROFILE_WALLET_9506_FULL_WALLET,
        core.LIVE_PROFILE_WALLET_42B79C_FULL_WALLET,
        core.LIVE_PROFILE_MAURICEBISHOP_FULL_WALLET,
        core.LIVE_PROFILE_ALBUMISTA_FULL_WALLET,
    ):
        f2, _ = build(other, PAGE * MAXP + 5000, tmp)
        try:
            f2._establish_public_wallet_forward_watermark()
            print(f"[2] {other}: 没有报错  FAIL(回归!)")
            fails.append(f"case2:{other}")
        except ConnectionError as exc:
            good = str(exc) == "PUBLIC_WALLET_FORWARD_WATERMARK_CURSOR_GAP"
            print(f"[2] {other}: {exc}  {'PASS' if good else 'FAIL'}")
            if not good:
                fails.append(f"case2:{other}")

    # 3. small wallet -> original complete-baseline path, for every profile
    for profile in (
        core.LIVE_PROFILE_MEAN_IMPEDIMENT_BTC5M,
        core.LIVE_PROFILE_ZOCKDO_FULL_WALLET,
    ):
        f3, s3 = build(profile, 1500, tmp)
        n3 = f3._establish_public_wallet_forward_watermark()
        end3 = s3.runtime_value("public_wallet_forward_watermark_reached_history_end") \
            if hasattr(s3, "runtime_value") else None
        ok3 = n3 == 1500 and str(end3) == "true"
        print(f"[3] 小钱包 {profile}: 基线 {n3} 行, end={end3!r}  {'PASS' if ok3 else 'FAIL'}")
        if not ok3:
            fails.append(f"case3:{profile}")

print()
print("ALL PASS" if not fails else f"FAILURES: {fails}")
sys.exit(1 if fails else 0)
