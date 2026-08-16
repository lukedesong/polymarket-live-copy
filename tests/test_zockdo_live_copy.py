from decimal import Decimal

import cd90_live_copy as core
import zockdo_live_copy as zockdo


def test_zockdo_profile_contract(tmp_path):
    assert core.minimum_size_policy_for_profile(zockdo.PROFILE_KEY) == (
        core.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert core.source_action_detection_contract_for_profile(zockdo.PROFILE_KEY) == (
        core.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    )
    store = core.LiveStore(tmp_path / "live.sqlite3")
    receipt = zockdo.prepare_store(store=store, observed_at_ms=123)
    assert store.config("source_wallet") == zockdo.SOURCE_WALLET
    assert store.fixed_share_scale() == Decimal("0.5")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert receipt["historical_catch_up"] is False


def test_zockdo_profile_contract_is_restart_stable(tmp_path):
    store = core.LiveStore(tmp_path / "live.sqlite3")
    first = zockdo.prepare_store(store=store, observed_at_ms=123)
    restarted = zockdo.prepare_store(store=store, observed_at_ms=456)
    assert restarted == first
    assert restarted["observed_at_ms"] == 123


def test_zockdo_env_uses_exact_source_and_scale_input():
    values = zockdo.build_core_env(
        {
            "POLYMARKET_SHARED_WALLET_LOCK_PATH": "/tmp/wallet.lock",
            "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": "/tmp/coordinator.sqlite3",
            "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        }
    )
    assert values["CD90_SOURCE_WALLET"] == zockdo.SOURCE_WALLET
    assert values["CD90_ALLOCATION_USD"] == "0"
