from decimal import Decimal

import cd90_live_copy as core
import wallet9506_live_copy as wallet9506


def test_wallet9506_profile_contract(tmp_path):
    assert core.minimum_size_policy_for_profile(wallet9506.PROFILE_KEY) == (
        core.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert core.source_action_detection_contract_for_profile(wallet9506.PROFILE_KEY) == (
        core.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    )
    store = core.LiveStore(tmp_path / "live.sqlite3")
    receipt = wallet9506.prepare_store(store=store, observed_at_ms=123)
    assert store.config("source_wallet") == wallet9506.SOURCE_WALLET
    assert store.fixed_share_scale() == Decimal("0.1")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert receipt["historical_catch_up"] is False


def test_wallet9506_profile_contract_is_restart_stable(tmp_path):
    store = core.LiveStore(tmp_path / "live.sqlite3")
    first = wallet9506.prepare_store(store=store, observed_at_ms=123)
    restarted = wallet9506.prepare_store(store=store, observed_at_ms=456)
    assert restarted == first
    assert restarted["observed_at_ms"] == 123


def test_wallet9506_env_uses_exact_source_and_scale_input():
    values = wallet9506.build_core_env(
        {
            "POLYMARKET_SHARED_WALLET_LOCK_PATH": "/tmp/wallet.lock",
            "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": "/tmp/coordinator.sqlite3",
            "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        }
    )
    assert values["CD90_SOURCE_WALLET"] == wallet9506.SOURCE_WALLET
    assert values["CD90_ALLOCATION_USD"] == "0"
