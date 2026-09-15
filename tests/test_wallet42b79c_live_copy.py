from decimal import Decimal
import json

import pytest

import cd90_live_copy as core
import wallet42b79c_live_copy as wallet42b79c
from live_copy_profiles import FullWalletEventScope


def test_wallet42b79c_profile_contract(tmp_path):
    assert core.minimum_size_policy_for_profile(wallet42b79c.PROFILE_KEY) == (
        core.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert core.source_action_detection_contract_for_profile(
        wallet42b79c.PROFILE_KEY
    ) == (core.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET)
    store = core.LiveStore(tmp_path / "live.sqlite3")
    receipt = wallet42b79c.prepare_store(store=store, observed_at_ms=123)
    assert store.config("source_wallet") == wallet42b79c.SOURCE_WALLET
    assert store.fixed_share_scale() == Decimal("0.2")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert receipt["historical_catch_up"] is False
    assert receipt["fixed_share_scale"] == "0.2"
    assert receipt["scale_basis"] == wallet42b79c.SCALE_BASIS
    assert wallet42b79c.SOURCE_WALLET == (
        "0x42b79c6507a44be938f181ca6b8d379d1e3d5cd6"
    )


def test_wallet42b79c_uses_full_wallet_event_scope():
    assert wallet42b79c.PROFILE_SCOPE == "FULL_WALLET"
    scope = FullWalletEventScope(lambda _url: {})
    assert isinstance(scope, FullWalletEventScope)


def test_wallet42b79c_profile_contract_is_restart_stable(tmp_path):
    store = core.LiveStore(tmp_path / "live.sqlite3")
    first = wallet42b79c.prepare_store(store=store, observed_at_ms=123)
    restarted = wallet42b79c.prepare_store(store=store, observed_at_ms=456)
    assert restarted == first
    assert restarted["observed_at_ms"] == 123


def test_wallet42b79c_env_uses_exact_source_and_scale_input():
    values = wallet42b79c.build_core_env(
        {
            "POLYMARKET_SHARED_WALLET_LOCK_PATH": "/tmp/wallet.lock",
            "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": "/tmp/coordinator.sqlite3",
            "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        }
    )
    assert values["CD90_SOURCE_WALLET"] == wallet42b79c.SOURCE_WALLET
    assert values["CD90_ALLOCATION_USD"] == "0"


def _legacy_half_scale_store(tmp_path):
    store = core.LiveStore(tmp_path / "live.sqlite3")
    store.lock_config_once("profile_key", wallet42b79c.PROFILE_KEY)
    store.lock_config_once("profile_scope", wallet42b79c.PROFILE_SCOPE)
    store.lock_config_once("source_wallet", wallet42b79c.SOURCE_WALLET)
    store.initialize_explicit_fixed_scale_once(
        allocation_usd=Decimal("0"),
        fixed_share_scale=Decimal("0.5"),
        scale_basis=wallet42b79c.LEGACY_SCALE_BASIS,
        observed_at_ms=123,
    )
    receipt = {
        "profile_key": wallet42b79c.PROFILE_KEY,
        "source_wallet": wallet42b79c.SOURCE_WALLET,
        "profile_scope": wallet42b79c.PROFILE_SCOPE,
        "allocation_usd": "0",
        "fixed_share_scale": "0.5",
        "scale_basis": wallet42b79c.LEGACY_SCALE_BASIS,
        "historical_catch_up": False,
        "observed_at_ms": 123,
    }
    receipt["receipt_hash"] = core.canonical_hash(receipt)
    store.lock_config_once("profile_launch_receipt_hash", receipt["receipt_hash"])
    store.lock_config_once(
        "profile_launch_receipt_json",
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
    )
    return store


def test_legacy_half_scale_store_cannot_start_until_twenty_percent_rebase(tmp_path):
    store = _legacy_half_scale_store(tmp_path)
    with pytest.raises(
        wallet42b79c.Wallet42b79cConfigurationError,
        match="STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:fixed_share_scale",
    ):
        wallet42b79c.prepare_store(store=store, observed_at_ms=456)
    assert store.fixed_share_scale() == Decimal("0.5")


def test_tenth_rebase_alone_does_not_unlock_legacy_half_scale_store(tmp_path):
    store = _legacy_half_scale_store(tmp_path)
    first = wallet42b79c.apply_tenth_scale_rebase(
        store=store,
        change_id="grand-vista-20260824-user-authorized-tenth",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )
    second = wallet42b79c.apply_tenth_scale_rebase(
        store=store,
        change_id="grand-vista-20260824-user-authorized-tenth",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=4,
    )

    assert first["previous_scale"] == Decimal("0.5")
    assert first["new_scale"] == Decimal("0.1")
    assert first["multiplier"] == Decimal("0.2")
    assert second.get("idempotent") is True
    assert store.fixed_share_scale() == Decimal("0.1")
    with pytest.raises(
        wallet42b79c.Wallet42b79cConfigurationError,
        match="STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:fixed_share_scale",
    ):
        wallet42b79c.prepare_store(store=store, observed_at_ms=456)


def test_user_authorized_twenty_percent_rebase_unlocks_legacy_half_scale_store(
    tmp_path,
):
    store = _legacy_half_scale_store(tmp_path)
    wallet42b79c.apply_tenth_scale_rebase(
        store=store,
        change_id="grand-vista-20260824-user-authorized-tenth",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )
    first = wallet42b79c.apply_twenty_percent_scale_rebase(
        store=store,
        change_id="grand-vista-20260826-user-authorized-twenty-percent",
        effective_after_block=110,
        resume_from_block=100,
        requested_at_ms=5,
    )
    second = wallet42b79c.apply_twenty_percent_scale_rebase(
        store=store,
        change_id="grand-vista-20260826-user-authorized-twenty-percent",
        effective_after_block=110,
        resume_from_block=100,
        requested_at_ms=6,
    )
    restarted = wallet42b79c.prepare_store(store=store, observed_at_ms=456)

    assert first["previous_scale"] == Decimal("0.1")
    assert first["new_scale"] == Decimal("0.2")
    assert first["multiplier"] == Decimal("2")
    assert second.get("idempotent") is True
    assert store.fixed_share_scale() == Decimal("0.2")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert restarted["fixed_share_scale"] == "0.5"
    assert restarted["observed_at_ms"] == 123
    assert restarted["historical_catch_up"] is False
    assert len(store.scale_rebase_receipts()) == 2


def test_twenty_percent_rebase_allows_pending_confirmed_zero_fill_without_submission(
    tmp_path,
):
    store = _legacy_half_scale_store(tmp_path)
    source = core.SourceAction(
        transaction_hash="0x" + "a" * 64,
        token_id="123",
        side="BUY",
        order_hash="0x" + "b" * 64,
        source_quantity=Decimal("10"),
        source_notional=Decimal("4"),
        source_timestamp=1_700_000_000,
        block_number=90,
        block_hash="0x" + "c" * 64,
        source_role="taker",
        discovered_at_ms=1_700_000_000_100,
    )
    assert store.record_action_receipt(source) is True
    store.append_transition(
        source=source,
        status="PENDING_CONFIRMED_ZERO_FILL",
        reason="CURRENT_BOOK_UNAVAILABLE_FOR_LIQUIDITY_RETRY",
    )
    wallet42b79c.apply_tenth_scale_rebase(
        store=store,
        change_id="grand-vista-20260824-user-authorized-tenth",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )
    result = wallet42b79c.apply_twenty_percent_scale_rebase(
        store=store,
        change_id="grand-vista-20260826-user-authorized-twenty-percent",
        effective_after_block=110,
        resume_from_block=100,
        requested_at_ms=5,
    )
    assert result["new_scale"] == Decimal("0.2")
    assert store.fixed_share_scale() == Decimal("0.2")
    restarted = wallet42b79c.prepare_store(store=store, observed_at_ms=456)
    assert restarted["fixed_share_scale"] == "0.5"


def test_twenty_percent_rebase_is_not_economic_ledger_rewrite(tmp_path):
    import live_release_transaction as release

    store = _legacy_half_scale_store(tmp_path)
    before = release.forbidden_ledger_fingerprint(store.path)
    wallet42b79c.apply_tenth_scale_rebase(
        store=store,
        change_id="grand-vista-20260824-user-authorized-tenth",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )
    wallet42b79c.apply_twenty_percent_scale_rebase(
        store=store,
        change_id="grand-vista-20260826-user-authorized-twenty-percent",
        effective_after_block=110,
        resume_from_block=100,
        requested_at_ms=5,
    )
    after = release.forbidden_ledger_fingerprint(store.path)
    assert before == after
    assert store.fixed_share_scale() == Decimal("0.2")
