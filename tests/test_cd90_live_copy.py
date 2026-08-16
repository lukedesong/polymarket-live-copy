from dataclasses import replace
from decimal import Decimal
import asyncio
import fcntl
import inspect
import json
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest
from py_clob_client_v2.exceptions import PolyApiException

import cd90_live_copy as live
from live_chain_client import ORDER_FILLED_TOPIC as CANONICAL_ORDER_FILLED_TOPIC
from live_copy_profiles import ScopeDecision
from live_wallet_coordinator import SharedWalletCoordinator, SleeveSpec

from cd90_live_copy import (
    CLOBExecutionAdapter,
    CD90RedemptionAdapter,
    LiveDisabledError,
    LiveConfigurationError,
    LiveSourceFollower,
    LiveStore,
    SourceAction,
    auto_redeem_resolved_positions,
    decode_followable_source_action,
    extract_ws_new_head_number,
    execute_source_action,
    initialize_scale_once,
    parse_source_open_position_value,
    reconcile_platform_settled_winners,
    reconcile_redemption_submissions,
    reconcile_submitted_actions,
    retry_pending_actions,
    run_redemption_cycle,
    write_status_files,
    _matched_shares,
    _is_retryable_external_error,
    _shared_wallet_submission_lock,
)


D = Decimal


def test_known_profiles_skip_buys_below_the_current_market_minimum():
    assert (
        live.minimum_size_policy_for_profile(live.LIVE_PROFILE_CD90)
        == live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert (
        live.minimum_size_policy_for_profile(live.LIVE_PROFILE_BDDC_WEATHER)
        == live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert (
        live.minimum_size_policy_for_profile(live.LIVE_PROFILE_TENNIS_MAINLINE)
        == live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    with pytest.raises(LiveConfigurationError, match="UNKNOWN_LIVE_PROFILE"):
        live.minimum_size_policy_for_profile("unknown-profile")


def test_wallet44b0_netflix_profile_uses_full_wallet_detection_and_skips_below_minimum():
    assert (
        live.minimum_size_policy_for_profile(
            live.LIVE_PROFILE_WALLET_44B0_NETFLIX
        )
        == live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert (
        live.source_action_detection_contract_for_profile(
            live.LIVE_PROFILE_WALLET_44B0_NETFLIX
        )
        == live.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    )


def test_explicit_user_scale_initializes_once_without_inventing_source_value(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    scale = D("0.03277027447726496667590788407")

    receipt = store.initialize_explicit_fixed_scale_once(
        allocation_usd=D("200"),
        fixed_share_scale=scale,
        scale_basis="USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
        observed_at_ms=100,
    )
    replay = store.initialize_explicit_fixed_scale_once(
        allocation_usd=D("200"),
        fixed_share_scale=scale,
        scale_basis="USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
        observed_at_ms=100,
    )

    assert replay == receipt
    assert receipt == {
        "allocation_usd": "200",
        "fixed_share_scale": str(scale),
        "observed_at_ms": 100,
        "scale_basis": "USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
    }
    assert store.fixed_share_scale() == scale
    assert store.config("source_open_position_value_usd") is None
    assert store.account_snapshot()["cash_usd"] == D("200")


def test_explicit_user_scale_rejects_a_conflicting_replay(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize_explicit_fixed_scale_once(
        allocation_usd=D("200"),
        fixed_share_scale=D("0.03277027447726496667590788407"),
        scale_basis="USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
        observed_at_ms=100,
    )

    with pytest.raises(
        LiveConfigurationError,
        match="EXPLICIT_FIXED_SCALE_INITIALIZATION_MISMATCH",
    ):
        store.initialize_explicit_fixed_scale_once(
            allocation_usd=D("200"),
            fixed_share_scale=D("0.1"),
            scale_basis="USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
            observed_at_ms=100,
        )


def test_existing_sleeve_accepts_only_a_proven_coordinator_descendant_receipt(
    tmp_path: Path,
):
    stores = [LiveStore(tmp_path / f"sleeve-{index}.sqlite3") for index in range(3)]
    for store in stores:
        store.initialize_explicit_fixed_scale_once(
            allocation_usd=D("100"),
            fixed_share_scale=D("0.1"),
            scale_basis="TEST_FIXED_SCALE",
            observed_at_ms=1,
        )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    first = coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("profile-0", stores[0].path, "RESIDUAL"),
            SleeveSpec("profile-1", stores[1].path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("200"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=10,
    )
    stores[0].lock_config_once(
        "shared_wallet_migration_receipt_hash",
        first["migration_receipt_hash"],
    )
    before_account = stores[0].account_snapshot()
    migrated = coordinator.extend_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("profile-0", stores[0].path, "RESIDUAL"),
            SleeveSpec("profile-1", stores[1].path, "RESERVED"),
            SleeveSpec("profile-2", stores[2].path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("300"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=20,
    )

    accepted = stores[0].migrate_shared_wallet_migration_receipt_hash(
        expected_receipt_hash=migrated["migration_receipt_hash"],
        receipt_history=coordinator.receipt_history(),
        changed_at_ms=30,
    )

    assert accepted == migrated["migration_receipt_hash"]
    assert stores[0].account_snapshot() == before_account
    with stores[0].connect() as connection:
        receipt = connection.execute(
            """
            SELECT previous_value, new_value, reason, details_json
            FROM config_change_receipts
            WHERE config_key = 'shared_wallet_migration_receipt_hash'
            """
        ).fetchone()
    assert receipt["previous_value"] == first["migration_receipt_hash"]
    assert receipt["new_value"] == migrated["migration_receipt_hash"]
    assert receipt["reason"] == "VERIFIED_ADDITIVE_COORDINATOR_DESCENDANT"
    details = json.loads(receipt["details_json"])
    assert details["from_generation"] == 1
    assert details["to_generation"] == 2
    assert details["cash_mutated"] is False
    assert details["historical_ledger_rewritten"] is False


def test_coordinator_receipt_migration_rejects_an_unproven_hash(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.lock_config_once("shared_wallet_migration_receipt_hash", "a" * 64)

    with pytest.raises(
        LiveConfigurationError,
        match="UNPROVEN_SHARED_WALLET_MIGRATION_RECEIPT_DESCENDANT",
    ):
        store.migrate_shared_wallet_migration_receipt_hash(
            expected_receipt_hash="b" * 64,
            receipt_history=(
                {
                    "generation": 2,
                    "parent_migration_receipt_hash": "c" * 64,
                    "migration_receipt_hash": "b" * 64,
                    "funder_address": "0x" + "a" * 40,
                },
            ),
            changed_at_ms=2,
        )

    assert store.config("shared_wallet_migration_receipt_hash") == "a" * 64


def test_tennis_skips_buys_below_the_current_market_minimum():
    """The tennis copier follows the same below-minimum rule as CD90."""

    assert (
        live.minimum_size_policy_for_profile(live.LIVE_PROFILE_TENNIS_MAINLINE)
        == live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )


def test_arm_runtime_preserves_skip_below_minimum_without_rebasing(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    before_account = store.account_snapshot()
    before_scale = store.fixed_share_scale()

    class Adapter:
        minimum_marketable_buy_notional_usd = D("1")

    env = {
        "POLYMARKET_LIVE_TRADING": "1",
        "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER": "1",
        "POLYMARKET_PRIVATE_KEY": "secret-is-not-read-by-this-test",
        "POLYMARKET_API_KEY": "api-key",
        "POLYMARKET_API_SECRET": "api-secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "1",
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40,
        "CD90_SOURCE_WALLET": "0x" + "b" * 40,
        "CD90_ALLOCATION_USD": "100",
        "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        "POLYMARKET_SHARED_WALLET_LOCK_PATH": str(tmp_path / "wallet.lock"),
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": str(
            tmp_path / "coordinator.sqlite3"
        ),
    }

    allocation = live._arm_runtime(
        store=store,
        adapter=Adapter(),
        env=env,
        minimum_size_policy=live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    )

    assert allocation is None
    assert store.config("minimum_size_policy") == (
        live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert store.config("source_wallet") == "0x" + "b" * 40
    assert store.account_snapshot() == before_account
    assert store.fixed_share_scale() == before_scale
    assert store.runtime_value("live_enabled") == "true"


def test_arm_runtime_does_not_persist_a_strategy_wallet_policy(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once("profile_key", live.LIVE_PROFILE_TENNIS_MAINLINE)
    store.set_runtime("last_processed_block", "99")
    before_account = store.account_snapshot()
    before_scale = store.fixed_share_scale()

    class Adapter:
        minimum_marketable_buy_notional_usd = D("1")

    env = {
        "POLYMARKET_LIVE_TRADING": "1",
        "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER": "1",
        "POLYMARKET_PRIVATE_KEY": "secret-is-not-read-by-this-test",
        "POLYMARKET_API_KEY": "api-key",
        "POLYMARKET_API_SECRET": "api-secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "1",
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40,
        "CD90_SOURCE_WALLET": "0x" + "b" * 40,
        "CD90_ALLOCATION_USD": "100",
        "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        "POLYMARKET_SHARED_WALLET_LOCK_PATH": str(tmp_path / "wallet.lock"),
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": str(
            tmp_path / "coordinator.sqlite3"
        ),
    }

    live._arm_runtime(store=store, adapter=Adapter(), env=env)

    assert store.config("local_debt_policy") is None
    assert store.config("local_debt_policy_effective_after_block") is None
    assert store.account_snapshot() == before_account
    assert store.fixed_share_scale() == before_scale
def test_arm_runtime_never_creates_a_strategy_debt_activation_state(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once("profile_key", live.LIVE_PROFILE_CD90)

    class Adapter:
        minimum_marketable_buy_notional_usd = D("1")

    env = {
        "POLYMARKET_LIVE_TRADING": "1",
        "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER": "1",
        "POLYMARKET_PRIVATE_KEY": "secret-is-not-read-by-this-test",
        "POLYMARKET_API_KEY": "api-key",
        "POLYMARKET_API_SECRET": "api-secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "1",
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40,
        "CD90_SOURCE_WALLET": "0x" + "b" * 40,
        "CD90_ALLOCATION_USD": "100",
        "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        "POLYMARKET_SHARED_WALLET_LOCK_PATH": str(tmp_path / "wallet.lock"),
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": str(
            tmp_path / "coordinator.sqlite3"
        ),
    }

    live._arm_runtime(store=store, adapter=Adapter(), env=env)

    assert store.config("local_debt_policy") is None
    assert store.runtime_value("local_debt_policy_activation_state") is None


def test_arm_runtime_initializes_scale_when_authenticated_cash_is_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = LiveStore(tmp_path / "live.sqlite3")
    class Adapter:
        minimum_marketable_buy_notional_usd = D("1")

        def collateral_balance_usd(self):
            return D("0")

    monkeypatch.setattr(
        live, "fetch_source_open_position_value_usd", lambda _wallet: D("400")
    )
    env = {
        "POLYMARKET_LIVE_TRADING": "1",
        "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER": "1",
        "POLYMARKET_PRIVATE_KEY": "secret-is-not-read-by-this-test",
        "POLYMARKET_API_KEY": "api-key",
        "POLYMARKET_API_SECRET": "api-secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "1",
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40,
        "CD90_SOURCE_WALLET": "0x" + "b" * 40,
        "CD90_ALLOCATION_USD": "100",
        "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        "POLYMARKET_SHARED_WALLET_LOCK_PATH": str(tmp_path / "wallet.lock"),
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": str(
            tmp_path / "coordinator.sqlite3"
        ),
    }

    assert live._arm_runtime(store=store, adapter=Adapter(), env=env) is None
    assert store.fixed_share_scale() == D("0.25")


def test_arm_runtime_migrates_cd90_to_the_verified_full_wallet_source_contract(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    store.lock_config_once(
        "source_action_detection_contract",
        live.SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY,
    )

    class Adapter:
        minimum_marketable_buy_notional_usd = D("1")

    env = {
        "POLYMARKET_LIVE_TRADING": "1",
        "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER": "1",
        "POLYMARKET_PRIVATE_KEY": "secret-is-not-read-by-this-test",
        "POLYMARKET_API_KEY": "api-key",
        "POLYMARKET_API_SECRET": "api-secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "1",
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40,
        "CD90_SOURCE_WALLET": "0x" + "b" * 40,
        "CD90_ALLOCATION_USD": "100",
        "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        "POLYMARKET_SHARED_WALLET_LOCK_PATH": str(tmp_path / "wallet.lock"),
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": str(
            tmp_path / "coordinator.sqlite3"
        ),
    }

    live._arm_runtime(
        store=store,
        adapter=Adapter(),
        env=env,
        minimum_size_policy=live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
        source_action_detection_contract=(
            live.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
        ),
    )

    assert store.config("source_action_detection_contract") == (
        live.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    )
    with store.connect() as connection:
        receipt = connection.execute(
            """
            SELECT previous_value, new_value, reason, details_json
            FROM config_change_receipts
            WHERE config_key = 'source_action_detection_contract'
            """
        ).fetchone()
    assert dict(receipt) == {
        "previous_value": live.SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY,
        "new_value": live.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET,
        "reason": "USER_AUTHORIZED_FULL_WALLET_SOURCE_ACTION_DISCOVERY",
        "details_json": json.dumps(
            {
                "applies_to": "FORWARD_SOURCE_ACTIONS_ONLY",
                "chain_counterparty_log_promoted_directly": False,
                "historical_ledger_rewritten": False,
                "historical_orders_submitted": False,
            },
            sort_keys=True,
        ),
    }


def test_arm_runtime_migrates_legacy_minimum_upscale_policy_without_rebasing(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    before_account = store.account_snapshot()
    before_scale = store.fixed_share_scale()

    class Adapter:
        minimum_marketable_buy_notional_usd = D("1")

    env = {
        "POLYMARKET_LIVE_TRADING": "1",
        "I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER": "1",
        "POLYMARKET_PRIVATE_KEY": "secret-is-not-read-by-this-test",
        "POLYMARKET_API_KEY": "api-key",
        "POLYMARKET_API_SECRET": "api-secret",
        "POLYMARKET_API_PASSPHRASE": "passphrase",
        "POLYMARKET_SIGNATURE_TYPE": "1",
        "POLYMARKET_FUNDER_ADDRESS": "0x" + "a" * 40,
        "CD90_SOURCE_WALLET": "0x" + "b" * 40,
        "CD90_ALLOCATION_USD": "100",
        "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        "POLYMARKET_SHARED_WALLET_LOCK_PATH": str(tmp_path / "wallet.lock"),
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": str(
            tmp_path / "coordinator.sqlite3"
        ),
    }

    allocation = live._arm_runtime(
        store=store,
        adapter=Adapter(),
        env=env,
        minimum_size_policy=live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    )

    assert allocation is None
    assert store.config("minimum_size_policy") == (
        live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert store.account_snapshot() == before_account
    assert store.fixed_share_scale() == before_scale
    with store.connect() as connection:
        receipt = connection.execute(
            """
            SELECT config_key, previous_value, new_value, reason
            FROM config_change_receipts
            """
        ).fetchone()
    assert dict(receipt) == {
        "config_key": "minimum_size_policy",
        "previous_value": live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
        "new_value": live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
        "reason": "P0_FIXED_SHARE_SCALE_NO_MINIMUM_UPSCALE",
    }


def test_minimum_policy_migrates_from_upscale_to_skip_without_rebasing(
    tmp_path: Path,
):
    """A safety migration changes only future below-minimum BUY handling."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    before_account = store.account_snapshot()
    before_scale = store.fixed_share_scale()

    try:
        actual = store.migrate_legacy_minimum_size_policy(
            expected_policy=live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
            changed_at_ms=2,
        )
    except LiveConfigurationError:
        actual = None

    assert actual == live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    assert store.config("minimum_size_policy") == (
        live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert store.account_snapshot() == before_account
    assert store.fixed_share_scale() == before_scale
    with store.connect() as connection:
        receipt = connection.execute(
            """
            SELECT config_key, previous_value, new_value, reason
            FROM config_change_receipts
            """
        ).fetchone()
    assert dict(receipt) == {
        "config_key": "minimum_size_policy",
        "previous_value": live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
        "new_value": live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
        "reason": "P0_FIXED_SHARE_SCALE_NO_MINIMUM_UPSCALE",
    }


def action(*, side: str = "BUY", quantity: str = "40", marker: str = "1") -> SourceAction:
    return SourceAction(
        transaction_hash="0x" + marker * 64,
        token_id="123",
        side=side,
        order_hash="0x" + "2" * 64,
        source_quantity=D(quantity),
        source_notional=D(quantity) * D("0.40"),
        source_timestamp=1_700_000_000,
        block_number=100,
        block_hash="0x" + "3" * 64,
        source_role="maker",
        discovered_at_ms=1_700_000_000_100,
    )


def _available_cash_usd(store: LiveStore) -> Decimal:
    """Current cash authority less only active authenticated BUY reservations."""

    return store.account_snapshot()["cash_usd"] - store.active_buy_reservations_usd()


def _action_transition_count(store: LiveStore) -> int:
    with store.connect() as connection:
        return int(
            connection.execute("SELECT COUNT(*) FROM action_transitions").fetchone()[0]
        )


def _fill_correction_count(store: LiveStore) -> int:
    with store.connect() as connection:
        return int(
            connection.execute("SELECT COUNT(*) FROM fill_corrections").fetchone()[0]
        )


def _seed_local_fill(
    *,
    store: LiveStore,
    source: SourceAction,
    quantity: Decimal,
    price: Decimal,
    fee_usd: Decimal,
) -> None:
    """Create a pre-existing local fill for setup without pretending to submit."""

    store.record_action_receipt(source)
    with store.connect() as connection:
        connection.execute("BEGIN IMMEDIATE")
        LiveStore._apply_fill_on_connection(
            connection,
            source=source,
            quantity=quantity,
            price=price,
            fee_usd=fee_usd,
            notional_usd=None,
        )
    store.append_transition(
        source=source,
        status="FILLED",
        reason="TEST_SEEDED_LOCAL_FILL",
        created_at_ms=source.discovered_at_ms,
    )


def _begin_test_submission_attempt(
    *,
    store: LiveStore,
    source: SourceAction,
    requested_quantity: Decimal,
    snapshot: dict[str, str],
    created_at_ms: int,
):
    price = D(snapshot["best_price"])
    buy_notional = requested_quantity * price if source.side == "BUY" else D("0")
    return store.begin_submission_attempt(
        source=source,
        plan=live.ActionPlan(
            terminal_status="SUBMIT_STARTED",
            reason="TEST_SUBMISSION_ATTEMPT",
            side=source.side,
            proportional_quantity=requested_quantity,
            requested_quantity=requested_quantity,
            order_amount_usd=buy_notional,
            worst_price=price,
            reserved_cash_usd=buy_notional,
        ),
        snapshot=snapshot,
        condition_id="condition-123",
        created_at_ms=created_at_ms,
        transition_details={"test_fixture": True},
    )


def _set_authoritative_fill(
    *,
    execution,
    quantity: Decimal,
    notional_usd: Decimal,
    vwap_price: Decimal,
    fee_usd: Decimal = D("0"),
    receipt_evidence=None,
) -> None:
    """Provide the current reconciliation contract without a fake order row."""

    evidence = (
        [{"transaction_hash": "0x" + "1" * 64}]
        if receipt_evidence is None
        else receipt_evidence
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": quantity,
        "notional_usd": notional_usd,
        "fee_usd": fee_usd,
        "vwap_price": vwap_price,
        "receipt_evidence": evidence,
    }


def test_fidelity_schema_migration_is_additive_and_preserves_existing_ledger(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO positions(token_id, quantity, cost_basis_usd) "
            "VALUES('123', '7', '2.8')"
        )
    before_account = store.account_snapshot()
    before_scale = store.fixed_share_scale()
    before_position = store.position_quantity("123")

    store._initialized = False
    store.initialize()

    assert store.account_snapshot() == before_account
    assert store.fixed_share_scale() == before_scale
    assert store.position_quantity("123") == before_position
    with store.connect() as connection:
        tables = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {
        "action_market_metadata",
        "action_targets",
        "submission_attempts",
        "decision_units",
    } <= tables


def test_existing_action_receipts_gain_log_index_without_rewriting_old_rows(
    tmp_path: Path,
):
    database = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE action_receipts (
                action_id TEXT PRIMARY KEY,
                transaction_hash TEXT NOT NULL,
                token_id TEXT NOT NULL,
                side TEXT NOT NULL,
                order_hash TEXT NOT NULL,
                source_quantity TEXT NOT NULL,
                source_notional TEXT NOT NULL,
                source_timestamp INTEGER NOT NULL,
                block_number INTEGER NOT NULL,
                block_hash TEXT NOT NULL,
                source_role TEXT NOT NULL,
                discovered_at_ms INTEGER NOT NULL,
                UNIQUE(transaction_hash, token_id, side, order_hash)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO action_receipts VALUES(
                'legacy-action', '0xsource', '123', 'BUY', '0xorder',
                '2', '0.8', 1700000000, 100, '0xblock', 'maker',
                1700000000100
            )
            """
        )

    store = LiveStore(database)
    store.initialize()

    with store.connect() as connection:
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(action_receipts)")
        }
        row = connection.execute(
            "SELECT action_id, source_log_index FROM action_receipts"
        ).fetchone()
    assert "source_log_index" in columns
    assert dict(row) == {
        "action_id": "legacy-action",
        "source_log_index": 0,
    }


def test_frozen_metadata_target_and_attempts_are_idempotent_and_auditable(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    assert store.record_action_receipt(source) is True
    metadata = {
        "condition_id": "condition-123",
        "market_slug": "high-temperature-in-paris-on-august-8",
        "event_slug": "temperature-in-paris-on-august-8",
    }

    assert store.freeze_action_metadata(
        source=source,
        metadata=metadata,
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=10,
    ) is True
    assert store.freeze_action_metadata(
        source=source,
        metadata=metadata,
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=11,
    ) is False

    target = store.ensure_action_target(
        source=source,
        proportional_quantity=D("4"),
        target_quantity=D("5"),
        state="READY",
        reason="MINIMUM_ORDER_SIZE_UPSCALE",
        updated_at_ms=12,
    )
    assert target["target_quantity"] == D("5")
    assert target["remaining_quantity"] == D("5")

    first = _begin_test_submission_attempt(
        store=store,
        source=source,
        requested_quantity=D("5"),
        snapshot={"best_price": "0.40"},
        created_at_ms=13,
    )
    store.set_attempt_order_id(
        attempt_id=first["attempt_id"],
        order_id="order-1",
        response={"success": True, "orderID": "order-1"},
        updated_at_ms=14,
    )
    store.update_attempt_state(
        attempt_id=first["attempt_id"],
        state="NO_FILL",
        response={"status": "ORDER_STATUS_CANCELED"},
        updated_at_ms=15,
    )
    store.release_reservation_and_finalize(
        source=source,
        terminal_status="PENDING_LIQUIDITY",
        reason="TEST_FIRST_ATTEMPT_NO_FILL",
        created_at_ms=15,
        details={"test_fixture": True},
        attempt_id=first["attempt_id"],
        attempt_state="NO_FILL",
        attempt_response={"status": "ORDER_STATUS_CANCELED"},
    )
    second = _begin_test_submission_attempt(
        store=store,
        source=source,
        requested_quantity=D("5"),
        snapshot={"best_price": "0.41"},
        created_at_ms=16,
    )

    assert first["attempt_number"] == 1
    assert second["attempt_number"] == 2
    assert first["attempt_id"] != second["attempt_id"]
    assert store.submission_attempt_count(source.action_id) == 2
    assert store.decision_unit_summary() == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 1,
            "filled": 0,
            "partial": 0,
            "pending": 1,
            "external_or_causal": 0,
            "internal_error": 0,
        }
    ]


def test_non_netflix_alert_is_deduplicated_and_does_not_skip_action(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(side="BUY")
    store.record_action_receipt(source)
    metadata = {
        "condition_id": "condition-123",
        "event_slug": "trump-speech",
        "market_slug": "will-trump-say-tariff",
        "question": "Will Trump say tariff?",
        "topic_classification": "NON_NETFLIX",
    }

    first = store.record_source_topic_alert(
        source=source,
        metadata=metadata,
        processing_state="SCOPE_ELIGIBLE",
        created_at_ms=10,
    )
    second = store.record_source_topic_alert(
        source=source,
        metadata=metadata,
        processing_state="SCOPE_ELIGIBLE",
        created_at_ms=11,
    )

    assert first is True
    assert second is False
    alerts = store.source_topic_alerts(unacknowledged_only=True)
    assert len(alerts) == 1
    assert alerts[0]["action_id"] == source.action_id
    assert alerts[0]["topic_classification"] == "NON_NETFLIX"
    assert alerts[0]["side"] == "BUY"


def test_netflix_classification_does_not_create_an_exception_alert(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(side="BUY")
    store.record_action_receipt(source)

    inserted = store.record_source_topic_alert(
        source=source,
        metadata={
            "condition_id": "condition-123",
            "event_slug": "netflix-top-show-this-week",
            "market_slug": "netflix-top-show-this-week",
            "topic_classification": "NETFLIX",
        },
        processing_state="SCOPE_ELIGIBLE",
        created_at_ms=10,
    )

    assert inserted is False
    assert store.source_topic_alerts(unacknowledged_only=False) == []


class FakeExecution:
    def __init__(self, *, response=None, error=None, collateral="100"):
        self.response = response or {"success": True, "orderID": "order-1"}
        self.error = error
        self.collateral = D(collateral)
        self.calls = []
        self.orders = {}
        self.associated_trades = {}

    def collateral_balance_usd(self) -> Decimal:
        return self.collateral

    def snapshot(self, *, token_id: str, side: str):
        assert token_id == "123"
        return {
            "minimum_order_size": "5",
            "minimum_marketable_buy_notional_usd": "1",
            "best_price": "0.40" if side == "BUY" else "0.30",
            "tick_size": "0.01",
            "visible_best_level_size": "100",
            "fee_bps": "0",
            "raw_book": {"asks": [], "bids": []},
        }

    def submit_fak_market(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ):
        call = {
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
        }
        if user_usdc_balance is not None:
            call["user_usdc_balance"] = user_usdc_balance
        self.calls.append(call)
        if self.error is not None:
            raise self.error
        return self.response

    def submit_fak_exact_shares(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ):
        call = {
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "quantity_mode": "EXACT_SHARES",
        }
        if user_usdc_balance is not None:
            call["user_usdc_balance"] = user_usdc_balance
        self.calls.append(call)
        if self.error is not None:
            raise self.error
        return self.response

    def get_order(self, order_id: str):
        return self.orders[order_id]

    def get_associated_trades(self, *, order_id: str, trade_ids: list[str]):
        return self.associated_trades[order_id]


class FakePreparedExecution(FakeExecution):
    def __init__(self, *, response=None, error=None):
        super().__init__(response=response, error=error)
        self.prepared = {
            "order_id": "0x" + "9" * 64,
            "order_version": 2,
            "order_type": "FAK",
            "neg_risk": False,
            "order_fields": {"salt": "123", "makerAmount": "4000000"},
            "_signed_order": SimpleNamespace(signature="0xsecret-signature"),
        }
        self.prepare_calls = []
        self.exact_prepare_calls = []
        self.prepared_submit_calls = []

    def prepare_fak_market(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ):
        self.prepare_calls.append(
            {
                "token_id": token_id,
                "side": side,
                "price": price,
                "size": size,
                "user_usdc_balance": user_usdc_balance,
            }
        )
        return dict(self.prepared)

    def submit_prepared_fak_market(self, prepared_order):
        self.prepared_submit_calls.append(dict(prepared_order))
        if self.error is not None:
            raise self.error
        return self.response

    def prepare_fak_exact_shares(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ):
        self.exact_prepare_calls.append(
            {
                "token_id": token_id,
                "side": side,
                "price": price,
                "size": size,
                "user_usdc_balance": user_usdc_balance,
            }
        )
        return dict(self.prepared)


class FailingSnapshotExecution(FakeExecution):
    def snapshot(self, *, token_id: str, side: str):
        raise ConnectionError("book endpoint unavailable")


class ShallowBookExecution(FakeExecution):
    def snapshot(self, *, token_id: str, side: str):
        result = super().snapshot(token_id=token_id, side=side)
        return {**result, "visible_best_level_size": "0.59"}


class ShallowSellBookExecution(FakeExecution):
    def snapshot(self, *, token_id: str, side: str):
        result = super().snapshot(token_id=token_id, side=side)
        return {**result, "visible_best_level_size": "8"}


class FakeCLOBClient:
    def __init__(self):
        self.submissions = []
        self.created_orders = []
        self.cancellations = []
        self.open_order_ids = set()

        class FakeSigner:
            def get_chain_id(self):
                return 137

        class FakeBuilder:
            signer = FakeSigner()

        self.builder = FakeBuilder()

    def get_order_book(self, token_id: str):
        assert token_id == "123"
        return {
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
            "market": "condition-123",
            "asks": [{"price": "0.40", "size": "100"}],
            "bids": [{"price": "0.30", "size": "100"}],
        }

    def get_fee_rate_bps(self, token_id: str):
        assert token_id == "123"
        return 0

    def get_tick_size(self, token_id: str):
        assert token_id == "123"
        return "0.01"

    def get_clob_market_info(self, condition_id: str):
        assert condition_id == "condition-123"
        return {
            "mos": "5",
            "mts": "0.01",
            "fd": {"r": "0.05", "e": 1, "to": True},
        }

    def create_and_post_market_order(self, order_args, *, options, order_type):
        self.submissions.append(
            {"order_args": order_args, "options": options, "order_type": order_type}
        )
        return {"success": True, "orderID": "order-1"}

    def create_market_order(self, order_args, options):
        from py_clob_client_v2.order_utils import Side, SignatureTypeV2
        from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2

        self.created_orders.append({"order_args": order_args, "options": options})
        return SignedOrderV2(
            salt="123",
            maker="0x" + "1" * 40,
            signer="0x" + "2" * 40,
            tokenId=str(order_args.token_id),
            makerAmount="4000000",
            takerAmount="10000000",
            side=Side.BUY if str(order_args.side).upper() == "BUY" else Side.SELL,
            signatureType=SignatureTypeV2.EOA,
            timestamp="1700000000000",
            metadata="0x" + "0" * 64,
            builder="0x" + "0" * 64,
            expiration="0",
            signature="0x" + "3" * 130,
        )

    def create_order(self, order_args, options):
        self.created_orders.append({"order_args": order_args, "options": options})
        from py_clob_client_v2.order_utils import Side, SignatureTypeV2
        from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2

        return SignedOrderV2(
            salt="123",
            maker="0x" + "1" * 40,
            signer="0x" + "2" * 40,
            tokenId=str(order_args.token_id),
            makerAmount="4000000",
            takerAmount="10000000",
            side=Side.BUY if str(order_args.side).upper() == "BUY" else Side.SELL,
            signatureType=SignatureTypeV2.EOA,
            timestamp="1700000000000",
            metadata="0x" + "0" * 64,
            builder="0x" + "0" * 64,
            expiration=str(order_args.expiration),
            signature="0x" + "3" * 130,
        )

    def post_order(self, order, order_type):
        self.submissions.append({"order": order, "order_type": order_type})
        order_id = "0x" + "9" * 64
        self.open_order_ids.add(order_id)
        return {"success": True, "orderID": order_id, "status": "live"}

    def cancel_order(self, payload):
        self.cancellations.append(payload.orderID)
        self.open_order_ids.discard(payload.orderID)
        return {"canceled": [payload.orderID], "not_canceled": {}}

    def get_open_orders(self):
        return [{"id": order_id} for order_id in self.open_order_ids]


class FakeCollateralClient:
    def __init__(self, *, balance: str = "100000000", allowances=None):
        self.balance = balance
        self.allowances = allowances if allowances is not None else {"exchange-a": "1"}

    def get_balance_allowance(self, params):
        return {"balance": self.balance, "allowances": self.allowances}


class FakeCollateralReader:
    def __init__(self, *, balance: str):
        self.balance = D(balance)
        self.calls = 0

    def collateral_balance_usd(self) -> Decimal:
        self.calls += 1
        return self.balance


class FakeRedemptionAdapter:
    """Authoritative-market and wallet adapter for redemption state tests."""

    condition_id = "0x" + "b" * 64
    wallet_address = "0x" + "a" * 40

    def __init__(self, *, primary_raw: int = 10_000_000, status: str = "STATE_NEW"):
        self.primary_raw = primary_raw
        self.secondary_raw = 0
        self.confirmed_collateral_payout_raw = primary_raw
        self.status = status
        self.submissions = []
        self.status_reads = []

    def condition_mapping_for_token(self, token_id: str):
        assert token_id in {"123", "456"}
        return {
            "condition_id": self.condition_id,
            "primary_token_id": "123",
            "secondary_token_id": "456",
        }

    def condition_resolution(self, condition_id: str):
        assert condition_id == self.condition_id
        return {
            "condition_id": condition_id,
            "closed": True,
            "winner_token_id": "123",
        }

    def outcome_token_balance_raw(self, *, wallet_address: str, token_id: str) -> int:
        assert wallet_address == self.wallet_address
        return self.primary_raw if token_id == "123" else self.secondary_raw

    def collateral_balance_raw(self, *, wallet_address: str) -> int:
        assert wallet_address == self.wallet_address
        return 0

    def confirmed_redemption_collateral_payout_raw(
        self, *, transaction_hash: str, wallet_address: str
    ) -> int:
        assert transaction_hash.startswith("0x") and len(transaction_hash) == 66
        assert wallet_address == self.wallet_address
        return self.confirmed_collateral_payout_raw

    def submit_redeem(self, *, condition_id: str):
        assert condition_id == self.condition_id
        self.submissions.append(condition_id)
        return {"transaction_id": "redeem-1", "transaction_hash": None}

    def redemption_transaction_status(self, transaction_id: str):
        assert transaction_id == "redeem-1"
        self.status_reads.append(transaction_id)
        return {"state": self.status, "transaction_hash": "0x" + "c" * 64}

    def official_redemption_activity_for_transaction(
        self, *, condition_id: str, transaction_hash: str
    ):
        assert condition_id == self.condition_id
        assert transaction_hash == "0x" + "c" * 64
        return {
            "condition_id": condition_id,
            "transaction_hash": transaction_hash,
            "payout_usd": D("10"),
            "official_activity_type": "REDEEM",
        }


def test_redemption_transaction_payout_reader_counts_only_inbound_collateral_transfer():
    from eth_utils import keccak
    from types import SimpleNamespace

    wallet = "0x" + "a" * 40
    collateral = "0x" + "d" * 40
    transaction_hash = "0x" + "c" * 64
    transfer_topic = "0x" + keccak(
        text="Transfer(address,address,uint256)"
    ).hex()
    recipient_topic = "0x" + ("0" * 24) + wallet[2:]
    other_recipient_topic = "0x" + ("0" * 24) + ("b" * 40)

    class FakeRpc:
        def eth_get_transaction_receipt(self, requested_hash: str):
            assert requested_hash == transaction_hash
            return {
                "status": "0x1",
                "logs": [
                    {
                        "address": collateral,
                        "topics": [transfer_topic, "0x" + "0" * 64, recipient_topic],
                        "data": "0x" + f"{10_000_000:064x}",
                    },
                    {
                        "address": collateral,
                        "topics": [
                            transfer_topic,
                            "0x" + "0" * 64,
                            other_recipient_topic,
                        ],
                        "data": "0x" + f"{7_000_000:064x}",
                    },
                    {
                        "address": "0x" + "e" * 40,
                        "topics": [transfer_topic, "0x" + "0" * 64, recipient_topic],
                        "data": "0x" + f"{3_000_000:064x}",
                    },
                ],
            }

    secure_client = SimpleNamespace(
        _ctx=SimpleNamespace(
            wallet=wallet,
            rpc=FakeRpc(),
            environment=SimpleNamespace(collateral_token=collateral),
        )
    )
    adapter = CD90RedemptionAdapter(secure_client=secure_client)

    assert adapter.confirmed_redemption_collateral_payout_raw(
        transaction_hash=transaction_hash,
        wallet_address=wallet,
    ) == 10_000_000


def test_redemption_token_mapping_uses_shared_bounded_public_reader(monkeypatch):
    """A stalled public CLOB read must not leave the redemption task running forever."""

    requested_urls = []

    def fake_bounded_public_json(url: str):
        requested_urls.append(url)
        return {
            "condition_id": "0x" + "b" * 64,
            "primary_token_id": "123",
            "secondary_token_id": "456",
        }

    monkeypatch.setattr("cd90_live_copy._bounded_public_json", fake_bounded_public_json)
    condition_id = "0x" + "b" * 64
    market = SimpleNamespace(
        condition_id=condition_id,
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="123"),
            no=SimpleNamespace(token_id="456"),
        ),
    )
    paginator = SimpleNamespace(iter_items=lambda: iter([market]))
    secure_client = SimpleNamespace(
        _ctx=SimpleNamespace(wallet="0x" + "a" * 40),
        list_markets=lambda **kwargs: paginator,
    )
    adapter = CD90RedemptionAdapter(secure_client=secure_client)

    mapping = adapter.condition_mapping_for_token("123")

    assert requested_urls == ["https://clob.polymarket.com/markets-by-token/123"]
    assert mapping["condition_id"] == "0x" + "b" * 64


def test_redemption_mapping_uses_official_yes_no_order_when_clob_pair_is_reversed(
    monkeypatch,
):
    condition_id = "0x" + "b" * 64
    monkeypatch.setattr(
        "cd90_live_copy._bounded_public_json",
        lambda url: {
            "condition_id": condition_id,
            "primary_token_id": "456",
            "secondary_token_id": "123",
        },
    )
    market = SimpleNamespace(
        condition_id=condition_id,
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="123"),
            no=SimpleNamespace(token_id="456"),
        ),
    )
    paginator = SimpleNamespace(iter_items=lambda: iter([market]))
    secure_client = SimpleNamespace(
        _ctx=SimpleNamespace(wallet="0x" + "a" * 40),
        list_markets=lambda **kwargs: paginator,
    )

    mapping = CD90RedemptionAdapter(
        secure_client=secure_client
    ).condition_mapping_for_token("123")

    assert mapping == {
        "condition_id": condition_id,
        "primary_token_id": "123",
        "secondary_token_id": "456",
    }


def test_redemption_resolution_uses_canonical_clob_outcome_order_not_local_pair_order(
    monkeypatch,
):
    """Payout index zero must map to CLOB token zero, even if local order is reversed."""

    condition_id = "0x" + "b" * 64
    secure_client = SimpleNamespace(
        _ctx=SimpleNamespace(
            wallet="0x" + "a" * 40,
            environment=SimpleNamespace(conditional_tokens="0x" + "d" * 40),
        )
    )
    adapter = CD90RedemptionAdapter(secure_client=secure_client)
    chain_values = iter([1, 1, 0])
    monkeypatch.setattr(
        adapter,
        "_read_uint_call",
        lambda **kwargs: next(chain_values),
    )
    monkeypatch.setattr(
        "cd90_live_copy._bounded_public_json",
        lambda url: {
            "condition_id": condition_id,
            "tokens": [
                {"token_id": "456", "outcome": "Over", "winner": True},
                {"token_id": "123", "outcome": "Under", "winner": False},
            ],
        },
    )

    resolution = adapter.condition_resolution(condition_id)

    assert resolution == {
        "condition_id": condition_id,
        "closed": True,
        "winner_index": 0,
        "winner_token_id": "456",
    }


class FakeRpc:
    def __init__(self, head: int):
        self.head = head
        self.log_queries = []

    def latest_block_number(self):
        return self.head

    def get_block(self, number: int):
        return {
            "number": hex(number),
            "hash": "0x" + f"{number:064x}",
            "parentHash": "0x" + f"{number - 1:064x}",
            "timestamp": hex(1_700_000_000),
        }

    def source_logs_range(self, from_block, to_block, source_wallet, role):
        self.log_queries.append((from_block, to_block, source_wallet, role))
        return []


def v2_order_filled_log(
    *,
    maker: str,
    taker: str,
    order_marker: str,
    side: int,
    token_id: int,
    maker_amount: int,
    taker_amount: int,
    log_index: int,
) -> dict:
    from eth_abi import encode
    from eth_utils import keccak

    def address_topic(value: str) -> str:
        return "0x" + "0" * 24 + value.lower().removeprefix("0x")

    return {
        "address": "0x" + "e" * 40,
        "blockNumber": hex(100),
        "blockHash": "0x" + "3" * 64,
        "transactionHash": "0x" + "4" * 64,
        "logIndex": hex(log_index),
        "topics": [
            "0x"
            + keccak(
                text=(
                    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,"
                    "uint256,uint256,bytes32,bytes32)"
                )
            ).hex(),
            "0x" + order_marker * 64,
            address_topic(maker),
            address_topic(taker),
        ],
        "data": "0x"
        + encode(
            [
                "uint8",
                "uint256",
                "uint256",
                "uint256",
                "uint256",
                "bytes32",
                "bytes32",
            ],
            [
                side,
                token_id,
                maker_amount,
                taker_amount,
                0,
                b"\0" * 32,
                b"\0" * 32,
            ],
        ).hex(),
    }


def test_source_follower_uses_only_source_maker_orders_not_counterparty_logs(
    tmp_path: Path,
):
    source_wallet = "0x" + "a" * 40

    class RoleRpc(FakeRpc):
        def __init__(self):
            super().__init__(head=100)
            self.logs = {
                "maker": [
                    v2_order_filled_log(
                        maker=source_wallet,
                        taker="0x" + "b" * 40,
                        order_marker="5",
                        side=0,
                        token_id=123,
                        maker_amount=4_000_000,
                        taker_amount=10_000_000,
                        log_index=9,
                    )
                ],
                # This is the counterparty's order.  The followed wallet is
                # merely named as taker by the exchange event and it must not
                # become a second, inverted source action.
                "taker": [
                    v2_order_filled_log(
                        maker="0x" + "c" * 40,
                        taker=source_wallet,
                        order_marker="6",
                        side=0,
                        token_id=456,
                        maker_amount=6_000_000,
                        taker_amount=10_000_000,
                        log_index=7,
                    )
                ],
            }

        def source_logs_range(self, from_block, to_block, wallet, role):
            self.log_queries.append((from_block, to_block, wallet, role))
            return self.logs[role]

    rpc = RoleRpc()
    follower = LiveSourceFollower(
        store=LiveStore(tmp_path / "live.sqlite3"),
        rpc=rpc,
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
    )

    actions = follower._new_source_actions(from_block=100, to_block=100)

    assert rpc.log_queries == [(100, 100, source_wallet, "maker")]
    assert [(item.side, item.token_id, item.source_quantity) for item in actions] == [
        ("BUY", "123", D("10"))
    ]


def test_public_maker_match_is_not_blocked_by_same_transaction_counterparty_context(
    tmp_path: Path,
):
    """An exact source-maker public row must not be double-counted or stalled.

    V2 may emit counterparty OrderFilled logs in the same transaction as the
    source maker's own order.  Those logs name the source as taker but do not
    turn an exactly matched public maker row into a second source action.
    """

    source_wallet = "0x" + "a" * 40

    class MixedRoleRpc(FakeRpc):
        def __init__(self):
            super().__init__(head=100)
            self.logs = {
                "maker": [
                    v2_order_filled_log(
                        maker=source_wallet,
                        taker="0x" + "b" * 40,
                        order_marker="5",
                        side=0,
                        token_id=456,
                        maker_amount=4_000_000,
                        taker_amount=10_000_000,
                        log_index=9,
                    )
                ],
                "taker": [
                    v2_order_filled_log(
                        maker="0x" + "c" * 40,
                        taker=source_wallet,
                        order_marker="6",
                        side=1,
                        token_id=456,
                        maker_amount=5_000_000,
                        taker_amount=2_000_000,
                        log_index=7,
                    )
                ],
            }

        def source_logs_range(self, from_block, to_block, wallet, role):
            self.log_queries.append((from_block, to_block, wallet, role))
            return self.logs[role]

    store = LiveStore(tmp_path / "live.sqlite3")
    rpc = MixedRoleRpc()
    follower = LiveSourceFollower(
        store=store,
        rpc=rpc,
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=lambda _url: [
            {
                "proxyWallet": source_wallet,
                "transactionHash": "0x" + "4" * 64,
                "asset": "456",
                "side": "BUY",
                "size": "10",
                "price": "0.4",
                "timestamp": 1_700_000_000,
            }
        ],
    )

    actions = follower._new_source_actions(from_block=100, to_block=100)

    assert rpc.log_queries == [
        (100, 100, source_wallet, "maker"),
        (100, 100, source_wallet, "taker"),
    ]
    assert [(item.side, item.token_id, item.source_quantity, item.source_role) for item in actions] == [
        ("BUY", "456", D("10"), live.SOURCE_ROLE_CHAIN_MAKER),
    ]
    with store.connect() as connection:
        observation = connection.execute(
            "SELECT state, source_action_id FROM public_source_observations"
        ).fetchone()
    assert dict(observation) == {
        "state": "RECONCILED_CHAIN_MAKER_WITH_COUNTERPARTY_CONTEXT",
        "source_action_id": actions[0].action_id,
    }


def test_public_maker_match_accepts_sub_raw_unit_display_rounding(
    tmp_path: Path,
):
    """The public price product may differ below one on-chain USDC raw unit."""

    source_wallet = "0x" + "a" * 40

    class MakerRpc(FakeRpc):
        def __init__(self):
            super().__init__(head=100)
            self.logs = {
                "maker": [
                    v2_order_filled_log(
                        maker=source_wallet,
                        taker="0x" + "b" * 40,
                        order_marker="5",
                        side=0,
                        token_id=456,
                        maker_amount=4_000_000,
                        taker_amount=10_000_000,
                        log_index=9,
                    )
                ],
                "taker": [],
            }

        def source_logs_range(self, from_block, to_block, wallet, role):
            self.log_queries.append((from_block, to_block, wallet, role))
            return self.logs[role]

    store = LiveStore(tmp_path / "live.sqlite3")
    follower = LiveSourceFollower(
        store=store,
        rpc=MakerRpc(),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=lambda _url: [
            {
                "proxyWallet": source_wallet,
                "transactionHash": "0x" + "4" * 64,
                "asset": "456",
                "side": "BUY",
                "size": "10",
                # Formula-derived public notional: 10 * 0.40000000005 =
                # 4.00000000050 USD.  The chain's external fixed-math unit
                # is one millionth USD, so both reconcile to 4.000000.
                "price": "0.40000000005",
                "timestamp": 1_700_000_000,
            }
        ],
    )

    actions = follower._new_source_actions(from_block=100, to_block=100)

    assert [(item.source_quantity, item.source_notional) for item in actions] == [
        (D("10"), D("4")),
    ]
    with store.connect() as connection:
        observation = connection.execute(
            "SELECT state FROM public_source_observations"
        ).fetchone()
    assert observation["state"] == "RECONCILED_CHAIN_MAKER_ACTION"


def test_public_maker_match_rejects_a_difference_of_more_than_one_raw_unit(
    tmp_path: Path,
):
    """Display rounding cannot be used to accept a materially different fill."""

    source_wallet = "0x" + "a" * 40

    class MakerRpc(FakeRpc):
        def __init__(self):
            super().__init__(head=100)
            self.logs = {
                "maker": [
                    v2_order_filled_log(
                        maker=source_wallet,
                        taker="0x" + "b" * 40,
                        order_marker="5",
                        side=0,
                        token_id=456,
                        maker_amount=4_000_000,
                        taker_amount=10_000_000,
                        log_index=9,
                    )
                ],
                "taker": [],
            }

        def source_logs_range(self, from_block, to_block, wallet, role):
            return self.logs[role]

    follower = LiveSourceFollower(
        store=LiveStore(tmp_path / "live.sqlite3"),
        rpc=MakerRpc(),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=lambda _url: [
            {
                "proxyWallet": source_wallet,
                "transactionHash": "0x" + "4" * 64,
                "asset": "456",
                "side": "BUY",
                "size": "10",
                "price": "0.4000002",
                "timestamp": 1_700_000_000,
            }
        ],
    )

    with pytest.raises(
        LiveConfigurationError,
        match="PUBLIC_CHAIN_MAKER_ACTION_RECONCILIATION_MISMATCH",
    ):
        follower._new_source_actions(from_block=100, to_block=100)


def test_source_v2_taker_order_is_followed_from_its_own_maker_topic_event(
    tmp_path: Path,
):
    """V2 emits the taker order separately with the source in the maker topic."""

    source_wallet = "0x" + "a" * 40
    exchange_wallet = "0xe2222d279d744050d28e00520010520000310f59"

    class PairedRoleRpc(FakeRpc):
        def __init__(self):
            super().__init__(head=100)
            self.logs = {
                # The source's own V2 taker order is emitted as an OrderFilled
                # whose maker field is the source and taker field is exchange.
                "maker": [
                    v2_order_filled_log(
                        maker=source_wallet,
                        taker=exchange_wallet,
                        order_marker="7",
                        side=0,
                        token_id=456,
                        maker_amount=900_000,
                        taker_amount=20_000_000,
                        log_index=8,
                    )
                ],
                # This paired maker-order event names the source only as a
                # counterparty.  It must not create a second or inverted leg.
                "taker": [
                    v2_order_filled_log(
                        maker="0x" + "c" * 40,
                        taker=source_wallet,
                        order_marker="6",
                        side=1,
                        token_id=456,
                        maker_amount=20_000_000,
                        taker_amount=900_000,
                        log_index=7,
                    )
                ],
            }

        def source_logs_range(self, from_block, to_block, wallet, role):
            self.log_queries.append((from_block, to_block, wallet, role))
            return self.logs[role]

    rpc = PairedRoleRpc()
    follower = LiveSourceFollower(
        store=LiveStore(tmp_path / "live.sqlite3"),
        rpc=rpc,
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
    )

    actions = follower._new_source_actions(from_block=100, to_block=100)

    assert rpc.log_queries == [(100, 100, source_wallet, "maker")]
    assert [(item.side, item.token_id, item.source_quantity, item.source_notional) for item in actions] == [
        ("BUY", "456", D("20"), D("0.9"))
    ]


def test_source_follower_reconstructs_a_taker_action_only_from_a_verified_public_wallet_trade(
    tmp_path: Path,
):
    """A public wallet row supplies the source side; a taker log alone never does."""

    source_wallet = "0x" + "a" * 40

    class RoleRpc(FakeRpc):
        def __init__(self):
            super().__init__(head=100)
            self.logs = {
                "maker": [],
                "taker": [
                    v2_order_filled_log(
                        maker="0x" + "c" * 40,
                        taker=source_wallet,
                        order_marker="6",
                        side=1,
                        token_id=456,
                        maker_amount=10_000_000,
                        taker_amount=6_000_000,
                        log_index=7,
                    )
                ],
            }

        def source_logs_range(self, from_block, to_block, wallet, role):
            self.log_queries.append((from_block, to_block, wallet, role))
            return self.logs[role]

    public_urls: list[str] = []

    def public_get_json(url: str):
        public_urls.append(url)
        return [
            {
                "proxyWallet": source_wallet,
                "transactionHash": "0x" + "4" * 64,
                "asset": "456",
                "side": "BUY",
                "size": "10",
                "price": "0.6",
                "timestamp": 1_700_000_000,
            }
        ]

    rpc = RoleRpc()
    follower = LiveSourceFollower(
        store=LiveStore(tmp_path / "live.sqlite3"),
        rpc=rpc,
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=public_get_json,
    )

    actions = follower._new_source_actions(from_block=100, to_block=100)

    assert rpc.log_queries == [
        (100, 100, source_wallet, "maker"),
        (100, 100, source_wallet, "taker"),
    ]
    assert len(public_urls) == 1
    assert "takerOnly=false" in public_urls[0]
    assert [(item.side, item.token_id, item.source_quantity, item.source_role) for item in actions] == [
        ("BUY", "456", D("10"), live.SOURCE_ROLE_VERIFIED_PUBLIC_WALLET),
    ]
    assert actions[0].source_notional == D("6.0")
    assert actions[0].block_number == 100
    assert actions[0].log_index == 7


def test_public_wallet_forward_watermark_does_not_replay_visible_history(
    tmp_path: Path,
):
    source_wallet = "0x" + "a" * 40
    public_row = {
        "proxyWallet": source_wallet,
        "transactionHash": "0x" + "4" * 64,
        "asset": "456",
        "side": "BUY",
        "size": "10",
        "price": "0.6",
        "timestamp": 1_700_000_000,
    }
    follower = LiveSourceFollower(
        store=LiveStore(tmp_path / "live.sqlite3"),
        rpc=FakeRpc(head=100),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=lambda _url: [public_row],
    )

    watermark = follower.establish_forward_watermark()

    assert watermark["public_wallet_baseline_row_count"] == 1
    assert follower._new_source_actions(from_block=101, to_block=101) == []


def test_public_wallet_forward_watermark_paginates_before_marking_history_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A saturated first page is not a cursor gap when a later page bounds it."""

    source_wallet = "0x" + "a" * 40
    monkeypatch.setattr(live, "PUBLIC_WALLET_TRADE_PAGE_SIZE", 2)

    def row(sequence: int) -> dict[str, object]:
        return {
            "proxyWallet": source_wallet,
            "transactionHash": "0x" + f"{sequence:064x}",
            "asset": str(400 + sequence),
            "side": "BUY",
            "size": "10",
            "price": "0.6",
            "timestamp": 1_700_000_000 + sequence,
        }

    pages = {0: [row(3), row(2)], 2: [row(1)]}
    requested_offsets: list[int] = []

    def public_get_json(url: str) -> list[dict[str, object]]:
        offset = next(
            int(component.split("=", 1)[1])
            for component in url.split("?", 1)[1].split("&")
            if component.startswith("offset=")
        )
        requested_offsets.append(offset)
        return pages[offset]

    store = LiveStore(tmp_path / "live.sqlite3")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=public_get_json,
    )

    watermark = follower.establish_forward_watermark()

    assert requested_offsets == [0, 2]
    assert watermark["public_wallet_baseline_row_count"] == 3
    assert store.runtime_value("public_wallet_forward_watermark_row_count") == "3"
    assert follower._new_verified_public_wallet_rows() == []


def test_public_wallet_forward_watermark_does_not_persist_a_partial_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    source_wallet = "0x" + "a" * 40
    monkeypatch.setattr(live, "PUBLIC_WALLET_TRADE_PAGE_SIZE", 2)
    public_rows = [
        {
            "proxyWallet": source_wallet,
            "transactionHash": "0x" + f"{sequence:064x}",
            "asset": str(500 + sequence),
            "side": "BUY",
            "size": "10",
            "price": "0.6",
            "timestamp": 1_700_000_000 + sequence,
        }
        for sequence in (2, 1)
    ]

    def public_get_json(url: str) -> list[dict[str, object]]:
        if "offset=0" in url:
            return public_rows
        raise ConnectionError("UPSTREAM_TIMEOUT")

    store = LiveStore(tmp_path / "live.sqlite3")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=public_get_json,
    )

    with pytest.raises(ConnectionError, match="UPSTREAM_TIMEOUT"):
        follower.establish_forward_watermark()

    store.initialize()
    with store.connect() as connection:
        persisted = connection.execute(
            "SELECT COUNT(*) FROM public_source_observations"
        ).fetchone()[0]
    assert persisted == 0


def test_delayed_public_wallet_trade_is_constrained_before_any_current_book_read(
    tmp_path: Path,
):
    source_wallet = "0x" + "a" * 40

    class LateRpc(FakeRpc):
        def get_receipt(self, _transaction_hash: str):
            return {"blockNumber": hex(99)}

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    follower = LiveSourceFollower(
        store=store,
        rpc=LateRpc(head=100),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_100,
        public_get_json=lambda _url: [
            {
                "proxyWallet": source_wallet,
                "transactionHash": "0x" + "4" * 64,
                "asset": "123",
                "side": "BUY",
                "size": "10",
                "price": "0.4",
                "timestamp": 1_700_000_000,
            }
        ],
    )

    actions = follower._new_source_actions(from_block=100, to_block=100)

    assert len(actions) == 1
    action_from_public = actions[0]
    assert action_from_public.source_role == live.SOURCE_ROLE_VERIFIED_PUBLIC_WALLET
    assert store.action_execution_constraint(action_from_public)["reason"] == (
        "PUBLIC_WALLET_ACTION_DISCOVERED_AFTER_CHAIN_CURSOR_NO_ACTION_TIME_BOOK"
    )

    class SnapshotMustNotRun(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("delayed public action reached a current book")

    result = execute_source_action(
        store=store,
        source=action_from_public,
        execution=SnapshotMustNotRun(),
        allocated_cash=D("100"),
        live_enabled=True,
    )
    assert result["terminal_status"] == "EXTERNAL_UNFILLABLE"


def test_scale_is_initialized_once_and_never_recomputed_from_a_later_snapshot(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")

    first = initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    resumed = initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("800"),
        observed_at_ms=2,
    )

    assert first == D("0.25")
    assert resumed == D("0.25")
    assert store.config("source_open_position_value_usd") == "400"


def test_user_authorized_scale_rebase_is_idempotent_and_preserves_ledger(
    tmp_path: Path,
):
    """A live scale change affects only future plans, never prior accounting."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO positions(token_id, quantity, cost_basis_usd) VALUES('123', '7', '2.8')"
        )

    before_account = store.account_snapshot()
    before_position = store.position_quantity("123")
    first = store.rebase_fixed_share_scale(
        multiplier=D("2"),
        change_id="cd90-20260806-user-authorized-200pct",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )
    second = store.rebase_fixed_share_scale(
        multiplier=D("2"),
        change_id="cd90-20260806-user-authorized-200pct",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=4,
    )

    assert first == {
        "change_id": "cd90-20260806-user-authorized-200pct",
        "previous_scale": D("0.25"),
        "new_scale": D("0.50"),
        "multiplier": D("2"),
        "effective_after_block": 100,
        "resume_from_block": 90,
        "requested_at_ms": 3,
        "idempotent": False,
    }
    assert second == {**first, "idempotent": True}
    assert store.fixed_share_scale() == D("0.50")
    assert store.config("allocation_usd") == "100"
    assert store.config("source_open_position_value_usd") == "400"
    assert store.config("scale_basis") == "USER_AUTHORIZED_FIXED_SHARE_SCALE_MULTIPLIER"
    assert store.account_snapshot() == before_account
    assert store.position_quantity("123") == before_position
    assert store.latest_scale_rebase() == {
        "change_id": "cd90-20260806-user-authorized-200pct",
        "previous_scale": "0.25",
        "new_scale": "0.50",
        "requested_multiplier": "2",
        "effective_after_block": 100,
        "resume_from_block": 90,
        "requested_at_ms": 3,
        "prior_scale_basis": "OBSERVABLE_OPEN_POSITION_SLEEVE",
        "resulting_scale_basis": "USER_AUTHORIZED_FIXED_SHARE_SCALE_MULTIPLIER",
        "details": {
            "allocation_usd_unchanged": "100",
            "applies_to": "FUTURE_SOURCE_ACTIONS_AFTER_EFFECTIVE_BLOCK",
            "existing_positions_changed": False,
            "historical_ledger_rewritten": False,
            "source_open_position_value_recomputed": False,
            "source_open_position_value_usd_unchanged": "400",
        },
    }


def test_scale_rebase_refuses_an_active_order_reservation(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action()
    assert store.record_action_receipt(source) is True
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO order_reservations(
                action_id, token_id, side, quantity, cash_reserved_usd, active, created_at_ms
            ) VALUES(?, ?, 'BUY', '10', '4', 1, 1)
            """,
            (source.action_id, source.token_id),
        )

    with pytest.raises(
        LiveConfigurationError,
        match="ACTIVE_ORDER_RESERVATIONS_BLOCK_SCALE_REBASE",
    ):
        store.rebase_fixed_share_scale(
            multiplier=D("2"),
            change_id="cd90-20260806-user-authorized-200pct",
            effective_after_block=100,
            resume_from_block=90,
            requested_at_ms=3,
        )

    assert store.fixed_share_scale() == D("0.25")


def test_operator_planned_resume_is_idempotent_and_requires_the_persisted_cursor(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")

    first = store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="cd90-20260806-post-scale-status-fix",
        reason="POST_SCALE_STATUS_FIX",
        armed_at_ms=1,
    )
    second = store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="cd90-20260806-post-scale-status-fix",
        reason="POST_SCALE_STATUS_FIX",
        armed_at_ms=2,
    )

    assert first == {
        "resume_from_block": 100,
        "change_id": "cd90-20260806-post-scale-status-fix",
        "reason": "POST_SCALE_STATUS_FIX",
        "armed_at_ms": 1,
        "idempotent": False,
    }
    assert second == {**first, "idempotent": True}
    assert store.runtime_value("operator_planned_resume_from_block") == "100"
    assert store.runtime_value("operator_planned_resume_state") == "PENDING"


def test_operator_planned_resume_supersedes_stalled_side_effect_free_resume(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="old-release",
        reason="OLD_RELEASE",
        armed_at_ms=1,
    )

    result = store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="replacement-release",
        reason="REPLACEMENT_RELEASE",
        armed_at_ms=2,
    )

    assert result == {
        "resume_from_block": 100,
        "change_id": "replacement-release",
        "reason": "REPLACEMENT_RELEASE",
        "armed_at_ms": 2,
        "idempotent": False,
        "superseded_change_id": "old-release",
    }
    assert store.runtime_value("operator_planned_resume_change_id") == (
        "replacement-release"
    )
    with store.connect() as connection:
        receipt = connection.execute(
            """
            SELECT previous_value, new_value, reason, details_json
            FROM config_change_receipts
            WHERE config_key = 'operator_planned_resume'
            """
        ).fetchone()
    assert receipt["previous_value"] == "old-release"
    assert receipt["new_value"] == "replacement-release"
    assert receipt["reason"] == "SUPERSEDE_STALLED_NO_SIDE_EFFECT_OPERATOR_RESUME"
    details = json.loads(receipt["details_json"])
    assert details["resume_from_block"] == 100
    assert details["prior_armed_at_ms"] == 1


def test_operator_planned_resume_does_not_supersede_after_a_submission_attempt(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="old-release",
        reason="OLD_RELEASE",
        armed_at_ms=1,
    )
    source = replace(action(), block_number=100)
    store.record_action_receipt(source)
    store.append_transition(source=source, status="FILLED")
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO submission_attempts(
                attempt_id, action_id, attempt_number, state,
                requested_quantity, snapshot_json, response_json,
                created_at_ms, updated_at_ms
            ) VALUES('attempt-after-arm', ?, 1, 'FILLED', '1', '{}', '{}', 2, 2)
            """,
            (source.action_id,),
        )

    with pytest.raises(
        LiveConfigurationError,
        match="OPERATOR_PLANNED_RESUME_ALREADY_ARMED_WITH_SIDE_EFFECTS",
    ):
        store.arm_planned_operator_resume(
            resume_from_block=100,
            change_id="replacement-release",
            reason="REPLACEMENT_RELEASE",
            armed_at_ms=3,
        )


def test_operator_planned_resume_accepts_immutable_error_terminal(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    source = action()
    assert store.record_action_receipt(source) is True
    store.append_transition(source=source, status="OBSERVED")
    store.append_transition(
        source=source,
        status="ERROR",
        reason="BOOK_SNAPSHOT_ERROR: external transport unavailable",
    )

    result = store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="shared-wallet-lock-release",
        reason="SHARED_WALLET_LOCK_RELEASE_SWITCH",
        armed_at_ms=2,
    )

    assert result["resume_from_block"] == 100
    assert store.runtime_value("operator_planned_resume_state") == "PENDING"


def test_operator_planned_resume_preserves_pre_release_retryable_for_recovery(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    source = action()
    assert store.record_action_receipt(source) is True
    store.append_transition(
        source=source,
        status="PENDING_MINIMUM_REMAINDER",
        reason="REMAINING_QUANTITY_BELOW_CURRENT_MARKET_MINIMUM",
    )
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_MINIMUM_REMAINDER",
        reason="REMAINING_QUANTITY_BELOW_CURRENT_MARKET_MINIMUM",
        updated_at_ms=1,
    )

    result = store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="safe-retryable-release",
        reason="FORWARD_ONLY_RELEASE",
        armed_at_ms=2,
    )

    assert result["resume_from_block"] == 100
    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_MINIMUM_REMAINDER"
    )
    assert source.action_id in {
        pending.action_id for pending in store.retryable_actions()
    }


def test_counterparty_receipts_cannot_reenter_retry_or_fidelity_denominator(
    tmp_path: Path,
):
    """Legacy counterparty rows stay immutable but are never source actions."""

    store = LiveStore(tmp_path / "live.sqlite3")
    maker = action(marker="a")
    counterparty = replace(action(marker="b"), source_role="taker")
    metadata = {
        "condition_id": "0x" + "1" * 64,
        "market_slug": "high-temperature-in-paris-on-august-8",
        "event_slug": "temperature-in-paris-on-august-8",
    }
    for source in (maker, counterparty):
        store.record_action_receipt(source)
        store.freeze_action_metadata(
            source=source,
            metadata=metadata,
            profile_follow=True,
            profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
            frozen_at_ms=source.discovered_at_ms,
        )
        store.ensure_action_target(
            source=source,
            proportional_quantity=D("4"),
            target_quantity=D("4"),
            state="PENDING_EXTERNAL_RETRY",
            reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
            updated_at_ms=source.discovered_at_ms,
        )
        store.append_transition(
            source=source,
            status="PENDING_EXTERNAL_RETRY",
            reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
        )

    assert [item.action_id for item in store.retryable_actions()] == [
        maker.action_id
    ]
    summary = store.action_fidelity_summary()
    assert summary["total_action_receipts"] == 2
    assert summary["source_maker_action_receipts"] == 1
    assert summary["legacy_nonmaker_receipt_count"] == 1
    assert summary["legacy_nonmaker_metadata_count"] == 1
    assert summary["legacy_nonmaker_target_count"] == 1
    assert summary["profile_eligible_observed"] == 1
    assert summary["frozen_target_count"] == 1
    assert summary["pending"] == 1
    assert summary["accounted"] == 1
    assert summary["conservation_passed"] is True
    assert store.decision_unit_summary() == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 1,
            "filled": 0,
            "partial": 0,
            "pending": 1,
            "external_or_causal": 0,
            "internal_error": 0,
        }
    ]


def test_verified_public_wallet_actions_join_retry_and_fidelity_without_readding_counterparties(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    maker = action(marker="m")
    public = replace(
        action(marker="p"),
        source_role=live.SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
        order_hash="public:" + "p" * 64,
    )
    counterparty = replace(action(marker="t"), source_role="taker")
    metadata = {
        "condition_id": "0x" + "1" * 64,
        "market_slug": "high-temperature-in-paris-on-august-8",
        "event_slug": "temperature-in-paris-on-august-8",
    }
    for source in (maker, public, counterparty):
        store.record_action_receipt(source)
        store.freeze_action_metadata(
            source=source,
            metadata=metadata,
            profile_follow=True,
            profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
            frozen_at_ms=source.discovered_at_ms,
        )
        store.ensure_action_target(
            source=source,
            proportional_quantity=D("4"),
            target_quantity=D("4"),
            state="PENDING_EXTERNAL_RETRY",
            reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
            updated_at_ms=source.discovered_at_ms,
        )
        store.append_transition(
            source=source,
            status="PENDING_EXTERNAL_RETRY",
            reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
        )

    assert {item.action_id for item in store.retryable_actions()} == {
        maker.action_id,
        public.action_id,
    }
    summary = store.action_fidelity_summary()
    assert summary["source_maker_action_receipts"] == 1
    assert summary["source_verified_public_wallet_action_receipts"] == 1
    assert summary["followable_source_action_receipts"] == 2
    assert summary["legacy_nonmaker_receipt_count"] == 1
    assert summary["profile_eligible_observed"] == 2
    assert summary["frozen_target_count"] == 2
    assert summary["pending"] == 2
    assert summary["accounted"] == 2
    assert summary["conservation_passed"] is True
    assert store.decision_unit_summary() == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 2,
            "filled": 0,
            "partial": 0,
            "pending": 2,
            "external_or_causal": 0,
            "internal_error": 0,
        }
    ]


def test_direct_counterparty_execution_fails_closed_before_book_or_ledger_write(
    tmp_path: Path,
):
    """A caller cannot bypass source decoding to create a counterparty order."""

    store = LiveStore(tmp_path / "live.sqlite3")
    counterparty = replace(action(), source_role="taker")

    class SnapshotMustNotRun(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("counterparty action reached the book reader")

    with pytest.raises(
        LiveConfigurationError,
        match="COUNTERPARTY_ORDER_LOG_NOT_SOURCE_ACTION",
    ):
        execute_source_action(
            store=store,
            source=counterparty,
            execution=SnapshotMustNotRun(),
            allocated_cash=D("100"),
            live_enabled=True,
        )

    assert store.latest_transition(counterparty) is None
    assert store.submission_attempt_count(counterparty.action_id) == 0

    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_100,
    )
    with pytest.raises(
        LiveConfigurationError,
        match="COUNTERPARTY_ORDER_LOG_NOT_SOURCE_ACTION",
    ):
        follower._process_source_action(
            action=counterparty,
            execution=SnapshotMustNotRun(),
            live_enabled=True,
        )

    assert store.latest_transition(counterparty) is None
    assert store.submission_attempt_count(counterparty.action_id) == 0


def test_verified_public_wallet_source_action_is_admissible_but_still_requires_live_enablement(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    public_source = replace(
        action(marker="p"),
        source_role=live.SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
        order_hash="public:" + "p" * 64,
    )

    with pytest.raises(LiveDisabledError):
        execute_source_action(
            store=store,
            source=public_source,
            execution=FakeExecution(),
            allocated_cash=D("100"),
            live_enabled=False,
        )


def test_late_verified_public_wallet_action_never_reads_a_current_book(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    public_source = replace(
        action(marker="q"),
        source_role=live.SOURCE_ROLE_VERIFIED_PUBLIC_WALLET,
        order_hash="public:" + "q" * 64,
    )
    store.constrain_action_to_no_action_time_book(
        source=public_source,
        reason="PUBLIC_WALLET_ACTION_DISCOVERED_AFTER_CHAIN_CURSOR_NO_ACTION_TIME_BOOK",
        created_at_ms=2,
        details={"public_source_row_ids": ["row"]},
    )

    class SnapshotMustNotRun(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("late public action reached the current book")

    result = execute_source_action(
        store=store,
        source=public_source,
        execution=SnapshotMustNotRun(),
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result["terminal_status"] == "EXTERNAL_UNFILLABLE"
    assert result["reason"] == (
        "PUBLIC_WALLET_ACTION_DISCOVERED_AFTER_CHAIN_CURSOR_NO_ACTION_TIME_BOOK"
    )


def test_counterparty_unknown_submission_never_reaches_authenticated_reconciliation(
    tmp_path: Path,
):
    """Existing non-maker attempts remain auditable but are not reconciled."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    counterparty = replace(action(marker="c"), source_role="taker")
    assert store.record_action_receipt(counterparty) is True
    store.ensure_action_target(
        source=counterparty,
        proportional_quantity=D("4"),
        target_quantity=D("4"),
        state="READY",
        reason="LEGACY_COUNTERPARTY_AUDIT_ONLY",
        updated_at_ms=counterparty.discovered_at_ms,
    )
    attempt = _begin_test_submission_attempt(
        store=store,
        source=counterparty,
        requested_quantity=D("4"),
        snapshot={"best_price": "0.40"},
        created_at_ms=counterparty.discovered_at_ms,
    )
    store.set_attempt_order_id(
        attempt_id=str(attempt["attempt_id"]),
        order_id="legacy-counterparty-order",
        response={"orderID": "legacy-counterparty-order"},
        updated_at_ms=counterparty.discovered_at_ms,
    )
    store.append_transition(
        source=counterparty,
        status="SUBMITTED_UNRECONCILED",
        reason="LEGACY_COUNTERPARTY_AUDIT_ONLY",
        details={"plan": {"requested_quantity": "4"}},
    )
    before_account = store.account_snapshot()

    class AuthenticatedReaderMustNotRun:
        def get_order(self, order_id: str):
            raise AssertionError(f"counterparty order was polled: {order_id}")

    assert reconcile_submitted_actions(
        store=store,
        execution=AuthenticatedReaderMustNotRun(),
    ) == []
    assert store.account_snapshot() == before_account
    assert store.latest_transition(counterparty)["terminal_status"] == (
        "SUBMITTED_UNRECONCILED"
    )
    assert store.unreconciled_submissions() == []


def test_operator_planned_resume_accepts_observed_action_without_submission_attempt(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    source = replace(action(), block_number=99)
    assert store.record_action_receipt(source) is True
    store.append_transition(source=source, status="OBSERVED")

    result = store.arm_planned_operator_resume(
        resume_from_block=100,
        change_id="recover-observed-release",
        reason="OBSERVED_ACTION_HAS_NO_SUBMISSION_SIDE_EFFECT",
        armed_at_ms=2,
    )

    assert result["resume_from_block"] == 100
    assert store.latest_transition(source) == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "PRE_RELEASE_ACTION_NOT_REPLAYED_FORWARD_ONLY",
        "created_at_ms": 2,
        "details": {
            "effective_after_block": 100,
            "historical_action_executed": False,
            "operator_resume_change_id": "recover-observed-release",
            "prior_status": "OBSERVED",
        },
    }


def test_operator_planned_resume_blocks_observed_action_with_submission_attempt(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    source = replace(action(), block_number=99)
    assert store.record_action_receipt(source) is True
    store.append_transition(source=source, status="OBSERVED")
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO submission_attempts(
                attempt_id, action_id, attempt_number, state,
                requested_quantity, snapshot_json, response_json,
                created_at_ms, updated_at_ms
            ) VALUES('attempt-1', ?, 1, 'SUBMIT_STARTED', '1', '{}', '{}', 1, 1)
            """,
            (source.action_id,),
        )

    with pytest.raises(
        LiveConfigurationError,
        match="NONTERMINAL_ACTIONS_BLOCK_OPERATOR_PLANNED_RESUME:OBSERVED",
    ):
        store.arm_planned_operator_resume(
            resume_from_block=100,
            change_id="unsafe-observed-release",
            reason="OBSERVED_WITH_ATTEMPT_MUST_RECONCILE",
            armed_at_ms=2,
        )


def test_operator_planned_resume_still_blocks_unknown_submission_without_reservation(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    source = action()
    assert store.record_action_receipt(source) is True
    store.append_transition(
        source=source,
        status="UNKNOWN_SUBMISSION",
        reason="SUBMISSION_TRANSPORT_UNKNOWN:TimeoutError",
    )

    with pytest.raises(
        LiveConfigurationError,
        match="NONTERMINAL_ACTIONS_BLOCK_OPERATOR_PLANNED_RESUME",
    ):
        store.arm_planned_operator_resume(
            resume_from_block=100,
            change_id="unsafe-unknown-release",
            reason="UNKNOWN_MUST_RECONCILE",
            armed_at_ms=2,
        )


def test_source_actions_use_the_scale_version_of_their_chain_block(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.rebase_fixed_share_scale(
        multiplier=D("2"),
        change_id="cd90-20260806-user-authorized-200pct",
        effective_after_block=100,
        resume_from_block=100,
        requested_at_ms=2,
    )
    execution = FakeExecution()
    old_block_action = action(quantity="40", marker="1")
    new_block_action = action(quantity="40", marker="2")
    old_block_action = replace(old_block_action, block_number=100)
    new_block_action = replace(new_block_action, block_number=101)

    execute_source_action(
        store=store,
        source=old_block_action,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("10"),
        "notional_usd": D("4"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [{"transaction_hash": "0x" + "1" * 64}],
    }
    reconcile_submitted_actions(store=store, execution=execution)
    execution.response = {"success": True, "orderID": "order-2"}
    execute_source_action(
        store=store,
        source=new_block_action,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert [call["size"] for call in execution.calls] == [D("10"), D("20")]


def test_source_open_position_value_accepts_the_official_single_item_list_shape():
    assert parse_source_open_position_value([{"user": "public-source", "value": "410.4341"}]) == D(
        "410.4341"
    )


def test_existing_locked_scale_still_initializes_the_cash_account_if_a_prior_boot_was_interrupted(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize()
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO config(key, value) VALUES('fixed_share_scale', '0.25')"
        )
        connection.execute(
            "INSERT INTO config(key, value) VALUES('allocation_usd', '100')"
        )

    resumed = initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("800"),
        observed_at_ms=2,
    )

    assert resumed == D("0.25")
    assert store.account_snapshot()["cash_usd"] == D("100")


def test_disabled_live_mode_never_submits_an_order(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()

    with pytest.raises(LiveDisabledError):
        execute_source_action(
            store=store,
            source=action(),
            execution=execution,
            allocated_cash=D("100"),
            live_enabled=False,
        )

    assert execution.calls == []
    assert _action_transition_count(store) == 0


def test_exact_proportional_size_posts_fak_without_rounding_and_reserves_notional(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()

    result = execute_source_action(
        store=store,
        source=action(quantity="40"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "BUY",
            "price": D("0.40"),
            "size": D("10"),
            "user_usdc_balance": D("100"),
        }
    ]
    # Formula-derived: 100 cash - (10 shares * 0.40 limit) = 96 available
    # while the FAK receipt remains unreconciled.
    assert _available_cash_usd(store) == D("96")
    assert store.action_receipt_count() == 1


def test_buy_requires_an_authenticated_collateral_reader(tmp_path: Path):
    """A strategy ledger must never substitute for the physical account."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    execution.collateral_balance_usd = None
    source = action(quantity="40")

    with pytest.raises(
        LiveConfigurationError,
        match="MISSING_AUTHENTICATED_COLLATERAL_READER",
    ):
        execute_source_action(
            store=store,
            source=source,
            execution=execution,
            allocated_cash=D("100"),
            live_enabled=True,
        )

    assert execution.calls == []
    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_INTERNAL_INVARIANT"
    )


def test_below_minimum_scaled_size_is_recorded_without_a_new_submission(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()

    result = execute_source_action(
        store=store,
        source=action(quantity="10"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SKIPPED",
        "reason": "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
    }
    assert execution.calls == []


def test_cd90_skip_policy_never_submits_a_below_minimum_scaled_action(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    execution = FakeExecution()

    result = execute_source_action(
        store=store,
        source=action(quantity="10"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SKIPPED",
        "reason": "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
    }
    assert execution.calls == []
    latest = store.latest_transition(action(quantity="10"))
    assert latest["terminal_status"] == "SKIPPED"
    assert latest["reason"] == "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM"


def test_cd90_skip_policy_never_upsizes_a_sell_even_with_available_inventory(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO positions(token_id, quantity, cost_basis_usd) "
            "VALUES('123', '100', '40')"
        )
    execution = FakeExecution()
    source = action(side="SELL", quantity="10")

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SKIPPED",
        "reason": "PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
    }
    assert execution.calls == []
    assert store.position_quantity("123") == D("100")


def test_shallow_buy_book_still_submits_a_minimum_fak_attempt(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = ShallowBookExecution()

    result = execute_source_action(
        store=store,
        source=action(quantity="20"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SUBMITTED_UNRECONCILED",
        "reason": "FAK_PARTIAL_ATTEMPT",
    }
    assert execution.calls[0]["size"] == D("5")


def _next_causal_action(
    *, side: str = "BUY", quantity: str = "4", marker: str, block: int
) -> SourceAction:
    return replace(
        action(side=side, quantity=quantity, marker=marker),
        block_number=block,
        log_index=1,
        source_timestamp=1_700_000_000 + block,
        discovered_at_ms=(1_700_000_000 + block) * 1000,
    )


def _fill_first_minimum_buy(
    *, store: LiveStore, execution: FakeExecution, source: SourceAction
) -> None:
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("5"),
        "notional_usd": D("2"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [{"transaction_hash": "0xminimum-fill"}],
    }
    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT",
        }
    ]


def test_confirmed_minimum_buy_surplus_covers_later_same_token_buy(tmp_path: Path):
    """One five-share minimum must not be repeated for every BUY fragment."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    execution = FakeExecution()
    first = _next_causal_action(marker="1", block=100)
    second = _next_causal_action(marker="4", block=101)
    _fill_first_minimum_buy(store=store, execution=execution, source=first)

    result = execute_source_action(
        store=store,
        source=second,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SKIPPED",
        "reason": "PRIOR_MINIMUM_UPSCALE_COVERS_PROPORTIONAL_BUY",
    }
    assert len(execution.calls) == 1
    assert store.position_quantity("123") == D("5")
    target = store.action_target(second.action_id)
    assert target["proportional_quantity"] == D("1")
    assert target["state"] == "SKIPPED"


def test_future_source_sell_unwinds_scaled_sell_plus_confirmed_minimum_surplus(
    tmp_path: Path,
):
    """A future SELL is the only forward trigger allowed to remove old surplus."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    execution = FakeExecution()
    first = _next_causal_action(marker="1", block=100)
    source_sell = _next_causal_action(
        side="SELL", marker="5", block=101
    )
    _fill_first_minimum_buy(store=store, execution=execution, source=first)
    execution.response = {"success": True, "orderID": "order-2"}

    result = execute_source_action(
        store=store,
        source=source_sell,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert execution.calls[-1] == {
        "token_id": "123",
        "side": "SELL",
        "price": D("0.30"),
        "size": D("5"),
    }
    target = store.action_target(source_sell.action_id)
    assert target["proportional_quantity"] == D("1")
    assert target["target_quantity"] == D("5")


def test_later_same_token_action_waits_for_prior_unknown_fill_before_sizing(
    tmp_path: Path,
):
    """An active same-token attempt cannot be counted as confirmed surplus."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    execution = FakeExecution()
    first = _next_causal_action(marker="1", block=100)
    second = _next_causal_action(marker="6", block=101)
    execute_source_action(
        store=store,
        source=first,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    waiting = execute_source_action(
        store=store,
        source=second,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert waiting == {
        "terminal_status": "PENDING_CAUSAL_ORDER",
        "reason": "PRIOR_SAME_TOKEN_SUBMISSION_UNRESOLVED",
    }
    assert len(execution.calls) == 1
    assert store.action_target(second.action_id) is None

def test_later_same_token_action_waits_for_prior_metadata_before_sizing(
    tmp_path: Path,
):
    """A later BUY cannot size before an earlier chain action has a target."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.lock_config_once(
        "minimum_size_policy", live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    )
    execution = FakeExecution()
    earlier = _next_causal_action(marker="metadata-first", block=100)
    later = _next_causal_action(marker="metadata-later", block=101)
    store.record_action_receipt(earlier)
    store.append_transition(
        source=earlier,
        status="PENDING_METADATA",
        reason="TEMPORARY_GAMMA_UNAVAILABLE",
    )

    waiting = execute_source_action(
        store=store,
        source=later,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert waiting == {
        "terminal_status": "PENDING_CAUSAL_ORDER",
        "reason": "PRIOR_SAME_TOKEN_ACTION_NOT_TERMINAL",
    }
    assert execution.calls == []
    assert store.action_target(later.action_id) is None
    latest = store.latest_transition(later)
    assert latest["details"]["prior_action"] == {
        "action_id": earlier.action_id,
        "side": "BUY",
        "block_number": 100,
        "source_log_index": 1,
        "latest_status": "PENDING_METADATA",
        "target_state": "",
    }

def test_unknown_submission_is_not_reposted_after_restart_or_duplicate_delivery(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution(error=TimeoutError("network uncertain after post"))

    first = execute_source_action(
        store=store,
        source=action(),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    second = execute_source_action(
        store=store,
        source=action(),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert first["terminal_status"] == "UNKNOWN_SUBMISSION"
    assert second["terminal_status"] == "UNKNOWN_SUBMISSION"
    assert len(execution.calls) == 1


def test_unknown_submission_exception_text_cannot_persist_signature_material(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    secret = "0x" + "ab" * 65
    execution = FakePreparedExecution(
        error=TimeoutError(f"signature={secret}")
    )
    source = action()

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "UNKNOWN_SUBMISSION",
        "reason": "SUBMISSION_TRANSPORT_UNKNOWN:TimeoutError",
    }
    with store.connect() as connection:
        persisted = "\n".join(
            str(value)
            for row in connection.execute(
                """
                SELECT response_json AS value FROM submission_attempts
                UNION ALL
                SELECT reason AS value FROM action_targets
                UNION ALL
                SELECT reason AS value FROM action_transitions
                UNION ALL
                SELECT details_json AS value FROM action_transitions
                """
            ).fetchall()
            for value in row
        )
    assert secret not in persisted
    assert "signature=" not in persisted.lower()


def test_unknown_prepared_submission_keeps_order_hash_and_reconciles_fill(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakePreparedExecution(
        error=TimeoutError("network uncertain after post")
    )
    source = action()

    submitted = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert submitted["terminal_status"] == "UNKNOWN_SUBMISSION"
    with store.connect() as connection:
        attempt = connection.execute(
            """
            SELECT order_id, state, prepared_order_json
            FROM submission_attempts
            WHERE action_id = ?
            """,
            (source.action_id,),
        ).fetchone()
    assert attempt["order_id"] == execution.prepared["order_id"]
    assert attempt["state"] == "UNKNOWN_SUBMISSION"
    persisted_prepared = json.loads(attempt["prepared_order_json"])
    assert persisted_prepared == {
        key: value
        for key, value in execution.prepared.items()
        if not key.startswith("_")
    }
    assert "0xsecret-signature" not in attempt["prepared_order_json"]
    assert len(execution.prepare_calls) == 1
    assert len(execution.prepared_submit_calls) == 1

    execution.error = None
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("4"),
        vwap_price=D("0.40"),
    )
    reconciled = reconcile_submitted_actions(store=store, execution=execution)

    assert reconciled == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT",
        }
    ]
    assert store.position_quantity("123") == D("10")
    assert len(execution.prepare_calls) == 1
    assert len(execution.prepared_submit_calls) == 1


def test_reconciled_order_response_cannot_persist_signature_material(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakePreparedExecution(
        error=TimeoutError("network uncertain after post")
    )
    source = action()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    secret = "0x" + "cd" * 65
    execution.error = None
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("4"),
        vwap_price=D("0.40"),
    )

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT",
        }
    ]
    with store.connect() as connection:
        persisted = "\n".join(
            str(value)
            for row in connection.execute(
                """
                SELECT response_json AS value FROM submission_attempts
                UNION ALL
                SELECT details_json AS value FROM action_transitions
                """
            ).fetchall()
            for value in row
        )
    assert secret not in persisted
    assert "signature" not in persisted.lower()
    assert "signedorder" not in persisted.lower()
    assert store.position_quantity("123") == D("10")
    assert len(execution.prepare_calls) == 1
    assert len(execution.prepared_submit_calls) == 1


def test_unknown_prepared_submission_is_read_only_reconciled_without_repost(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakePreparedExecution(
        error=TimeoutError("network uncertain after post")
    )
    source = action()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.error = None

    first = reconcile_submitted_actions(store=store, execution=execution)
    second = reconcile_submitted_actions(store=store, execution=execution)
    assert first == [
        {"terminal_status": "PENDING", "reason": "ORDER_RECONCILIATION_UNAVAILABLE"}
    ]
    assert second == [
        {"terminal_status": "PENDING", "reason": "ORDER_RECONCILIATION_UNAVAILABLE"}
    ]
    assert len(execution.prepare_calls) == 1
    assert execution.prepared_submit_calls == [execution.prepared]
    assert store.submission_attempt_count(source.action_id) == 1
    assert store.latest_transition(source)["terminal_status"] == "UNKNOWN_SUBMISSION"
    with store.connect() as connection:
        error_count = connection.execute(
            """
            SELECT COUNT(*) AS count FROM runtime_errors
            WHERE category = 'EXTERNAL_ORDER_RECONCILIATION'
            """
        ).fetchone()["count"]
    assert error_count == 1


def test_absent_official_order_after_accepted_submission_stays_unknown_without_repost(
    tmp_path: Path,
):
    """A missing CLOB order is not proof of a zero fill.

    The original accepted response can be delayed while the order endpoint has
    already dropped the record.  Keep the cash reservation and reconcile-only
    state rather than treating a ``None`` response as an internal error or
    retrying the order.
    """

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.orders["order-1"] = None

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {"terminal_status": "UNKNOWN_SUBMISSION", "reason": "OFFICIAL_ORDER_NOT_FOUND"}
    ]
    assert store.latest_transition(source)["terminal_status"] == "UNKNOWN_SUBMISSION"
    assert store.position_quantity("123") == D("0")
    assert store.submission_attempt_count(source.action_id) == 1
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
        attempt = connection.execute(
            "SELECT state FROM submission_attempts WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
    assert int(reservation["active"]) == 1
    assert attempt["state"] == "UNKNOWN_SUBMISSION"
    assert len(execution.calls) == 1


def test_finalized_chain_zero_fill_releases_unknown_without_repost(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: None
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 1,
        "finality": "polygon_finalized_block",
    }
    execution.get_order = lambda _order_id: (_ for _ in ()).throw(
        AssertionError("finalized zero-fill proof must not query or repost")
    )

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL",
        }
    ]
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
        attempt = connection.execute(
            "SELECT state FROM submission_attempts WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
    assert int(reservation["active"]) == 0
    assert attempt["state"] == "NO_FILL"
    assert len(execution.calls) == 1


def test_unavailable_transaction_receipt_falls_back_to_finalized_order_hash_scan(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: (_ for _ in ()).throw(
        TimeoutError("transaction receipt temporarily unavailable")
    )
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 1,
        "finality": "polygon_finalized_block",
    }
    execution.get_order = lambda _order_id: (_ for _ in ()).throw(
        AssertionError("finalized zero-fill proof must not query or repost")
    )

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL",
        }
    ]
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
        attempt = connection.execute(
            "SELECT state FROM submission_attempts WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
    assert int(reservation["active"]) == 0
    assert attempt["state"] == "NO_FILL"
    assert len(execution.calls) == 1


def test_success_response_without_any_order_id_remains_unknown_without_crashing(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution(response={"success": True})
    source = action()

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "UNKNOWN_SUBMISSION",
        "reason": "MISSING_ORDER_ID_IN_SUBMISSION_RESPONSE",
    }
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "UNKNOWN_SUBMISSION"
    assert latest["reason"] == "MISSING_ORDER_ID_IN_SUBMISSION_RESPONSE"
    assert store.submission_attempt_count(source.action_id) == 1

    execution.get_order_calls = []

    def tracked_get_order(order_id: str):
        execution.get_order_calls.append(order_id)
        raise AssertionError("missing order ID must not reach the external API")

    execution.get_order = tracked_get_order
    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "UNKNOWN_SUBMISSION",
            "reason": "MISSING_ORDER_ID_FOR_RECONCILIATION",
        }
    ]
    assert execution.get_order_calls == []


def test_prepared_response_order_id_mismatch_preserves_predicted_hash_as_unknown(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakePreparedExecution(
        response={"success": True, "orderID": "0x" + "8" * 64}
    )
    source = action()

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "UNKNOWN_SUBMISSION",
        "reason": "SUBMISSION_ORDER_ID_MISMATCH",
    }
    with store.connect() as connection:
        attempt = connection.execute(
            "SELECT order_id, state FROM submission_attempts WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
    assert attempt["order_id"] == execution.prepared["order_id"]
    assert attempt["state"] == "UNKNOWN_SUBMISSION"
    assert len(execution.prepared_submit_calls) == 1


@pytest.mark.parametrize(
    "response",
    [
        {
            "success": False,
            "errorMsg": "no orders found to match with FAK order",
        },
        {
            "success": False,
            "errorMsg": "no orders found to match with FAK order",
            "orderID": "0x" + "8" * 64,
        },
    ],
)
def test_prepared_rejection_without_matching_order_id_remains_unknown(
    tmp_path: Path,
    response: dict,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakePreparedExecution(response=response)
    source = action()

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result["terminal_status"] == "UNKNOWN_SUBMISSION"
    assert result["reason"] in {
        "MISSING_RESPONSE_ORDER_ID_FOR_PREPARED_SUBMISSION",
        "SUBMISSION_ORDER_ID_MISMATCH",
    }
    assert store.action_target(source.action_id)["state"] == "UNKNOWN_SUBMISSION"
    assert store.submission_attempt_count(source.action_id) == 1
    assert len(execution.prepared_submit_calls) == 1


def test_prepared_success_without_response_order_id_stays_unknown_for_read_only_reconcile(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakePreparedExecution(response={"success": True})
    source = action()

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "UNKNOWN_SUBMISSION",
        "reason": "MISSING_RESPONSE_ORDER_ID_FOR_PREPARED_SUBMISSION",
    }
    with store.connect() as connection:
        attempt = connection.execute(
            "SELECT order_id, state FROM submission_attempts WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
    assert attempt["order_id"] == execution.prepared["order_id"]
    assert attempt["state"] == "UNKNOWN_SUBMISSION"
    assert len(execution.prepared_submit_calls) == 1


def test_prepared_submission_response_cannot_persist_echoed_signature_material(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    secret = "0xechoed-signature-must-not-enter-sqlite"
    api_key = "api-key-must-not-enter-sqlite"
    authorization = "Bearer must-not-enter-sqlite"
    access_token = "access-token-must-not-enter-sqlite"
    execution = FakePreparedExecution()
    execution.response = {
        "success": True,
        "orderID": execution.prepared["order_id"],
        "signature": secret,
        "apiKey": api_key,
        "nested": {
            "signedOrder": {"signature": secret},
            "Authorization": authorization,
            "access_token": access_token,
        },
    }
    source = action()

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result["terminal_status"] == "SUBMITTED_UNRECONCILED"
    with store.connect() as connection:
        attempt_json = connection.execute(
            "SELECT response_json FROM submission_attempts WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()["response_json"]
        transition_json = connection.execute(
            """
            SELECT details_json FROM action_transitions
            WHERE action_id = ? ORDER BY id DESC LIMIT 1
            """,
            (source.action_id,),
        ).fetchone()["details_json"]
    assert secret not in attempt_json
    assert secret not in transition_json
    assert api_key not in attempt_json
    assert authorization not in attempt_json
    assert access_token not in transition_json
    assert "signature" not in attempt_json.lower()
    assert "signedorder" not in transition_json.lower()
    assert "authorization" not in transition_json.lower()
    assert "accesstoken" not in transition_json.replace("_", "").lower()


def test_submitted_buy_reserves_the_live_sleeve_cash_before_reconciliation(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("4"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    class AnyTokenExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            return {
                "minimum_order_size": "5",
                "minimum_marketable_buy_notional_usd": "1",
                "best_price": "0.40" if side == "BUY" else "0.30",
                "tick_size": "0.01",
                "visible_best_level_size": "100",
                "fee_bps": "0",
                "raw_book": {"asks": [], "bids": []},
            }

    execution = AnyTokenExecution(collateral="4")

    first = execute_source_action(
        store=store,
        source=action(quantity="1000", marker="1"),
        execution=execution,
        allocated_cash=D("4"),
        live_enabled=True,
    )
    second = execute_source_action(
        store=store,
        source=replace(
            action(quantity="1000", marker="2"), token_id="456"
        ),
        execution=execution,
        allocated_cash=D("4"),
        live_enabled=True,
    )

    assert first["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert second["terminal_status"] == "PENDING_CAPITAL"
    assert second["reason"] == "INSUFFICIENT_AVAILABLE_CASH"
    assert _available_cash_usd(store) == D("0")
    assert len(execution.calls) == 1


def test_reconciled_fok_fill_updates_cash_and_local_inventory_once(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("4"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="1000")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("4"),
        live_enabled=True,
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("4"),
        vwap_price=D("0.40"),
    )

    results = reconcile_submitted_actions(store=store, execution=execution)

    assert results == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT",
        }
    ]
    assert store.position_quantity("123") == D("10")
    assert _available_cash_usd(store) == D("0")
    assert store.latest_transition(source)["terminal_status"] == "FILLED"
    assert store.latest_transition(source)["details"]["receipt_evidence"] == [
        {"transaction_hash": "0x" + "1" * 64}
    ]


def test_fill_and_terminal_receipt_commit_atomically(tmp_path: Path):
    """A crash/failure at terminal receipt insertion must not double-book a fill."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="40")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("4"),
        vwap_price=D("0.40"),
    )
    with store.connect() as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_fill_terminal
            BEFORE INSERT ON action_transitions
            WHEN NEW.status IN ('FILLED', 'PARTIAL_PENDING')
            BEGIN
                SELECT RAISE(ABORT, 'injected terminal receipt failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected terminal receipt failure"):
        reconcile_submitted_actions(store=store, execution=execution)

    assert store.account_snapshot()["cash_usd"] == D("100")
    assert store.position_quantity("123") == D("0")
    assert store.latest_transition(source)["terminal_status"] == "SUBMITTED_UNRECONCILED"
    with store.connect() as connection:
        active = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
        attempt = connection.execute(
            """
            SELECT state FROM submission_attempts
            WHERE action_id = ? ORDER BY attempt_number DESC LIMIT 1
            """,
            (source.action_id,),
        ).fetchone()
    assert int(active["active"]) == 1
    assert attempt["state"] == "SUBMITTED_UNRECONCILED"


def test_submission_attempt_start_rolls_back_reservation_if_audit_insert_fails(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(quantity="40")
    store.record_action_receipt(source)
    store.append_transition(source=source, status="OBSERVED")
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="READY",
        reason="",
        updated_at_ms=1,
    )
    plan = live.ActionPlan(
        terminal_status="READY",
        reason="",
        side="BUY",
        proportional_quantity=D("10"),
        requested_quantity=D("10"),
        order_amount_usd=D("4"),
        worst_price=D("0.40"),
        reserved_cash_usd=D("4"),
    )
    with store.connect() as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_submission_attempt_audit
            BEFORE INSERT ON submission_attempts
            BEGIN
                SELECT RAISE(ABORT, 'injected attempt audit failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected attempt audit failure"):
        store.begin_submission_attempt(
            source=source,
            plan=plan,
            snapshot={"best_price": "0.40"},
            condition_id="0x" + "1" * 64,
            created_at_ms=2,
            transition_details={"plan": "frozen"},
        )

    with store.connect() as connection:
        reservation_count = connection.execute(
            """
            SELECT COUNT(*) AS count FROM order_reservations
            WHERE action_id = ? AND active = 1
            """,
            (source.action_id,),
        ).fetchone()["count"]
    assert int(reservation_count) == 0
    assert store.action_target(source.action_id)["state"] == "READY"
    assert store.latest_transition(source)["terminal_status"] == "OBSERVED"


def test_submission_attempt_persists_only_prepared_order_receipt_whitelist(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(quantity="40")
    store.record_action_receipt(source)
    store.append_transition(source=source, status="OBSERVED")
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="READY",
        reason="",
        updated_at_ms=1,
    )
    plan = live.ActionPlan(
        terminal_status="READY",
        reason="",
        side="BUY",
        proportional_quantity=D("10"),
        requested_quantity=D("10"),
        order_amount_usd=D("4"),
        worst_price=D("0.40"),
        reserved_cash_usd=D("4"),
    )
    secret = "0xmust-not-enter-sqlite"

    store.begin_submission_attempt(
        source=source,
        plan=plan,
        snapshot={"best_price": "0.40"},
        condition_id="0x" + "1" * 64,
        created_at_ms=2,
        transition_details={"plan": "frozen"},
        prepared_order={
            "order_id": "0x" + "9" * 64,
            "order_version": 2,
            "order_type": "FAK",
            "neg_risk": False,
            "order_fields": {"salt": "123", "makerAmount": "4000000"},
            "signed_order": SimpleNamespace(signature=secret),
            "signature": secret,
            "future_sdk_private_payload": secret,
        },
    )

    with store.connect() as connection:
        raw = connection.execute(
            "SELECT prepared_order_json FROM submission_attempts"
        ).fetchone()["prepared_order_json"]
    assert json.loads(raw) == {
        "order_fields": {"makerAmount": "4000000", "salt": "123"},
        "order_id": "0x" + "9" * 64,
        "order_type": "FAK",
        "order_version": 2,
        "neg_risk": False,
    }
    assert secret not in raw


def test_submission_attempt_rejects_signature_nested_in_unsigned_order_fields(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(quantity="40")
    store.record_action_receipt(source)
    store.append_transition(source=source, status="OBSERVED")
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="READY",
        reason="",
        updated_at_ms=1,
    )
    plan = live.ActionPlan(
        terminal_status="READY",
        reason="",
        side="BUY",
        proportional_quantity=D("10"),
        requested_quantity=D("10"),
        order_amount_usd=D("4"),
        worst_price=D("0.40"),
        reserved_cash_usd=D("4"),
    )

    with pytest.raises(
        LiveConfigurationError,
        match="PREPARED_ORDER_FIELDS_CONTAIN_SECRET_MATERIAL",
    ):
        store.begin_submission_attempt(
            source=source,
            plan=plan,
            snapshot={"best_price": "0.40"},
            condition_id="0x" + "1" * 64,
            created_at_ms=2,
            transition_details={"plan": "frozen"},
            prepared_order={
                "order_id": "0x" + "9" * 64,
                "order_version": 2,
                "order_type": "FAK",
                "neg_risk": False,
                "order_fields": {
                    "salt": "123",
                    "nested": {"signature": "0xmust-not-enter-sqlite"},
                },
            },
        )
    assert store.submission_attempt_count(source.action_id) == 0


def test_canceled_order_release_and_terminal_receipt_commit_atomically(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="40")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.orders["order-1"] = {
        "status": "ORDER_STATUS_CANCELED",
        "original_size": "10",
        "size_matched": "0",
        "price": "0.40",
    }
    with store.connect() as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_pending_terminal
            BEFORE INSERT ON action_transitions
            WHEN NEW.status = 'EXTERNAL_UNFILLABLE'
            BEGIN
                SELECT RAISE(ABORT, 'injected terminal receipt failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected terminal receipt failure"):
        reconcile_submitted_actions(store=store, execution=execution)

    assert store.latest_transition(source)["terminal_status"] == "SUBMITTED_UNRECONCILED"
    with store.connect() as connection:
        active = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id = ?",
            (source.action_id,),
        ).fetchone()
        attempt = connection.execute(
            """
            SELECT state FROM submission_attempts
            WHERE action_id = ? ORDER BY attempt_number DESC LIMIT 1
            """,
            (source.action_id,),
        ).fetchone()
    assert int(active["active"]) == 1
    assert attempt["state"] == "SUBMITTED_UNRECONCILED"


def test_reconciliation_uses_exact_onchain_fill_not_limit_price_or_requested_shares(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("10"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="420")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("10"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("10.5"),
        "notional_usd": D("4.2"),
        "fee_usd": D("0.06187"),
        "vwap_price": D("0.4"),
        "receipt_evidence": [{"fee_raw": "61870"}],
    }

    results = reconcile_submitted_actions(store=store, execution=execution)

    assert results == [{"terminal_status": "FILLED", "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT"}]
    assert store.position_quantity("123") == D("10.5")
    assert store.account_snapshot()["cash_usd"] == D("5.73813")
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "FILLED"
    assert latest["details"]["matched_notional_usd"] == "4.2"
    assert latest["details"]["fee_usd"] == "0.06187"


def test_buy_price_improvement_books_all_authoritative_shares_within_order_cash(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("10"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="200")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("10"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        # The BUY was submitted as 2 USD at a 0.40 worst price.  Price
        # improvement legitimately returns more than the 5-share target.
        "quantity": D("5.113635"),
        "notional_usd": D("1.999999"),
        "fee_usd": D("0"),
        "vwap_price": D("0.391111024"),
        "receipt_evidence": [{"transaction_hash": "0xprice-improved"}],
    }

    results = reconcile_submitted_actions(store=store, execution=execution)

    assert results == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_BUY_PRICE_IMPROVEMENT_FILL",
        }
    ]
    assert store.position_quantity("123") == D("5.113635")
    assert store.account_snapshot()["cash_usd"] == D("8.000001")
    target = store.action_target(source.action_id)
    assert target["state"] == "FILLED"
    assert target["cumulative_filled_quantity"] == D("5.113635")
    assert target["remaining_quantity"] == D("0")
    assert target["fill_surplus_quantity"] == D("0.113635")


def test_buy_cash_order_one_raw_unit_rounding_dust_is_complete_not_retried(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("10"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    class OneDollarBuyExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            payload = super().snapshot(token_id=token_id, side=side)
            payload["best_price"] = "0.14"
            return payload

    execution = OneDollarBuyExecution()
    source = action(quantity="400")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("10"),
        live_enabled=True,
    )
    planned_quantity = store.action_target(source.action_id)["target_quantity"]
    execution.authoritative_submission_execution = lambda **_kwargs: {
        # The exact 1.4 USD BUY is represented on-chain in six-decimal raw
        # units, leaving one raw collateral unit and a tiny share shortfall.
        "quantity": D("9.999993"),
        "notional_usd": D("1.399999"),
        "fee_usd": D("0"),
        "vwap_price": D("0.14"),
        "receipt_evidence": [{"transaction_hash": "0xrounding-dust"}],
    }

    results = reconcile_submitted_actions(store=store, execution=execution)

    assert results == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_BUY_CASH_ORDER_COMPLETE",
        }
    ]
    target = store.action_target(source.action_id)
    assert target["state"] == "FILLED"
    assert target["remaining_quantity"] == D("0")
    assert target["fill_shortfall_quantity"] == planned_quantity - D("9.999993")
    assert retry_pending_actions(
        store=store,
        execution=execution,
    ) == []


def test_buy_share_surplus_cannot_exceed_the_frozen_order_notional(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("10"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="200")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("10"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("5.1"),
        "notional_usd": D("2.01"),
        "fee_usd": D("0"),
        "vwap_price": D("0.394117647"),
        "receipt_evidence": [],
    }

    with pytest.raises(
        LiveConfigurationError,
        match="BUY_PRICE_IMPROVEMENT_EXCEEDS_FROZEN_ORDER_NOTIONAL",
    ):
        reconcile_submitted_actions(store=store, execution=execution)

    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.latest_transition(source)["terminal_status"] == (
        "SUBMITTED_UNRECONCILED"
    )


def test_sell_authoritative_fill_can_never_exceed_the_requested_shares(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _seed_local_fill(
        store=store,
        source=action(side="BUY", quantity="40", marker="8"),
        quantity=D("20"),
        price=D("0.40"),
        fee_usd=D("0"),
    )
    execution = FakeExecution()
    source = action(side="SELL", quantity="40", marker="9")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("10.1"),
        "notional_usd": D("3.03"),
        "fee_usd": D("0"),
        "vwap_price": D("0.30"),
        "receipt_evidence": [],
    }

    with pytest.raises(
        LiveConfigurationError,
        match="FILL_EXCEEDS_ACTION_TARGET_REMAINDER",
    ):
        reconcile_submitted_actions(store=store, execution=execution)

    assert store.position_quantity("123") == D("20")
    assert store.latest_transition(source)["terminal_status"] == (
        "SUBMITTED_UNRECONCILED"
    )


def test_authoritative_fak_partial_fill_below_original_minimum_is_terminal(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="40")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("6"),
        "notional_usd": D("2.4"),
        "fee_usd": D("0.024"),
        "vwap_price": D("0.4"),
        "receipt_evidence": [{"transaction_hash": "0xreceipt"}],
    }

    results = reconcile_submitted_actions(store=store, execution=execution)

    assert results == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM",
        }
    ]
    assert store.position_quantity("123") == D("6")
    assert _available_cash_usd(store) == D("97.576")
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "EXTERNAL_UNFILLABLE"
    assert latest["reason"] == "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM"
    assert latest["details"]["execution_order_type"] == "FAK"
    assert store.action_target(source.action_id)["remaining_quantity"] == D("4")
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }


@pytest.mark.parametrize(
    ("first_minimum", "retry_minimum", "expected_terminal"),
    [
        ("5", "1", "EXTERNAL_UNFILLABLE"),
        ("1", "5", "PARTIAL_PENDING"),
    ],
)
def test_multi_attempt_partial_remainder_uses_first_submitted_minimum(
    tmp_path: Path,
    first_minimum: str,
    retry_minimum: str,
    expected_terminal: str,
):
    class VariableMinimumExecution(FakeExecution):
        def __init__(self, minimum: str):
            super().__init__()
            self.minimum = minimum

        def snapshot(self, *, token_id: str, side: str):
            snapshot = super().snapshot(token_id=token_id, side=side)
            return {**snapshot, "minimum_order_size": self.minimum}

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = VariableMinimumExecution(first_minimum)
    source = action(quantity="40")

    assert execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    ) == {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    _set_authoritative_fill(
        execution=execution,
        quantity=D("4"),
        notional_usd=D("1.6"),
        vwap_price=D("0.4"),
    )
    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {"terminal_status": "PARTIAL_PENDING", "reason": "FAK_PARTIAL_FILL"}
    ]

    execution.minimum = retry_minimum
    execution.response = {"success": True, "orderID": "order-2"}
    assert execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    ) == {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    _set_authoritative_fill(
        execution=execution,
        quantity=D("2"),
        notional_usd=D("0.8"),
        vwap_price=D("0.4"),
    )

    result = reconcile_submitted_actions(store=store, execution=execution)

    assert result[0]["terminal_status"] == expected_terminal
    target = store.action_target(source.action_id)
    assert target is not None
    assert target["state"] == expected_terminal
    assert target["remaining_quantity"] == D("4")
    latest = store.latest_transition(source)
    assert latest is not None
    evidence = latest["details"]["partial_remainder_evidence"]
    if expected_terminal == "EXTERNAL_UNFILLABLE":
        assert result[0]["reason"] == (
            "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM"
        )
        assert evidence is not None
        assert evidence["original_minimum_order_size"] == first_minimum
        assert D(evidence["remaining_quantity"]) == D("4")
    else:
        assert result[0]["reason"] == "FAK_PARTIAL_FILL"
        assert evidence is None


@pytest.mark.parametrize(
    "retry_state", ["PARTIAL_PENDING", "PENDING_MINIMUM_REMAINDER"]
)
def test_post_boundary_partial_dust_is_finalized_from_its_recorded_snapshot(
    tmp_path: Path,
    retry_state: str,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="40")
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    _unreconciled_source, submitted = store.unreconciled_submissions()[0]
    store.apply_fill_and_finalize(
        source=source,
        quantity=D("6"),
        price=D("0.4"),
        notional_usd=D("2.4"),
        fee_usd=D("0.024"),
        terminal_status="PARTIAL_PENDING",
        reason="FAK_PARTIAL_FILL",
        created_at_ms=3,
        details={"attempt_id": submitted["attempt_id"]},
    )
    if retry_state != "PARTIAL_PENDING":
        store.set_action_target_state(
            source=source,
            state=retry_state,
            reason="REMAINING_QUANTITY_BELOW_CURRENT_MARKET_MINIMUM",
            updated_at_ms=4,
        )
        store.append_transition(
            source=source,
            status=retry_state,
            reason="REMAINING_QUANTITY_BELOW_CURRENT_MARKET_MINIMUM",
        )
    # A later closed retry observed a lower exchange minimum. The stale-dust
    # repair must still use the first submitted snapshot (five shares), not
    # this later retry snapshot (one share).
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO submission_attempts(
                attempt_id, action_id, attempt_number, order_id, state,
                requested_quantity, snapshot_json, response_json,
                created_at_ms, updated_at_ms
            ) VALUES(?, ?, 2, NULL, 'NO_FILL', ?, ?, '{}', 5, 5)
            """,
            (
                "second-attempt",
                source.action_id,
                "4",
                json.dumps({"minimum_order_size": "1"}, sort_keys=True),
            ),
        )
    before_account = store.account_snapshot()
    before_position = store.position_quantity("123")

    finalized = store.finalize_post_boundary_partial_dust(
        effective_after_block=99,
        finalized_at_ms=6,
    )

    assert finalized == [source.action_id]
    target = store.action_target(source.action_id)
    assert target["state"] == "EXTERNAL_UNFILLABLE"
    assert target["reason"] == "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM"
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "EXTERNAL_UNFILLABLE"
    assert latest["details"]["original_minimum_order_size"] == "5"
    assert D(latest["details"]["remaining_quantity"]) == D("4")
    assert store.account_snapshot() == before_account
    assert store.position_quantity("123") == before_position
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }


def test_authoritative_v2_onchain_receipt_uses_raw_notional_shares_and_fee():
    from eth_abi import encode
    from eth_utils import keccak

    order_id = "0x" + "4" * 64
    transaction_hash = "0x" + "5" * 64
    topic = "0x" + keccak(
        text="OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)"
    ).hex()
    data = "0x" + encode(
        ["uint8", "uint256", "uint256", "uint256", "uint256", "bytes32", "bytes32"],
        [0, 123, 3_300_000, 5_156_250, 59_400, b"\0" * 32, b"\0" * 32],
    ).hex()

    class ReceiptReader:
        def get_receipt(self, value: str):
            assert value == transaction_hash
            return {
                "logs": [
                    {
                        "topics": [topic, order_id, "0x" + "0" * 64, "0x" + "0" * 64],
                        "data": data,
                    }
                ]
            }

    adapter = CLOBExecutionAdapter(
        SimpleNamespace(),
        minimum_marketable_buy_notional_usd=D("1"),
        receipt_reader=ReceiptReader(),
    )
    result = adapter.authoritative_submission_execution(
        source=action(quantity="7.07"),
        order_id=order_id,
        response={
            "success": True,
            "status": "matched",
            "transactionsHashes": [transaction_hash],
        },
    )

    assert result is not None
    assert result["quantity"] == D("5.15625")
    assert result["notional_usd"] == D("3.3")
    assert result["fee_usd"] == D("0.0594")
    assert result["vwap_price"] == D("0.64")


def test_definitive_clob_rejection_releases_cash_without_calling_it_unknown(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution(
        error=RuntimeError(
            "PolyApiException[status_code=400, error_message={'error': 'invalid amount for a marketable BUY order ($1.00), min size: 5'}]"
        )
    )

    result = execute_source_action(
        store=store,
        source=action(quantity="40"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "ERROR_INTERNAL",
        "reason": "CLOB_REJECTED_INVALID_BUY_AMOUNT",
    }
    assert _available_cash_usd(store) == D("100")


def test_polyexception_invalid_tick_is_a_definitive_rejection(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution(
        error=RuntimeError(
            "PolyException: invalid tick size (0.123), minimum tick size: 0.01"
        )
    )

    result = execute_source_action(
        store=store,
        source=action(quantity="40"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "ERROR_INTERNAL",
        "reason": "CLOB_REJECTED_INVALID_TICK_SIZE",
    }
    assert _available_cash_usd(store) == D("100")


def test_fak_no_match_releases_cash_and_never_reposts_the_action(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution(
        error=RuntimeError(
            "PolyApiException[status_code=400, error_message={'error': "
            "'no orders found to match with FAK order. FAK orders are partially "
            "filled or killed if no match is found.'}]"
        )
    )

    source = action(quantity="40")
    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "FAK_ZERO_FILL_NOT_REOPENED",
    }
    assert _available_cash_usd(store) == D("100")
    target = store.action_target(source.action_id)
    assert target["state"] == "EXTERNAL_UNFILLABLE"
    assert target["remaining_quantity"] == D("10")
    assert store.submission_attempt_count(source.action_id) == 1

    execution.error = None
    execution.response = {"success": True, "orderID": "order-2"}
    duplicate = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert duplicate["terminal_status"] == result["terminal_status"]
    assert duplicate["reason"] == result["reason"]
    assert len(execution.calls) == 1
    assert store.submission_attempt_count(source.action_id) == 1


def test_later_opposite_source_action_never_reopens_a_terminal_zero_fill(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    buy = action(quantity="40", marker="1")
    sell = replace(
        action(side="SELL", quantity="40", marker="4"),
        block_number=101,
        source_timestamp=1_700_000_001,
    )
    execution = FakeExecution(
        error=RuntimeError(
            "PolyApiException[status_code=400, error_message={'error': "
            "'no orders found to match with FAK order.'}]"
        )
    )

    assert execute_source_action(
        store=store,
        source=buy,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )["terminal_status"] == "EXTERNAL_UNFILLABLE"
    execution.error = None
    sell_result = execute_source_action(
        store=store,
        source=sell,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert store.action_target(buy.action_id)["state"] == "EXTERNAL_UNFILLABLE"
    assert store.latest_transition(buy)["terminal_status"] == "EXTERNAL_UNFILLABLE"
    assert buy.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }
    assert sell_result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "NO_LOCAL_INVENTORY_AFTER_PRIOR_UNREPLICATED_BUY",
    }


def test_later_opposite_source_action_does_not_supersede_a_partial_fill(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    buy = action(quantity="40", marker="1")
    sell = replace(
        action(side="SELL", quantity="40", marker="4"),
        block_number=101,
        source_timestamp=1_700_000_001,
    )
    store.record_action_receipt(buy)
    store.ensure_action_target(
        source=buy,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PARTIAL_PENDING",
        reason="FAK_PARTIAL_FILL",
        updated_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            """
            UPDATE action_targets
            SET cumulative_filled_quantity = '3'
            WHERE action_id = ?
            """,
            (buy.action_id,),
        )
    store.record_action_receipt(sell)

    superseded = store.supersede_earlier_fully_unfilled_opposites(source=sell)

    assert superseded == []
    target = store.action_target(buy.action_id)
    assert target["state"] == "PARTIAL_PENDING"
    assert target["remaining_quantity"] == D("7")


def test_zero_inventory_sell_is_causal_terminal_and_never_retried(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(side="SELL", quantity="40", marker="9")

    class SnapshotMustNotRun(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("a zero-inventory SELL must not read a later book")

    result = execute_source_action(
        store=store,
        source=source,
        execution=SnapshotMustNotRun(),
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "NO_LOCAL_INVENTORY_PRE_WATERMARK_OR_PRIOR_MISS",
    }
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }


def test_zero_inventory_sell_closes_an_existing_retryable_target(tmp_path: Path):
    """A prior external retry state may not survive a causal SELL terminal."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(side="SELL", quantity="40", marker="a")
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_EXTERNAL_RETRY",
        reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="PENDING_EXTERNAL_RETRY",
        reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
        created_at_ms=1,
    )

    class SnapshotMustNotRun(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("a zero-inventory SELL must not read a later book")

    result = execute_source_action(
        store=store,
        source=source,
        execution=SnapshotMustNotRun(),
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "NO_LOCAL_INVENTORY_PRE_WATERMARK_OR_PRIOR_MISS",
    }
    assert store.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }


def test_zero_inventory_sell_preserves_existing_fill_as_terminal_partial(
    tmp_path: Path,
):
    """A filled prefix remains PARTIAL when later SELL inventory is exhausted."""

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    prior_buy = replace(action(marker="c"), block_number=90)
    store.record_action_receipt(prior_buy)
    store.ensure_action_target(
        source=prior_buy,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="FILLED",
        reason="",
        updated_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE action_targets SET cumulative_filled_quantity = '10' "
            "WHERE action_id = ?",
            (prior_buy.action_id,),
        )
    store.append_transition(
        source=prior_buy,
        status="FILLED",
        created_at_ms=1,
    )
    source = replace(
        action(side="SELL", quantity="40", marker="d"),
        block_number=100,
    )
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PARTIAL_PENDING",
        reason="FAK_PARTIAL_FILL",
        updated_at_ms=2,
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE action_targets SET cumulative_filled_quantity = '3' "
            "WHERE action_id = ?",
            (source.action_id,),
        )
    store.append_transition(
        source=source,
        status="PARTIAL_PENDING",
        reason="FAK_PARTIAL_FILL",
        created_at_ms=2,
    )

    class SnapshotMustNotRun(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("an exhausted SELL must not read a later book")

    result = execute_source_action(
        store=store,
        source=source,
        execution=SnapshotMustNotRun(),
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "PARTIAL",
        "reason": "NO_LOCAL_INVENTORY_AFTER_LOCAL_POSITION_EXHAUSTED",
    }
    target = store.action_target(source.action_id)
    assert target["state"] == "PARTIAL"
    assert target["cumulative_filled_quantity"] == D("3")
    assert target["remaining_quantity"] == D("7")
    assert store.latest_transition(source)["terminal_status"] == "PARTIAL"
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }


def test_terminal_transition_target_alignment_repair_is_additive_and_idempotent(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(side="SELL", quantity="40", marker="b")
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_EXTERNAL_RETRY",
        reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="EXTERNAL_UNFILLABLE",
        reason="NO_LOCAL_INVENTORY_AFTER_LOCAL_POSITION_EXHAUSTED",
        created_at_ms=2,
    )
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "terminal-target-alignment",
            "event_slug": "terminal-target-alignment",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=2,
    )
    assert (
        store.action_fidelity_summary()[
            "retryable_target_terminal_transition_mismatch"
        ]
        == 1
    )
    before_account = store.account_snapshot()
    with store.connect() as connection:
        before_positions = connection.execute(
            "SELECT token_id, quantity, cost_basis_usd FROM positions ORDER BY token_id"
        ).fetchall()
        before_transitions = connection.execute(
            "SELECT COUNT(*) FROM action_transitions"
        ).fetchone()[0]
        before_attempts = connection.execute(
            "SELECT COUNT(*) FROM submission_attempts"
        ).fetchone()[0]

    repaired = store.repair_retryable_targets_with_terminal_latest_transition(
        changed_at_ms=3
    )
    repeated = store.repair_retryable_targets_with_terminal_latest_transition(
        changed_at_ms=4
    )

    assert repaired == [source.action_id]
    assert repeated == []
    assert store.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"
    assert (
        store.action_fidelity_summary()[
            "retryable_target_terminal_transition_mismatch"
        ]
        == 0
    )
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }
    assert store.account_snapshot() == before_account
    with store.connect() as connection:
        assert connection.execute(
            "SELECT token_id, quantity, cost_basis_usd FROM positions ORDER BY token_id"
        ).fetchall() == before_positions
        assert connection.execute(
            "SELECT COUNT(*) FROM action_transitions"
        ).fetchone()[0] == before_transitions
        assert connection.execute(
            "SELECT COUNT(*) FROM submission_attempts"
        ).fetchone()[0] == before_attempts
        receipt = connection.execute(
            """
            SELECT previous_value, new_value, reason, details_json
            FROM config_change_receipts
            WHERE config_key LIKE 'action_target_terminal_alignment:%'
            """
        ).fetchone()
    assert receipt["previous_value"] == "PENDING_EXTERNAL_RETRY"
    assert receipt["new_value"] == "EXTERNAL_UNFILLABLE"
    assert receipt["reason"] == "ALIGN_TARGET_WITH_IMMUTABLE_TERMINAL_TRANSITION"
    details = json.loads(receipt["details_json"])
    assert details["action_id"] == source.action_id
    assert details["historical_transition_changed"] is False
    assert details["orders_submitted_by_repair"] is False
    assert details["cash_positions_or_pnl_changed"] is False


def test_terminal_alignment_preserves_a_filled_prefix_as_corrective_partial(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(side="SELL", quantity="40", marker="e")
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_EXTERNAL_RETRY",
        reason="REPAIRED_EMPTY_BOOK_RETRYABLE",
        updated_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE action_targets SET cumulative_filled_quantity = '3' "
            "WHERE action_id = ?",
            (source.action_id,),
        )
    store.append_transition(
        source=source,
        status="EXTERNAL_UNFILLABLE",
        reason="NO_LOCAL_INVENTORY_AFTER_LOCAL_POSITION_EXHAUSTED",
        created_at_ms=2,
    )
    before_account = store.account_snapshot()
    with store.connect() as connection:
        before_transition_count = connection.execute(
            "SELECT COUNT(*) FROM action_transitions"
        ).fetchone()[0]

    repaired = store.repair_retryable_targets_with_terminal_latest_transition(
        changed_at_ms=3
    )

    assert repaired == [source.action_id]
    target = store.action_target(source.action_id)
    assert target["state"] == "PARTIAL"
    assert target["cumulative_filled_quantity"] == D("3")
    assert target["remaining_quantity"] == D("7")
    assert store.latest_transition(source)["terminal_status"] == "PARTIAL"
    assert store.account_snapshot() == before_account
    with store.connect() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM action_transitions"
        ).fetchone()[0] == before_transition_count + 1
        receipt = connection.execute(
            """
            SELECT new_value, details_json
            FROM config_change_receipts
            WHERE config_key LIKE 'action_target_terminal_alignment:%'
            """
        ).fetchone()
    assert receipt["new_value"] == "PARTIAL"
    details = json.loads(receipt["details_json"])
    assert details["cumulative_filled_quantity"] == "3"
    assert details["corrective_transition_appended"] is True
    assert details["cash_positions_or_pnl_changed"] is False


def test_zero_inventory_sell_links_to_the_prior_unpriced_buy_without_replaying_it(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    prior_buy = replace(action(marker="7"), block_number=90)
    store.record_action_receipt(prior_buy)
    store.append_transition(
        source=prior_buy,
        status="SKIPPED",
        reason="UNPRICED_RESTART_GAP",
    )
    source = replace(action(side="SELL", marker="8"), block_number=100)

    result = execute_source_action(
        store=store,
        source=source,
        execution=FakeExecution(),
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "NO_LOCAL_INVENTORY_AFTER_PRIOR_UNREPLICATED_BUY",
    }
    latest = store.latest_transition(source)
    assert latest["details"] == {
        "historical_action_executed": False,
        "prior_buy_action_id": prior_buy.action_id,
        "prior_buy_reason": "UNPRICED_RESTART_GAP",
        "prior_buy_status": "SKIPPED",
    }


def test_receipt_inserted_later_cannot_supersede_a_later_chain_action(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    later_sell = replace(
        action(side="SELL", marker="4"),
        block_number=101,
        log_index=2,
        source_timestamp=1_700_000_001,
    )
    earlier_buy = replace(action(marker="1"), block_number=100, log_index=9)
    store.record_action_receipt(later_sell)
    store.ensure_action_target(
        source=later_sell,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_LIQUIDITY",
        reason="FAK_ZERO_FILL_RETRYABLE",
        updated_at_ms=1,
    )
    store.record_action_receipt(earlier_buy)

    superseded = store.supersede_earlier_fully_unfilled_opposites(
        source=earlier_buy
    )

    assert superseded == []
    assert store.action_target(later_sell.action_id)["state"] == "PENDING_LIQUIDITY"


def test_canonical_later_inverse_does_not_reopen_a_retired_zero_fill_target(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    later_sell = replace(
        action(side="SELL", marker="4"),
        block_number=101,
        log_index=2,
        source_timestamp=1_700_000_001,
    )
    earlier_buy = replace(action(marker="1"), block_number=100, log_index=9)
    store.record_action_receipt(later_sell)
    store.record_action_receipt(earlier_buy)
    store.ensure_action_target(
        source=earlier_buy,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_LIQUIDITY",
        reason="FAK_ZERO_FILL_RETRYABLE",
        updated_at_ms=1,
    )

    superseded = store.supersede_earlier_fully_unfilled_opposites(
        source=later_sell
    )

    assert superseded == []
    assert store.action_target(earlier_buy.action_id)["state"] == "PENDING_LIQUIDITY"


def test_historical_definitive_rejection_releases_only_its_own_existing_reservation(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="40")
    store.record_action_receipt(source)
    store.append_transition(
        source=source,
        status="UNKNOWN_SUBMISSION",
        reason=(
            "PolyApiException[status_code=400, error_message={'error': \"order couldn't be fully filled. "
            "FOK orders are fully filled or killed.\", 'orderID': '0xabc'}]"
        ),
    )
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO order_reservations(
                action_id, token_id, side, quantity, cash_reserved_usd, active, created_at_ms
            ) VALUES(?, ?, 'BUY', '10', '4', 1, 1)
            """,
            (source.action_id, source.token_id),
        )

    applied = store.reconcile_definitive_submission_rejection(
        source=source,
        created_at_ms=2,
    )

    assert applied is True
    assert _available_cash_usd(store) == D("100")
    assert store.latest_transition(source) == {
        "terminal_status": "SKIPPED",
        "reason": "CLOB_REJECTED_FOK_KILLED",
        "created_at_ms": 2,
        "details": {
            "prior_unknown_transition_id": 1,
            "original_rejection_message": (
                "PolyApiException[status_code=400, error_message={'error': \"order couldn't be fully filled. "
                "FOK orders are fully filled or killed.\", 'orderID': '0xabc'}]"
            ),
        },
    }


def test_reconciliation_accepts_documented_fixed_math_matched_shares():
    quantity, encoding = _matched_shares(
        {"original_size": "10000000", "size_matched": "10000000"},
        expected_quantity=D("10"),
    )

    assert quantity == D("10")
    assert encoding == "FIXED_MATH_6DP"


def test_immutable_buy_fill_correction_repairs_underreported_human_share_order_once(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="20")
    store.record_action_receipt(source)
    _seed_local_fill(
        store=store,
        source=source,
        quantity=D("0.000005"),
        price=D("0.40"),
        fee_usd=D("0"),
    )
    store.append_transition(
        source=source,
        status="PARTIAL",
        reason="FAK_PARTIAL_FILL",
        details={"matched_quantity": "0.000005", "matched_price": "0.40", "fee_usd": "0"},
    )

    first = store.apply_authoritative_buy_fill_correction(
        source=source,
        previous_quantity=D("0.000005"),
        authoritative_quantity=D("5"),
        previous_fee_usd=D("0"),
        authoritative_fee_usd=D("0"),
        expected_quantity=D("5"),
        price=D("0.40"),
        created_at_ms=2,
        details={"quantity_encoding": "HUMAN_SHARES"},
    )
    second = store.apply_authoritative_buy_fill_correction(
        source=source,
        previous_quantity=D("0.000005"),
        authoritative_quantity=D("5"),
        previous_fee_usd=D("0"),
        authoritative_fee_usd=D("0"),
        expected_quantity=D("5"),
        price=D("0.40"),
        created_at_ms=3,
        details={"quantity_encoding": "HUMAN_SHARES"},
    )

    assert first is True
    assert second is False
    assert _fill_correction_count(store) == 1
    assert store.position_quantity("123") == D("5")
    assert store.account_snapshot()["cash_usd"] == D("98")
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "FILLED"
    assert latest["reason"] == "ACCOUNTING_CORRECTED_FROM_OFFICIAL_ONCHAIN_FILL"


def test_authoritative_correction_can_book_a_proven_match_previously_marked_error(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="20")
    store.record_action_receipt(source)
    store.append_transition(
        source=source,
        status="ERROR",
        reason="FILL_EXCEEDS_RECORDED_TARGET",
        details={"order": {"status": "MATCHED"}},
    )

    applied = store.apply_authoritative_buy_fill_correction(
        source=source,
        previous_quantity=D("0"),
        previous_notional_usd=D("0"),
        previous_fee_usd=D("0"),
        authoritative_quantity=D("5.15625"),
        authoritative_notional_usd=D("3.3"),
        authoritative_fee_usd=D("0.0594"),
        expected_quantity=D("5"),
        price=D("0.64"),
        full_match=True,
        created_at_ms=2,
        details={"receipt_evidence": [{"fee_raw": "59400"}]},
    )

    assert applied is True
    assert store.position_quantity("123") == D("5.15625")
    assert store.account_snapshot()["cash_usd"] == D("96.6406")
    assert store.latest_transition(source)["terminal_status"] == "FILLED"


def test_reconciled_sell_uses_only_inventory_acquired_by_this_live_sleeve(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("4"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    buy = action(quantity="1000", marker="1")
    execute_source_action(
        store=store,
        source=buy,
        execution=execution,
        allocated_cash=D("4"),
        live_enabled=True,
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("4"),
        vwap_price=D("0.40"),
    )
    reconcile_submitted_actions(store=store, execution=execution)
    execution.response = {"success": True, "orderID": "order-2"}
    sell = action(side="SELL", quantity="1000", marker="2")

    submitted = execute_source_action(
        store=store,
        source=sell,
        execution=execution,
        allocated_cash=D("4"),
        live_enabled=True,
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("3"),
        vwap_price=D("0.30"),
    )
    reconciled = reconcile_submitted_actions(store=store, execution=execution)

    assert submitted["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert reconciled == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT",
        }
    ]
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("3.00")
    assert store.account_snapshot()["realized_pnl_usd"] == D("-1.00")


def test_partial_followed_buy_caps_later_sell_at_actual_local_inventory(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    prior_buy = action(side="BUY", quantity="40", marker="1")
    _seed_local_fill(
        store=store,
        source=prior_buy,
        quantity=D("6"),
        price=D("0.40"),
        fee_usd=D("0"),
    )
    execution = FakeExecution(response={"success": True, "orderID": "order-partial-exit"})

    result = execute_source_action(
        store=store,
        source=action(side="SELL", quantity="40", marker="2"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SUBMITTED_UNRECONCILED",
        "reason": "SELL_AVAILABLE_INVENTORY_CAP",
    }
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "SELL",
            "price": D("0.30"),
            "size": D("6"),
        }
    ]


def test_shallow_sell_book_submits_fak_when_the_market_minimum_is_visible(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    prior_buy = action(side="BUY", quantity="40", marker="1")
    _seed_local_fill(
        store=store,
        source=prior_buy,
        quantity=D("10"),
        price=D("0.40"),
        fee_usd=D("0"),
    )
    execution = ShallowSellBookExecution()

    result = execute_source_action(
        store=store,
        source=action(side="SELL", quantity="40", marker="2"),
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "SUBMITTED_UNRECONCILED",
        "reason": "FAK_PARTIAL_ATTEMPT",
    }
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "SELL",
            "price": D("0.30"),
            "size": D("10"),
        }
    ]


def test_fak_partial_buy_books_only_the_actual_fill_and_finalizes_unfillable_remainder(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FakeExecution()
    source = action(quantity="40")

    submitted = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("6"),
        notional_usd=D("2.4"),
        vwap_price=D("0.40"),
    )
    reconciled = reconcile_submitted_actions(store=store, execution=execution)

    assert submitted == {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    assert reconciled == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM",
        }
    ]
    assert store.position_quantity("123") == D("6")
    assert _available_cash_usd(store) == D("97.60")
    latest = store.latest_transition(source)
    assert latest["details"]["execution_order_type"] == "FAK"
    assert store.action_target(source.action_id)["remaining_quantity"] == D("4")
    assert source.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }


def test_finalized_zero_fill_is_terminal_without_a_second_order(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="40")
    execution = FakeExecution()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.orders["order-1"] = {
        "status": "ORDER_STATUS_CANCELED",
        "original_size": "10",
        "size_matched": "0",
        "price": "0.40",
    }
    results = reconcile_submitted_actions(
        store=store,
        execution=execution,
    )

    assert results == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "FAK_ZERO_FILL_NOT_REOPENED",
        }
    ]
    assert len(execution.calls) == 1
    assert store.submission_attempt_count(source.action_id) == 1
    assert store.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"


def test_first_live_attempt_has_no_retry_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.activate_bounded_retry_policy(
        effective_after_block=99,
        activated_at_ms=1_700_000_000_000,
        change_id="legacy-policy-receipt",
    )
    monkeypatch.setattr(
        live,
        "now_ms",
        lambda: 1_700_000_000_000 + live.BOUNDED_RETRY_DEADLINE_MS + 1,
    )
    execution = FakeExecution()

    result = execute_source_action(
        store=store,
        source=action(quantity="40"),
        execution=execution,
        live_enabled=True,
    )

    assert result["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert len(execution.calls) == 1


def test_buy_above_user_price_ceiling_is_not_submitted_when_it_has_execution_loss(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )

    class HighPriceExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            snapshot = super().snapshot(token_id=token_id, side=side)
            snapshot["best_price"] = "0.91"
            return snapshot

    execution = HighPriceExecution()
    source = replace(action(quantity="40"), source_notional=D("36"))

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "BUY_PRICE_ABOVE_0_90_WITH_EXECUTION_LOSS",
    }
    assert execution.calls == []


def test_buy_above_user_price_ceiling_allows_fees_when_execution_price_has_no_loss(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )

    class HighPriceExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            snapshot = super().snapshot(token_id=token_id, side=side)
            snapshot["best_price"] = "0.91"
            snapshot["fee_bps"] = "500"
            return snapshot

    execution = HighPriceExecution()
    source = replace(action(quantity="40"), source_notional=D("36.4"))

    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        live_enabled=True,
    )

    assert result["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert len(execution.calls) == 1


def test_retry_closure_does_not_submit_non_liquidity_pending_actions(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    unavailable_source = action(marker="1")
    later_source = replace(
        action(marker="4"),
        token_id="456",
        block_number=101,
    )
    for source in (unavailable_source, later_source):
        store.record_action_receipt(source)
        store.ensure_action_target(
            source=source,
            proportional_quantity=D("10"),
            target_quantity=D("10"),
            state="PENDING_EXTERNAL_RETRY",
            reason="RETRYABLE_BOOK_READ",
            updated_at_ms=1,
        )
        store.append_transition(
            source=source,
            status="PENDING_EXTERNAL_RETRY",
            reason="RETRYABLE_BOOK_READ",
            created_at_ms=1,
        )

    class FirstBookUnavailableExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            if token_id == "123":
                error = PolyApiException(
                    error_msg="No orderbook exists for the requested token id"
                )
                error.status_code = 404
                raise error
            assert token_id == "456"
            return {
                "minimum_order_size": "5",
                "minimum_marketable_buy_notional_usd": "1",
                "best_price": "0.40",
                "tick_size": "0.01",
                "visible_best_level_size": "100",
                "fee_bps": "0",
                "raw_book": {"asks": [], "bids": []},
            }

    execution = FirstBookUnavailableExecution()

    first_head_results = retry_pending_actions(
        store=store,
        execution=execution,
    )

    assert first_head_results == []
    assert execution.calls == []

    restarted_store = LiveStore(store.path)
    second_head_results = retry_pending_actions(
        store=restarted_store,
        execution=execution,
    )

    assert second_head_results == []
    assert execution.calls == []
    assert restarted_store.action_target(unavailable_source.action_id)["state"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    assert restarted_store.action_target(later_source.action_id)["state"] == (
        "PENDING_EXTERNAL_RETRY"
    )


def test_retry_closure_does_not_submit_non_liquidity_closed_market_target(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(marker="closed-market")
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_EXTERNAL_RETRY",
        reason="BOOK_SNAPSHOT_ERROR: official book unavailable",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="PENDING_EXTERNAL_RETRY",
        reason="BOOK_SNAPSHOT_ERROR: official book unavailable",
        created_at_ms=1,
    )

    class ClosedMarketExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("closed market must not be read from CLOB again")

    def current_lifecycle(_source):
        return ScopeDecision(
            True,
            "FULL_WALLET_ACTION_ELIGIBLE",
            {
                "condition_id": "0xclosed",
                "event_slug": "closed-event",
                "market_slug": "closed-market",
                "closed": True,
                "accepting_orders": False,
            },
        )

    results = retry_pending_actions(
        store=store,
        execution=ClosedMarketExecution(),
        market_lifecycle_resolver=current_lifecycle,
    )

    assert results == []
    assert store.action_target(source.action_id)["state"] == "PENDING_EXTERNAL_RETRY"
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "PENDING_EXTERNAL_RETRY"


def test_unproven_partial_order_status_does_not_trigger_a_second_order(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="40")
    execution = FakeExecution()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.orders["order-1"] = {
        "status": "ORDER_STATUS_CANCELED",
        "original_size": "10",
        "size_matched": "5",
        "price": "0.40",
    }
    reconcile_submitted_actions(store=store, execution=execution)
    execution.response = {"success": True, "orderID": "order-2"}

    retry_pending_actions(
        store=store,
        execution=execution,
    )

    assert len(execution.calls) == 1


def _funded_resolved_position_store(tmp_path: Path) -> tuple[LiveStore, SourceAction]:
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("4"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="1000")
    _seed_local_fill(
        store=store,
        source=source,
        quantity=D("10"),
        price=D("0.40"),
        fee_usd=D("0"),
    )
    store.bind_condition_for_token(
        token_id="123",
        condition_id=FakeRedemptionAdapter.condition_id,
        primary_token_id="123",
        secondary_token_id="456",
        observed_at_ms=1,
    )
    return store, source


def _shared_resolved_position_stores(
    tmp_path: Path,
) -> tuple[SharedWalletCoordinator, LiveStore, LiveStore, FakeRedemptionAdapter]:
    first = LiveStore(tmp_path / "first.sqlite3")
    second = LiveStore(tmp_path / "second.sqlite3")
    for store in (first, second):
        initialize_scale_once(
            store=store,
            allocation_usd=D("50"),
            source_open_position_value_usd=D("100"),
            observed_at_ms=1,
        )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("first", first.path, "RESIDUAL"),
            SleeveSpec("second", second.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address=FakeRedemptionAdapter.wallet_address,
        observed_at_ms=2,
    )
    for store, quantity, cost in (
        (first, "4", "1.6"),
        (second, "6", "2.4"),
    ):
        store.bind_condition_for_token(
            token_id="123",
            condition_id=FakeRedemptionAdapter.condition_id,
            primary_token_id="123",
            secondary_token_id="456",
            observed_at_ms=3,
        )
        with store.connect() as connection:
            connection.execute(
                """
                INSERT INTO positions(
                    token_id, quantity, cost_basis_usd, condition_id
                ) VALUES('123', ?, ?, ?)
                """,
                (quantity, cost, FakeRedemptionAdapter.condition_id),
            )
    return coordinator, first, second, FakeRedemptionAdapter()


def test_shared_redemption_cycle_submits_once_and_allocates_each_sleeve_once(
    tmp_path: Path,
):
    coordinator, first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )

    submitted = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    repeated = run_redemption_cycle(
        store=second,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="second",
    )

    assert adapter.submissions == [adapter.condition_id]
    assert submitted["shared_decisions"][-1]["state"] == (
        "SUBMITTED_UNRECONCILED"
    )
    assert repeated["shared_decisions"][-1]["state"] == "PENDING"
    assert first.redemption_receipt(adapter.condition_id) is None
    assert second.redemption_receipt(adapter.condition_id) is None

    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    confirmed = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    idempotent = run_redemption_cycle(
        store=second,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="second",
    )

    assert confirmed["shared_decisions"][-1]["state"] == "REDEEMED"
    assert idempotent["shared_decisions"] == []
    assert adapter.submissions == [adapter.condition_id]
    assert first.account_snapshot()["cash_usd"] == D("54")
    assert second.account_snapshot()["cash_usd"] == D("56")
    assert first.account_snapshot()["realized_pnl_usd"] == D("2.4")
    assert second.account_snapshot()["realized_pnl_usd"] == D("3.6")
    assert first.position_quantity("123") == D("0")
    assert second.position_quantity("123") == D("0")
    assert first.redemption_receipt(adapter.condition_id)["state"] == (
        "REDEEMED_SHARED_WALLET"
    )
    assert second.redemption_receipt(adapter.condition_id)["state"] == (
        "REDEEMED_SHARED_WALLET"
    )


def test_confirmed_shared_redemption_requires_exact_onchain_collateral_payout(
    tmp_path: Path,
):
    """A relayer confirmation cannot distribute cash absent from its tx receipt."""

    coordinator, first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    adapter.confirmed_collateral_payout_raw = 0

    outcome = run_redemption_cycle(
        store=second,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="second",
    )

    assert outcome["shared_decisions"][-1]["state"] == "PENDING"
    assert coordinator.shared_redemption_receipt(adapter.condition_id)["state"] == (
        "PENDING"
    )
    assert first.account_snapshot()["cash_usd"] == D("50")
    assert second.account_snapshot()["cash_usd"] == D("50")
    assert first.position_quantity("123") == D("4")
    assert second.position_quantity("123") == D("6")
    assert adapter.submissions == [adapter.condition_id]


def test_peer_buy_reservation_prevents_shared_and_local_redemption_submission(
    tmp_path: Path,
):
    coordinator, first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    with second.connect() as connection:
        connection.execute(
            "UPDATE positions SET quantity = '0', cost_basis_usd = '0'"
        )
    source = action(quantity="1", marker="9")
    assert second.record_action_receipt(source)
    with second.connect() as connection:
        connection.execute(
            """
            INSERT INTO order_reservations(
                action_id, token_id, side, quantity, cash_reserved_usd,
                condition_id, active, created_at_ms
            ) VALUES(?, '456', 'BUY', '1', '0.4', ?, 1, 4)
            """,
            (source.action_id, adapter.condition_id),
        )
    adapter.primary_raw = 4_000_000

    outcome = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )

    assert adapter.submissions == []
    assert first.redemption_receipt(adapter.condition_id) is None
    assert second.redemption_receipt(adapter.condition_id) is None
    assert outcome["shared_decisions"][-1]["state"] == (
        "PENDING_SHARED_EVIDENCE"
    )


def test_existing_local_redemption_is_read_only_reconciled_after_condition_becomes_shared(
    tmp_path: Path,
):
    coordinator, first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    with second.connect() as connection:
        connection.execute(
            "UPDATE positions SET quantity = '0', cost_basis_usd = '0'"
        )
    adapter.primary_raw = 4_000_000
    local_submission = auto_redeem_resolved_positions(
        store=first,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    with second.connect() as connection:
        connection.execute(
            "UPDATE positions SET quantity = '6', cost_basis_usd = '2.4'"
        )
    adapter.primary_raw = 10_000_000

    pending = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )

    assert local_submission == [
        {"condition_id": adapter.condition_id, "state": "SUBMITTED_UNRECONCILED"}
    ]
    assert pending["shared_decisions"][-1]["state"] == "PENDING_SHARED_EVIDENCE"
    assert pending["reconciled"] == [
        {"condition_id": adapter.condition_id, "state": "PENDING"}
    ]
    assert adapter.status_reads == ["redeem-1"]
    assert adapter.submissions == [adapter.condition_id]
    assert coordinator.shared_redemption_receipt(adapter.condition_id) is None
    assert first.redemption_receipt(adapter.condition_id)["state"] == "PENDING"

    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    confirmed = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )

    assert confirmed["reconciled"] == [
        {"condition_id": adapter.condition_id, "state": "ERROR"}
    ]
    receipt = first.redemption_receipt(adapter.condition_id)
    assert receipt["state"] == "ERROR"
    with first.connect() as connection:
        reason = connection.execute(
            """
            SELECT reason FROM redemption_transitions
            WHERE condition_id = ? ORDER BY id DESC LIMIT 1
            """,
            (adapter.condition_id,),
        ).fetchone()["reason"]
    assert reason == "ERROR_LOCAL_REDEMPTION_CROSSES_SHARED_INVENTORY"
    assert first.account_snapshot()["cash_usd"] == D("50")
    assert second.account_snapshot()["cash_usd"] == D("50")
    assert first.position_quantity("123") == D("4")
    assert second.position_quantity("123") == D("6")
    assert adapter.status_reads == ["redeem-1", "redeem-1"]
    assert adapter.submissions == [adapter.condition_id]


def test_shared_distribution_resumes_after_local_apply_before_coordinator_ack(
    tmp_path: Path,
    monkeypatch,
):
    coordinator, first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    original_ack = coordinator.mark_shared_allocation_applied
    crashed = False

    def crash_after_first_local_apply(**kwargs):
        nonlocal crashed
        if kwargs["profile_key"] == "first" and not crashed:
            crashed = True
            raise RuntimeError("simulated crash before coordinator ack")
        return original_ack(**kwargs)

    monkeypatch.setattr(
        coordinator,
        "mark_shared_allocation_applied",
        crash_after_first_local_apply,
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        run_redemption_cycle(
            store=first,
            adapter=adapter,
            coordinator=coordinator,
            profile_key="first",
        )

    assert coordinator.shared_redemption_receipt(adapter.condition_id)["state"] == (
        "DISTRIBUTING"
    )
    assert first.account_snapshot()["cash_usd"] == D("54")
    assert second.account_snapshot()["cash_usd"] == D("50")
    assert coordinator.shared_condition_allocations(adapter.condition_id)[0][
        "apply_state"
    ] == "PENDING"

    monkeypatch.setattr(
        coordinator,
        "mark_shared_allocation_applied",
        original_ack,
    )
    resumed = run_redemption_cycle(
        store=second,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="second",
    )

    assert resumed["shared_decisions"][-1]["state"] == "REDEEMED"
    assert first.account_snapshot()["cash_usd"] == D("54")
    assert second.account_snapshot()["cash_usd"] == D("56")
    assert first.account_snapshot()["realized_pnl_usd"] == D("2.4")
    assert second.account_snapshot()["realized_pnl_usd"] == D("3.6")
    assert {
        row["apply_state"]
        for row in coordinator.shared_condition_allocations(adapter.condition_id)
    } == {"APPLIED"}
    assert adapter.submissions == [adapter.condition_id]


def test_shared_submit_started_restart_becomes_unknown_without_repost(tmp_path: Path):
    coordinator, first, _second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    coordinator.freeze_shared_condition_redemption(
        condition_id=adapter.condition_id,
        winner_token_id="123",
        created_at_ms=4,
    )
    assert coordinator.start_shared_redemption_submission(
        condition_id=adapter.condition_id,
        created_at_ms=5,
    )

    recovered = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    repeated = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )

    assert recovered["shared_decisions"][-1]["state"] == "UNKNOWN_SUBMISSION"
    assert repeated["shared_decisions"][-1]["state"] == "UNKNOWN_SUBMISSION"
    assert coordinator.shared_redemption_receipt(adapter.condition_id)["state"] == (
        "UNKNOWN_SUBMISSION"
    )
    assert adapter.submissions == []


@pytest.mark.parametrize("failure_mode", ["transport", "missing_transaction_id"])
def test_uncertain_shared_redemption_submission_is_never_reposted(
    tmp_path: Path,
    failure_mode: str,
):
    coordinator, first, _second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    attempts = []

    def uncertain_submit(*, condition_id: str):
        attempts.append(condition_id)
        if failure_mode == "transport":
            raise TimeoutError("uncertain after submit")
        return {}

    adapter.submit_redeem = uncertain_submit

    first_cycle = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    repeated = run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )

    assert first_cycle["shared_decisions"][-1]["state"] == "UNKNOWN_SUBMISSION"
    assert repeated["shared_decisions"][-1]["state"] == "UNKNOWN_SUBMISSION"
    assert attempts == [adapter.condition_id]
    assert coordinator.shared_redemption_receipt(adapter.condition_id)["state"] == (
        "UNKNOWN_SUBMISSION"
    )


def test_confirmed_shared_redemption_inventory_mismatch_never_mutates_local_cash(
    tmp_path: Path,
):
    coordinator, first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    run_redemption_cycle(
        store=first,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="first",
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 1

    outcome = run_redemption_cycle(
        store=second,
        adapter=adapter,
        coordinator=coordinator,
        profile_key="second",
    )

    assert outcome["shared_decisions"][-1]["state"] == "ERROR"
    assert coordinator.shared_redemption_receipt(adapter.condition_id)["state"] == (
        "ERROR"
    )
    assert first.account_snapshot()["cash_usd"] == D("50")
    assert second.account_snapshot()["cash_usd"] == D("50")
    assert first.position_quantity("123") == D("4")
    assert second.position_quantity("123") == D("6")
    assert adapter.submissions == [adapter.condition_id]


@pytest.mark.parametrize("sleeve_count", [3, 4])
def test_shared_redemption_allocations_sum_to_one_physical_payout_for_many_sleeves(
    tmp_path: Path,
    sleeve_count: int,
):
    stores = [LiveStore(tmp_path / f"sleeve-{index}.sqlite3") for index in range(sleeve_count)]
    for store in stores:
        initialize_scale_once(
            store=store,
            allocation_usd=D("50"),
            source_open_position_value_usd=D("100"),
            observed_at_ms=1,
        )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=tuple(
            SleeveSpec(
                f"profile-{index}",
                store.path,
                "RESIDUAL" if index == 0 else "RESERVED",
            )
            for index, store in enumerate(stores)
        ),
        authenticated_collateral_usd=D(str(50 * sleeve_count)),
        funder_address=FakeRedemptionAdapter.wallet_address,
        observed_at_ms=2,
    )
    total_quantity = D("0")
    total_cost = D("0")
    for index, store in enumerate(stores):
        quantity = D(index + 1)
        cost = quantity * D("0.4")
        total_quantity += quantity
        total_cost += cost
        store.bind_condition_for_token(
            token_id="123",
            condition_id=FakeRedemptionAdapter.condition_id,
            primary_token_id="123",
            secondary_token_id="456",
            observed_at_ms=3,
        )
        with store.connect() as connection:
            connection.execute(
                """
                INSERT INTO positions(
                    token_id, quantity, cost_basis_usd, condition_id
                ) VALUES('123', ?, ?, ?)
                """,
                (str(quantity), str(cost), FakeRedemptionAdapter.condition_id),
            )
    adapter = FakeRedemptionAdapter(
        primary_raw=int(total_quantity * live.TOKEN_SCALE)
    )

    run_redemption_cycle(
        store=stores[0],
        adapter=adapter,
        coordinator=coordinator,
        profile_key="profile-0",
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    run_redemption_cycle(
        store=stores[-1],
        adapter=adapter,
        coordinator=coordinator,
        profile_key=f"profile-{sleeve_count - 1}",
    )

    allocations = coordinator.shared_condition_allocations(adapter.condition_id)
    assert sum((D(row["payout_usd"]) for row in allocations), D("0")) == (
        total_quantity
    )
    assert sum(
        (store.account_snapshot()["realized_pnl_usd"] for store in stores),
        D("0"),
    ) == total_quantity - total_cost
    assert sum(
        (store.account_snapshot()["cash_usd"] for store in stores), D("0")
    ) == D(str(50 * sleeve_count)) + total_quantity
    assert adapter.submissions == [adapter.condition_id]


def test_auto_redemption_submits_one_condition_once_and_waits_for_confirmation(tmp_path: Path):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()

    first = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    second = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )

    assert first == [{"condition_id": adapter.condition_id, "state": "SUBMITTED_UNRECONCILED"}]
    assert second == [{"condition_id": adapter.condition_id, "state": "SUBMITTED_UNRECONCILED"}]
    assert adapter.submissions == [adapter.condition_id]
    receipt = store.redemption_receipt(adapter.condition_id)
    assert receipt["expected_payout_usd"] == "10"
    assert receipt["state"] == "SUBMITTED_UNRECONCILED"
    assert store.account_snapshot()["cash_usd"] == D("0")


def test_redemption_cycle_labels_pending_decisions_without_claiming_submission(tmp_path: Path):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    adapter.condition_resolution = lambda condition_id: {
        "condition_id": condition_id,
        "closed": False,
        "winner_token_id": None,
    }

    outcome = run_redemption_cycle(store=store, adapter=adapter)

    assert "submitted" not in outcome
    assert outcome["mappings"] == []
    assert outcome["reconciled"] == []
    assert outcome["decisions"] == [
        {"condition_id": adapter.condition_id, "state": "PENDING_OFFICIAL_RESOLUTION"}
    ]


def test_confirmed_auto_redemption_releases_only_the_verified_winning_payout(tmp_path: Path):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0

    result = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    assert result == [{"condition_id": adapter.condition_id, "state": "REDEEMED"}]
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.account_snapshot()["realized_pnl_usd"] == D("6")


def test_confirmed_redemption_requires_exact_onchain_collateral_payout_before_credit(
    tmp_path: Path,
):
    """A public REDEEM row cannot override the transaction's zero payout."""

    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    adapter.confirmed_collateral_payout_raw = 0

    result = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    assert result == [{"condition_id": adapter.condition_id, "state": "PENDING"}]
    assert store.redemption_receipt(adapter.condition_id)["state"] == "PENDING"
    assert store.position_quantity("123") == D("10")
    assert store.account_snapshot()["cash_usd"] == D("0")
    assert store.account_snapshot()["realized_pnl_usd"] == D("0")


def test_legacy_redemption_with_zero_onchain_payout_is_permanently_excluded_from_shared_cash(
    tmp_path: Path,
):
    """A forward safety receipt protects cash without rewriting legacy ledger rows."""

    coordinator, _first, second, adapter = _shared_resolved_position_stores(
        tmp_path
    )
    with second.connect() as connection:
        connection.execute(
            """
            INSERT INTO redemption_receipts(
                condition_id, state, expected_payout_usd, transaction_id,
                transaction_hash, created_at_ms, updated_at_ms
            ) VALUES(?, 'REDEEMED', '10', 'redeem-1', ?, 4, 5)
            """,
            (adapter.condition_id, "0x" + "c" * 64),
        )
        connection.execute(
            "UPDATE account_state SET cash_usd = '60' WHERE singleton = 1"
        )
    adapter.confirmed_collateral_payout_raw = 0

    blocked = live.backfill_redeemed_cash_credit_permanent_blocks_from_chain(
        store=second,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        created_at_ms=6,
    )
    snapshot = coordinator.authenticated_account_cash_snapshot(
        authenticated_collateral_usd=D("100"),
    )

    assert blocked == [
        {
            "condition_id": adapter.condition_id,
            "state": "PERMANENT_CASH_EXCLUSION",
        }
    ]
    assert snapshot.expected_accounting_cash_high_usd == D("100")
    assert snapshot.available_for_new_buy_usd == D("100")
    assert snapshot.permanent_redeemed_cash_credit_block_usd == D("10")
    assert second.account_snapshot()["cash_usd"] == D("60")
    assert second.position_quantity("123") == D("6")


def test_confirmed_redemption_uses_official_zero_payout_instead_of_predicted_payout(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    adapter.confirmed_collateral_payout_raw = 0
    adapter.official_redemption_activity_for_transaction = lambda **kwargs: {
        "condition_id": adapter.condition_id,
        "transaction_hash": "0x" + "c" * 64,
        "payout_usd": D("0"),
        "official_activity_type": "REDEEM",
    }

    result = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    assert result == [
        {"condition_id": adapter.condition_id, "state": "LOSS_RESOLVED_NO_PAYOUT"}
    ]
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("0")
    assert store.account_snapshot()["realized_pnl_usd"] == D("-4")
    assert store.redemption_receipt(adapter.condition_id)["expected_payout_usd"] == "0"


def test_terminal_redemption_payout_correction_is_append_only_and_idempotent(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    correction = {
        "condition_id": adapter.condition_id,
        "transaction_hash": "0x" + "c" * 64,
        "official_payout_usd": D("0"),
        "official_activity_type": "REDEEM",
        "evidence_hash": "e" * 64,
    }
    assert store.correct_terminal_redemption_payout(
        **correction, created_at_ms=100
    ) is True
    assert store.correct_terminal_redemption_payout(
        **correction, created_at_ms=101
    ) is False
    assert store.account_snapshot()["cash_usd"] == D("0")
    assert store.account_snapshot()["realized_pnl_usd"] == D("-4")
    receipt = store.redemption_receipt(adapter.condition_id)
    assert receipt["state"] == "LOSS_RESOLVED_OFFICIAL_ACTIVITY_CORRECTED"
    assert receipt["expected_payout_usd"] == "0"
    with store.connect() as connection:
        rows = connection.execute(
            "SELECT * FROM redemption_payout_corrections"
        ).fetchall()
    assert len(rows) == 1


def test_terminal_redemption_payout_correction_voids_uncredited_cash_quarantine(
    tmp_path: Path,
):
    """A corrected zero payout cannot remain reserved as future wallet cash."""

    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )
    assert store.record_redeemed_cash_credit_quarantine(
        condition_id=adapter.condition_id,
        payout_usd=D("10"),
        created_at_ms=99,
        details={"reason": "TEST_UNCREDITED_PAYOUT"},
    )

    assert store.correct_terminal_redemption_payout(
        condition_id=adapter.condition_id,
        transaction_hash="0x" + "c" * 64,
        official_payout_usd=D("0"),
        official_activity_type="REDEEM",
        evidence_hash="e" * 64,
        created_at_ms=100,
    )

    with store.connect() as connection:
        original = connection.execute(
            "SELECT * FROM redeemed_cash_credit_quarantines"
        ).fetchall()
        voids = connection.execute(
            "SELECT * FROM redeemed_cash_credit_quarantine_voids"
        ).fetchall()
    assert len(original) == 1
    assert len(voids) == 1
    assert voids[0]["condition_id"] == adapter.condition_id
    assert voids[0]["reason"] == "OFFICIAL_PAYOUT_CORRECTION_VOIDED_QUARANTINE"


def test_confirmed_redemption_reconciles_after_a_pending_relayer_state(tmp_path: Path):
    """A submitted redemption may be observed as pending before confirmation."""

    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(status="STATE_MINED")
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )

    pending = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    assert pending == [{"condition_id": adapter.condition_id, "state": "PENDING"}]
    assert store.redemption_receipt(adapter.condition_id)["state"] == "PENDING"

    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0
    confirmed = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    assert confirmed == [{"condition_id": adapter.condition_id, "state": "REDEEMED"}]
    assert store.redemption_receipt(adapter.condition_id)["state"] == "REDEEMED"
    assert store.account_snapshot()["cash_usd"] == D("10")


def test_pre_dispatch_redemption_metadata_failure_is_retried_without_unknown_submission(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()

    def not_submitted(*, condition_id: str):
        raise live.RedemptionNotSubmittedError("No market found for condition")

    adapter.submit_redeem = not_submitted
    first = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )

    assert first == [
        {"condition_id": adapter.condition_id, "state": "NOT_SUBMITTED_RETRYABLE"}
    ]
    assert store.redemption_receipt(adapter.condition_id)["transaction_id"] is None

    adapter.submit_redeem = FakeRedemptionAdapter.submit_redeem.__get__(
        adapter, FakeRedemptionAdapter
    )
    second = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )

    assert second == [
        {"condition_id": adapter.condition_id, "state": "SUBMITTED_UNRECONCILED"}
    ]
    assert adapter.submissions == [adapter.condition_id]


def test_unknown_redemption_can_be_settled_only_from_external_chain_proof(tmp_path: Path):
    store, _ = _funded_resolved_position_store(tmp_path)
    condition_id = FakeRedemptionAdapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="UNKNOWN_SUBMISSION",
        reason="pre-dispatch metadata error",
        created_at_ms=3,
    )

    store.settle_externally_verified_redemption(
        condition_id=condition_id,
        payout_usd=D("10"),
        transaction_hash="0x" + "d" * 64,
        created_at_ms=4,
        details={
            "official_activity_type": "REDEEM",
            "onchain_outcome_balances_zero": True,
        },
    )

    assert store.redemption_receipt(condition_id)["state"] == "REDEEMED_EXTERNAL_VERIFIED"
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.account_snapshot()["realized_pnl_usd"] == D("6")


def test_orphaned_local_redemption_submit_started_becomes_unknown_without_repost(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    assert store.start_redemption_submission(
        condition_id=adapter.condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )

    reconciled = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )
    retry = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )

    assert reconciled == [
        {"condition_id": adapter.condition_id, "state": "UNKNOWN_SUBMISSION"}
    ]
    assert retry == [
        {"condition_id": adapter.condition_id, "state": "UNKNOWN_SUBMISSION"}
    ]
    assert store.redemption_receipt(adapter.condition_id)["state"] == (
        "UNKNOWN_SUBMISSION"
    )
    assert adapter.submissions == []


def test_unknown_local_redemption_with_known_transaction_is_read_only_reconciled(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    store.mark_redemption_terminal(
        condition_id=adapter.condition_id,
        state="UNKNOWN_SUBMISSION",
        reason="recovery uncertainty",
        created_at_ms=3,
    )
    adapter.status = "STATE_CONFIRMED"
    adapter.primary_raw = 0

    reconciled = reconcile_redemption_submissions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
    )

    assert reconciled == [
        {"condition_id": adapter.condition_id, "state": "REDEEMED"}
    ]
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.position_quantity("123") == D("0")
    assert adapter.status_reads == ["redeem-1"]
    assert adapter.submissions == [adapter.condition_id]


def test_unknown_local_redemption_without_transaction_uses_exact_platform_cash_proof(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter()
    attempts = []

    def missing_transaction_id(*, condition_id: str):
        attempts.append(condition_id)
        return {}

    adapter.submit_redeem = missing_transaction_id
    submitted = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    repeated = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )
    adapter.primary_raw = 0

    reconciled = reconcile_platform_settled_winners(
        store=store,
        adapter=adapter,
        observed_collateral_usd=D("10"),
        created_at_ms=4,
    )

    assert submitted == [
        {"condition_id": adapter.condition_id, "state": "UNKNOWN_SUBMISSION"}
    ]
    assert repeated == [
        {"condition_id": adapter.condition_id, "state": "UNKNOWN_SUBMISSION"}
    ]
    assert attempts == [adapter.condition_id]
    assert reconciled["state"] == "RECONCILED"
    assert reconciled["condition_count"] == 1
    assert store.redemption_receipt(adapter.condition_id)["state"] == (
        "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"
    )
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.position_quantity("123") == D("0")


def test_shared_wallet_settlement_applies_frozen_allocation_exactly_once(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    condition_id = "0x" + "a" * 64
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO condition_mappings(
                condition_id, primary_token_id, secondary_token_id, observed_at_ms
            ) VALUES(?, 'primary', 'secondary', 1)
            """,
            (condition_id,),
        )
        connection.execute(
            """
            INSERT INTO positions(token_id, quantity, cost_basis_usd, condition_id)
            VALUES('primary', '2', '0.8', ?)
            """,
            (condition_id,),
        )
        connection.execute(
            """
            INSERT INTO positions(token_id, quantity, cost_basis_usd, condition_id)
            VALUES('secondary', '1', '0.3', ?)
            """,
            (condition_id,),
        )
    allocation = {
        "primary_token_id": "primary",
        "secondary_token_id": "secondary",
        "primary_quantity": "2",
        "primary_cost_basis_usd": "0.8",
        "secondary_quantity": "1",
        "secondary_cost_basis_usd": "0.3",
        "payout_usd": "2",
        "inventory_hash": "frozen-inventory-hash",
    }

    assert store.apply_shared_condition_settlement(
        condition_id=condition_id,
        terminal_state="REDEEMED_SHARED_WALLET",
        allocation=allocation,
        transaction_hash="0x" + "b" * 64,
        created_at_ms=2,
    ) is True
    assert store.apply_shared_condition_settlement(
        condition_id=condition_id,
        terminal_state="REDEEMED_SHARED_WALLET",
        allocation=allocation,
        transaction_hash="0x" + "b" * 64,
        created_at_ms=3,
    ) is False
    account = store.account_snapshot()
    assert account["cash_usd"] == D("102")
    assert account["realized_pnl_usd"] == D("0.9")
    assert store.position_quantity("primary") == D("0")
    assert store.position_quantity("secondary") == D("0")
    assert store.redemption_receipt(condition_id)["state"] == (
        "REDEEMED_SHARED_WALLET"
    )


def test_proven_not_submitted_redemption_can_be_settled_from_exact_external_proof(
    tmp_path: Path,
):
    """A later platform REDEEM must not leave a proven payout off-ledger."""

    store, _ = _funded_resolved_position_store(tmp_path)
    condition_id = FakeRedemptionAdapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="NOT_SUBMITTED_RETRYABLE",
        reason="relayer did not accept a transaction",
        created_at_ms=3,
    )

    store.settle_externally_verified_redemption(
        condition_id=condition_id,
        payout_usd=D("10"),
        transaction_hash="0x" + "e" * 64,
        created_at_ms=4,
        details={
            "official_activity_type": "REDEEM",
            "onchain_outcome_balances_zero": True,
        },
    )

    assert store.redemption_receipt(condition_id)["state"] == "REDEEMED_EXTERNAL_VERIFIED"
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.account_snapshot()["realized_pnl_usd"] == D("6")


def test_platform_settled_winner_reconciles_only_when_authenticated_cash_delta_is_exact(
    tmp_path: Path,
):
    """A wallet-side settlement may credit CLOB collateral without our relayer tx."""

    store, _ = _funded_resolved_position_store(tmp_path)
    condition_id = FakeRedemptionAdapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="NOT_SUBMITTED_RETRYABLE",
        reason="RELAYER_NO_MARKET",
        created_at_ms=3,
    )
    adapter = FakeRedemptionAdapter(primary_raw=0)

    outcome = reconcile_platform_settled_winners(
        store=store,
        adapter=adapter,
        observed_collateral_usd=D("10"),
        created_at_ms=4,
    )

    assert outcome["state"] == "RECONCILED"
    assert outcome["condition_count"] == 1
    assert store.redemption_receipt(condition_id)["state"] == "REDEEMED_PLATFORM_SETTLEMENT_VERIFIED"
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.account_snapshot()["realized_pnl_usd"] == D("6")


def test_platform_settlement_reconciliation_never_mutates_on_a_non_exact_cash_delta(
    tmp_path: Path,
):
    """A coincidental top-up must not be misclassified as a strategy redemption."""

    store, _ = _funded_resolved_position_store(tmp_path)
    condition_id = FakeRedemptionAdapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="NOT_SUBMITTED_RETRYABLE",
        reason="RELAYER_NO_MARKET",
        created_at_ms=3,
    )
    adapter = FakeRedemptionAdapter(primary_raw=0)

    outcome = reconcile_platform_settled_winners(
        store=store,
        adapter=adapter,
        observed_collateral_usd=D("9.99"),
        created_at_ms=4,
    )

    assert outcome["state"] == "BLOCK_DELTA_MISMATCH"
    assert store.redemption_receipt(condition_id)["state"] == "NOT_SUBMITTED_RETRYABLE"
    assert store.position_quantity("123") == D("10")
    assert store.account_snapshot()["cash_usd"] == D("0")
    assert store.account_snapshot()["realized_pnl_usd"] == D("0")


def test_official_redeem_activity_reconciles_despite_an_unrelated_wallet_cash_delta(
    tmp_path: Path,
):
    """A separate deposit must not strand an exactly identified redemption."""

    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(primary_raw=0)
    adapter.confirmed_collateral_payout_raw = 10_000_000
    condition_id = adapter.condition_id
    store.record_redemption_terminal_without_submission(
        condition_id=condition_id,
        state="BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
        reason="legacy aggregate-wallet ambiguity",
        expected_payout_usd=D("0"),
        created_at_ms=2,
        details={},
    )

    outcome = live.reconcile_official_redeem_activities(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        official_activities=[
            {
                "proxyWallet": adapter.wallet_address,
                "timestamp": 20,
                "conditionId": condition_id,
                "type": "REDEEM",
                "usdcSize": 10,
                "transactionHash": "0x" + "d" * 64,
            },
            {
                "proxyWallet": adapter.wallet_address,
                "timestamp": 21,
                "conditionId": "",
                "type": "DEPOSIT",
                "usdcSize": 25,
                "transactionHash": "0x" + "e" * 64,
            },
        ],
        created_at_ms=30_000,
    )

    assert outcome["state"] == "RECONCILED_OFFICIAL_ACTIVITY"
    assert outcome["condition_count"] == 1
    assert outcome["cash_credited_usd"] == D("10")
    assert outcome["baseline_cash_reclassified_usd"] == D("0")
    assert store.position_quantity("123") == D("0")
    assert store.account_snapshot()["cash_usd"] == D("10")
    assert store.account_snapshot()["realized_pnl_usd"] == D("6")
    receipt = store.redemption_receipt(condition_id)
    assert receipt["state"] == "REDEEMED_OFFICIAL_ACTIVITY_VERIFIED"
    assert receipt["transaction_hash"] == "0x" + "d" * 64


def test_shared_official_redeem_cash_stays_quarantined_until_clob_collateral_arrives(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    peer = LiveStore(tmp_path / "peer.sqlite3")
    initialize_scale_once(
        store=peer,
        allocation_usd=D("4"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", store.path, "RESIDUAL"),
            SleeveSpec("peer", peer.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("4"),
        funder_address=FakeRedemptionAdapter.wallet_address,
        observed_at_ms=2,
    )
    adapter = FakeRedemptionAdapter(primary_raw=0)
    adapter.confirmed_collateral_payout_raw = 10_000_000
    store.record_redemption_terminal_without_submission(
        condition_id=adapter.condition_id,
        state="BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
        reason="legacy aggregate-wallet ambiguity",
        expected_payout_usd=D("0"),
        created_at_ms=2,
        details={},
    )

    outcome = live.reconcile_official_redeem_activities(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        official_activities=[
            {
                "proxyWallet": adapter.wallet_address,
                "timestamp": 20,
                "conditionId": adapter.condition_id,
                "type": "REDEEM",
                "usdcSize": 10,
                "transactionHash": "0x" + "d" * 64,
            }
        ],
        created_at_ms=30_000,
        quarantine_confirmed_cash_credit=True,
    )

    assert outcome["cash_credited_usd"] == D("10")
    pending = coordinator.authenticated_account_cash_snapshot(
        authenticated_collateral_usd=D("4")
    )
    assert pending.expected_accounting_cash_high_usd == D("4")
    assert pending.available_for_new_buy_usd == D("4")
    assert pending.redeemed_cash_credit_quarantine_usd == D("10")
    assert pending.state == "QUARANTINED_REDEEMED_CASH_CREDIT"

    coordinator.reconcile_redeemed_cash_credit_quarantines(
        authenticated_collateral_usd=D("14"), observed_at_ms=30_001
    )
    credited = coordinator.authenticated_account_cash_snapshot(
        authenticated_collateral_usd=D("14")
    )
    assert credited.expected_accounting_cash_high_usd == D("14")
    assert credited.available_for_new_buy_usd == D("14")


def test_pre_migration_official_redeem_reclassifies_cash_without_double_credit(
    tmp_path: Path,
):
    """A redemption already inside the frozen cash baseline is not paid twice."""

    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(primary_raw=0)
    adapter.confirmed_collateral_payout_raw = 10_000_000
    condition_id = adapter.condition_id
    # This reproduces a historical receipt written before strategy-ledger cash
    # stopped being a spendable-wallet concept.  New runtime code cannot write
    # this receipt; reconciliation still has to read it without double credit.
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO external_cash_reserve_receipts(
                observed_at_ms, observed_collateral_usd,
                ledger_cash_before_usd, credited_cash_usd, reason
            ) VALUES(20000, '10', '0', '10', 'HISTORICAL_AUDIT_ONLY')
            """
        )
        connection.execute(
            "UPDATE account_state SET cash_usd = '10' WHERE singleton = 1"
        )

    outcome = live.reconcile_official_redeem_activities(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        official_activities=[
            {
                "proxyWallet": adapter.wallet_address,
                "timestamp": 20,
                "conditionId": condition_id,
                "type": "REDEEM",
                "usdcSize": 10,
                "transactionHash": "0x" + "f" * 64,
            }
        ],
        created_at_ms=30_000,
        frozen_cash_baseline_at_ms=25_000,
    )

    assert outcome["state"] == "RECONCILED_OFFICIAL_ACTIVITY"
    assert outcome["cash_credited_usd"] == D("0")
    assert outcome["baseline_cash_reclassified_usd"] == D("10")
    account = store.account_snapshot()
    assert account["cash_usd"] == D("10")
    assert account["realized_pnl_usd"] == D("6")
    assert account["external_cash_reserve_usd"] == D("0")
    assert account["settlement_cash_reclassified_usd"] == D("10")
    assert account["total_capital_contributed_usd"] == D("4")
    assert store.position_quantity("123") == D("0")


def test_official_redeem_activity_requires_exact_condition_payout_and_wallet(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(primary_raw=0)

    outcome = live.reconcile_official_redeem_activities(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        official_activities=[
            {
                "proxyWallet": "0x" + "9" * 40,
                "timestamp": 20,
                "conditionId": adapter.condition_id,
                "type": "REDEEM",
                "usdcSize": 9.99,
                "transactionHash": "0x" + "1" * 64,
            }
        ],
        created_at_ms=30_000,
    )

    assert outcome["state"] == "NO_EXACT_OFFICIAL_REDEEM_MATCH"
    assert outcome["condition_count"] == 0
    assert store.position_quantity("123") == D("10")
    assert store.account_snapshot()["cash_usd"] == D("0")


def test_official_redeem_activity_does_not_credit_without_exact_onchain_collateral_payout(
    tmp_path: Path,
):
    """An activity row and zero outcome balances cannot substitute for tx payout proof."""

    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(primary_raw=0)
    adapter.confirmed_collateral_payout_raw = 0
    condition_id = adapter.condition_id
    store.record_redemption_terminal_without_submission(
        condition_id=condition_id,
        state="BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY",
        reason="legacy aggregate-wallet ambiguity",
        expected_payout_usd=D("0"),
        created_at_ms=2,
        details={},
    )

    outcome = live.reconcile_official_redeem_activities(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        official_activities=[
            {
                "proxyWallet": adapter.wallet_address,
                "timestamp": 20,
                "conditionId": condition_id,
                "type": "REDEEM",
                "usdcSize": 10,
                "transactionHash": "0x" + "d" * 64,
            }
        ],
        created_at_ms=30_000,
    )

    assert outcome["state"] == "BLOCK_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH"
    assert outcome["condition_count"] == 0
    assert outcome["payout_proof_mismatch_count"] == 1
    assert store.position_quantity("123") == D("10")
    assert store.account_snapshot()["cash_usd"] == D("0")
    receipt = store.redemption_receipt(condition_id)
    assert receipt["state"] == "BLOCK_OFFICIAL_REDEEM_ONCHAIN_COLLATERAL_PAYOUT_MISMATCH"
    assert receipt["transaction_hash"] == "0x" + "d" * 64


def test_official_redeem_mismatch_never_overwrites_an_unknown_submission_identity(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(primary_raw=0)
    condition_id = adapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_submission(
        condition_id=condition_id,
        transaction_id="redeem-1",
        transaction_hash="0x" + "c" * 64,
        created_at_ms=3,
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="UNKNOWN_SUBMISSION",
        reason="transport result uncertain",
        created_at_ms=4,
        transaction_hash="0x" + "c" * 64,
    )

    outcome = live.reconcile_official_redeem_activities(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        official_activities=[
            {
                "proxyWallet": adapter.wallet_address,
                "timestamp": 20,
                "conditionId": condition_id,
                "type": "REDEEM",
                "usdcSize": 10,
                "transactionHash": "0x" + "d" * 64,
            }
        ],
        created_at_ms=30_000,
    )

    assert outcome["state"] == "NO_EXACT_OFFICIAL_REDEEM_MATCH"
    assert outcome["external_error_count"] == 1
    receipt = store.redemption_receipt(condition_id)
    assert receipt["state"] == "UNKNOWN_SUBMISSION"
    assert receipt["transaction_hash"] == "0x" + "c" * 64
    assert store.account_snapshot()["cash_usd"] == D("0")


def test_redemption_cycle_records_authenticated_cash_and_reconciles_platform_settlement(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    condition_id = FakeRedemptionAdapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="NOT_SUBMITTED_RETRYABLE",
        reason="RELAYER_NO_MARKET",
        created_at_ms=3,
    )
    adapter = FakeRedemptionAdapter(primary_raw=0)
    collateral = FakeCollateralReader(balance="10")

    outcome = run_redemption_cycle(
        store=store,
        adapter=adapter,
        execution=collateral,
    )

    assert collateral.calls == 1
    assert outcome["platform_settlement"]["state"] == "RECONCILED"
    assert store.runtime_value("last_authenticated_collateral_usd") == "10"
    assert store.account_snapshot()["cash_usd"] == D("10")


def test_platform_settlement_uses_aggregate_shared_wallet_cash_not_one_sleeve_cash(
    tmp_path: Path,
):
    strategy, _ = _funded_resolved_position_store(tmp_path)
    peer = LiveStore(tmp_path / "peer.sqlite3")
    initialize_scale_once(
        store=peer,
        allocation_usd=D("10"),
        source_open_position_value_usd=D("10"),
        observed_at_ms=1,
    )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", strategy.path, "RESIDUAL"),
            SleeveSpec("peer", peer.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("10"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=2,
    )

    result = reconcile_platform_settled_winners(
        store=strategy,
        adapter=FakeRedemptionAdapter(primary_raw=0),
        observed_collateral_usd=D("20"),
        created_at_ms=3,
        coordinator=coordinator,
        profile_key="cd90",
    )

    assert result["state"] == "RECONCILED"


def test_platform_settlement_cash_mutation_refreshes_the_settled_sleeve_snapshot(
    tmp_path: Path,
):
    strategy, _ = _funded_resolved_position_store(tmp_path)
    peer = LiveStore(tmp_path / "peer.sqlite3")
    initialize_scale_once(
        store=peer,
        allocation_usd=D("10"),
        source_open_position_value_usd=D("10"),
        observed_at_ms=1,
    )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", strategy.path, "RESIDUAL"),
            SleeveSpec("peer", peer.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("10"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=2,
    )
    execution = FakeCollateralReader(balance="10")
    live._persist_authenticated_collateral_observation(
        store=strategy,
        observed_collateral_usd=D("10"),
        observed_at_ms=3,
        coordinator=coordinator,
        profile_key="cd90",
    )

    result = reconcile_platform_settled_winners(
        store=strategy,
        adapter=FakeRedemptionAdapter(primary_raw=0),
        observed_collateral_usd=D("20"),
        created_at_ms=4,
        coordinator=coordinator,
        profile_key="cd90",
    )
    assert result["state"] == "RECONCILED"
    execution.balance = D("20")

    snapshot = live._refresh_authenticated_collateral_after_cash_mutation(
        store=strategy,
        execution=execution,
        coordinator=coordinator,
        profile_key="cd90",
    )

    assert snapshot is not None
    assert snapshot.state == "NO_ACTIVE_CASH_HOLDS"
    assert strategy.runtime_value("last_authenticated_collateral_usd") == "20"
    assert D(
        strategy.runtime_value(
            "authenticated_account_cash_available_for_new_buy_usd"
        )
    ) == D("20")
    assert D(result["aggregate_wallet_cash_before_usd"]) == D("10")
    assert strategy.account_snapshot()["cash_usd"] == D("10")
    assert peer.account_snapshot()["cash_usd"] == D("10")


def test_audited_token_order_correction_allows_unknown_receipt_to_settle_as_loss(
    tmp_path: Path,
):
    store, _ = _funded_resolved_position_store(tmp_path)
    condition_id = FakeRedemptionAdapter.condition_id
    assert store.start_redemption_submission(
        condition_id=condition_id,
        expected_payout_usd=D("10"),
        created_at_ms=2,
        details={},
    )
    store.mark_redemption_terminal(
        condition_id=condition_id,
        state="UNKNOWN_SUBMISSION",
        reason="wrong outcome order",
        created_at_ms=3,
    )

    store.correct_condition_mapping_order(
        condition_id=condition_id,
        primary_token_id="456",
        secondary_token_id="123",
        created_at_ms=4,
        details={"source": "official_gamma_yes_no_order"},
    )
    store.settle_verified_losing_condition_from_terminal(
        condition_id=condition_id,
        created_at_ms=5,
        details={
            "winner_token_id": "456",
            "payout_numerators": [1, 0],
            "onchain_inventory_exact": True,
            "onchain_winner_balance_raw": 0,
        },
    )

    assert store.redemption_receipt(condition_id)["state"] == "LOSS_RESOLVED_NO_PAYOUT"
    assert store.account_snapshot()["cash_usd"] == D("0")
    assert store.account_snapshot()["realized_pnl_usd"] == D("-4")
    assert store.condition_inventory(condition_id) == []


def test_auto_redemption_refuses_to_touch_a_condition_with_extra_non_cd90_inventory(tmp_path: Path):
    store, _ = _funded_resolved_position_store(tmp_path)
    adapter = FakeRedemptionAdapter(primary_raw=11_000_000)

    result = auto_redeem_resolved_positions(
        store=store,
        adapter=adapter,
        wallet_address=adapter.wallet_address,
        live_enabled=True,
    )

    assert result == [{"condition_id": adapter.condition_id, "state": "BLOCK_AMBIGUOUS_ONCHAIN_INVENTORY"}]
    assert adapter.submissions == []


def test_book_failure_keeps_the_source_receipt_but_never_attempts_submission(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    execution = FailingSnapshotExecution()

    source = action()
    with pytest.raises(ConnectionError, match="book endpoint unavailable"):
        execute_source_action(
            store=store,
            source=source,
            execution=execution,
            allocated_cash=D("100"),
            live_enabled=True,
        )

    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    assert store.latest_transition(source)["reason"].startswith("BOOK_SNAPSHOT_ERROR")
    assert store.action_receipt_count() == 1
    assert execution.calls == []


def test_clob_adapter_submits_a_fak_market_order_with_buy_amount_derived_from_exact_scaled_shares():
    client = FakeCLOBClient()
    adapter = CLOBExecutionAdapter(
        client, minimum_marketable_buy_notional_usd=D("1")
    )

    snapshot = adapter.snapshot(token_id="123", side="BUY")
    response = adapter.submit_fak_market(
        token_id="123", side="BUY", price=D("0.40"), size=D("10")
    )

    assert snapshot["minimum_order_size"] == "5"
    assert snapshot["best_price"] == "0.40"
    assert snapshot["visible_best_level_size"] == "100"
    assert snapshot["fee_bps"] == "500.00"
    assert snapshot["fee_exponent"] == "1"
    assert response["orderID"] == "order-1"
    submitted = client.submissions[0]
    assert submitted["order_args"].price == 0.4
    assert submitted["order_args"].amount == 4.0
    assert str(submitted["order_type"]) == "FAK"


def test_clob_adapter_posts_multiple_active_cancel_orders_without_waiting(monkeypatch):
    client = FakeCLOBClient()

    class ReceiptReader:
        @staticmethod
        def latest_block_number():
            assert client.cancellations
            assert client.get_open_orders() == []
            return 101

    adapter = CLOBExecutionAdapter(
        client,
        minimum_marketable_buy_notional_usd=D("1"),
        receipt_reader=ReceiptReader(),
    )
    adapter.snapshot(token_id="123", side="BUY")

    waits = []
    monkeypatch.setattr(live.time, "sleep", waits.append)
    prepared = adapter.prepare_gtd_limit(
        token_id="123",
        side="BUY",
        price=D("0.40"),
        size=D("10"),
    )

    assert prepared["order_version"] == 2
    assert prepared["order_type"] == "GTC"
    assert prepared["order_fields"]["expiration"] == "0"
    assert prepared["order_id"].startswith("0x")
    assert len(prepared["order_id"]) == 66
    assert prepared["order_fields"]["salt"] == "123"
    assert "signature" not in prepared["order_fields"]
    adapter.snapshot(token_id="123", side="BUY")
    second = adapter.prepare_gtd_limit(
        token_id="123",
        side="BUY",
        price=D("0.40"),
        size=D("10"),
    )
    first_response = adapter.submit_prepared_gtd_limit(prepared)
    second_response = adapter.submit_prepared_gtd_limit(second)

    assert first_response["success"] is True
    assert second_response["success"] is True
    assert waits == []
    assert client.cancellations == []
    assert len(client.created_orders) == 2
    assert len(client.submissions) == 2

    first_cancel = adapter.cancel_active_gtd_order(first_response["orderID"])
    second_cancel = adapter.cancel_active_gtd_order(second_response["orderID"])
    assert first_cancel["active_cancel_observed_head_block"] == 101
    assert second_cancel["active_cancel_observed_head_block"] == 101
    assert client.cancellations == [
        first_response["orderID"],
        second_response["orderID"],
    ]
    assert client.get_open_orders() == []


def test_due_active_cancel_is_run_after_nonblocking_submission():
    first = action(marker="a")
    second = action(marker="b")

    class DueStore:
        def __init__(self):
            self.updated = []
            self.transitions = []

        def unreconciled_submissions(self):
            return [
                (
                    first,
                    {
                        "attempt_id": "attempt-a",
                        "order_id": "order-a",
                        "execution_order_type": "GTC_ACTIVE_CANCEL",
                        "response": {"active_cancel_due_at_ms": 100},
                    },
                ),
                (
                    second,
                    {
                        "attempt_id": "attempt-b",
                        "order_id": "order-b",
                        "execution_order_type": "GTC_ACTIVE_CANCEL",
                        "response": {"active_cancel_due_at_ms": 101},
                    },
                ),
            ]

        def update_attempt_state(self, **kwargs):
            self.updated.append(kwargs)

        def append_transition(self, **kwargs):
            self.transitions.append(kwargs)

    class Execution:
        def __init__(self):
            self.cancelled = []

        def cancel_active_gtd_order(self, order_id):
            self.cancelled.append(order_id)
            return {"active_cancel_verified": True}

    store = DueStore()
    execution = Execution()

    result = live.cancel_due_active_gtd_orders(
        store=store, execution=execution, due_at_ms=100
    )

    assert result == [{"order_id": "order-a", "terminal_status": "CANCELED"}]
    assert execution.cancelled == ["order-a"]
    assert store.updated[0]["attempt_id"] == "attempt-a"
    assert store.updated[0]["response"] == {
        "active_cancel_due_at_ms": 100,
        "active_cancel_verified": True,
    }
    assert store.transitions[0]["status"] == "ACTIVE_CANCEL_COMPLETED"


def test_partial_gtc_is_canceled_before_the_active_cancel_deadline():
    source = action(marker="partial")

    class PartialStore:
        def __init__(self):
            self.updated = []
            self.transitions = []

        def unreconciled_submissions(self):
            return [
                (
                    source,
                    {
                        "attempt_id": "attempt-partial",
                        "order_id": "order-partial",
                        "execution_order_type": "GTC_ACTIVE_CANCEL",
                        "plan": {"requested_quantity": "10"},
                        "response": {"active_cancel_due_at_ms": 1_000},
                    },
                )
            ]

        def update_attempt_state(self, **kwargs):
            self.updated.append(kwargs)

        def append_transition(self, **kwargs):
            self.transitions.append(kwargs)

    class Execution:
        def __init__(self):
            self.cancelled = []

        @staticmethod
        def get_order(order_id):
            assert order_id == "order-partial"
            return {
                "status": "MATCHED",
                "size_matched": "2",
                "original_size": "10",
            }

        def cancel_active_gtd_order(self, order_id):
            self.cancelled.append(order_id)
            return {
                "active_cancel_verified": True,
                "active_cancel_observed_head_block": 101,
            }

    store = PartialStore()
    execution = Execution()

    result = live.cancel_due_active_gtd_orders(
        store=store, execution=execution, due_at_ms=100
    )

    assert result == [{"order_id": "order-partial", "terminal_status": "CANCELED"}]
    assert execution.cancelled == ["order-partial"]
    assert store.updated[0]["response"]["active_cancel_trigger"] == (
        "PARTIAL_FILL_DETECTED"
    )
    assert store.updated[0]["response"][
        "active_cancel_trigger_matched_quantity"
    ] == "2"
    assert store.transitions[0]["reason"] == (
        "GTC_ACTIVE_CANCEL_PARTIAL_FILL_DETECTED"
    )


def test_zero_or_full_gtc_is_not_canceled_before_the_active_cancel_deadline():
    source = action(marker="not-partial")

    class Store:
        def unreconciled_submissions(self):
            return [
                (
                    source,
                    {
                        "attempt_id": "attempt-not-partial",
                        "order_id": "order-not-partial",
                        "execution_order_type": "GTC_ACTIVE_CANCEL",
                        "plan": {"requested_quantity": "10"},
                        "response": {"active_cancel_due_at_ms": 1_000},
                    },
                )
            ]

    for matched_quantity in ("0", "10"):
        class Execution:
            @staticmethod
            def get_order(_order_id):
                return {
                    "status": "MATCHED",
                    "size_matched": matched_quantity,
                    "original_size": "10",
                }

            @staticmethod
            def cancel_active_gtd_order(_order_id):
                raise AssertionError("zero/full fill must not cancel early")

        assert live.cancel_due_active_gtd_orders(
            store=Store(), execution=Execution(), due_at_ms=100
        ) == []


def test_order_hash_reconciliation_uses_the_canonical_order_filled_topic():
    from eth_abi import encode

    order_id = "0x" + "9" * 64

    class ReceiptReader:
        @staticmethod
        def finalized_block_number():
            return 101

        @staticmethod
        def order_fill_logs_range(**_kwargs):
            return [
                {
                    "topics": [
                        CANONICAL_ORDER_FILLED_TOPIC,
                        order_id,
                        "0x" + "0" * 64,
                        "0x" + "0" * 64,
                    ],
                    "data": "0x"
                    + encode(
                        [
                            "uint8",
                            "uint256",
                            "uint256",
                            "uint256",
                            "uint256",
                            "bytes32",
                            "bytes32",
                        ],
                        [
                            0,
                            123,
                            4_000_000,
                            10_000_000,
                            0,
                            b"\0" * 32,
                            b"\0" * 32,
                        ],
                    ).hex(),
                    "transactionHash": "0x" + "a" * 64,
                }
            ]

    adapter = CLOBExecutionAdapter(
        FakeCLOBClient(),
        minimum_marketable_buy_notional_usd=D("1"),
        receipt_reader=ReceiptReader(),
    )

    result = adapter.authoritative_order_hash_execution(
        source=action(),
        order_id=order_id,
    )

    assert result["quantity"] == D("10")
    assert result["notional_usd"] == D("4")
    assert result["vwap_price"] == D("0.4")
    assert result["scan_from_block"] == 100
    assert result["scan_to_block"] == 101
    assert result["finality"] == "polygon_finalized_block"


def test_websocket_liveness_timeout_is_independent_from_order_retry_policy():
    source = inspect.getsource(live._run_ws_new_head_service)

    assert "WS_NEW_HEAD_LIVENESS_TIMEOUT_SECONDS" in source
    assert "BOUNDED_RETRY_DEADLINE_MS" not in source


def test_collateral_balance_refuses_to_arm_when_any_returned_exchange_allowance_is_zero():
    adapter = CLOBExecutionAdapter(
        FakeCollateralClient(allowances={"exchange-a": "1", "exchange-b": "0"}),
        minimum_marketable_buy_notional_usd=D("1"),
    )

    with pytest.raises(RuntimeError, match="COLLATERAL_ALLOWANCE_NOT_GRANTED"):
        adapter.collateral_balance_usd()


def test_source_follower_bootstraps_at_the_current_head_without_backfilling_history(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    rpc = FakeRpc(head=100)
    follower = LiveSourceFollower(
        store=store,
        rpc=rpc,
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )

    result = follower.establish_forward_watermark()

    assert result == {"previous_head": None, "start_head": 100, "skipped_block_count": 0}
    assert store.runtime_value("last_processed_block") == "100"
    assert rpc.log_queries == []


def test_source_follower_restart_refuses_to_silently_skip_an_unpriced_source_action(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "80")
    rpc = FakeRpc(head=100)
    follower = LiveSourceFollower(
        store=store,
        rpc=rpc,
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )

    missed = action(quantity="2", marker="7")
    follower._new_source_actions = lambda **kwargs: [missed]
    with pytest.raises(LiveConfigurationError, match="LOSSLESS_HANDOFF_REQUIRED"):
        follower.establish_forward_watermark()

    assert store.runtime_value("last_processed_block") == "80"
    assert store.action_receipt_count() == 1
    assert store.latest_transition(missed)["terminal_status"] == (
        "PENDING_LOSSLESS_HANDOFF"
    )
    assert store.latest_transition(missed)["reason"] == "LOSSLESS_HANDOFF_REQUIRED"
    assert store.runtime_gap_receipt_count() == 1


def test_completed_lossless_handoff_actions_remain_auditable_but_not_unresolved(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    first = replace(action(marker="7"), block_number=81, log_index=1)
    second = replace(action(marker="8"), block_number=82, log_index=2)
    store.record_unpriced_runtime_gap(
        previous_processed_block=80,
        resume_head=100,
        actions=[first, second],
        detected_at_ms=1_700_000_000_000,
        reason="LOSSLESS_HANDOFF_REQUIRED",
        terminal_status="PENDING_LOSSLESS_HANDOFF",
        pricing_status="LOSSLESS_HANDOFF_REQUIRED",
    )

    assert store.lossless_handoff_failure_action_count() == 2
    assert store.unresolved_lossless_handoff_action_count() == 2

    store.append_transition(source=first, status="FILLED", reason="ORDER_FILLED")
    store.append_transition(
        source=second,
        status="SKIPPED",
        reason="PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
    )

    assert store.lossless_handoff_failure_action_count() == 2
    assert store.unresolved_lossless_handoff_action_count() == 0


def test_expired_forward_only_handoff_action_is_terminal_for_current_health(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    missed = replace(action(marker="9"), block_number=83, log_index=3)
    store.record_unpriced_runtime_gap(
        previous_processed_block=80,
        resume_head=100,
        actions=[missed],
        detected_at_ms=1_700_000_000_000,
        reason="LOSSLESS_HANDOFF_REQUIRED",
        terminal_status="PENDING_LOSSLESS_HANDOFF",
        pricing_status="LOSSLESS_HANDOFF_REQUIRED",
    )
    store.ensure_action_target(
        source=missed,
        proportional_quantity=D("1"),
        target_quantity=D("1"),
        state="EXPIRED_RETRY_WINDOW",
        reason="SOURCE_ACTION_RETRY_WINDOW_EXPIRED",
        updated_at_ms=1_700_000_000_001,
    )
    store.append_transition(
        source=missed,
        status="EXPIRED_RETRY_WINDOW",
        reason="SOURCE_ACTION_RETRY_WINDOW_EXPIRED",
    )

    assert store.lossless_handoff_failure_action_count() == 1
    assert store.unresolved_lossless_handoff_action_count() == 0


def test_source_follower_can_explicitly_record_a_pre_repair_internal_gap_and_advance(
    tmp_path: Path,
):
    """Recovery never sends a late order or hides the stale source action."""

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "80")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )
    missed = action(quantity="2", marker="9")
    follower._new_source_actions = lambda **_kwargs: [missed]

    result = follower.establish_forward_watermark(
        advance_after_recorded_internal_repair_gap=True
    )

    assert result == {
        "previous_head": 80,
        "start_head": 100,
        "skipped_block_count": 20,
        "unpriced_source_action_count": 1,
        "pre_repair_internal_gap_action_count": 1,
    }
    assert store.runtime_value("last_processed_block") == "100"
    assert store.latest_transition(missed)["terminal_status"] == "ERROR_INTERNAL"
    assert store.latest_transition(missed)["reason"] == (
        "PRE_REPAIR_INTERNAL_UNPRICED_GAP_NO_ACTION_TIME_CLOB"
    )
    assert missed.action_id not in {
        pending.action_id for pending in store.retryable_actions()
    }
    with store.connect() as connection:
        gap = connection.execute(
            """
            SELECT reason, pricing_status, source_action_count
            FROM runtime_gap_receipts
            """
        ).fetchone()
        runtime_error = connection.execute(
            """
            SELECT category, message
            FROM runtime_errors
            ORDER BY id DESC LIMIT 1
            """
        ).fetchone()
    assert dict(gap) == {
        "reason": "PRE_REPAIR_INTERNAL_UNPRICED_GAP_NO_ACTION_TIME_CLOB",
        "pricing_status": "PRE_REPAIR_INTERNAL_UNPRICED_NO_ACTION_TIME_CLOB",
        "source_action_count": 1,
    }
    assert dict(runtime_error) == {
        "category": "INTERNAL_PRE_REPAIR_FORWARD_RECOVERY",
        "message": "PRE_REPAIR_INTERNAL_UNPRICED_GAP_NO_ACTION_TIME_CLOB",
    }


def test_restart_gap_does_not_reclassify_an_already_terminal_action_as_unpriced(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "80")
    already_filled = action(quantity="2", marker="8")
    assert store.record_action_receipt(already_filled) is True
    store.append_transition(
        source=already_filled,
        status="FILLED",
        reason="OFFICIAL_ONCHAIN_FILL_RECEIPT",
    )
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )
    follower._new_source_actions = lambda **kwargs: [already_filled]

    result = follower.establish_forward_watermark()

    assert result["unpriced_source_action_count"] == 0
    assert store.unpriced_gap_action_count() == 0
    assert store.latest_transition(already_filled)["terminal_status"] == "FILLED"


def test_websocket_subscription_error_is_retryable_external():
    with pytest.raises(ConnectionError, match="WS_SUBSCRIPTION_REJECTED"):
        live.parse_ws_subscription_ack(
            {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "busy"}}
        )


def test_websocket_connect_http_429_retries_inside_the_existing_daemon():
    class InvalidStatus(Exception):
        def __init__(self):
            self.response = SimpleNamespace(status_code=429)

    failure = InvalidStatus()

    assert live._is_retryable_external_error(failure) is True
    assert live._ws_connect_exception_decision(
        failure,
        default_process_exception=lambda exc: exc,
    ) is None


def test_websocket_connect_http_403_remains_fatal():
    class InvalidStatus(Exception):
        def __init__(self):
            self.response = SimpleNamespace(status_code=403)

    failure = InvalidStatus()

    assert live._is_retryable_external_error(failure) is False
    assert live._ws_connect_exception_decision(
        failure,
        default_process_exception=lambda exc: exc,
    ) is failure


def test_source_follower_restart_moves_to_current_head_instead_of_repricing_old_events(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "80")
    rpc = FakeRpc(head=100)
    follower = LiveSourceFollower(
        store=store,
        rpc=rpc,
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )

    result = follower.establish_forward_watermark()

    assert result == {
        "previous_head": 80,
        "start_head": 100,
        "skipped_block_count": 20,
        "unpriced_source_action_count": 0,
    }
    assert store.runtime_value("last_processed_block") == "100"
    assert store.runtime_value("restart_skipped_block_count") == "20"
    assert rpc.log_queries == [
        (81, 100, "0x" + "a" * 40, "maker"),
    ]


def test_source_follower_accepts_a_websocket_announced_head_without_polling_latest_head(tmp_path: Path):
    class NoPollingRpc(FakeRpc):
        def latest_block_number(self):
            raise AssertionError("websocket path must not poll latest head")

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    follower = LiveSourceFollower(
        store=store,
        rpc=NoPollingRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )

    result = follower.run_cycle_to_head(
        head=100,
        execution=FakeExecution(),
        live_enabled=True,
    )

    assert result == {
        "source_action_count": 0,
        "source_action_ids": [],
        "last_processed_block": 100,
        "current_head": 100,
    }


def test_source_follower_records_definitive_profile_skip_without_submitting(
    tmp_path: Path,
):
    class RejectScope:
        def resolve(self, token_id):
            assert token_id == "123"
            return ScopeDecision(
                follow=False,
                reason="SCOPE_EXCLUDED_NOT_MAINLINE",
                metadata={
                    "condition_id": "0x" + "1" * 64,
                    "market_slug": "atp-a-b-set-handicap",
                    "event_slug": "atp-a-b",
                },
            )

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=RejectScope(),
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = FakeExecution()

    result = follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert result["source_action_count"] == 1
    assert execution.calls == []
    assert store.runtime_value("last_processed_block") == "100"
    assert store.latest_transition(source)["terminal_status"] == "SKIPPED"
    assert store.latest_transition(source)["reason"] == "SCOPE_EXCLUDED_NOT_MAINLINE"


def test_source_follower_passes_the_full_action_to_action_aware_scope(
    tmp_path: Path,
):
    source = action()

    class ActionAwareScope:
        def resolve_action(self, candidate):
            assert candidate is source
            return ScopeDecision(
                follow=False,
                reason="SCOPE_EXCLUDED_AT_OR_AFTER_OFFICIAL_GAME_START",
                metadata={
                    "condition_id": "condition-123",
                    "market_slug": "atp-a-b",
                    "event_slug": "atp-a-b",
                    "source_stage": "AT_OR_AFTER_OFFICIAL_GAME_START",
                },
            )

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=ActionAwareScope(),
    )
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = FakeExecution()

    result = follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert result["source_action_count"] == 1
    assert execution.calls == []
    assert store.latest_transition(source)["terminal_status"] == "SKIPPED"
    assert store.latest_transition(source)["reason"] == (
        "SCOPE_EXCLUDED_AT_OR_AFTER_OFFICIAL_GAME_START"
    )


def test_source_follower_executes_an_action_accepted_by_the_profile(tmp_path: Path):
    class AcceptScope:
        def resolve(self, token_id):
                return ScopeDecision(
                follow=True,
                reason="ATP_WTA_MAINLINE_ELIGIBLE",
                metadata={
                    "condition_id": "0x" + "1" * 64,
                    "market_slug": "atp-a-b",
                    "event_slug": "atp-a-b",
                },
            )

    store = LiveStore(tmp_path / "live.sqlite3")
    lock_path = tmp_path / "authenticated-wallet.lock"

    class LockAwareExecution(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            with lock_path.open("a+") as competing_handle:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(
                        competing_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                    )
            return super().snapshot(token_id=token_id, side=side)

    initialize_scale_once(
        store=store,
        allocation_usd=D("300"),
        source_open_position_value_usd=D("600"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=AcceptScope(),
        wallet_lock_path=lock_path,
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = LockAwareExecution()

    follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert execution.calls == [
        {
            "token_id": "123",
            "side": "BUY",
            "price": D("0.40"),
            "size": D("20"),
            "user_usdc_balance": D("100"),
        }
    ]
    assert store.latest_transition(source)["terminal_status"] == "SUBMITTED_UNRECONCILED"


def test_wallet44b0_non_netflix_notice_records_alert_without_submitting_order(
    tmp_path: Path,
):
    class FullWalletScope:
        def resolve(self, token_id):
            assert token_id == "123"
            return ScopeDecision(
                follow=False,
                reason="SCOPE_EXCLUDED_NON_NETFLIX",
                metadata={
                    "condition_id": "0x" + "1" * 64,
                    "market_slug": "will-trump-say-tariff",
                    "event_slug": "trump-speech",
                    "question": "Will Trump say tariff?",
                    "topic_classification": "NON_NETFLIX",
                },
            )

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("200"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=FullWalletScope(),
        profile_key=live.LIVE_PROFILE_WALLET_44B0_NETFLIX,
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = FakeExecution()

    follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert execution.calls == []
    assert store.latest_transition(source)["terminal_status"] == "SKIPPED"
    assert store.latest_transition(source)["reason"] == (
        "SCOPE_EXCLUDED_NON_NETFLIX"
    )
    alerts = store.source_topic_alerts(unacknowledged_only=True)
    assert [row["action_id"] for row in alerts] == [source.action_id]
    status = live._status_payload(store)
    assert status["source_topic_alerts"]["unacknowledged_count"] == 1


def test_wallet44b0_netflix_action_executes_without_topic_alert(tmp_path: Path):
    class NetflixScope:
        def resolve(self, token_id):
            assert token_id == "123"
            return ScopeDecision(
                follow=True,
                reason="NETFLIX_ACTION_ELIGIBLE",
                metadata={
                    "condition_id": "0x" + "1" * 64,
                    "market_slug": "netflix-top-show-next-week",
                    "event_slug": "netflix-top-show-next-week",
                    "question": "Will Netflix have the top show next week?",
                    "topic_classification": "NETFLIX",
                },
            )

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("200"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=NetflixScope(),
        profile_key=live.LIVE_PROFILE_WALLET_44B0_NETFLIX,
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = FakeExecution()

    follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert execution.calls == [
        {
            "token_id": "123",
            "side": "BUY",
            "price": D("0.40"),
            "size": D("20"),
            "user_usdc_balance": D("100"),
        }
    ]
    assert store.latest_transition(source)["terminal_status"] == (
        "SUBMITTED_UNRECONCILED"
    )
    assert store.source_topic_alerts(unacknowledged_only=False) == []


def test_retryable_profile_metadata_failure_is_durable_before_cursor_advance(
    tmp_path: Path,
):
    class UnavailableScope:
        def resolve(self, _token_id):
            raise ConnectionError("GAMMA_METADATA_UNAVAILABLE")

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=UnavailableScope(),
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]

    result = follower.run_cycle_to_head(
        head=100,
        execution=FakeExecution(),
        live_enabled=True,
    )

    assert result["source_action_ids"] == [source.action_id]
    assert store.runtime_value("last_processed_block") == "100"
    assert store.latest_transition(source)["terminal_status"] == "PENDING_METADATA"


def test_retryable_book_failure_is_durable_before_cursor_advance(
    tmp_path: Path,
):
    class RecoveringExecution(FakeExecution):
        def __init__(self):
            super().__init__()
            self.fail = True

        def snapshot(self, *, token_id: str, side: str):
            if self.fail:
                raise ConnectionError("temporary book transport failure")
            return super().snapshot(token_id=token_id, side=side)

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = RecoveringExecution()

    result = follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )
    assert result["source_action_ids"] == [source.action_id]
    assert store.runtime_value("last_processed_block") == "100"
    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    assert execution.calls == []


def test_no_ask_book_failure_is_durable_before_cursor_advance(
    tmp_path: Path,
):
    class RecoveringExecution(FakeExecution):
        def __init__(self):
            super().__init__()
            self.fail = True

        def snapshot(self, *, token_id: str, side: str):
            if self.fail:
                raise RuntimeError("NO_ASK_BOOK_LEVEL")
            return super().snapshot(token_id=token_id, side=side)

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = RecoveringExecution()

    result = follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert result["source_action_ids"] == [source.action_id]
    assert store.runtime_value("last_processed_block") == "100"
    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    assert store.submission_attempt_count(source.action_id) == 0
    assert execution.calls == []



def test_reopen_no_book_error_preserves_history_and_restores_retryable_state(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="ERROR",
        reason="BOOK_SNAPSHOT_ERROR: RuntimeError: NO_ASK_BOOK_LEVEL",
        created_at_ms=2,
    )

    reopened = store.reopen_pre_submission_no_book_errors(created_at_ms=3)

    assert reopened == [source.action_id]
    assert store.action_target(source.action_id)["state"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "PENDING_EXTERNAL_RETRY"
    assert latest["reason"] == "REPAIRED_EMPTY_BOOK_RETRYABLE"
    with store.connect() as connection:
        errors = connection.execute(
            "SELECT category, details_json FROM runtime_errors"
        ).fetchall()
    assert [(row["category"], json.loads(row["details_json"])["action_id"])
            for row in errors] == [
        ("INTERNAL_ACTION_STATE_REPAIR", source.action_id)
    ]
    assert store.reopen_pre_submission_no_book_errors(created_at_ms=4) == []


def test_reopen_no_orderbook_404_preserves_history_and_restores_retryable_state(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="ERROR",
        reason=(
            "BOOK_SNAPSHOT_ERROR: PolyApiException: "
            "PolyApiException[status_code=404, error_message="
            "No orderbook exists for the requested token id]"
        ),
        created_at_ms=2,
    )

    reopened = store.reopen_pre_submission_no_book_errors(created_at_ms=3)

    assert reopened == [source.action_id]
    assert store.action_target(source.action_id)["state"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_EXTERNAL_RETRY"
    )


def test_reopen_no_book_error_refuses_an_unresolved_submission(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        updated_at_ms=1,
    )
    _begin_test_submission_attempt(
        store=store,
        source=source,
        requested_quantity=D("10"),
        snapshot={"best_price": "0.40"},
        created_at_ms=3,
    )
    store.append_transition(
        source=source,
        status="ERROR",
        reason="BOOK_SNAPSHOT_ERROR: RuntimeError: NO_ASK_BOOK_LEVEL",
        created_at_ms=2,
    )

    assert store.reopen_pre_submission_no_book_errors(created_at_ms=4) == []
    assert store.latest_transition(source)["terminal_status"] == "ERROR"


def test_frozen_scope_eligibility_is_retained_after_book_failure(
    tmp_path: Path,
):
    class ChangingScope:
        def __init__(self):
            self.calls = 0

        def resolve(self, _token_id):
            self.calls += 1
            return ScopeDecision(
                follow=self.calls == 1,
                reason=(
                    "PREMATCH_AT_SOURCE_TIME"
                    if self.calls == 1
                    else "NOW_AFTER_MATCH_START"
                ),
                metadata={
                    "condition_id": "condition-123",
                    "market_slug": "atp-a-b",
                    "event_slug": "atp-a-b",
                    "scope_call": self.calls,
                },
            )

    class RecoveringExecution(FakeExecution):
        def __init__(self):
            super().__init__()
            self.fail = True

        def snapshot(self, *, token_id: str, side: str):
            if self.fail:
                raise ConnectionError("temporary book transport failure")
            return super().snapshot(token_id=token_id, side=side)

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    scope = ChangingScope()
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=scope,
    )
    source = action()
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = RecoveringExecution()

    follower.run_cycle_to_head(
        head=100,
        execution=execution,
        live_enabled=True,
    )

    assert scope.calls == 1
    assert store.latest_transition(source)["terminal_status"] == (
        "PENDING_EXTERNAL_RETRY"
    )
    assert store.runtime_value("last_processed_block") == "100"


def test_shared_wallet_submission_lock_excludes_another_process_descriptor(
    tmp_path: Path,
):
    lock_path = tmp_path / "authenticated-wallet.lock"

    with _shared_wallet_submission_lock(lock_path):
        with lock_path.open("a+") as competing_handle:
            with pytest.raises(BlockingIOError):
                fcntl.flock(
                    competing_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                )


def test_redemption_cycle_uses_the_same_shared_wallet_lock_as_order_submission(
    tmp_path: Path,
):
    lock_path = tmp_path / "authenticated-wallet.lock"
    store, _ = _funded_resolved_position_store(tmp_path)

    class LockAwareRedemptionAdapter(FakeRedemptionAdapter):
        def condition_resolution(self, condition_id: str):
            with lock_path.open("a+") as competing_handle:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(
                        competing_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                    )
            return super().condition_resolution(condition_id)

    run_redemption_cycle(
        store=store,
        adapter=LockAwareRedemptionAdapter(),
        wallet_lock_path=lock_path,
    )


def test_isolated_sleeve_never_uses_authenticated_cash_above_its_allocation(
    tmp_path: Path,
):
    class RichAccountExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("1000")

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "visible_best_level_size": "1000",
            }

    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("300"),
        source_open_position_value_usd=D("300"),
        observed_at_ms=1,
    )
    execution = RichAccountExecution()

    result = execute_source_action(
        store=store,
        source=action(quantity="750"),
        execution=execution,
        allocated_cash=D("300"),
        live_enabled=True,
        wallet_lock_path=tmp_path / "authenticated-wallet.lock",
    )

    assert result["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "BUY",
            "price": D("0.40"),
            "size": D("750"),
            "user_usdc_balance": D("1000"),
        }
    ]


def test_shared_account_buy_ignores_strategy_budget_and_strategy_cash(
    tmp_path: Path,
):
    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    initialize_scale_once(
        store=cd90,
        allocation_usd=D("80"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    initialize_scale_once(
        store=tennis,
        allocation_usd=D("40"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address="0x" + "f" * 40,
        observed_at_ms=2,
    )

    class CoordinatedExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("100")

        def condition_mapping_for_token(self, token_id: str):
            assert token_id in {"123", "456"}
            return {
                "condition_id": "0x" + "b" * 64,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "condition_id": "0x" + "b" * 64,
                "visible_best_level_size": "120",
            }

    execution = CoordinatedExecution()
    result = execute_source_action(
        store=tennis,
        source=action(quantity="300"),
        execution=execution,
        allocated_cash=D("1"),
        live_enabled=True,
        coordinator=coordinator,
        profile_key="tennis_atp_wta_mainline",
    )

    assert result == {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "BUY",
            "price": D("0.40"),
            "size": D("120"),
            "user_usdc_balance": D("100"),
        }
    ]
    assert tennis.account_snapshot()["cash_usd"] == D("40")


@pytest.mark.parametrize(
    "profile_key",
    (
        live.LIVE_PROFILE_CD90,
        live.LIVE_PROFILE_TENNIS_MAINLINE,
        live.LIVE_PROFILE_WALLET_44B0_NETFLIX,
    ),
)
def test_every_live_profile_uses_shared_wallet_cash_without_rebasing_scale(
    tmp_path: Path,
    profile_key: str,
):
    residual = LiveStore(tmp_path / "residual.sqlite3")
    target = LiveStore(tmp_path / f"{profile_key}.sqlite3")
    initialize_scale_once(
        store=residual,
        allocation_usd=D("80"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    initialize_scale_once(
        store=target,
        allocation_usd=D("40"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    target.lock_config_once("profile_key", profile_key)
    original_scale = target.fixed_share_scale()
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cash_residual_profile", residual.path, "RESIDUAL"),
            SleeveSpec(profile_key, target.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address="0x" + "f" * 40,
        observed_at_ms=2,
    )

    class CoordinatedExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("100")

        def condition_mapping_for_token(self, token_id: str):
            assert token_id in {"123", "456"}
            return {
                "condition_id": "0x" + "b" * 64,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "condition_id": "0x" + "b" * 64,
                "visible_best_level_size": "120",
            }

    execution = CoordinatedExecution()
    source = action(quantity="300")
    submitted = execute_source_action(
        store=target,
        source=source,
        execution=execution,
        allocated_cash=None,
        live_enabled=True,
        coordinator=coordinator,
        profile_key=profile_key,
    )

    assert submitted == {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "BUY",
            "price": D("0.40"),
            "size": D("120"),
            "user_usdc_balance": D("100"),
        }
    ]
    _set_authoritative_fill(
        execution=execution,
        quantity=D("120"),
        notional_usd=D("48"),
        vwap_price=D("0.40"),
    )

    assert reconcile_submitted_actions(store=target, execution=execution) == [
        {
            "terminal_status": "FILLED",
            "reason": "OFFICIAL_ONCHAIN_FILL_RECEIPT",
        }
    ]
    assert target.account_snapshot()["cash_usd"] == D("-8")
    assert target.fixed_share_scale() == original_scale
    snapshot = coordinator.authenticated_account_cash_snapshot(
        authenticated_collateral_usd=D("52"),
    )
    assert snapshot.expected_accounting_cash_high_usd == D("52")
    assert snapshot.available_for_new_buy_usd == D("52")


def test_future_buy_without_authenticated_wallet_capacity_is_terminal_not_pending(
    tmp_path: Path,
):
    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    initialize_scale_once(
        store=cd90,
        allocation_usd=D("80"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    initialize_scale_once(
        store=tennis,
        allocation_usd=D("40"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address="0x" + "f" * 40,
        observed_at_ms=2,
    )

    class CoordinatedExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("100")

        def condition_mapping_for_token(self, token_id: str):
            assert token_id in {"123", "456"}
            return {
                "condition_id": "0x" + "b" * 64,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "condition_id": "0x" + "b" * 64,
                "visible_best_level_size": "1000",
            }

    execution = CoordinatedExecution()
    source = action(quantity="1000")
    result = execute_source_action(
        store=tennis,
        source=source,
        execution=execution,
        allocated_cash=None,
        live_enabled=True,
        coordinator=coordinator,
        profile_key="tennis_atp_wta_mainline",
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "INSUFFICIENT_AUTHENTICATED_ACCOUNT_CASH_AT_DISCOVERY",
    }
    assert execution.calls == []
    assert tennis.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"
    assert source.action_id not in {
        pending.action_id for pending in tennis.retryable_actions()
    }


def test_stale_forward_retry_target_fails_closed(
    tmp_path: Path,
):
    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    initialize_scale_once(
        store=cd90,
        allocation_usd=D("80"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    initialize_scale_once(
        store=tennis,
        allocation_usd=D("40"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address="0x" + "f" * 40,
        observed_at_ms=2,
    )

    class CoordinatedExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("100")

        def condition_mapping_for_token(self, token_id: str):
            assert token_id in {"123", "456"}
            return {
                "condition_id": "0x" + "b" * 64,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "condition_id": "0x" + "b" * 64,
                "visible_best_level_size": "1000",
            }

    source = action(quantity="1000", marker="d")
    assert tennis.record_action_receipt(source)
    tennis.ensure_action_target(
        source=source,
        proportional_quantity=D("400"),
        target_quantity=D("400"),
        state="PENDING_LIQUIDITY",
        reason="FAK_ZERO_FILL_RETRYABLE",
        updated_at_ms=3,
    )
    tennis.append_transition(
        source=source,
        status="PENDING_LIQUIDITY",
        reason="FAK_ZERO_FILL_RETRYABLE",
    )
    execution = CoordinatedExecution()

    result = execute_source_action(
        store=tennis,
        source=source,
        execution=execution,
        allocated_cash=None,
        live_enabled=True,
        coordinator=coordinator,
        profile_key="tennis_atp_wta_mainline",
    )

    assert result == {
        "terminal_status": "ERROR_INTERNAL",
        "reason": "INTERNAL_STALE_CAUSAL_TARGET",
    }
    assert execution.calls == []


def test_pending_capital_action_is_not_replayed_against_a_new_book(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action()
    assert store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        created_at_ms=1,
    )

    class NewBookMustNotBeRead(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("historical capital action must not be repriced")

    assert retry_pending_actions(
        store=store,
        execution=NewBookMustNotBeRead(),
    ) == []
    assert store.action_target(source.action_id)["state"] == "PENDING_CAPITAL"


def test_pending_capital_target_is_immutable_on_direct_execution_request(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(marker="7")
    assert store.record_action_receipt(source)
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        updated_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="PENDING_CAPITAL",
        reason="INSUFFICIENT_AVAILABLE_CASH",
        created_at_ms=1,
    )
    with store.connect() as connection:
        before_transition_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM action_transitions WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()[0]
        )

    class NewBookMustNotBeRead(FakeExecution):
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("historical capital action must not be repriced")

    assert execute_source_action(
        store=store,
        source=source,
        execution=NewBookMustNotBeRead(),
        allocated_cash=D("100"),
        live_enabled=True,
    ) == {"terminal_status": "PENDING_CAPITAL", "reason": "INSUFFICIENT_AVAILABLE_CASH"}
    assert store.action_target(source.action_id)["state"] == "PENDING_CAPITAL"
    latest = store.latest_transition(source)
    assert latest is not None
    assert latest["terminal_status"] == "PENDING_CAPITAL"
    assert latest["reason"] == "INSUFFICIENT_AVAILABLE_CASH"
    with store.connect() as connection:
        assert int(
            connection.execute(
                "SELECT COUNT(*) FROM action_transitions WHERE action_id = ?",
                (source.action_id,),
            ).fetchone()[0]
        ) == before_transition_count


def test_sell_remains_executable_when_strategy_attribution_cash_is_negative(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("100"),
        observed_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE account_state SET cash_usd = '-8' WHERE singleton = 1"
        )
        connection.execute(
            """
            INSERT INTO positions(token_id, quantity, cost_basis_usd)
            VALUES('123', '10', '48')
            """
        )
    source = action(side="SELL", quantity="10", marker="8")
    execution = FakeExecution()

    assert execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    ) == {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    assert execution.calls == [
        {
            "token_id": "123",
            "side": "SELL",
            "price": D("0.30"),
            "size": D("10"),
        }
    ]


def test_shared_coordinator_blocks_cross_sleeve_token_ownership_before_buy(
    tmp_path: Path,
):
    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    for store in (cd90, tennis):
        initialize_scale_once(
            store=store,
            allocation_usd=D("50"),
            source_open_position_value_usd=D("100"),
            observed_at_ms=1,
        )
    with tennis.connect() as connection:
        connection.execute(
            "INSERT INTO positions(token_id, quantity, cost_basis_usd) VALUES('123', '1', '0.4')"
        )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address="0x" + "f" * 40,
        observed_at_ms=2,
    )

    class CoordinatedExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("100")

        def condition_mapping_for_token(self, token_id: str):
            assert token_id == "123"
            return {
                "condition_id": "0x" + "b" * 64,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "condition_id": "0x" + "b" * 64,
            }

    execution = CoordinatedExecution()
    source = action(quantity="10")
    with pytest.raises(
        live.SharedWalletCoordinatorError,
        match="BLOCK_CROSS_SLEEVE_TOKEN_OWNERSHIP",
    ):
        execute_source_action(
            store=cd90,
            source=source,
            execution=execution,
            allocated_cash=None,
            live_enabled=True,
            coordinator=coordinator,
            profile_key="cd90",
        )

    assert cd90.latest_transition(source)["terminal_status"] == (
        "PENDING_INTERNAL_INVARIANT"
    )
    assert cd90.latest_transition(source)["reason"] == (
        "BLOCK_CROSS_SLEEVE_TOKEN_OWNERSHIP"
    )
    with cd90.connect() as connection:
        error_count = connection.execute(
            "SELECT COUNT(*) AS count FROM runtime_errors"
        ).fetchone()["count"]
    assert int(error_count) == 1
    assert execution.calls == []


def test_coordinated_buy_fill_persists_condition_ownership_for_other_sleeves(
    tmp_path: Path,
):
    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    for store in (cd90, tennis):
        initialize_scale_once(
            store=store,
            allocation_usd=D("50"),
            source_open_position_value_usd=D("100"),
            observed_at_ms=1,
        )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("100"),
        funder_address="0x" + "f" * 40,
        observed_at_ms=2,
    )
    condition_id = "0x" + "b" * 64

    class CoordinatedExecution(FakeExecution):
        def collateral_balance_usd(self):
            return D("100")

        def condition_mapping_for_token(self, token_id: str):
            assert token_id in {"123", "456"}
            return {
                "condition_id": condition_id,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                "condition_id": condition_id,
                "minimum_order_size": "5",
                    "minimum_marketable_buy_notional_usd": "1",
                    "best_price": "0.40" if side == "BUY" else "0.30",
                    "tick_size": "0.01",
                    "visible_best_level_size": "100",
                "fee_bps": "0",
                "raw_book": {"asks": [], "bids": []},
            }

    execution = CoordinatedExecution()
    source = action(quantity="10")
    execute_source_action(
        store=tennis,
        source=source,
        execution=execution,
        allocated_cash=None,
        live_enabled=True,
        coordinator=coordinator,
        profile_key="tennis_atp_wta_mainline",
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("5"),
        notional_usd=D("2"),
        vwap_price=D("0.40"),
    )

    reconcile_submitted_actions(store=tennis, execution=execution)

    with tennis.connect() as connection:
        row = connection.execute(
            "SELECT condition_id FROM positions WHERE token_id = '123'"
        ).fetchone()
    assert row is not None and row["condition_id"] == condition_id
    collision = coordinator.buy_collision(
        profile_key="cd90",
        token_id="456",
        condition_id=condition_id,
    )
    assert collision["state"] == "CLEAR_SHARED_CONDITION"

    complementary = replace(source, token_id="456", transaction_hash="0x" + "8" * 64)
    second = execute_source_action(
        store=cd90,
        source=complementary,
        execution=execution,
        allocated_cash=None,
        live_enabled=True,
        coordinator=coordinator,
        profile_key="cd90",
    )
    assert second["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert cd90.latest_transition(complementary)["terminal_status"] != (
        "PENDING_INTERNAL_INVARIANT"
    )


def test_store_initialize_does_not_rewrite_existing_position_condition_metadata(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize()
    condition_id = "0x" + "c" * 64
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO positions(token_id, quantity, cost_basis_usd)
            VALUES('123', '5', '2')
            """
        )
        connection.execute(
            """
            INSERT INTO condition_mappings(
                condition_id, primary_token_id, secondary_token_id, observed_at_ms
            ) VALUES(?, '123', '456', 1)
            """,
            (condition_id,),
        )
    store._initialized = False

    store.initialize()

    with store.connect() as connection:
        row = connection.execute(
            "SELECT condition_id FROM positions WHERE token_id = '123'"
        ).fetchone()
    assert row is not None and row["condition_id"] == ""


def test_identical_external_retry_transition_is_singleton_and_timestamped_at_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    assert store.record_action_receipt(source)
    store.append_transition(source=source, status="OBSERVED")

    monkeypatch.setattr(live, "now_ms", lambda: 123_456)
    store.append_transition(
        source=source,
        status="PENDING_EXTERNAL_RETRY",
        reason="BOOK_SNAPSHOT_ERROR: PolyApiException: exact 404",
    )
    monkeypatch.setattr(live, "now_ms", lambda: 123_999)
    store.append_transition(
        source=source,
        status="PENDING_EXTERNAL_RETRY",
        reason="BOOK_SNAPSHOT_ERROR: PolyApiException: exact 404",
    )

    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT status, reason, created_at_ms
            FROM action_transitions
            WHERE action_id = ? AND status = 'PENDING_EXTERNAL_RETRY'
            """,
            (source.action_id,),
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["reason"] == "BOOK_SNAPSHOT_ERROR: PolyApiException: exact 404"
    assert rows[0]["created_at_ms"] == 123_456


def test_in_process_websocket_reconnect_resumes_the_existing_cursor_without_a_new_gap(
    tmp_path: Path,
):
    class NoRewatermarkRpc(FakeRpc):
        def latest_block_number(self):
            raise AssertionError("in-process reconnect must not rebuild the watermark")

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    follower = LiveSourceFollower(
        store=store,
        rpc=NoRewatermarkRpc(head=101),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )

    result = live._bootstrap_ws_connection(
        follower=follower,
        initial_connection=False,
    )

    assert result == {
        "previous_head": 100,
        "start_head": 100,
        "skipped_block_count": 0,
        "resumed_in_process": True,
    }
    assert store.runtime_value("last_processed_block") == "100"
    assert store.runtime_gap_receipt_count() == 0
    assert store.runtime_value("ws_resume_mode") == "IN_PROCESS_CURSOR_RESUME"


def test_hot_standby_joins_a_lagging_cursor_without_rewatermarking_it(
    tmp_path: Path,
):
    class NoRewatermarkRpc(FakeRpc):
        def latest_block_number(self):
            raise AssertionError("hot standby must not scan or advance a live cursor")

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    follower = LiveSourceFollower(
        store=store,
        rpc=NoRewatermarkRpc(head=102),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )

    result = live._join_hot_standby_at_existing_cursor(follower=follower)

    assert result["hot_standby_joined"] is True
    assert store.runtime_value("last_processed_block") == "100"
    assert store.runtime_gap_receipt_count() == 0


def test_hot_standby_waits_for_the_primary_runtime_lock_before_processing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    calls: list[int] = []
    monkeypatch.setattr(live, "now_ms", lambda: 1_700_000_000_000)
    monkeypatch.setattr(
        live,
        "_process_live_ws_head",
        lambda **kwargs: calls.append(int(kwargs["head"])) or True,
    )

    kwargs = {
        "store": store,
        "runtime_dir": tmp_path,
        "follower": object(),
        "execution": object(),
        "head": 101,
        "start_redemption_cycle": lambda: None,
    }
    if "allocation" in inspect.signature(
        live._process_hot_standby_ws_head
    ).parameters:
        kwargs["allocation"] = Decimal("1")
    with live._exclusive_runtime_lock(live._profile_runtime_lock_path(tmp_path)):
        assert live._process_hot_standby_ws_head(**kwargs) is True

    assert calls == []
    assert store.runtime_value("hot_standby_primary_runtime_lock_seen_at_ms") == (
        "1700000000000"
    )

    assert live._process_hot_standby_ws_head(**kwargs) is True
    assert calls == [101]
    assert store.runtime_value("hot_standby_takeover_count") == "1"


def test_hot_standby_does_not_process_before_operator_forward_watermark_completes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    store.set_runtime("operator_planned_resume_from_block", "100")
    store.set_runtime("operator_planned_resume_state", "PENDING")
    calls: list[int] = []
    monkeypatch.setattr(live, "now_ms", lambda: 1_700_000_000_000)
    monkeypatch.setattr(
        live,
        "_process_live_ws_head",
        lambda **kwargs: calls.append(int(kwargs["head"])) or True,
    )

    kwargs = dict(
        store=store,
        runtime_dir=tmp_path,
        follower=object(),
        execution=object(),
        head=101,
        start_redemption_cycle=lambda: None,
    )
    if "allocation" in inspect.signature(
        live._process_hot_standby_ws_head
    ).parameters:
        kwargs["allocation"] = Decimal("1")

    assert live._process_hot_standby_ws_head(**kwargs) is True

    assert calls == []
    assert store.runtime_value("operator_planned_resume_state") == "PENDING"
    assert store.runtime_value("hot_standby_last_observed_head") == "101"
    assert store.runtime_value("hot_standby_takeover_count") is None


def test_primary_runtime_lock_waits_for_a_hot_standby_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    observed: dict[str, object] = {}

    @contextmanager
    def fake_exclusive_runtime_lock(path: Path, *, wait_for_release: bool = False):
        observed["path"] = path
        observed["wait_for_release"] = wait_for_release
        yield

    monkeypatch.setattr(live, "_exclusive_runtime_lock", fake_exclusive_runtime_lock)

    with live._primary_runtime_lock(tmp_path / "live.lock"):
        pass

    assert observed == {
        "path": tmp_path / "live.lock",
        "wait_for_release": True,
    }


def test_exclusive_runtime_lock_wait_mode_uses_blocking_flock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    operations: list[int] = []
    real_flock = live.fcntl.flock

    def spy_flock(fd: int, operation: int) -> None:
        operations.append(operation)
        real_flock(fd, operation)

    monkeypatch.setattr(live.fcntl, "flock", spy_flock)

    with live._exclusive_runtime_lock(
        tmp_path / "live.lock", wait_for_release=True
    ):
        pass

    assert operations == [fcntl.LOCK_EX, fcntl.LOCK_UN]


def test_planned_restart_refuses_to_advance_past_an_unowned_source_action(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    store.set_runtime("operator_planned_resume_from_block", "100")
    store.set_runtime("operator_planned_resume_change_id", "forward-only-release")
    store.set_runtime("operator_planned_resume_state", "PENDING")
    missed = replace(action(marker="7"), block_number=103)
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=105),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )
    follower._new_source_actions = lambda **_kwargs: [missed]

    with pytest.raises(LiveConfigurationError, match="LOSSLESS_HANDOFF_REQUIRED"):
        live._bootstrap_ws_connection(
            follower=follower,
            initial_connection=True,
        )

    assert store.runtime_value("last_processed_block") == "100"
    assert store.runtime_gap_receipt_count() == 1
    assert store.latest_transition(missed)["reason"] == "LOSSLESS_HANDOFF_REQUIRED"
    assert store.runtime_value("operator_planned_resume_state") == "PENDING"
    assert store.runtime_value("status") == "LOSSLESS_HANDOFF_REQUIRED"


def test_pre_repair_recovery_bootstraps_a_new_forward_boundary_without_late_order(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")
    store.set_runtime("operator_planned_resume_from_block", "100")
    store.set_runtime("operator_planned_resume_change_id", "debt-repair")
    store.set_runtime("operator_planned_resume_state", "PENDING")
    store.set_runtime("operator_pre_repair_forward_recovery_armed", "true")
    missed = replace(action(marker="8"), block_number=103)
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=105),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
    )
    follower._new_source_actions = lambda **_kwargs: [missed]

    result = live._bootstrap_ws_connection(follower=follower, initial_connection=True)

    assert result["resumed_planned_operator_change"] is True
    assert result["pre_repair_internal_gap_action_count"] == 1
    assert store.runtime_value("last_processed_block") == "105"
    assert store.runtime_value("operator_planned_resume_state") == "COMPLETED"
    assert store.runtime_value("operator_pre_repair_forward_recovery_armed") == "false"
    assert store.latest_transition(missed)["terminal_status"] == "ERROR_INTERNAL"


def test_planned_restart_baselines_public_history_before_scanning_chain_gap(
    tmp_path: Path,
):
    """A restart gap must not turn the visible public-history page into orders."""

    source_wallet = "0x" + "a" * 40
    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize()
    store.set_runtime("last_processed_block", "100")
    public_row = {
        "proxyWallet": source_wallet,
        "transactionHash": "0x" + "4" * 64,
        "asset": "456",
        "side": "BUY",
        "size": "10",
        "price": "0.6",
        "timestamp": 1_700_000_000,
    }
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=105),
        source_wallet=source_wallet,
        clock_ms=lambda: 1_700_000_000_000,
        public_get_json=lambda _url: [public_row],
    )
    chain_gap_calls: list[dict[str, object]] = []

    def record_chain_gap(**kwargs: object) -> list[SourceAction]:
        with store.connect() as connection:
            baseline_count = connection.execute(
                "SELECT COUNT(*) FROM public_source_observations "
                "WHERE state = 'FORWARD_WATERMARK_BASELINE_NO_REPLAY'"
            ).fetchone()[0]
        assert baseline_count == 1
        chain_gap_calls.append(kwargs)
        return []

    follower._new_source_actions = record_chain_gap  # type: ignore[method-assign]

    result = follower.establish_forward_watermark()

    assert result["public_wallet_baseline_row_count"] == 1
    assert chain_gap_calls == [
        {
            "from_block": 101,
            "to_block": 105,
            "include_verified_public_wallet": False,
        }
    ]
    assert store.action_receipt_count() == 0


def test_forward_watermark_cli_mode_is_available_without_running_the_service():
    args = live._parser().parse_args(
        ["--establish-forward-watermark", "--runtime-dir", "/tmp/runtime"]
    )

    assert args.establish_forward_watermark is True
    assert args.run is False


def test_pre_repair_recovery_cli_mode_requires_an_explicit_operator_receipt():
    args = live._parser().parse_args(
        [
            "--arm-pre-repair-forward-recovery",
            "--runtime-dir",
            "/tmp/runtime",
            "--operator-resume-change-id",
            "debt-repair",
            "--operator-resume-reason",
            "stalled-cursor-recovery",
        ]
    )

    assert args.arm_pre_repair_forward_recovery is True
    assert args.establish_forward_watermark is False


def test_hot_standby_cli_mode_is_available_without_rebuilding_a_watermark():
    args = live._parser().parse_args(
        ["--run-hot-standby", "--runtime-dir", "/tmp/runtime"]
    )

    assert args.run_hot_standby is True
    assert args.run is False


def test_websocket_new_head_extractor_only_accepts_the_active_subscription():
    message = '{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-1","result":{"number":"0x7b"}}}'

    assert extract_ws_new_head_number(message, subscription_id="sub-1") == 123
    assert extract_ws_new_head_number(message, subscription_id="another-subscription") is None


def test_slow_head_processing_coalesces_buffered_notifications_to_the_latest_head():
    def notification(head: int) -> str:
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {
                    "subscription": "sub-1",
                    "result": {"number": hex(head)},
                },
            }
        )

    class BufferedWebsocket:
        def __init__(self) -> None:
            self.messages = [notification(102), notification(103)]

        async def recv(self) -> str:
            if self.messages:
                return self.messages.pop(0)
            await asyncio.Future()
            raise AssertionError("unreachable")

    processed: list[int] = []

    async def slow_processor(head: int) -> bool:
        processed.append(head)
        await asyncio.sleep(0.02)
        return True

    async def scenario() -> tuple[bool, int | None, int]:
        return await live._process_head_while_coalescing_notifications(
            websocket=BufferedWebsocket(),
            subscription_id="sub-1",
            head=101,
            process_head=slow_processor,
        )

    handled, latest_buffered_head, buffered_count = asyncio.run(scenario())

    assert handled is True
    assert processed == [101]
    assert latest_buffered_head == 103
    assert buffered_count == 2


def test_malformed_active_new_head_is_retryable_external_input():
    message = '{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-1","result":{}}}'

    with pytest.raises(ConnectionError, match="INVALID_WS_NEW_HEAD") as captured:
        extract_ws_new_head_number(message, subscription_id="sub-1")

    assert _is_retryable_external_error(captured.value)


def test_counterparty_order_log_cannot_be_promoted_to_a_source_action():
    decoded = {
        "transaction_hash": "0x" + "4" * 64,
        "token_id": "123",
        "order_hash": "0x" + "5" * 64,
        "source_role": "taker",
        "source_order": False,
        "side_code": 0,
        "maker_amount_raw": "4000000",
        "taker_amount_raw": "10000000",
        "block_number": 100,
        "block_hash": "0x" + "6" * 64,
        "block_timestamp": 1_700_000_000,
        "chain_seen_at_ms": 1_700_000_000_100,
    }

    with pytest.raises(live.SourceDecodeError, match="COUNTERPARTY_ORDER_LOG"):
        decode_followable_source_action(decoded)


def test_same_block_actions_preserve_polygon_log_order_not_hash_sort_order():
    chain_first = replace(
        action(marker="f"),
        log_index=2,
        transaction_hash="0x" + "f" * 64,
    )
    chain_second = replace(
        action(marker="1"),
        log_index=9,
        transaction_hash="0x" + "1" * 64,
    )

    ordered = LiveSourceFollower._aggregate([chain_second, chain_first])

    assert [item.action_id for item in ordered] == [
        chain_first.action_id,
        chain_second.action_id,
    ]


def test_status_page_writes_a_renderable_html_and_json_snapshot(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    payload = write_status_files(store, tmp_path)

    assert payload["config"]["fixed_share_scale"] == "0.25"
    assert payload["account"]["external_cash_reserve_usd"] == "0"
    assert payload["account"]["total_capital_contributed_usd"] == "100"
    assert (tmp_path / "status.json").is_file()
    html = (tmp_path / "status.html").read_text(encoding="utf-8")
    assert "font-family" in html
    assert "CD90 cash live copy" in html
    assert "there is one authenticated trading account" in html


def test_status_quantifies_action_fidelity_and_decision_unit_conservation(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    first = action(marker="1")
    second = replace(
        action(marker="4"),
        block_number=101,
        source_timestamp=1_700_000_001,
    )
    metadata_pending = replace(
        action(marker="5"),
        block_number=102,
        source_timestamp=1_700_000_002,
    )
    metadata = {
        "condition_id": "0x" + "1" * 64,
        "market_slug": "high-temperature-in-paris-on-august-8",
        "event_slug": "temperature-in-paris-on-august-8",
    }
    for source, state in ((first, "FILLED"), (second, "PENDING_LIQUIDITY")):
        store.record_action_receipt(source)
        store.append_transition(source=source, status=state)
        store.freeze_action_metadata(
            source=source,
            metadata=metadata,
            profile_follow=True,
            profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
            frozen_at_ms=source.discovered_at_ms,
        )
        store.ensure_action_target(
            source=source,
            proportional_quantity=D("4"),
            target_quantity=D("5"),
            state=state,
            reason="",
            updated_at_ms=source.discovered_at_ms,
        )
    store.record_action_receipt(metadata_pending)
    store.append_transition(source=metadata_pending, status="PENDING_METADATA")

    payload = write_status_files(store, tmp_path)

    assert payload["action_fidelity"] == {
        "total_action_receipts": 3,
        "source_maker_action_receipts": 3,
        "source_verified_public_wallet_action_receipts": 0,
        "followable_source_action_receipts": 3,
        "legacy_nonmaker_receipt_count": 0,
        "frozen_metadata_count": 2,
        "legacy_nonmaker_metadata_count": 0,
        "legacy_nonmaker_target_count": 0,
        "profile_excluded_observed": 0,
        "legacy_or_unclassified_without_metadata": 0,
        "profile_eligible_observed": 2,
        "frozen_target_count": 2,
        "filled": 1,
        "partial": 0,
        "pending": 1,
        "external_or_causal_unfilled": 0,
        "external_or_causal_unfilled_acknowledged_baseline": 0,
        "internal_error": 0,
        "unclassified_target": 0,
        "active_repair_managed_without_target": 0,
        "missing_target": 0,
        "metadata_pending": 1,
        "retryable_target_terminal_transition_mismatch": 0,
        "accounted": 2,
        "conservation_passed": True,
        "oldest_pending_updated_at_ms": second.discovered_at_ms,
        "recoverable_legacy_stable_causal_prefix_action_count": 0,
    }
    assert payload["decision_units"] == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 2,
            "filled": 1,
            "partial": 0,
            "pending": 1,
            "external_or_causal": 0,
            "internal_error": 0,
        }
    ]
    assert payload["execution_drift_monitor"]["mode"] == (
        "MONITOR_ONLY_NO_EXECUTION_GATE"
    )
    assert payload["execution_drift_monitor"]["historical_reference_evidence"] == (
        "UNREPRODUCED_MISSING_RAW_CUTOFF_AND_HASH"
    )
    html = (tmp_path / "status.html").read_text(encoding="utf-8")
    assert "Action fidelity" in html
    assert "Decision units" in html


def test_fidelity_summary_counts_an_eligible_internal_block_even_before_target_freeze(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "high-temperature-in-paris-on-august-8",
            "event_slug": "temperature-in-paris-on-august-8",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="PENDING_INTERNAL_INVARIANT",
        reason="BLOCK_CROSS_SLEEVE_TOKEN_OWNERSHIP",
    )

    summary = store.action_fidelity_summary()

    assert summary["profile_eligible_observed"] == 1
    assert summary["frozen_target_count"] == 0
    assert summary["missing_target"] == 1
    assert summary["internal_error"] == 1
    assert summary["accounted"] == 1
    assert summary["conservation_passed"] is False
    assert store.decision_unit_summary() == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 1,
            "filled": 0,
            "partial": 0,
            "pending": 0,
            "external_or_causal": 0,
            "internal_error": 1,
        }
    ]


def test_active_repair_managed_pending_action_is_conserved_without_a_target(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "high-temperature-in-paris-on-august-8",
            "event_slug": "temperature-in-paris-on-august-8",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=1,
    )
    store.append_transition(
        source=source,
        status="PENDING_CAUSAL_ORDER",
        reason="PRIOR_SAME_TOKEN_ACTION_NOT_TERMINAL",
    )
    store.initialize()
    with store.connect() as connection:
        connection.execute(
            """
            INSERT INTO repair_recovery_manifests(
                manifest_hash, profile_key, gap_receipt_id, policy_hash,
                state, activated_at_ms, last_processed_head, manifest_json
            ) VALUES('manifest', 'cd90', 1, 'policy', 'ACTIVE', 1, 100, '{}')
            """
        )
        connection.execute(
            """
            INSERT INTO repair_recovery_actions(
                manifest_hash, action_id, action_kind, state,
                source_unit_price, last_evaluated_head, last_snapshot_json,
                created_at_ms, updated_at_ms
            ) VALUES('manifest', ?, 'CAUSAL_DEPENDENT_ACTION', 'PENDING_PRICE',
                     '0.5', 100, '{}', 1, 1)
            """,
            (source.action_id,),
        )

    summary = store.action_fidelity_summary()

    assert summary["profile_eligible_observed"] == 1
    assert summary["frozen_target_count"] == 0
    assert summary["active_repair_managed_without_target"] == 1
    assert summary["missing_target"] == 0
    assert summary["pending"] == 1
    assert summary["internal_error"] == 0
    assert summary["accounted"] == 1
    assert summary["conservation_passed"] is True


def test_action_fidelity_reports_partial_as_a_conserved_terminal(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "high-temperature-in-paris-on-august-8",
            "event_slug": "temperature-in-paris-on-august-8",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=source.discovered_at_ms,
    )
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("4"),
        target_quantity=D("4"),
        state="PARTIAL",
        reason="OFFICIAL_ONCHAIN_PARTIAL_FILL_RECEIPT",
        updated_at_ms=source.discovered_at_ms,
    )
    store.append_transition(
        source=source,
        status="PARTIAL",
        reason="OFFICIAL_ONCHAIN_PARTIAL_FILL_RECEIPT",
    )

    summary = store.action_fidelity_summary()

    assert summary["filled"] == 0
    assert summary["partial"] == 1
    assert summary["pending"] == 0
    assert summary["unclassified_target"] == 0
    assert summary["accounted"] == 1
    assert summary["conservation_passed"] is True
    assert store.decision_unit_summary() == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 1,
            "filled": 0,
            "partial": 1,
            "pending": 0,
            "external_or_causal": 0,
            "internal_error": 0,
        }
    ]


def test_fixed_scale_minimum_skip_is_an_external_fidelity_constraint(
    tmp_path: Path,
):
    """A fixed-ratio order below the venue minimum is not an internal error."""

    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "high-temperature-in-paris-on-august-8",
            "event_slug": "temperature-in-paris-on-august-8",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=source.discovered_at_ms,
    )
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("2"),
        target_quantity=D("2"),
        state="SKIPPED",
        reason="PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
        updated_at_ms=source.discovered_at_ms,
    )
    store.append_transition(
        source=source,
        status="SKIPPED",
        reason="PROPORTIONAL_QUANTITY_BELOW_MARKET_MINIMUM",
    )

    summary = store.action_fidelity_summary()

    assert summary["external_or_causal_unfilled"] == 1
    assert summary["internal_error"] == 0
    assert summary["accounted"] == 1
    assert summary["conservation_passed"] is True
    assert store.decision_unit_summary() == [
        {
            "event_slug": "temperature-in-paris-on-august-8",
            "eligible_observed": 1,
            "filled": 0,
            "partial": 0,
            "pending": 0,
            "external_or_causal": 1,
            "internal_error": 0,
        }
    ]


def test_operator_acknowledgement_never_subtracts_from_raw_unfilled_count(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "high-temperature-in-paris-on-august-8",
            "event_slug": "temperature-in-paris-on-august-8",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=source.discovered_at_ms,
    )
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("2"),
        target_quantity=D("2"),
        state="EXTERNAL_UNFILLABLE",
        reason="NO_ASK_BOOK_LEVEL",
        updated_at_ms=source.discovered_at_ms,
    )
    store.append_transition(
        source=source,
        status="EXTERNAL_UNFILLABLE",
        reason="NO_ASK_BOOK_LEVEL",
    )
    store.lock_config_once(
        "operator_written_off_external_or_causal_unfilled_count", "1"
    )

    summary = store.action_fidelity_summary()

    assert summary["external_or_causal_unfilled"] == 1
    assert summary["external_or_causal_unfilled_acknowledged_baseline"] == 1


def test_fixed_scale_buy_notional_minimum_skip_is_an_external_fidelity_constraint(
    tmp_path: Path,
):
    """A fixed-ratio BUY below the venue notional minimum is external."""

    store = LiveStore(tmp_path / "live.sqlite3")
    source = action()
    store.record_action_receipt(source)
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "market_slug": "high-temperature-in-paris-on-august-8",
            "event_slug": "temperature-in-paris-on-august-8",
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=source.discovered_at_ms,
    )
    store.ensure_action_target(
        source=source,
        proportional_quantity=D("2"),
        target_quantity=D("2"),
        state="SKIPPED",
        reason="PROPORTIONAL_BUY_NOTIONAL_BELOW_MARKETABLE_MINIMUM",
        updated_at_ms=source.discovered_at_ms,
    )
    store.append_transition(
        source=source,
        status="SKIPPED",
        reason="PROPORTIONAL_BUY_NOTIONAL_BELOW_MARKETABLE_MINIMUM",
    )

    summary = store.action_fidelity_summary()

    assert summary["external_or_causal_unfilled"] == 1
    assert summary["internal_error"] == 0
    assert summary["accounted"] == 1
    assert summary["conservation_passed"] is True
    assert store.decision_unit_summary()[0]["external_or_causal"] == 1
    assert store.decision_unit_summary()[0]["internal_error"] == 0


def test_status_page_separates_authenticated_account_cash_from_the_cd90_sleeve(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_authenticated_collateral_usd", "100")
    store.set_runtime("platform_settlement_reconciliation_state", "RECONCILED")

    payload = write_status_files(store, tmp_path)

    assert payload["account"]["authenticated_clob_collateral_usd"] == "100"
    assert payload["account"]["cash_reconciliation_delta_usd"] == "0"
    assert payload["account"]["cash_reconciliation_state"] == "RECONCILED"
    html = (tmp_path / "status.html").read_text(encoding="utf-8")
    assert "authenticated CLOB cash: 100 USD" in html
    assert "there is one authenticated trading account" in html


def test_status_available_cash_uses_authenticated_account_not_strategy_ledger_cash(
    tmp_path: Path,
):
    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    for store in (cd90, tennis):
        initialize_scale_once(
            store=store,
            allocation_usd=D("100"),
            source_open_position_value_usd=D("400"),
            observed_at_ms=1,
        )
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    receipt = coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("150"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=2,
    )
    cd90.lock_config_once("profile_key", "cd90")
    cd90.lock_config_once("shared_wallet_coordinator_path", coordinator.path)
    cd90.lock_config_once(
        "shared_wallet_migration_receipt_hash",
        receipt["migration_receipt_hash"],
    )
    cd90.set_runtime("last_authenticated_collateral_usd", "150")

    payload = write_status_files(cd90, tmp_path)

    assert payload["account"]["strategy_attribution_cash_usd"] == "100"
    assert payload["account"]["account_expected_cash_high_usd"] == "150"
    assert payload["account"]["available_cash_usd"] == "150"
    assert payload["account"]["shared_wallet_cash_state"] == "NO_ACTIVE_CASH_HOLDS"
    html = (tmp_path / "status.html").read_text(encoding="utf-8")
    assert (
        "new-BUY cash authority: 150 USD "
        "(AUTHENTICATED_ACCOUNT_COLLATERAL_MINUS_ACTIVE_BUY_RESERVATIONS)"
    ) in html


def test_post_fill_cash_refresh_is_shared_by_every_registered_status_page(
    tmp_path: Path,
):
    """One sleeve fill changes one physical wallet, so every sleeve sees it."""

    cd90 = LiveStore(tmp_path / "cd90.sqlite3")
    tennis = LiveStore(tmp_path / "tennis.sqlite3")
    for store in (cd90, tennis):
        initialize_scale_once(
            store=store,
            allocation_usd=D("100"),
            source_open_position_value_usd=D("400"),
            observed_at_ms=1,
        )
        store.set_runtime("last_authenticated_collateral_usd", "200")
        store.set_runtime("last_authenticated_collateral_at_ms", "1")
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    receipt = coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("200"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=2,
    )
    for profile_key, store in (
        ("cd90", cd90),
        ("tennis_atp_wta_mainline", tennis),
    ):
        store.lock_config_once("profile_key", profile_key)
        store.lock_config_once("shared_wallet_coordinator_path", coordinator.path)
        store.lock_config_once(
            "shared_wallet_migration_receipt_hash",
            receipt["migration_receipt_hash"],
        )

    class BalanceExecution(FakeExecution):
        def __init__(self):
            super().__init__()
            self.balance = D("200")

        def collateral_balance_usd(self):
            return self.balance

        def condition_mapping_for_token(self, token_id: str):
            assert token_id == "123"
            return {
                "condition_id": "0x" + "b" * 64,
                "primary_token_id": "123",
                "secondary_token_id": "456",
            }

        def snapshot(self, *, token_id: str, side: str):
            return {
                **super().snapshot(token_id=token_id, side=side),
                "condition_id": "0x" + "b" * 64,
            }

    execution = BalanceExecution()
    source = action(quantity="40")
    execute_source_action(
        store=cd90,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
        coordinator=coordinator,
        profile_key="cd90",
    )
    _set_authoritative_fill(
        execution=execution,
        quantity=D("10"),
        notional_usd=D("4"),
        vwap_price=D("0.40"),
    )
    execution.balance = D("196")

    class NoActionFollower:
        wallet_lock_path = tmp_path / "shared-wallet.lock"
        profile_key = "cd90"

        def __init__(self):
            self.coordinator = coordinator

        def run_cycle_to_head(self, **kwargs):
            cd90.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    handled = live._process_live_ws_head(
        store=cd90,
        runtime_dir=tmp_path / "cd90-status",
        follower=NoActionFollower(),
        execution=execution,
        head=101,
        start_redemption_cycle=lambda: None,
    )

    assert handled is True
    assert cd90.runtime_value("last_authenticated_collateral_usd") == "196"
    observation = coordinator.latest_authenticated_collateral_observation()
    assert observation["profile_key"] == "cd90"
    assert observation["authenticated_collateral_usd"] == "196"
    tennis_payload = write_status_files(tennis, tmp_path / "tennis-status")
    assert tennis_payload["account"]["authenticated_clob_collateral_usd"] == "196"
    assert tennis_payload["account"]["available_cash_usd"] == "196"
    assert tennis_payload["account"]["shared_wallet_cash_state"] == "NO_ACTIVE_CASH_HOLDS"


def test_only_transport_failures_are_retried_in_place_without_a_service_restart():
    assert _is_retryable_external_error(ConnectionError("temporary transport failure"))
    assert _is_retryable_external_error(TimeoutError("endpoint timed out"))
    assert not _is_retryable_external_error(ValueError("internal decode error"))


def test_clob_request_exception_without_http_status_is_retryable_external():
    assert _is_retryable_external_error(
        PolyApiException(error_msg="Request exception!")
    )
    try:
        raise RuntimeError("COLLATERAL_BALANCE_UNAVAILABLE") from PolyApiException(
            error_msg="Request exception!"
        )
    except RuntimeError as wrapped:
        assert _is_retryable_external_error(wrapped)


def test_sqlite_failure_wrapped_by_retryable_clob_error_remains_internal():
    unavailable_book = PolyApiException(
        error_msg="No orderbook exists for the requested token id"
    )
    unavailable_book.status_code = 404

    try:
        try:
            raise unavailable_book
        except PolyApiException:
            raise sqlite3.OperationalError("unable to open database file")
    except sqlite3.OperationalError as wrapped:
        assert _is_retryable_external_error(wrapped) is False


def test_clob_no_orderbook_404_is_retryable_external():
    unavailable_book = PolyApiException(
        error_msg="No orderbook exists for the requested token id"
    )
    unavailable_book.status_code = 404
    assert _is_retryable_external_error(unavailable_book)

    unrelated_not_found = PolyApiException(error_msg="Unknown endpoint")
    unrelated_not_found.status_code = 404
    assert not _is_retryable_external_error(unrelated_not_found)


def test_empty_book_level_errors_are_retryable_external():
    assert _is_retryable_external_error(RuntimeError("NO_ASK_BOOK_LEVEL"))
    assert _is_retryable_external_error(RuntimeError("NO_BID_BOOK_LEVEL"))


def test_retryable_chain_read_keeps_websocket_and_does_not_starve_redemption(
    tmp_path: Path,
):
    """A source-read miss must not block independent wallet maintenance."""

    class FailingFollower:
        def run_cycle_to_head(self, **_kwargs):
            raise ConnectionError("temporary public RPC failure")

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("ws_subscription_active", "true")
    store.set_runtime("ws_reconnect_count", "7")
    redemption_starts: list[bool] = []

    handled = live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=FailingFollower(),
        execution=FakeExecution(),
        head=101,
        start_redemption_cycle=lambda: redemption_starts.append(True),
    )

    assert handled is False
    assert store.runtime_value("ws_subscription_active") == "true"
    assert store.runtime_value("ws_reconnect_count") == "7"
    assert store.runtime_value("last_cycle_outcome") == "EXTERNAL_HEAD_RETRY_PENDING"
    assert redemption_starts == [True]


def test_public_chain_reconciliation_mismatch_keeps_the_daemon_alive_and_cursor_held(
    tmp_path: Path,
):
    class MismatchFollower:
        def run_cycle_to_head(self, **_kwargs):
            raise LiveConfigurationError("PUBLIC_CHAIN_MAKER_ACTION_RECONCILIATION_MISMATCH")

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "100")

    handled = live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=MismatchFollower(),
        execution=FakeExecution(),
        head=101,
        start_redemption_cycle=lambda: None,
    )

    assert handled is False
    assert store.runtime_value("last_processed_block") == "100"
    assert store.runtime_value("last_cycle_outcome") == (
        "INTERNAL_SOURCE_RECONCILIATION_PENDING"
    )
    with store.connect() as connection:
        row = connection.execute(
            "SELECT category FROM runtime_errors ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert row["category"] == "INTERNAL_SOURCE_RECONCILIATION"


def test_websocket_head_processes_only_the_block_with_a_successor(tmp_path: Path):
    class RecordingFollower:
        def __init__(self):
            self.heads = []

        def run_cycle_to_head(self, **kwargs):
            self.heads.append(kwargs["head"])
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    store = LiveStore(tmp_path / "live.sqlite3")
    follower = RecordingFollower()

    handled = live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=FakeExecution(),
        head=101,
        start_redemption_cycle=lambda: None,
    )

    assert handled is True
    assert follower.heads == [100]
    assert store.runtime_value("last_announced_head") == "101"


def test_status_snapshot_sqlite_failure_does_not_restart_head_processing(
    tmp_path: Path, monkeypatch,
):
    """A non-order status snapshot must not terminate the live WS worker."""

    store = LiveStore(tmp_path / "live.sqlite3")

    class RecordingFollower:
        wallet_lock_path = tmp_path / "shared-wallet.lock"
        coordinator = None
        profile_key = "cd90"

        def run_cycle_to_head(self, **kwargs):
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    def unavailable_status(*_args, **_kwargs):
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(live, "write_status_files", unavailable_status)

    handled = live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=RecordingFollower(),
        execution=FakeExecution(),
        head=101,
        start_redemption_cycle=lambda: None,
    )

    assert handled is True


def test_websocket_head_retries_pending_actions_after_observing_new_source_actions(
    tmp_path: Path, monkeypatch,
):
    events: list[str] = []
    store = LiveStore(tmp_path / "live.sqlite3")

    class RecordingFollower:
        wallet_lock_path = tmp_path / "shared-wallet.lock"
        coordinator = None
        profile_key = "cd90"

        def run_cycle_to_head(self, **kwargs):
            events.append("observe_source_actions")
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    def fake_reconcile(**_kwargs):
        events.append("reconcile")
        return []

    def fake_retry(**kwargs):
        events.append("retry_pending")
        assert kwargs["wallet_lock_path"] == tmp_path / "shared-wallet.lock"
        assert kwargs["coordinator"] is None
        assert kwargs["profile_key"] == "cd90"
        return []

    monkeypatch.setattr(live, "_reconcile_submissions_and_refresh_cash", fake_reconcile)
    monkeypatch.setattr(live, "retry_pending_actions", fake_retry)

    handled = live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=RecordingFollower(),
        execution=FakeExecution(),
        head=101,
        start_redemption_cycle=lambda: None,
    )

    assert handled is True
    assert events == [
        "observe_source_actions",
        "reconcile",
        "retry_pending",
        "reconcile",
    ]


def test_new_zero_fill_waits_for_the_next_chain_head_before_retrying(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    source = action(quantity="40")
    execution = FakeExecution(
        error=RuntimeError(
            "PolyApiException[status_code=400, error_message={'error': "
            "'no orders found to match with FAK order.'}]"
        )
    )

    class OneActionFollower:
        wallet_lock_path = None
        coordinator = None
        profile_key = "cd90"

        def run_cycle_to_head(self, **kwargs):
            execute_source_action(
                store=store,
                source=source,
                execution=execution,
                allocated_cash=D("100"),
                live_enabled=True,
            )
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 1,
                "source_action_ids": [source.action_id],
                "last_processed_block": kwargs["head"],
            }

    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=OneActionFollower(),
        execution=execution,
        head=101,
        start_redemption_cycle=lambda: None,
    ) is True

    assert store.submission_attempt_count(source.action_id) == 1
    assert len(execution.calls) == 1
    assert store.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"


def test_duplicate_websocket_head_does_not_retry_the_same_pending_fak_twice(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")
    source = action(quantity="40")
    execution = FakeExecution(
        error=RuntimeError(
            "PolyApiException[status_code=400, error_message={'error': "
            "'no orders found to match with FAK order.'}]"
        )
    )
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )

    class CursorFollower:
        wallet_lock_path = None
        coordinator = None
        profile_key = "cd90"

        def run_cycle_to_head(self, **kwargs):
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    for _ in range(2):
        assert live._process_live_ws_head(
            store=store,
            runtime_dir=tmp_path,
            follower=CursorFollower(),
            execution=execution,
            head=102,
            start_redemption_cycle=lambda: None,
        ) is True

    assert store.submission_attempt_count(source.action_id) == 1
    assert len(execution.calls) == 1


def test_duplicate_websocket_head_does_not_reconcile_unknown_submission_twice(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")

    class CountingPreparedExecution(FakePreparedExecution):
        def __init__(self):
            super().__init__(error=TimeoutError("network uncertain after post"))
            self.get_order_calls = []

        def get_order(self, order_id: str):
            self.get_order_calls.append(order_id)
            return {
                "status": "ORDER_STATUS_LIVE",
                "original_size": "10",
                "size_matched": "0",
                "price": "0.40",
            }

    execution = CountingPreparedExecution()
    source = action()
    execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("100"),
        live_enabled=True,
    )
    execution.error = None

    class CursorFollower:
        wallet_lock_path = None
        coordinator = None
        profile_key = "cd90"

        def run_cycle_to_head(self, **kwargs):
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    for _ in range(2):
        assert live._process_live_ws_head(
            store=store,
            runtime_dir=tmp_path,
            follower=CursorFollower(),
            execution=execution,
            head=102,
            start_redemption_cycle=lambda: None,
        ) is True

    assert execution.get_order_calls == [execution.prepared["order_id"]]
    assert len(execution.prepared_submit_calls) == 1


def test_restart_orphaned_pending_metadata_is_not_replayed_as_a_late_order(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")
    source = replace(action(quantity="40"), block_number=99)
    store.record_action_receipt(source)
    store.append_transition(
        source=source,
        status="PENDING_METADATA",
        reason="temporary metadata endpoint failure",
    )

    class RecoveredScope:
        def resolve_action(self, _source):
            return ScopeDecision(
                True,
                "FULL_WALLET_ACTION_ELIGIBLE",
                {
                    "condition_id": "condition-123",
                    "market_slug": "temperature-paris-august-8-high",
                    "event_slug": "temperature-paris-august-8",
                },
            )

    execution = FakeExecution()
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=101),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_200,
        action_scope=RecoveredScope(),
    )
    follower._new_source_actions = lambda **_kwargs: []

    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=execution,
        head=102,
        start_redemption_cycle=lambda: None,
    ) is True

    assert execution.calls == []
    assert store.action_target(source.action_id) is None
    assert store.runtime_value("last_processed_block") == "101"
    assert store.action_fidelity_summary()["metadata_pending"] == 1


def test_late_metadata_is_not_replayed_or_upsized_without_an_action_time_book(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")
    # The fixed share-scale is 100 / 400 = 0.25 (formula-derived in this
    # fixture), so this source action maps to 1 share.  The separately
    # observed market minimum is 5 shares; a late-metadata terminal must not
    # replace the fixed target with that minimum because it never has an
    # action-time executable book.
    source = replace(action(quantity="4"), block_number=99)
    store.record_action_receipt(source)
    store.append_transition(
        source=source,
        status="PENDING_METADATA",
        reason="Gamma condition lookup returned no market",
    )

    class RecoveredAfterCloseScope:
        def resolve_action(self, _source):
            return ScopeDecision(
                True,
                "ATP_WTA_MAINLINE_PRE_MATCH_METADATA_RECOVERED_AFTER_CLOSE",
                {
                    "condition_id": "0x" + "1" * 64,
                    "primary_token_id": source.token_id,
                    "secondary_token_id": "456",
                    "market_slug": "wta-player-a-player-b-2026-08-08",
                    "event_slug": "wta-player-a-player-b-2026-08-08",
                    "minimum_order_size": "5",
                    "execution_recovery_state": (
                        "EXTERNAL_UNFILLABLE_METADATA_LATE"
                    ),
                },
            )

    execution = FakeExecution()
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=101),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_200,
        action_scope=RecoveredAfterCloseScope(),
    )

    follower._new_source_actions = lambda **_kwargs: []
    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=execution,
        head=102,
        start_redemption_cycle=lambda: None,
    ) is True

    assert execution.calls == []
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "PENDING_METADATA"
    assert latest["reason"] == "Gamma condition lookup returned no market"
    assert store.action_target(source.action_id) is None


def test_frozen_eligible_metadata_is_not_refetched_or_replayed_as_a_late_order(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")
    source = replace(
        action(quantity="40"),
        block_number=99,
        source_timestamp=1_700_000_000,
    )
    store.record_action_receipt(source)
    store.append_transition(source=source, status="OBSERVED")
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "1" * 64,
            "primary_token_id": source.token_id,
            "secondary_token_id": "456",
            "market_slug": "wta-player-a-player-b-2026-08-08",
            "event_slug": "wta-player-a-player-b-2026-08-08",
            "source_timestamp": source.source_timestamp,
            "official_game_start_timestamp": 1_700_000_100,
            "source_stage": "PRE_MATCH",
        },
        profile_follow=True,
        profile_reason="ATP_WTA_MAINLINE_PRE_MATCH_ELIGIBLE",
        frozen_at_ms=1_700_000_000_100,
    )
    store.bind_condition_for_token(
        token_id=source.token_id,
        condition_id="0x" + "1" * 64,
        primary_token_id="456",
        secondary_token_id=source.token_id,
        observed_at_ms=1_700_000_000_050,
    )
    store.append_transition(
        source=source,
        status="PENDING_METADATA",
        reason="interrupted after immutable metadata freeze",
    )

    class DynamicScopeMustNotReplaceFrozenEvidence:
        def resolve_action(self, _source):
            raise AssertionError("immutable metadata must be reused")

    execution = FakeExecution()
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=101),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_200_000,
        action_scope=DynamicScopeMustNotReplaceFrozenEvidence(),
        coordinator=object(),
        profile_key="tennis_atp_wta_mainline",
    )

    follower._new_source_actions = lambda **_kwargs: []
    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=execution,
        head=102,
        start_redemption_cycle=lambda: None,
    ) is True

    assert execution.calls == []
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "PENDING_METADATA"
    assert latest["reason"] == "interrupted after immutable metadata freeze"
    assert store.action_target(source.action_id) is None


def test_orphaned_metadata_external_failure_does_not_block_new_chain_heads(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")
    source = replace(action(quantity="40"), block_number=99)
    store.record_action_receipt(source)
    store.append_transition(
        source=source,
        status="PENDING_METADATA",
        reason="temporary metadata endpoint failure",
    )

    class StillUnavailableScope:
        def resolve_action(self, _source):
            raise ConnectionError("AMBIGUOUS_GAMMA_MARKET")

    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=101),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_200,
        action_scope=StillUnavailableScope(),
    )
    follower._new_source_actions = lambda **_kwargs: []

    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=FakeExecution(),
        head=102,
        start_redemption_cycle=lambda: None,
    ) is True

    assert store.runtime_value("last_processed_block") == "101"
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "PENDING_METADATA"
    assert latest["reason"] == "temporary metadata endpoint failure"


def test_restart_orphaned_observed_action_is_not_replayed_as_a_late_order(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "100")
    source = replace(action(quantity="40"), block_number=99)
    store.record_action_receipt(source)
    store.append_transition(source=source, status="OBSERVED")

    execution = FakeExecution()
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=101),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_200,
    )
    follower._new_source_actions = lambda **_kwargs: []

    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=execution,
        head=102,
        start_redemption_cycle=lambda: None,
    ) is True

    assert execution.calls == []
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "OBSERVED"
    assert store.runtime_value("last_processed_block") == "101"


def test_partial_pending_fill_counts_as_a_cash_mutation_for_shared_refresh(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(quantity="40")
    store.record_action_receipt(source)
    store.append_transition(
        source=source,
        status="PARTIAL_PENDING",
        reason="FAK_PARTIAL_FILL",
        created_at_ms=2,
    )

    assert store.latest_cash_mutation_transition_id() > 0


def test_external_head_failure_is_aggregated_into_one_incident_and_one_recovery(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")

    class RecoveringFollower:
        def __init__(self):
            self.calls = 0

        def run_cycle_to_head(self, **kwargs):
            self.calls += 1
            if self.calls <= 2:
                raise ConnectionError("same temporary RPC incident")
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

    follower = RecoveringFollower()
    for head in (101, 102, 103):
        live._process_live_ws_head(
            store=store,
            runtime_dir=tmp_path,
            follower=follower,
            execution=FakeExecution(),
            head=head,
            start_redemption_cycle=lambda: None,
        )

    with store.connect() as connection:
        rows = connection.execute(
            """
            SELECT category, details_json
            FROM runtime_errors
            WHERE category LIKE 'EXTERNAL_HEAD_INCIDENT_%'
            ORDER BY id
            """
        ).fetchall()
    assert [row["category"] for row in rows] == [
        "EXTERNAL_HEAD_INCIDENT_STARTED",
        "EXTERNAL_HEAD_INCIDENT_RECOVERED",
    ]
    recovered = __import__("json").loads(rows[1]["details_json"])
    assert recovered["occurrence_count"] == 2


def test_redemption_maintenance_is_not_restarted_on_every_polygon_head(tmp_path: Path):
    """The user-specified hourly server maintenance cadence bounds RPC load."""

    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("auto_redemption_last_cycle_at_ms", "1000")

    assert not live.redemption_maintenance_due(
        store=store,
        observed_at_ms=1001,
    )
    assert live.redemption_maintenance_due(
        store=store,
        observed_at_ms=1000 + live.REDEMPTION_MAINTENANCE_INTERVAL_MS,
    )


def test_redemption_maintenance_runs_immediately_before_the_first_recorded_cycle(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")

    assert live.redemption_maintenance_due(store=store, observed_at_ms=1)


# LIQUIDITY_ONLY_RETRY_V2 -------------------------------------------------
# These tests deliberately use a new immutable forward boundary. They do not
# reopen any V1 receipt or pre-release action.


def _activate_liquidity_retry_v2(store: LiveStore, *, boundary: int = 99):
    return store.activate_liquidity_retry_policy(
        effective_after_block=boundary,
        activated_at_ms=1_700_000_000_000,
        change_id="liquidity-only-retry-v2-test",
    )


def _retry_through_execution(
    *, store: LiveStore, execution: FakeExecution, lifecycle=None
):
    if lifecycle is None:
        lifecycle = lambda _source: ScopeDecision(
            True,
            "OFFICIAL_RETRY_MARKET_LIFECYCLE",
            {"closed": False, "accepting_orders": True},
        )
    return retry_pending_actions(
        store=store,
        execution=execution,
        market_lifecycle_resolver=lifecycle,
        process_action=lambda source: execute_source_action(
            store=store,
            source=source,
            execution=execution,
            live_enabled=True,
        ),
    )


def _v2_zero_fill_store(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    execution = FakeExecution()
    source = action(quantity="40")
    assert execute_source_action(
        store=store,
        source=source,
        execution=execution,
        live_enabled=True,
    )["terminal_status"] == "SUBMITTED_UNRECONCILED"
    execution.authoritative_submission_execution = lambda **_kwargs: None
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 1,
        "finality": "polygon_finalized_block",
    }
    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING_CONFIRMED_ZERO_FILL",
            "reason": "FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE",
        }
    ]
    return store, source, execution


class DelayedActiveCancelExecution(FakePreparedExecution):
    def __init__(self):
        super().__init__()
        self.prepared["order_type"] = "GTC"
        self.response = {
            "success": True,
            "status": "delayed",
            "orderID": self.prepared["order_id"],
        }
        self.orders[self.prepared["order_id"]] = {
            "status": "MATCHED",
            "size_matched": "0",
            "original_size": "10",
        }

    prepare_gtd_limit = FakePreparedExecution.prepare_fak_market
    submit_prepared_gtd_limit = FakePreparedExecution.submit_prepared_fak_market


def _delayed_active_cancel_store(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    source = action(quantity="40")
    execution = DelayedActiveCancelExecution()
    assert execute_source_action(
        store=store,
        source=source,
        execution=execution,
        live_enabled=True,
    )["terminal_status"] == "SUBMITTED_UNRECONCILED"
    execution.authoritative_submission_execution = lambda **_kwargs: None
    return store, source, execution


def test_delayed_gtc_without_completed_cancel_never_becomes_retryable_zero_fill(
    tmp_path: Path,
):
    store, source, execution = _delayed_active_cancel_store(tmp_path)
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number,
        "finality": "polygon_finalized_block",
    }

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING",
            "reason": "GTC_ACTIVE_CANCEL_AWAITING_CANCEL_OR_FILL",
        }
    ]
    with store.connect() as connection:
        attempt = connection.execute(
            "SELECT state FROM submission_attempts WHERE action_id=?",
            (source.action_id,),
        ).fetchone()
        reservation = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id=?",
            (source.action_id,),
        ).fetchone()
    assert attempt["state"] == "SUBMITTED_UNRECONCILED"
    assert int(reservation["active"]) == 1
    assert _retry_through_execution(store=store, execution=execution) == []
    assert len(execution.prepared_submit_calls) == 1


def test_canceled_gtc_waits_until_finalized_scan_covers_cancel_head(
    tmp_path: Path,
):
    store, source, execution = _delayed_active_cancel_store(tmp_path)
    attempt = store.unreconciled_submissions()[0][1]
    response = dict(attempt["response"])
    response.update(
        {
            "active_cancel_verified": True,
            "active_cancel_observed_head_block": source.block_number + 10,
        }
    )
    store.update_attempt_state(
        attempt_id=attempt["attempt_id"],
        state="SUBMITTED_UNRECONCILED",
        response=response,
        updated_at_ms=2,
    )
    execution.orders[execution.prepared["order_id"]] = {
        "status": "CANCELED",
        "size_matched": "0",
        "original_size": "10",
    }
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 9,
        "finality": "polygon_finalized_block",
    }

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING",
            "reason": "GTC_ACTIVE_CANCEL_AWAITING_FINALITY",
        }
    ]
    assert store.submission_attempt_count(source.action_id) == 1


def test_canceled_gtc_zero_fill_retries_only_after_finalized_cancel_boundary(
    tmp_path: Path,
):
    store, source, execution = _delayed_active_cancel_store(tmp_path)
    attempt = store.unreconciled_submissions()[0][1]
    response = dict(attempt["response"])
    response.update(
        {
            "active_cancel_verified": True,
            "active_cancel_observed_head_block": source.block_number + 10,
        }
    )
    store.update_attempt_state(
        attempt_id=attempt["attempt_id"],
        state="SUBMITTED_UNRECONCILED",
        response=response,
        updated_at_ms=2,
    )
    execution.orders[execution.prepared["order_id"]] = {
        "status": "CANCELED",
        "size_matched": "0",
        "original_size": "10",
    }
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 10,
        "finality": "polygon_finalized_block",
    }

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING_CONFIRMED_ZERO_FILL",
            "reason": "GTC_ACTIVE_CANCEL_ZERO_FILL_RETRYABLE",
        }
    ]
    assert store.liquidity_retry_evidence(source) is not None


def test_partial_gtc_stays_reserved_and_cannot_retry_before_active_cancel(
    tmp_path: Path,
):
    store, source, execution = _delayed_active_cancel_store(tmp_path)
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "quantity": D("5"),
        "notional_usd": D("2"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [{"transaction_hash": "0x" + "4" * 64}],
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 1,
        "finality": "polygon_finalized_block",
    }

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING",
            "reason": "GTC_ACTIVE_CANCEL_AWAITING_CANCEL_OR_FILL",
        }
    ]
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id=?",
            (source.action_id,),
        ).fetchone()
    assert int(reservation["active"]) == 1
    assert store.liquidity_retry_evidence(source) is None
    assert _retry_through_execution(store=store, execution=execution) == []
    assert len(execution.prepared_submit_calls) == 1


def test_partial_gtc_retries_exact_remainder_only_after_post_cancel_finality(
    tmp_path: Path,
):
    store, source, execution = _delayed_active_cancel_store(tmp_path)
    attempt = store.unreconciled_submissions()[0][1]
    response = dict(attempt["response"])
    response.update(
        {
            "active_cancel_verified": True,
            "active_cancel_observed_head_block": source.block_number + 10,
        }
    )
    store.update_attempt_state(
        attempt_id=attempt["attempt_id"],
        state="SUBMITTED_UNRECONCILED",
        response=response,
        updated_at_ms=2,
    )
    execution.orders[execution.prepared["order_id"]] = {
        "status": "CANCELED",
        "size_matched": "5",
        "original_size": "10",
    }
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "quantity": D("5"),
        "notional_usd": D("2"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [{"transaction_hash": "0x" + "5" * 64}],
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 10,
        "finality": "polygon_finalized_block",
    }

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PARTIAL_PENDING",
            "reason": "GTC_ACTIVE_CANCEL_PARTIAL_FILL",
        }
    ]
    evidence = store.liquidity_retry_evidence(source)
    assert evidence is not None
    assert evidence["cumulative_official_filled_quantity"] == "5"
    execution.prepared["order_id"] = "0x" + "8" * 64
    execution.prepared["order_type"] = "FAK"
    execution.response = {
        "success": True,
        "orderID": execution.prepared["order_id"],
    }

    assert _retry_through_execution(store=store, execution=execution) == [
        {"terminal_status": "SUBMITTED_UNRECONCILED", "reason": ""}
    ]
    assert execution.prepare_calls[-1]["size"] == D("5")


def test_legacy_gtc_zero_fill_without_post_cancel_proof_is_not_retryable(
    tmp_path: Path,
):
    store, source, _execution = _delayed_active_cancel_store(tmp_path)
    attempt = store.unreconciled_submissions()[0][1]
    store.release_reservation_and_finalize(
        source=source,
        terminal_status="PENDING_CONFIRMED_ZERO_FILL",
        reason="GTC_ACTIVE_CANCEL_ZERO_FILL_RETRYABLE",
        created_at_ms=2,
        details={
            "order_id": attempt["order_id"],
            "attempt_id": attempt["attempt_id"],
            "chain_scan": {
                "from_block": source.block_number,
                "to_block": source.block_number + 10,
                "order_filled_log_count": 0,
                "finality": "polygon_finalized_block",
            },
        },
        attempt_id=attempt["attempt_id"],
        attempt_state="NO_FILL",
        attempt_response={"result": "legacy_zero_fill"},
    )

    assert store.liquidity_retry_evidence(source) is None


def test_canceled_gtc_without_finalized_order_hash_proof_keeps_reservation(
    tmp_path: Path,
):
    store, source, execution = _delayed_active_cancel_store(tmp_path)
    attempt = store.unreconciled_submissions()[0][1]
    response = dict(attempt["response"])
    response.update(
        {
            "active_cancel_verified": True,
            "active_cancel_observed_head_block": source.block_number + 10,
        }
    )
    store.update_attempt_state(
        attempt_id=attempt["attempt_id"],
        state="SUBMITTED_UNRECONCILED",
        response=response,
        updated_at_ms=2,
    )
    execution.orders[execution.prepared["order_id"]] = {
        "status": "CANCELED",
        "size_matched": "0",
        "original_size": "10",
    }
    execution.authoritative_order_hash_execution = lambda **_kwargs: None

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING",
            "reason": "GTC_ACTIVE_CANCEL_AWAITING_FINALIZED_ZERO_FILL_PROOF",
        }
    ]
    with store.connect() as connection:
        reservation = connection.execute(
            "SELECT active FROM order_reservations WHERE action_id=?",
            (source.action_id,),
        ).fetchone()
    assert int(reservation["active"]) == 1


def test_liquidity_retry_v2_is_prospective_has_no_deadline_and_preserves_v1(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.activate_bounded_retry_policy(
        effective_after_block=50,
        activated_at_ms=1_600_000_000_000,
        change_id="historical-v1",
    )

    receipt = _activate_liquidity_retry_v2(store, boundary=99)

    assert receipt["policy_id"] == "LIQUIDITY_ONLY_RETRY_V2"
    assert receipt["effective_after_block"] == 99
    assert receipt["deadline_ms"] is None
    assert receipt["historical_catch_up"] is False
    assert store.config("bounded_retry_policy_id") == live.BOUNDED_RETRY_POLICY_ID
    assert store.liquidity_retry_policy_for_source(action()) is not None
    assert store.liquidity_retry_policy_for_source(
        replace(action(), block_number=99)
    ) is None


def test_finalized_zero_fill_retries_exact_remaining_quantity_without_deadline(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    execution.response = {"success": True, "orderID": "order-2"}

    result = _retry_through_execution(store=store, execution=execution)

    assert result == [
        {
            "terminal_status": "SUBMITTED_UNRECONCILED",
            "reason": "",
        }
    ]
    assert len(execution.calls) == 2
    assert execution.calls[-1]["size"] == D("10")
    assert store.submission_attempt_count(source.action_id) == 2


def test_dynamic_buy_limit_uses_depth_and_retains_sixty_percent_of_source_upside():
    result = live.dynamic_buy_limit_price(
        source_average_price=D("0.7095365837"),
        target_quantity=D("21.5788731"),
        cumulative_filled_quantity=D("0"),
        cumulative_filled_notional_usd=D("0"),
        current_best_ask=D("0.74"),
        tick_size=D("0.01"),
        raw_asks=[
            {"price": "0.74", "size": "7.69"},
            {"price": "0.75", "size": "35"},
            {"price": "0.90", "size": "20"},
        ],
        preserve_first_attempt_price=True,
    )

    assert result["retained_upside_ratio"] == D("0.60")
    assert result["average_price_cap"] == D("0.82572195022")
    assert result["safe_worst_price"] == D("0.82")
    assert result["order_limit_price"] == D("0.75")
    assert result["snapshot_projected_vwap"] == D(
        "0.7464363291983027603049391861"
    )


def test_dynamic_buy_limit_spends_only_proven_price_surplus_on_a_later_retry():
    result = live.dynamic_buy_limit_price(
        source_average_price=D("0.70"),
        target_quantity=D("10"),
        cumulative_filled_quantity=D("5"),
        cumulative_filled_notional_usd=D("3.70"),
        current_best_ask=D("0.90"),
        tick_size=D("0.01"),
        raw_asks=[{"price": "0.90", "size": "5"}],
        preserve_first_attempt_price=False,
    )

    assert result["average_price_cap"] == D("0.82")
    assert result["safe_worst_price"] == D("0.90")
    assert result["order_limit_price"] == D("0.90")
    assert result["snapshot_projected_total_vwap"] == D("0.82")


def test_active_cancel_retries_the_exact_partial_remainder(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    source = action(quantity="40")
    execution = FakeExecution()
    execute_source_action(
        store=store, source=source, execution=execution, live_enabled=True
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("5"),
        "notional_usd": D("2"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [{"transaction_hash": "0x" + "1" * 64}],
    }
    assert reconcile_submitted_actions(store=store, execution=execution)[0][
        "terminal_status"
    ] == "PARTIAL_PENDING"
    execution.response = {"success": True, "orderID": "order-2"}

    result = _retry_through_execution(store=store, execution=execution)

    target = store.action_target(source.action_id)
    assert result == [
        {
            "terminal_status": "SUBMITTED_UNRECONCILED",
            "reason": "",
        }
    ]
    assert [call["size"] for call in execution.calls] == [D("10"), D("5")]
    assert target["target_quantity"] == D("10")
    assert target["cumulative_filled_quantity"] == D("5")
    assert target["remaining_quantity"] == D("5")
    assert target["state"] == "SUBMITTED_UNRECONCILED"


def test_partial_without_persisted_chain_receipt_never_enters_retry(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    source = action(quantity="40")
    execution = FakeExecution()
    execute_source_action(
        store=store, source=source, execution=execution, live_enabled=True
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("5"),
        "notional_usd": D("2"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [],
    }

    assert reconcile_submitted_actions(store=store, execution=execution) == [
        {
            "terminal_status": "PENDING",
            "reason": "ONCHAIN_FILL_RECEIPT_EVIDENCE_INVALID",
        }
    ]
    assert store.liquidity_retry_evidence(source) is None
    assert _retry_through_execution(store=store, execution=execution) == []
    assert len(execution.calls) == 1


def test_finalized_zero_fill_retries_once_without_overfill(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    execution.response = {"success": True, "orderID": "order-2"}
    assert _retry_through_execution(store=store, execution=execution) == [
        {
            "terminal_status": "SUBMITTED_UNRECONCILED",
            "reason": "",
        }
    ]
    assert store.submission_attempt_count(source.action_id) == 2
    assert len(execution.calls) == 2
    assert _retry_through_execution(store=store, execution=execution) == []


def test_finalized_zero_fill_restart_does_not_submit_an_unknown_replacement(
    tmp_path: Path,
):
    store, source, _initial_execution = _v2_zero_fill_store(tmp_path)
    restarted = LiveStore(store.path)
    retry_execution = FakeExecution(
        error=TimeoutError("network uncertain after retry post")
    )

    first = _retry_through_execution(
        store=restarted,
        execution=retry_execution,
    )
    second = _retry_through_execution(
        store=restarted,
        execution=retry_execution,
    )

    assert first == [
        {
            "terminal_status": "UNKNOWN_SUBMISSION",
            "reason": "SUBMISSION_TRANSPORT_UNKNOWN:TimeoutError",
        }
    ]
    assert second == []
    assert len(retry_execution.calls) == 1
    assert restarted.submission_attempt_count(source.action_id) == 2


def test_duplicate_websocket_head_does_not_replace_a_finalized_zero_fill(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    store.set_runtime("last_processed_block", "100")
    execution.response = {"success": True, "orderID": "order-2"}

    class RetryScope:
        def resolve_action(self, _source):
            return ScopeDecision(
                True,
                "OFFICIAL_RETRY_MARKET_LIFECYCLE",
                {"closed": False, "accepting_orders": True},
            )

    class CursorFollower:
        wallet_lock_path = None
        coordinator = None
        profile_key = "cd90"
        action_scope = RetryScope()

        def run_cycle_to_head(self, **kwargs):
            store.set_runtime("last_processed_block", kwargs["head"])
            return {
                "source_action_count": 0,
                "source_action_ids": [],
                "last_processed_block": kwargs["head"],
            }

        def _process_observed_action_safely(
            self, *, action, execution, live_enabled
        ):
            return execute_source_action(
                store=store,
                source=action,
                execution=execution,
                live_enabled=live_enabled,
            )

    follower = CursorFollower()
    for _ in range(2):
        assert live._process_live_ws_head(
            store=store,
            runtime_dir=tmp_path,
            follower=follower,
            execution=execution,
            head=102,
            start_redemption_cycle=lambda: None,
        ) is True

    assert store.submission_attempt_count(source.action_id) == 2
    assert len(execution.calls) == 2

    execution.response = {"success": True, "orderID": "order-3"}
    assert live._process_live_ws_head(
        store=store,
        runtime_dir=tmp_path,
        follower=follower,
        execution=execution,
        head=103,
        start_redemption_cycle=lambda: None,
    ) is True
    assert store.submission_attempt_count(source.action_id) == 3
    assert len(execution.calls) == 3


def test_finalized_zero_fill_retries_at_the_current_safe_book(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    current_price = {"value": "0.41"}
    original_snapshot = execution.snapshot

    def snapshot(*, token_id: str, side: str):
        value = original_snapshot(token_id=token_id, side=side)
        value["best_price"] = current_price["value"]
        return value

    execution.snapshot = snapshot
    execution.response = {"success": True, "orderID": "order-2"}
    assert _retry_through_execution(store=store, execution=execution) == [
        {
            "terminal_status": "SUBMITTED_UNRECONCILED",
            "reason": "",
        }
    ]
    assert len(execution.calls) == 2
    assert execution.calls[-1]["price"] == D("0.64")
    assert store.action_target(source.action_id)["state"] == "SUBMITTED_UNRECONCILED"


def test_liquidity_retry_ignores_unknown_and_pre_boundary_actions(tmp_path: Path):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store, boundary=100)
    execution = FakeExecution(error=TimeoutError("uncertain after post"))
    source = action()
    assert execute_source_action(
        store=store, source=source, execution=execution, live_enabled=True
    )["terminal_status"] == "UNKNOWN_SUBMISSION"

    assert _retry_through_execution(store=store, execution=execution) == []
    assert len(execution.calls) == 1
    assert store.submission_attempt_count(source.action_id) == 1


def test_finalized_zero_fill_closes_without_reading_a_closed_market(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    before_calls = len(execution.calls)

    def lifecycle(_source):
        return ScopeDecision(
            True,
            "FULL_WALLET_ACTION_ELIGIBLE",
            {
                "event_slug": "closed-event",
                "market_slug": "closed-market",
                "closed": True,
                "accepting_orders": False,
            },
        )

    result = _retry_through_execution(
        store=store, execution=execution, lifecycle=lifecycle
    )

    assert result == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "OFFICIAL_MARKET_CLOSED_BEFORE_RETRY",
        }
    ]
    assert len(execution.calls) == before_calls
    assert store.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"


def test_finalized_zero_fill_closes_without_a_lifecycle_refetch(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    before_calls = len(execution.calls)

    result = retry_pending_actions(
        store=store,
        execution=execution,
        market_lifecycle_resolver=None,
        process_action=lambda _source: pytest.fail("must not submit"),
    )

    assert result == [
        {
            "terminal_status": "PENDING_CONFIRMED_ZERO_FILL",
            "reason": "OFFICIAL_MARKET_STATE_RESOLVER_UNAVAILABLE_FOR_LIQUIDITY_RETRY",
        }
    ]
    assert len(execution.calls) == before_calls
    assert store.action_target(source.action_id)["state"] == "PENDING_CONFIRMED_ZERO_FILL"


def test_later_opposite_terminates_v2_partial_remainder_but_preserves_fill(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    _activate_liquidity_retry_v2(store)
    buy = action(quantity="40")
    sell = replace(
        action(side="SELL", quantity="40", marker="4"),
        block_number=101,
        source_timestamp=1_700_000_001,
    )
    store.record_action_receipt(buy)
    store.ensure_action_target(
        source=buy,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PARTIAL_PENDING",
        reason="FAK_PARTIAL_FILL",
        updated_at_ms=1,
    )
    with store.connect() as connection:
        connection.execute(
            "UPDATE action_targets SET cumulative_filled_quantity='3' WHERE action_id=?",
            (buy.action_id,),
        )
    store.record_action_receipt(sell)

    assert store.supersede_earlier_fully_unfilled_opposites(source=sell) == [
        buy.action_id
    ]
    target = store.action_target(buy.action_id)
    assert target["state"] == "PARTIAL"
    assert target["cumulative_filled_quantity"] == D("3")
    assert target["remaining_quantity"] == D("7")


def test_later_opposite_terminates_v2_zero_fill_without_historical_order(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    _activate_liquidity_retry_v2(store)
    buy = action(quantity="40")
    sell = replace(
        action(side="SELL", quantity="40", marker="4"),
        block_number=101,
        source_timestamp=1_700_000_001,
    )
    store.record_action_receipt(buy)
    store.ensure_action_target(
        source=buy,
        proportional_quantity=D("10"),
        target_quantity=D("10"),
        state="PENDING_CONFIRMED_ZERO_FILL",
        reason="FINALIZED_CHAIN_PROVES_FAK_ZERO_FILL_RETRYABLE",
        updated_at_ms=1,
    )
    store.record_action_receipt(sell)

    assert store.supersede_earlier_fully_unfilled_opposites(source=sell) == [
        buy.action_id
    ]
    target = store.action_target(buy.action_id)
    assert target["state"] == "SUPERSEDED_UNFILLED"
    assert target["cumulative_filled_quantity"] == D("0")
    assert target["remaining_quantity"] == D("10")


def test_liquidity_retry_status_is_current_and_does_not_advertise_v1_deadline(
    tmp_path: Path,
):
    store, _source, _execution = _v2_zero_fill_store(tmp_path)

    summary = store.liquidity_retry_summary()

    assert summary["policy_id"] == "LIQUIDITY_ONLY_RETRY_V2"
    assert summary["deadline_ms"] is None
    assert summary["pending_retry_action_count"] == 1
    assert D(summary["pending_actions"][0]["remaining_quantity"]) == D("10")
    assert summary["pending_actions"][0]["frozen_worst_price"] == "0.40"
    assert summary["history_current_policy"] is False


def test_liquidity_retry_status_html_shows_only_v2_as_current_policy(
    tmp_path: Path,
):
    store, _source, _execution = _v2_zero_fill_store(tmp_path)

    payload = write_status_files(store, tmp_path)
    html = (tmp_path / "status.html").read_text(encoding="utf-8")

    assert payload["liquidity_retry"]["policy_id"] == (
        "LIQUIDITY_ONLY_RETRY_V2"
    )
    assert "bounded_retry" not in payload
    assert payload["bounded_retry_history"]["current_policy"] is False
    assert "Controlled liquidity retry" in html
    assert "LIQUIDITY_ONLY_RETRY_V2" in html
    assert "USER_AUTHORIZED_BOUNDED_LIVE_RETRY_V1" not in html
    assert "86400000" not in html


def test_liquidity_retry_policy_activation_is_idempotent_at_forward_cursor(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.set_runtime("last_processed_block", "123")

    first = store.ensure_liquidity_retry_policy_at_current_cursor(
        activated_at_ms=10,
        change_id="release-primary",
    )
    second = LiveStore(store.path).ensure_liquidity_retry_policy_at_current_cursor(
        activated_at_ms=20,
        change_id="release-standby",
    )

    assert first == second
    assert first["effective_after_block"] == 123
    assert first["historical_catch_up"] is False
    assert first["deadline_ms"] is None


def test_v2_official_fak_rejection_without_finalized_chain_proof_is_not_retried(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    execution = FakeExecution(
        error=RuntimeError(
            "PolyApiException[status_code=400, error_message={'error': "
            "'no orders found to match with FAK order.'}]"
        )
    )

    result = execute_source_action(
        store=store,
        source=action(),
        execution=execution,
        live_enabled=True,
    )

    assert result == {
        "terminal_status": "EXTERNAL_UNFILLABLE",
        "reason": "OFFICIAL_FAK_ZERO_FILL_WITHOUT_FINALIZED_CHAIN_PROOF",
    }
    assert _retry_through_execution(store=store, execution=execution) == []
    assert len(execution.calls) == 1


def test_v2_retry_uses_an_exact_share_fak_limit_order(tmp_path: Path):
    client = FakeCLOBClient()
    adapter = CLOBExecutionAdapter(
        client, minimum_marketable_buy_notional_usd=D("1")
    )
    adapter.snapshot(token_id="123", side="BUY")

    prepared = adapter.prepare_fak_exact_shares(
        token_id="123",
        side="BUY",
        price=D("0.40"),
        size=D("5"),
    )

    assert prepared["order_type"] == "FAK"
    assert prepared["quantity_mode"] == "EXACT_SHARES"
    created = client.created_orders[0]["order_args"]
    assert created.size == 5.0
    assert created.price == 0.4


def test_finalized_zero_fill_rechecks_cash_before_retry(
    tmp_path: Path,
):
    store, source, execution = _v2_zero_fill_store(tmp_path)
    execution.collateral = D("1")

    result = _retry_through_execution(store=store, execution=execution)

    assert result == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "INSUFFICIENT_AUTHENTICATED_ACCOUNT_CASH_AT_RETRY",
        }
    ]
    assert len(execution.calls) == 1
    assert store.action_target(source.action_id)["state"] == "EXTERNAL_UNFILLABLE"


def test_sell_retry_without_inventory_remains_unfillable_without_reposting(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO positions(token_id, quantity, cost_basis_usd) "
            "VALUES('123', '10', '4')"
        )
    source = replace(action(side="SELL", quantity="40"), source_notional=D("16"))
    execution = FakeExecution()
    assert execute_source_action(
        store=store, source=source, execution=execution, live_enabled=True
    )["terminal_status"] == "SUBMITTED_UNRECONCILED"
    execution.authoritative_submission_execution = lambda **_kwargs: None
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 1,
        "finality": "polygon_finalized_block",
    }
    reconcile_submitted_actions(store=store, execution=execution)
    with store.connect() as connection:
        connection.execute(
            "UPDATE positions SET quantity='0', cost_basis_usd='0' "
            "WHERE token_id='123'"
        )

    assert _retry_through_execution(store=store, execution=execution) == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "NO_LOCAL_INVENTORY_PRE_WATERMARK_OR_PRIOR_MISS",
        }
    ]
    assert len(execution.calls) == 1


def test_v2_retry_remainder_below_current_minimum_is_terminal_without_upscale(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    source = action(quantity="40")
    execution = FakeExecution()
    execute_source_action(
        store=store, source=source, execution=execution, live_enabled=True
    )
    execution.authoritative_submission_execution = lambda **_kwargs: {
        "quantity": D("6"),
        "notional_usd": D("2.4"),
        "fee_usd": D("0"),
        "vwap_price": D("0.40"),
        "receipt_evidence": [{"transaction_hash": "0x" + "2" * 64}],
    }

    result = reconcile_submitted_actions(store=store, execution=execution)

    assert result == [
        {
            "terminal_status": "EXTERNAL_UNFILLABLE",
            "reason": "PARTIAL_REMAINDER_BELOW_ORIGINAL_MARKET_MINIMUM",
        }
    ]
    assert len(execution.calls) == 1


def test_finalized_zero_fill_above_ninety_retries_only_at_source_price(
    tmp_path: Path,
):
    store = LiveStore(tmp_path / "live.sqlite3")
    initialize_scale_once(
        store=store,
        allocation_usd=D("100"),
        source_open_position_value_usd=D("400"),
        observed_at_ms=1,
    )
    _activate_liquidity_retry_v2(store)
    source = replace(
        action(quantity="40"),
        source_notional=D("36.4"),
    )

    class HighPriceExecution(FakeExecution):
        def __init__(self):
            super().__init__(collateral="1000")
            self.current_price = "0.91"

        def snapshot(self, *, token_id: str, side: str):
            snapshot = super().snapshot(token_id=token_id, side=side)
            return {
                **snapshot,
                "best_price": self.current_price,
                "fee_bps": "500",
            }

    execution = HighPriceExecution()
    execute_source_action(
        store=store, source=source, execution=execution, live_enabled=True
    )
    execution.authoritative_submission_execution = lambda **_kwargs: None
    execution.authoritative_order_hash_execution = lambda **_kwargs: {
        "authoritative_no_fill": True,
        "scan_from_block": source.block_number,
        "scan_to_block": source.block_number + 1,
        "finality": "polygon_finalized_block",
    }
    reconcile_submitted_actions(store=store, execution=execution)
    execution.response = {"success": True, "orderID": "order-2"}
    assert _retry_through_execution(store=store, execution=execution) == [
        {
            "terminal_status": "SUBMITTED_UNRECONCILED",
            "reason": "",
        }
    ]
    assert len(execution.calls) == 2
    assert execution.calls[-1]["price"] == D("0.91")


def test_liquidity_retry_status_counts_official_market_close_termination(
    tmp_path: Path,
):
    store, _source, execution = _v2_zero_fill_store(tmp_path)
    _retry_through_execution(
        store=store,
        execution=execution,
        lifecycle=lambda _source: ScopeDecision(
            True,
            "OFFICIAL_RETRY_MARKET_LIFECYCLE",
            {"closed": True, "accepting_orders": False},
        ),
    )

    summary = store.liquidity_retry_summary()

    assert summary["termination_counts"] == {
        "OFFICIAL_MARKET_CLOSED_BEFORE_RETRY": 1
    }


def test_buy_uses_60_second_active_cancel_but_sell_uses_immediate_fak():
    prepare_gtd = lambda **_kwargs: None
    submit_gtd = lambda _prepared: None

    assert live.BUY_ACTIVE_CANCEL_WAIT_SECONDS == 60
    assert live._uses_active_cancel_limit(
        side="BUY",
        prepare_gtd=prepare_gtd,
        submit_prepared_gtd=submit_gtd,
    ) is True
    assert live._uses_active_cancel_limit(
        side="SELL",
        prepare_gtd=prepare_gtd,
        submit_prepared_gtd=submit_gtd,
    ) is False
