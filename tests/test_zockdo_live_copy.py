from decimal import Decimal
import inspect
import json
import pytest

import cd90_live_copy as core
import wallet9506_live_copy as wallet9506
import zockdo_live_copy as zockdo
from live_copy_profiles import FullWalletEventScope
from zockdo_soccer_sleeve import ZockdoEventScope


def test_zockdo_wires_full_wallet_event_scope():
    source = inspect.getsource(zockdo)
    assert "FullWalletEventScope" in source
    assert "TennisSleeveEventScope" not in source
    assert "build_action_scope" in source
    assert zockdo.PROFILE_SCOPE == "FULL_WALLET"
    assert zockdo.FIXED_SHARE_SCALE == Decimal("1")
    scope = zockdo.build_action_scope(lambda _url: {})
    assert isinstance(scope, ZockdoEventScope)
    assert isinstance(scope._inner, FullWalletEventScope)


def test_wallet9506_still_uses_full_wallet_event_scope():
    source = inspect.getsource(wallet9506)
    assert "FullWalletEventScope" in source
    assert "TennisSleeveEventScope" not in source


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
    assert store.fixed_share_scale() == Decimal("1")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert receipt["historical_catch_up"] is False
    assert receipt["fixed_share_scale"] == "1"
    assert receipt["scale_basis"] == zockdo.SCALE_BASIS


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


def _legacy_half_scale_store(tmp_path):
    store = core.LiveStore(tmp_path / "live.sqlite3")
    store.lock_config_once("profile_key", zockdo.PROFILE_KEY)
    store.lock_config_once("profile_scope", zockdo.PROFILE_SCOPE)
    store.lock_config_once("source_wallet", zockdo.SOURCE_WALLET)
    store.initialize_explicit_fixed_scale_once(
        allocation_usd=Decimal("0"),
        fixed_share_scale=Decimal("0.5"),
        scale_basis=zockdo.LEGACY_SCALE_BASIS,
        observed_at_ms=123,
    )
    receipt = {
        "profile_key": zockdo.PROFILE_KEY,
        "source_wallet": zockdo.SOURCE_WALLET,
        "profile_scope": zockdo.PROFILE_SCOPE,
        "allocation_usd": "0",
        "fixed_share_scale": "0.5",
        "scale_basis": zockdo.LEGACY_SCALE_BASIS,
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


def test_legacy_half_scale_store_cannot_start_until_one_to_one_rebase(tmp_path):
    store = _legacy_half_scale_store(tmp_path)
    with pytest.raises(
        zockdo.ZockdoConfigurationError,
        match="STORED_PROFILE_LAUNCH_RECEIPT_MISMATCH:fixed_share_scale",
    ):
        zockdo.prepare_store(store=store, observed_at_ms=456)
    assert store.fixed_share_scale() == Decimal("0.5")


def test_user_authorized_one_to_one_rebase_unlocks_legacy_half_scale_store(tmp_path):
    store = _legacy_half_scale_store(tmp_path)
    first = zockdo.apply_one_to_one_scale_rebase(
        store=store,
        change_id="zockdo-20260824-user-authorized-one-to-one",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )
    second = zockdo.apply_one_to_one_scale_rebase(
        store=store,
        change_id="zockdo-20260824-user-authorized-one-to-one",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=4,
    )
    restarted = zockdo.prepare_store(store=store, observed_at_ms=456)

    assert first["previous_scale"] == Decimal("0.5")
    assert first["new_scale"] == Decimal("1")
    assert first["multiplier"] == Decimal("2")
    assert second.get("idempotent") is True
    assert store.fixed_share_scale() == Decimal("1")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert restarted["fixed_share_scale"] == "0.5"
    assert restarted["observed_at_ms"] == 123
    assert restarted["historical_catch_up"] is False


def test_one_to_one_rebase_allows_pending_causal_order_without_submission(
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
        source_role="maker",
        discovered_at_ms=1_700_000_000_100,
    )
    assert store.record_action_receipt(source) is True
    store.append_transition(
        source=source,
        status="PENDING_CAUSAL_ORDER",
        reason="PRIOR_SAME_TOKEN_SUBMISSION_UNRESOLVED",
    )
    unfillable = core.SourceAction(
        transaction_hash="0x" + "d" * 64,
        token_id="456",
        side="BUY",
        order_hash="0x" + "e" * 64,
        source_quantity=Decimal("8"),
        source_notional=Decimal("3"),
        source_timestamp=1_700_000_001,
        block_number=91,
        block_hash="0x" + "f" * 64,
        source_role="taker",
        discovered_at_ms=1_700_000_001_100,
    )
    assert store.record_action_receipt(unfillable) is True
    store.append_transition(
        source=unfillable,
        status="EXTERNAL_UNFILLABLE",
        reason="BOOK_TOO_THIN",
    )

    result = zockdo.apply_one_to_one_scale_rebase(
        store=store,
        change_id="zockdo-20260824-user-authorized-one-to-one",
        effective_after_block=100,
        resume_from_block=90,
        requested_at_ms=3,
    )

    assert result["new_scale"] == Decimal("1")
    assert store.fixed_share_scale() == Decimal("1")
    restarted = zockdo.prepare_store(store=store, observed_at_ms=456)
    assert restarted["fixed_share_scale"] == "0.5"

from cd90_live_sizing import plan_action
from live_copy_profiles import ScopeDecision
from zockdo_nontennis_cap import (
    TENNIS_EVENT_P90_SOURCE_NOTIONAL_USD,
    is_tennis_event_slug,
    max_buy_notional_usd_for_profile,
    nontennis_max_copy_notional_usd,
)
from zockdo_soccer_sleeve import (
    ZOCKDO_SOCCER_SLEEVE_STOPPED,
    ZockdoEventScope,
    is_soccer_event_slug,
)

D = Decimal


def _source_action(*, side: str = "BUY", quantity: str = "40", marker: str = "1"):
    return core.SourceAction(
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


def test_tennis_event_slug_is_prefix_not_title_token():
    assert is_tennis_event_slug("atp-bu-cina-2026-08-27") is True
    assert is_tennis_event_slug("wta-yuan-stoiana-2026-08-27") is True
    assert is_tennis_event_slug("itf-smith-aksu-2026-08-01") is True
    assert is_tennis_event_slug("mls-fcc-vwh-2026-07-22") is False
    assert is_tennis_event_slug("ufc-gau-stador-2026-08-22") is False
    assert is_tennis_event_slug("lal-ala-vil-2026-08-28") is False
    assert is_tennis_event_slug("") is False


def test_nontennis_cap_is_tennis_p90_times_scale():
    assert nontennis_max_copy_notional_usd(D("1")) == TENNIS_EVENT_P90_SOURCE_NOTIONAL_USD
    assert max_buy_notional_usd_for_profile(
        profile_key=zockdo.PROFILE_KEY,
        event_slug="atp-alcaraz-sinner-2026-08-01",
        scale=D("1"),
    ) is None
    assert max_buy_notional_usd_for_profile(
        profile_key=zockdo.PROFILE_KEY,
        event_slug="ufc-gau-stador-2026-08-22",
        scale=D("1"),
    ) == D("543.30")
    assert max_buy_notional_usd_for_profile(
        profile_key="cd90",
        event_slug="ufc-gau-stador-2026-08-22",
        scale=D("1"),
    ) is None
    assert max_buy_notional_usd_for_profile(
        profile_key=zockdo.PROFILE_KEY,
        event_slug="",
        scale=D("1"),
    ) == D("543.30")


def test_plan_action_clips_nontennis_buy_notional_and_keeps_proportional():
    plan = plan_action(
        side="BUY",
        source_quantity=D("10000"),
        scale=D("1"),
        held_quantity=D("0"),
        minimum_order_size=D("5"),
        minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.40"),
        visible_best_level_size=D("100000"),
        taker_fee_bps=D("0"),
        available_cash=D("5000"),
        max_buy_notional_usd=D("543.30"),
    )
    assert plan.terminal_status == "READY"
    assert plan.reason == "BUY_NOTIONAL_CAPPED"
    assert plan.proportional_quantity == D("10000")
    assert plan.order_amount_usd == D("543.30")
    assert plan.requested_quantity == D("543.30") / D("0.40")


def test_plan_action_does_not_clip_sells():
    plan = plan_action(
        side="SELL",
        source_quantity=D("10000"),
        scale=D("1"),
        held_quantity=D("5000"),
        minimum_order_size=D("5"),
        minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.40"),
        visible_best_level_size=D("100000"),
        taker_fee_bps=D("0"),
        available_cash=D("0"),
        max_buy_notional_usd=D("543.30"),
    )
    assert plan.terminal_status == "READY"
    assert plan.requested_quantity == D("5000")


def test_soccer_event_slug_uses_official_sport_prefix():
    assert is_soccer_event_slug("lal-ala-vil-2026-08-28") is True
    assert is_soccer_event_slug("epl-ars-che-2026-08-23") is True
    assert is_soccer_event_slug("fifwc-arg-fra-2026-07-18") is True
    assert is_soccer_event_slug("uel-tot-rom-2026-09-01") is True
    assert is_soccer_event_slug("bra-fla-pal-2026-08-20") is True
    assert is_soccer_event_slug("elc-wre-bir-2026-08-16") is True
    assert is_soccer_event_slug("sea-juv-int-2026-08-29") is True
    assert is_soccer_event_slug("atp-alcaraz-sinner-2026-08-01") is False
    assert is_soccer_event_slug("ufc-son-uma-2026-08-29") is False
    assert is_soccer_event_slug("nfl-kc-bal-2026-08-23") is False
    assert is_soccer_event_slug("seattle-sounders-2026-08-01") is False
    assert is_soccer_event_slug("") is False


class _FullWalletStub:
    def __init__(self, event_slug: str, *, follow: bool = True):
        self.event_slug = event_slug
        self.follow = follow

    def _decision(self) -> ScopeDecision:
        reason = (
            "FULL_WALLET_ACTION_ELIGIBLE"
            if self.follow
            else "SCOPE_EXCLUDED_NON_NETFLIX"
        )
        return ScopeDecision(
            self.follow,
            reason,
            {
                "condition_id": "0x" + "ab" * 32,
                "market_slug": self.event_slug,
                "event_slug": self.event_slug,
            },
        )

    def resolve(self, token_id: str) -> ScopeDecision:
        return self._decision()

    def resolve_action(self, action):
        return self._decision()

    def resolve_retry_lifecycle(self, action, frozen_metadata):
        return ScopeDecision(True, "OFFICIAL_RETRY_MARKET_LIFECYCLE", frozen_metadata)


def test_zockdo_scope_skips_soccer_buy_and_keeps_sell():
    soccer = ZockdoEventScope(_FullWalletStub("lal-ala-vil-2026-08-28"))
    skipped = soccer.resolve_action(_source_action(side="BUY", marker="s"))
    assert skipped.follow is False
    assert skipped.reason == ZOCKDO_SOCCER_SLEEVE_STOPPED
    assert skipped.metadata["event_slug"] == "lal-ala-vil-2026-08-28"
    sold = soccer.resolve_action(_source_action(side="SELL", marker="t"))
    assert sold.follow is True
    assert sold.reason == "FULL_WALLET_ACTION_ELIGIBLE"


def test_zockdo_scope_still_follows_tennis_and_ufc_buys():
    tennis = ZockdoEventScope(_FullWalletStub("atp-alcaraz-sinner-2026-08-01"))
    ufc = ZockdoEventScope(_FullWalletStub("ufc-gau-stador-2026-08-22"))
    assert tennis.resolve_action(_source_action(side="BUY", marker="u")).follow is True
    assert ufc.resolve_action(_source_action(side="BUY", marker="v")).follow is True


def test_zockdo_follower_skips_soccer_buy_without_submitting(tmp_path):
    class FakeRpc:
        def __init__(self, head: int):
            self.head = head

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
            return []

    class FakeExecution:
        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("soccer BUY must not snapshot the book")

        def submit_fak_exact_shares(self, **kwargs):
            raise AssertionError("soccer BUY must not submit")

        def collateral_balance_usd(self):
            return D("5000")

    store = core.LiveStore(tmp_path / "live.sqlite3")
    zockdo.prepare_store(store=store, observed_at_ms=1)
    store.set_runtime("last_processed_block", "99")
    source = _source_action(side="BUY", quantity="10000", marker="w")
    follower = core.LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=ZockdoEventScope(_FullWalletStub("epl-ars-che-2026-08-23")),
        profile_key=zockdo.PROFILE_KEY,
    )
    follower._new_source_actions = lambda **_kwargs: [source]
    result = follower.run_cycle_to_head(
        head=100,
        execution=FakeExecution(),
        live_enabled=True,
    )
    assert result["source_action_count"] == 1
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "SKIPPED" or latest.get("status") == "SKIPPED" or latest.get("reason") == ZOCKDO_SOCCER_SLEEVE_STOPPED
    frozen = store.frozen_action_metadata(source.action_id)
    assert frozen["profile_follow"] is False
    assert frozen["profile_reason"] == ZOCKDO_SOCCER_SLEEVE_STOPPED

