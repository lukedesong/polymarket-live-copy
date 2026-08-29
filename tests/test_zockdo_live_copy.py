from decimal import Decimal

import cd90_live_copy as core
import zockdo_live_copy as zockdo
from cd90_live_copy import LiveSourceFollower
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


def action(*, side: str = "BUY", quantity: str = "40", marker: str = "1") -> core.SourceAction:
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


def test_tennis_event_slug_is_prefix_not_title_token():
    assert is_tennis_event_slug("atp-bu-cina-2026-08-27") is True
    assert is_tennis_event_slug("wta-yuan-stoiana-2026-08-27") is True
    assert is_tennis_event_slug("itf-smith-aksu-2026-08-01") is True
    assert is_tennis_event_slug("mls-fcc-vwh-2026-07-22") is False
    assert is_tennis_event_slug("ufc-gau-stador-2026-08-22") is False
    assert is_tennis_event_slug("lal-ala-vil-2026-08-28") is False
    assert is_tennis_event_slug("") is False


def test_nontennis_cap_is_tennis_p90_times_scale():
    assert nontennis_max_copy_notional_usd(D("0.5")) == (
        TENNIS_EVENT_P90_SOURCE_NOTIONAL_USD * D("0.5")
    )
    assert max_buy_notional_usd_for_profile(
        profile_key=zockdo.PROFILE_KEY,
        event_slug="atp-alcaraz-sinner-2026-08-01",
        scale=D("0.5"),
    ) is None
    assert max_buy_notional_usd_for_profile(
        profile_key=zockdo.PROFILE_KEY,
        event_slug="ufc-gau-stador-2026-08-22",
        scale=D("0.5"),
    ) == D("271.65")
    assert max_buy_notional_usd_for_profile(
        profile_key="cd90",
        event_slug="ufc-gau-stador-2026-08-22",
        scale=D("0.5"),
    ) is None
    assert max_buy_notional_usd_for_profile(
        profile_key=zockdo.PROFILE_KEY,
        event_slug="",
        scale=D("0.5"),
    ) == D("271.65")


def test_plan_action_clips_nontennis_buy_notional_and_keeps_proportional():
    plan = plan_action(
        side="BUY",
        source_quantity=D("10000"),
        scale=D("0.5"),
        held_quantity=D("0"),
        minimum_order_size=D("5"),
        minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.40"),
        visible_best_level_size=D("100000"),
        taker_fee_bps=D("0"),
        available_cash=D("5000"),
        max_buy_notional_usd=D("271.65"),
    )
    assert plan.terminal_status == "READY"
    assert plan.reason == "NON_TENNIS_COPY_NOTIONAL_CAP"
    assert plan.proportional_quantity == D("5000")
    assert plan.order_amount_usd == D("271.65")
    assert plan.requested_quantity == D("271.65") / D("0.40")


def test_plan_action_does_not_clip_when_under_cap():
    plan = plan_action(
        side="BUY",
        source_quantity=D("40"),
        scale=D("0.5"),
        held_quantity=D("0"),
        minimum_order_size=D("5"),
        minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.40"),
        visible_best_level_size=D("100"),
        taker_fee_bps=D("0"),
        available_cash=D("100"),
        max_buy_notional_usd=D("271.65"),
    )
    assert plan.terminal_status == "READY"
    assert plan.reason == ""
    assert plan.requested_quantity == D("20")
    assert plan.order_amount_usd == D("8")


def test_plan_action_does_not_clip_sells():
    plan = plan_action(
        side="SELL",
        source_quantity=D("10000"),
        scale=D("0.5"),
        held_quantity=D("5000"),
        minimum_order_size=D("5"),
        minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.40"),
        visible_best_level_size=D("100000"),
        taker_fee_bps=D("0"),
        available_cash=D("0"),
        max_buy_notional_usd=D("271.65"),
    )
    assert plan.terminal_status == "READY"
    assert plan.requested_quantity == D("5000")


class _CapExecution:
    def __init__(self):
        self.calls = []
        self.response = {"success": True, "orderID": "order-cap"}
        self.orders = {}
        self.associated_trades = {}

    def collateral_balance_usd(self):
        return D("5000")

    def snapshot(self, *, token_id: str, side: str):
        assert token_id == "123"
        return {
            "minimum_order_size": "5",
            "minimum_marketable_buy_notional_usd": "1",
            "best_price": "0.40" if side == "BUY" else "0.30",
            "tick_size": "0.01",
            "visible_best_level_size": "100000",
            "fee_bps": "0",
            "raw_book": {"asks": [], "bids": []},
        }

    def submit_fak_exact_shares(
        self,
        *,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        user_usdc_balance: Decimal | None = None,
    ):
        self.calls.append({"size": size, "price": price, "side": side})
        return self.response

    def submit_fak_market(self, **kwargs):
        return self.submit_fak_exact_shares(**kwargs)

    def get_order(self, order_id: str):
        return self.orders[order_id]


def _freeze_slug(store, source, event_slug: str) -> None:
    store.freeze_action_metadata(
        source=source,
        metadata={
            "condition_id": "0x" + "ab" * 32,
            "market_slug": event_slug,
            "event_slug": event_slug,
        },
        profile_follow=True,
        profile_reason="FULL_WALLET_ACTION_ELIGIBLE",
        frozen_at_ms=1,
    )


def test_zockdo_execute_clips_ufc_and_leaves_tennis_uncapped(tmp_path):
    def run(*, marker: str, event_slug: str):
        store = core.LiveStore(tmp_path / f"{marker}.sqlite3")
        core.initialize_scale_once(
            store=store,
            allocation_usd=D("5000"),
            source_open_position_value_usd=D("10000"),
            observed_at_ms=1,
        )
        source = action(quantity="10000", marker=marker)
        _freeze_slug(store, source, event_slug)
        execution = _CapExecution()
        result = core.execute_source_action(
            store=store,
            source=source,
            execution=execution,
            live_enabled=True,
            profile_key=zockdo.PROFILE_KEY,
        )
        return result, execution

    tennis_result, tennis_execution = run(
        marker="a", event_slug="atp-alcaraz-sinner-2026-08-01"
    )
    ufc_result, ufc_execution = run(
        marker="b", event_slug="ufc-gau-stador-2026-08-22"
    )
    assert tennis_result["terminal_status"] in {"SUBMITTED_UNRECONCILED", "FILLED"}
    assert ufc_result["terminal_status"] in {"SUBMITTED_UNRECONCILED", "FILLED"}
    assert tennis_execution.calls[0]["size"] == D("5000")
    assert ufc_execution.calls[0]["size"] == D("271.65") / D("0.40")


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

    def resolve_action(self, action) -> ScopeDecision:
        return self._decision()

    def resolve_retry_lifecycle(self, action, frozen_metadata):
        return ScopeDecision(True, "OFFICIAL_RETRY_MARKET_LIFECYCLE", frozen_metadata)


def test_zockdo_scope_skips_soccer_buy_and_keeps_sell():
    soccer = ZockdoEventScope(_FullWalletStub("lal-ala-vil-2026-08-28"))
    buy = action(side="BUY", marker="s")
    skipped = soccer.resolve_action(buy)
    assert skipped.follow is False
    assert skipped.reason == ZOCKDO_SOCCER_SLEEVE_STOPPED
    assert skipped.metadata["event_slug"] == "lal-ala-vil-2026-08-28"

    sold = soccer.resolve_action(action(side="SELL", marker="t"))
    assert sold.follow is True
    assert sold.reason == "FULL_WALLET_ACTION_ELIGIBLE"


def test_zockdo_scope_still_follows_tennis_and_ufc_buys():
    tennis = ZockdoEventScope(_FullWalletStub("atp-alcaraz-sinner-2026-08-01"))
    ufc = ZockdoEventScope(_FullWalletStub("ufc-gau-stador-2026-08-22"))
    assert tennis.resolve_action(action(side="BUY", marker="u")).follow is True
    assert ufc.resolve_action(action(side="BUY", marker="v")).follow is True


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
        def __init__(self):
            self.calls = []

        def collateral_balance_usd(self):
            return D("5000")

        def snapshot(self, *, token_id: str, side: str):
            raise AssertionError("soccer BUY must not snapshot the book")

        def submit_fak_exact_shares(self, **kwargs):
            raise AssertionError("soccer BUY must not submit")

    store = core.LiveStore(tmp_path / "live.sqlite3")
    core.initialize_scale_once(
        store=store,
        allocation_usd=D("5000"),
        source_open_position_value_usd=D("10000"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    source = action(side="BUY", quantity="10000", marker="w")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=ZockdoEventScope(_FullWalletStub("epl-ars-che-2026-08-23")),
        profile_key=zockdo.PROFILE_KEY,
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
    latest = store.latest_transition(source)
    assert latest["terminal_status"] == "SKIPPED"
    assert latest["reason"] == ZOCKDO_SOCCER_SLEEVE_STOPPED
    frozen = store.frozen_action_metadata(source.action_id)
    assert frozen["profile_follow"] is False
    assert frozen["profile_reason"] == ZOCKDO_SOCCER_SLEEVE_STOPPED


def test_zockdo_follower_still_plans_soccer_sell(tmp_path):
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
        def __init__(self):
            self.calls = []

        def collateral_balance_usd(self):
            return D("5000")

        def snapshot(self, *, token_id: str, side: str):
            return {
                "minimum_order_size": "5",
                "minimum_marketable_buy_notional_usd": "1",
                "best_price": "0.30",
                "tick_size": "0.01",
                "visible_best_level_size": "100000",
                "fee_bps": "0",
                "raw_book": {"bids": [], "asks": []},
            }

        def submit_fak_exact_shares(
            self,
            *,
            token_id: str,
            side: str,
            price,
            size,
            user_usdc_balance=None,
        ):
            self.calls.append({"side": side, "size": size, "price": price})
            return {"success": True, "orderID": "order-sell"}

        def submit_fak_market(self, **kwargs):
            return self.submit_fak_exact_shares(**kwargs)

        def get_order(self, order_id: str):
            return {}

    store = core.LiveStore(tmp_path / "live.sqlite3")
    core.initialize_scale_once(
        store=store,
        allocation_usd=D("5000"),
        source_open_position_value_usd=D("10000"),
        observed_at_ms=1,
    )
    store.set_runtime("last_processed_block", "99")
    with store.connect() as connection:
        connection.execute(
            "INSERT INTO positions(token_id, quantity, cost_basis_usd) "
            "VALUES('123', '20', '8')"
        )
    source = action(side="SELL", quantity="40", marker="x")
    follower = LiveSourceFollower(
        store=store,
        rpc=FakeRpc(head=100),
        source_wallet="0x" + "a" * 40,
        clock_ms=lambda: 1_700_000_000_000,
        action_scope=ZockdoEventScope(_FullWalletStub("lal-ala-vil-2026-08-28")),
        profile_key=zockdo.PROFILE_KEY,
    )
    follower._new_source_actions = lambda **_kwargs: [source]
    execution = FakeExecution()
    follower.run_cycle_to_head(head=100, execution=execution, live_enabled=True)
    assert execution.calls
    assert execution.calls[0]["side"] == "SELL"
    latest = store.latest_transition(source)
    assert latest["terminal_status"] in {"SUBMITTED_UNRECONCILED", "FILLED"}

