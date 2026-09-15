from decimal import Decimal

import cd90_live_copy as core
import mean_impediment_live_copy as mi


def test_mean_impediment_profile_contract(tmp_path):
    assert core.minimum_size_policy_for_profile(mi.PROFILE_KEY) == (
        core.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )
    assert core.source_action_detection_contract_for_profile(mi.PROFILE_KEY) == (
        core.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    )
    store = core.LiveStore(tmp_path / "live.sqlite3")
    receipt = mi.prepare_store(store=store, observed_at_ms=123)
    assert store.config("source_wallet") == mi.SOURCE_WALLET
    assert store.fixed_share_scale() == Decimal("0.1667")
    assert store.account_snapshot()["cash_usd"] == Decimal("0")
    assert receipt["historical_catch_up"] is False


def test_mean_impediment_profile_contract_is_restart_stable(tmp_path):
    store = core.LiveStore(tmp_path / "live.sqlite3")
    first = mi.prepare_store(store=store, observed_at_ms=123)
    restarted = mi.prepare_store(store=store, observed_at_ms=456)
    assert restarted == first
    assert restarted["observed_at_ms"] == 123


def test_mean_impediment_env_uses_exact_source_and_scale_input():
    values = mi.build_core_env(
        {
            "POLYMARKET_SHARED_WALLET_LOCK_PATH": "/tmp/wallet.lock",
            "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH": "/tmp/coordinator.sqlite3",
            "CD90_MARKETABLE_BUY_MIN_NOTIONAL_USD": "1",
        }
    )
    assert values["CD90_SOURCE_WALLET"] == mi.SOURCE_WALLET
    assert values["CD90_ALLOCATION_USD"] == "0"


def test_five_usd_per_thirty_usd_sizing_holds_at_the_source_ladder():
    """One sixth of source shares == $5 of ours per $30 of his."""

    for source_notional, price in (
        (Decimal("30"), Decimal("0.50")),
        (Decimal("30"), Decimal("0.35")),
        (Decimal("60"), Decimal("0.60")),
    ):
        source_shares = source_notional / price
        our_shares = source_shares * mi.FIXED_SHARE_SCALE
        our_notional = our_shares * price
        assert abs(our_notional - source_notional / 6) < Decimal("0.01")


def test_below_minimum_source_orders_are_skipped_not_rounded_up():
    """A $15 source order at 0.60 maps to 4.2 shares and must be skipped."""

    source_shares = Decimal("15") / Decimal("0.60")
    our_shares = source_shares * mi.FIXED_SHARE_SCALE
    assert our_shares < Decimal("5")
    assert core.minimum_size_policy_for_profile(mi.PROFILE_KEY) == (
        core.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )


def _stub_follower(tmp_path, profile_key, total_rows):
    """A follower whose public trade pager serves `total_rows` synthetic rows."""

    store = core.LiveStore(tmp_path / f"{profile_key}-{total_rows}.sqlite3")
    store.lock_config_once("profile_key", profile_key)
    follower = core.LiveSourceFollower(
        store=store,
        rpc=None,
        source_wallet=mi.SOURCE_WALLET,
        clock_ms=lambda: 1789500000000,
    )
    follower.public_get_json = lambda url: []
    page_size = core.PUBLIC_WALLET_TRADE_PAGE_SIZE

    def read_page(*, offset):
        return [
            {
                "row_id": f"row-{i:07d}",
                "transaction_hash": f"0x{i:064x}",
                "token_id": str(1000 + i),
                "side": "BUY",
                "source_quantity": "10",
                "source_price": "0.5",
                "source_timestamp": 1789500000 + i,
                "raw": {"i": i},
            }
            for i in range(offset, min(offset + page_size, total_rows))
        ]

    follower._read_verified_public_wallet_trade_page = read_page
    return follower, store


def _oversized(): 
    return core.PUBLIC_WALLET_TRADE_PAGE_SIZE * core.PUBLIC_WALLET_TRADE_MAX_PAGES + 5000


def test_bounded_baseline_accepted_for_high_frequency_source(tmp_path):
    """The source out-trades the pager, so no short final page ever arrives."""

    follower, store = _stub_follower(
        tmp_path, mi.PROFILE_KEY, _oversized()
    )
    rows = follower._establish_public_wallet_forward_watermark()
    assert rows == (
        core.PUBLIC_WALLET_TRADE_PAGE_SIZE * core.PUBLIC_WALLET_TRADE_MAX_PAGES
    )
    assert store.runtime_value(
        "public_wallet_forward_watermark_reached_history_end"
    ) == "false"


import pytest


def _other_profiles() -> list[str]:
    """Every live profile except the one that opted into the bounded path.

    Derived from the module so a profile imported from production later is
    guarded the moment its constant lands, instead of waiting for someone to
    remember to extend a hand-written list.
    """

    return sorted(
        value
        for name in dir(core)
        if name.startswith("LIVE_PROFILE_")
        and isinstance(value := getattr(core, name), str)
        and value != core.LIVE_PROFILE_MEAN_IMPEDIMENT_BTC5M
    )


@pytest.mark.parametrize("profile_key", _other_profiles())
def test_other_profiles_still_require_complete_history(tmp_path, profile_key):
    """Regression guard: the bounded path must stay opt-in per profile."""

    follower, _ = _stub_follower(tmp_path, profile_key, _oversized())
    with pytest.raises(ConnectionError) as excinfo:
        follower._establish_public_wallet_forward_watermark()
    assert str(excinfo.value) == "PUBLIC_WALLET_FORWARD_WATERMARK_CURSOR_GAP"


@pytest.mark.parametrize(
    "profile_key",
    [core.LIVE_PROFILE_CD90, "mean_impediment_btc5m"],
)
def test_small_source_still_takes_the_complete_baseline_path(tmp_path, profile_key):
    follower, store = _stub_follower(tmp_path, profile_key, 1500)
    assert follower._establish_public_wallet_forward_watermark() == 1500
    assert store.runtime_value(
        "public_wallet_forward_watermark_reached_history_end"
    ) == "true"
