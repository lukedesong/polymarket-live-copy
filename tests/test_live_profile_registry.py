"""Registry guards for the live-copy profile table.

The profile constants, the minimum-size policy map, the source-action
detection contract and the bounded-baseline opt-in are four separate tables
that have to agree.  A profile added to one table and forgotten in another is
exactly the divergence this file exists to catch, so every case is derived
from the module instead of a hand-written list: a profile introduced later is
covered the moment its constant lands.
"""

import pytest

import cd90_live_copy as core

KNOWN_DETECTION_CONTRACTS = frozenset(
    {
        core.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET,
        core.SOURCE_ACTION_DETECTION_CONTRACT_CHAIN_MAKER_ONLY,
    }
)

# The three wallets that run out of their own executor module rather than out
# of the generic cd90 follower.  Named explicitly because the point of the
# assertion is that these exact production profiles stay full-wallet.
IMPORTED_PRODUCTION_WALLETS = (
    core.LIVE_PROFILE_WALLET_42B79C_FULL_WALLET,
    core.LIVE_PROFILE_MAURICEBISHOP_FULL_WALLET,
    core.LIVE_PROFILE_ALBUMISTA_FULL_WALLET,
)


def declared_profiles() -> dict[str, str]:
    """Every LIVE_PROFILE_* string constant exported by the core module."""

    return {
        name: getattr(core, name)
        for name in dir(core)
        if name.startswith("LIVE_PROFILE_") and isinstance(getattr(core, name), str)
    }


def profile_ids() -> list[str]:
    return sorted(declared_profiles())


@pytest.fixture(name="profiles")
def _profiles() -> dict[str, str]:
    return declared_profiles()


def test_the_registry_is_not_accidentally_empty(profiles):
    assert set(profiles.values()) >= {
        core.LIVE_PROFILE_CD90,
        core.LIVE_PROFILE_ZOCKDO_FULL_WALLET,
        *IMPORTED_PRODUCTION_WALLETS,
    }


@pytest.mark.parametrize("name", profile_ids())
def test_every_declared_profile_has_a_minimum_size_policy(name):
    key = getattr(core, name)
    assert key in core.LIVE_PROFILE_MINIMUM_SIZE_POLICIES, (
        f"{name} is declared but missing from LIVE_PROFILE_MINIMUM_SIZE_POLICIES; "
        "the follower would raise UNKNOWN_LIVE_PROFILE at sizing time"
    )
    assert core.minimum_size_policy_for_profile(key) == (
        core.LIVE_PROFILE_MINIMUM_SIZE_POLICIES[key]
    )


@pytest.mark.parametrize("name", profile_ids())
def test_every_declared_profile_resolves_a_detection_contract(name):
    contract = core.source_action_detection_contract_for_profile(getattr(core, name))
    assert contract in KNOWN_DETECTION_CONTRACTS


@pytest.mark.parametrize("profile_key", IMPORTED_PRODUCTION_WALLETS)
def test_imported_production_wallets_are_full_wallet_followers(profile_key):
    assert core.source_action_detection_contract_for_profile(profile_key) == (
        core.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET
    )
    assert core.minimum_size_policy_for_profile(profile_key) == (
        core.MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM
    )


def test_an_unregistered_profile_is_rejected_rather_than_defaulted():
    with pytest.raises(core.LiveConfigurationError):
        core.minimum_size_policy_for_profile("not-a-profile")
    with pytest.raises(core.LiveConfigurationError):
        core.source_action_detection_contract_for_profile("not-a-profile")


def test_bounded_public_baseline_stays_opt_in(profiles):
    """Only the profile that out-trades the pager may skip end-of-history.

    Every other profile must keep the strict complete-baseline requirement,
    including profiles added after this test was written.
    """

    opted_in = set(core.PUBLIC_WALLET_BOUNDED_BASELINE_PROFILES)
    assert opted_in == {core.LIVE_PROFILE_MEAN_IMPEDIMENT_BTC5M}
    for name, key in sorted(profiles.items()):
        if key == core.LIVE_PROFILE_MEAN_IMPEDIMENT_BTC5M:
            continue
        assert key not in opted_in, (
            f"{name} was opted in to the bounded public baseline; that weakens "
            "its no-replay guarantee and needs an explicit decision"
        )
