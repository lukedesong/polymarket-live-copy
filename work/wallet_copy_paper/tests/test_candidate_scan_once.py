from __future__ import annotations

from candidate_scan_once import (
    discover_leaderboard_candidates,
    merge_candidate_pool,
    ordered_candidate_wallets,
)


def leaderboard_row(wallet: str, name: str, rank: int) -> dict[str, object]:
    return {
        "proxyWallet": wallet,
        "userName": name,
        "rank": str(rank),
        "pnl": 12.5,
        "vol": 50.0,
    }


def test_discovery_excludes_crypto_and_deduplicates_origins():
    calls: list[tuple[str, dict[str, object]]] = []

    def fetch(endpoint: str, params: dict[str, object]):
        calls.append((endpoint, dict(params)))
        if params["category"] == "WEATHER":
            return [leaderboard_row("0x" + "a" * 40, "weather-a", 1)]
        return [leaderboard_row("0x" + "A" * 40, "politics-a", 2)]

    found, coverage = discover_leaderboard_candidates(
        fetch,
        categories=("WEATHER", "CRYPTO", "POLITICS"),
        periods=("MONTH",),
        observed_at="2026-07-25T00:00:00+00:00",
    )

    assert {params["category"] for _, params in calls} == {
        "WEATHER",
        "POLITICS",
    }
    assert list(found) == ["0x" + "a" * 40]
    assert {
        origin["category"] for origin in found["0x" + "a" * 40]["origins"]
    } == {
        "WEATHER",
        "POLITICS",
    }
    assert coverage["complete"] is True


def test_pool_keeps_dynamic_wallet_and_legacy_is_only_provenance():
    dynamic = "0x" + "b" * 40
    legacy = "0x" + "c" * 40
    pool = merge_candidate_pool(
        {},
        {
            dynamic: {
                "wallet": dynamic,
                "name": "new-wallet",
                "origins": [{"source": "leaderboard", "rank": 1}],
            }
        },
        {"legacy-name": legacy},
        observed_at="2026-07-25T00:00:00+00:00",
    )

    assert set(pool) == {dynamic, legacy}
    assert pool[dynamic]["legacy_seed"] is False
    assert pool[legacy]["legacy_seed"] is True
    assert ordered_candidate_wallets(pool)[0] == dynamic
