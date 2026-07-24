from __future__ import annotations

from candidate_scan_once import (
    analyze_trade_lifecycle,
    discover_leaderboard_candidates,
    fetch_trades,
    merge_candidate_pool,
    ordered_candidate_wallets,
    partition_noncrypto_rows,
)


def leaderboard_row(wallet: str, name: str, rank: int) -> dict[str, object]:
    return {
        "proxyWallet": wallet,
        "userName": name,
        "rank": str(rank),
        "pnl": 12.5,
        "vol": 50.0,
    }


def trade(
    wallet: str,
    condition: str,
    asset: str,
    side: str,
    timestamp: int,
    title: str,
    outcome: str = "Yes",
) -> dict[str, object]:
    return {
        "proxyWallet": wallet,
        "conditionId": condition,
        "asset": asset,
        "side": side,
        "timestamp": timestamp,
        "title": title,
        "eventSlug": title.lower().replace(" ", "-"),
        "outcome": outcome,
        "transactionHash": f"{condition}-{asset}-{side}-{timestamp}",
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


def test_trade_history_uses_windows_and_taker_false():
    wallet = "0x" + "d" * 40
    calls: list[dict[str, object]] = []

    def fetch(endpoint: str, params: dict[str, object]):
        assert endpoint == "trades"
        calls.append(dict(params))
        end = params.get("end")
        offset = params["offset"]
        if end is None:
            if offset == 0:
                return [
                    trade(wallet, "c", "a", "BUY", 5, "Weather temperature"),
                    trade(wallet, "c", "a", "BUY", 4, "Weather temperature"),
                ]
            return [
                trade(wallet, "c", "a", "BUY", 3, "Weather temperature"),
                trade(wallet, "c", "a", "SELL", 3, "Weather temperature"),
            ]
        if end == 3 and offset == 0:
            return [
                trade(wallet, "c", "a", "BUY", 3, "Weather temperature"),
                trade(wallet, "c", "a", "SELL", 3, "Weather temperature"),
            ]
        if end == 3 and offset == 2:
            return [
                trade(wallet, "old", "b", "BUY", 2, "Politics election"),
                trade(wallet, "old", "b", "BUY", 1, "Politics election"),
            ]
        return [trade(wallet, "old", "b", "BUY", 1, "Politics election")]

    rows, complete, detail = fetch_trades(
        wallet,
        fetch=fetch,
        page_size=2,
        max_offset=2,
    )

    assert complete is True
    assert {row["timestamp"] for row in rows} == {1, 2, 3, 4, 5}
    assert all(call["takerOnly"] == "false" for call in calls)
    assert all(call["start"] == 1 for call in calls)
    assert any(call.get("end") == 3 for call in calls)
    assert detail["window_count"] > 1


def test_trade_history_blocks_if_one_timestamp_exceeds_window_capacity():
    wallet = "0x" + "e" * 40

    def fetch(endpoint: str, params: dict[str, object]):
        return [
            trade(wallet, "c", f"a-{params['offset']}", "BUY", 7, "Election"),
            trade(wallet, "c", f"b-{params['offset']}", "SELL", 7, "Election"),
        ]

    _, complete, detail = fetch_trades(
        wallet,
        fetch=fetch,
        page_size=2,
        max_offset=2,
    )

    assert complete is False
    assert detail["block_reason"] == "same_timestamp_exceeds_offset_window"


def test_crypto_rows_are_removed_but_mixed_wallet_survives():
    wallet = "0x" + "f" * 40
    rows = [
        trade(wallet, "crypto", "btc", "BUY", 1, "Bitcoin above 100k?"),
        trade(wallet, "weather", "temp", "BUY", 2, "NYC temperature"),
    ]

    kept, removed = partition_noncrypto_rows(rows)

    assert [row["conditionId"] for row in kept] == ["weather"]
    assert [row["conditionId"] for row in removed] == ["crypto"]


def test_lifecycle_separates_hold_exit_and_basket():
    wallet = "0x" + "1" * 40
    rows = [
        trade(wallet, "hold", "hold-yes", "BUY", 1, "Election", "Yes"),
        trade(wallet, "exit", "exit-yes", "BUY", 2, "Election", "Yes"),
        trade(wallet, "exit", "exit-yes", "SELL", 3, "Election", "Yes"),
        trade(wallet, "basket", "basket-yes", "BUY", 4, "Election", "Yes"),
        trade(wallet, "basket", "basket-no", "BUY", 5, "Election", "No"),
    ]

    result = analyze_trade_lifecycle(rows, closed_condition_ids={"hold"})

    assert result["conditions"]["hold"]["lifecycle"] == "HOLD_TO_RESOLUTION"
    assert result["conditions"]["exit"]["lifecycle"] == "ACTIVE_EXIT"
    assert result["conditions"]["basket"]["lifecycle"] == "BASKET_OR_HEDGE"
    assert result["strategy_state"] == "FORMULA_RESEARCH"


def test_exact_same_timestamp_opposite_sides_without_directional_evidence_is_speed_risk():
    wallet = "0x" + "2" * 40
    rows = [
        trade(wallet, "cycle", "cycle-yes", "BUY", 10, "Election", "Yes"),
        trade(wallet, "cycle", "cycle-yes", "SELL", 10, "Election", "Yes"),
    ]

    result = analyze_trade_lifecycle(rows, closed_condition_ids=set())

    assert result["same_timestamp_opposite_side_cycles"] == 1
    assert result["strategy_state"] == "OBSERVABLE_MM_OR_SPEED"
