from __future__ import annotations

from dataclasses import replace
import hashlib
import unittest

from sim.tennis_ev import data, research, statistics


def match_with_path(
    event_id: str = "event-1",
    *,
    finish_ts: int = 300,
    start_ts: int = 100,
    pregame_ts: int = 90,
    path: list[tuple[int, float]] | None = None,
    won: bool = True,
    source_sha256: str = "source-a",
) -> data.MatchRecord:
    points = tuple(path or [(100, 0.60), (160, 0.65), (220, 0.20)])
    return data.MatchRecord(
        event_id=event_id,
        market_id=f"market-{event_id}",
        level="ATP",
        title="Player A vs Player B",
        start_ts=start_ts,
        finish_ts=finish_ts,
        pregame_ts=pregame_ts,
        outcomes=(
            data.OutcomeRecord("yes", "Player A", 0.60, None, won, points),
            data.OutcomeRecord(
                "no", "Player B", 0.40, None, not won,
                tuple((timestamp, 1.0 - price) for timestamp, price in points),
            ),
        ),
        price_fidelity="HISTORICAL_REFERENCE_PRICE",
        match_state=None,
        source_sha256=source_sha256,
    )


class CausalEventTests(unittest.TestCase):
    def test_feature_builder_never_reads_points_after_decision(self) -> None:
        match = match_with_path()

        event = research.build_event(match, decision_ts=160, outcome_index=0)

        self.assertEqual(event.current_price, 0.65)
        self.assertEqual(event.path_high, 0.65)
        self.assertNotIn(0.20, event.observed_prices)
        self.assertIsNone(event.opening_price)
        self.assertEqual(event.reference_entry_price, 0.60)

    def test_decision_before_first_path_point_uses_reference_without_inventing_path(self) -> None:
        match = match_with_path(path=[(120, 0.61), (160, 0.65)])

        event = research.build_event(match, decision_ts=100, outcome_index=0)

        self.assertEqual(event.current_price, 0.60)
        self.assertEqual(event.observed_prices, ())
        self.assertEqual(event.path_high, 0.60)
        self.assertEqual(event.path_low, 0.60)

    def test_untimestamped_historical_book_is_not_available_at_decision(self) -> None:
        base = match_with_path()
        outcome = replace(base.outcomes[0], best_bid=0.59, best_ask=0.61, visible_depth_usd=100.0)
        match = replace(base, outcomes=(outcome, base.outcomes[1]))

        event = research.build_event(match, decision_ts=160, outcome_index=0)

        self.assertIsNone(event.best_bid)
        self.assertIsNone(event.best_ask)
        self.assertIsNone(event.visible_depth_usd)
        self.assertIn("BLOCK_DATA_EXECUTION_BOOK", event.feature_availability)
        self.assertIn("BLOCK_DATA_VISIBLE_DEPTH", event.feature_availability)

    def test_historical_family_builder_does_not_invent_state_checkpoints(self) -> None:
        match = match_with_path(finish_ts=600)

        events, coverage = research.build_decision_point_events((match,))

        self.assertEqual({event.decision_family for event in events}, {"PRE_MATCH_REFERENCE", "SCHEDULED_START_PROXY"})
        self.assertEqual(coverage["BLOCK_DATA_MATCH_STATE"], 1)
        self.assertEqual(coverage["FIRST_SET_END"], 0)
        self.assertEqual(coverage["SECOND_SET_END"], 0)
        self.assertEqual(coverage["DECIDING_SET"], 0)
        self.assertEqual(coverage["SCORE_LEAD"], 0)
        self.assertEqual(coverage["SERVER_STATE"], 0)

    def test_untimestamped_match_state_is_always_blocked(self) -> None:
        match = replace(match_with_path(), match_state={"sets": [1, 0]})

        event = research.build_event(match, decision_ts=160, outcome_index=0)

        self.assertIn("BLOCK_DATA_MATCH_STATE", event.feature_availability)

    def test_split_keeps_equal_finish_group_in_training_before_a_later_test_decision(self) -> None:
        matches = (
            match_with_path("finish-100-a", finish_ts=100, start_ts=20, pregame_ts=10),
            match_with_path("finish-200-a", finish_ts=200, start_ts=120, pregame_ts=110),
            match_with_path("finish-200-b", finish_ts=200, start_ts=120, pregame_ts=110),
            match_with_path("finish-300-a", finish_ts=300, start_ts=220, pregame_ts=210),
        )

        split = research.chronological_split(matches, train_fraction=0.70)

        train_ids = {match.event_id for match in split.train}
        test_ids = {match.event_id for match in split.test}
        self.assertFalse(train_ids & test_ids)
        self.assertTrue({"finish-200-a", "finish-200-b"} <= train_ids)
        self.assertFalse({"finish-200-a", "finish-200-b"} & test_ids)
        self.assertEqual(split.purged, ())
        self.assertEqual(split.boundary_ts, 210)
        self.assertEqual(split.achieved_train_fraction, 0.75)

    def test_split_rejects_an_all_tied_finish_group(self) -> None:
        matches = tuple(match_with_path(str(index), finish_ts=200) for index in range(3))

        with self.assertRaisesRegex(ValueError, "equal finish timestamp"):
            research.chronological_split(matches, train_fraction=0.70)

    def test_split_purges_match_that_crosses_from_training_labels_into_test_decisions(self) -> None:
        early_settlement = match_with_path("early-a", finish_ts=100, start_ts=20, pregame_ts=10)
        second_early_settlement = match_with_path("early-b", finish_ts=200, start_ts=120, pregame_ts=110)
        third_early_settlement = match_with_path("early-c", finish_ts=300, start_ts=220, pregame_ts=210)
        crossing_match = replace(
            match_with_path("crossing", finish_ts=400, start_ts=160, pregame_ts=150),
        )
        late_settlement = match_with_path("late", finish_ts=500, start_ts=310, pregame_ts=300)

        split = research.chronological_split(
            (
                early_settlement, second_early_settlement, third_early_settlement,
                crossing_match, late_settlement,
            ),
            train_fraction=0.70,
        )

        self.assertNotIn("crossing", {match.event_id for match in split.train})
        self.assertNotIn("crossing", {match.event_id for match in split.test})
        self.assertIn("crossing", {match.event_id for match in split.purged})
        self.assertLess(
            max(match.finish_ts for match in split.train),
            min(match.pregame_ts for match in split.test),
        )


class FrozenManifestTests(unittest.TestCase):
    def test_test_labels_and_event_ids_cannot_change_frozen_training_manifest(self) -> None:
        train = (
            research.build_event(match_with_path("train-a", won=True), decision_ts=100, outcome_index=0),
            research.build_event(match_with_path("train-b", won=False), decision_ts=100, outcome_index=0),
        )
        test = (
            research.build_event(match_with_path("test-a", won=True), decision_ts=100, outcome_index=0),
        )

        original = research.freeze_training_manifest(train, alpha=0.05)
        flipped_test = tuple(replace(row, event_id="different-test-id", won=not row.won) for row in test)
        repeated = research.freeze_training_manifest(train, alpha=0.05)

        self.assertEqual(original.to_json(), repeated.to_json())
        self.assertEqual(original.source_event_ids, repeated.source_event_ids)
        self.assertFalse(set(original.source_event_ids) & {row.event_id for row in flipped_test})
        self.assertNotIn("test-a", original.to_json())
        self.assertNotIn("different-test-id", repeated.to_json())

    def test_manifest_json_is_stable_and_contains_only_training_provenance(self) -> None:
        events = (
            research.build_event(match_with_path("z", source_sha256="sha-z"), decision_ts=100, outcome_index=0),
            research.build_event(match_with_path("a", source_sha256="sha-a"), decision_ts=100, outcome_index=0),
        )

        manifest = research.freeze_training_manifest(events, alpha=0.05)

        self.assertEqual(manifest.source_event_ids, ("a", "z"))
        self.assertEqual(manifest.training_source_hashes, ("sha-a", "sha-z"))
        self.assertEqual(
            manifest.training_source_hashes_sha256,
            hashlib.sha256(b"sha-a\nsha-z").hexdigest(),
        )
        self.assertEqual(manifest.to_json(), manifest.to_json())
        self.assertEqual(list(manifest.to_dict()), sorted(manifest.to_dict()))


def favorite_rows(prices: list[float]) -> tuple[research.ResearchEvent, ...]:
    """One eligible binary decision group per requested favorite price."""
    events: list[research.ResearchEvent] = []
    for index, price in enumerate(prices):
        match = match_with_path(
            f"favorite-{index}",
            finish_ts=1_000 + index,
            path=[(100, price)],
            won=index % 2 == 0,
        )
        high = replace(match.outcomes[0], pregame_price=price, path=((100, price),))
        low = replace(
            match.outcomes[1], pregame_price=1.0 - price,
            path=((100, 1.0 - price),),
            won=not high.won,
        )
        match = replace(match, outcomes=(high, low))
        events.extend(
            research.build_event(
                match,
                decision_ts=100,
                outcome_index=outcome_index,
                decision_family="PRE_MATCH_REFERENCE",
            )
            for outcome_index in (0, 1)
        )
    return tuple(events)


class BaselineAndStatisticsTests(unittest.TestCase):
    def test_default_baselines_use_only_one_pre_match_family_per_match(self) -> None:
        match = match_with_path("one-family", finish_ts=600)
        events, _ = research.build_decision_point_events((match,))

        favorites = research.favorite_baseline(events, fee_schedule=None)
        random = research.random_baseline(events, draws=1, seed=1)

        self.assertEqual(sum(row["eligible_matches"] for row in favorites), 1)
        self.assertEqual(random["eligible_matches"], 1)
        self.assertEqual(
            {item["decision_family"] for item in random["eligible_match_ledger"]},
            {"PRE_MATCH_REFERENCE"},
        )

    def test_favorite_bands_have_no_boundary_double_count(self) -> None:
        rows = favorite_rows([0.55, 0.60, 0.70, 0.80, 0.90])

        bands = research.favorite_baseline(rows, fee_schedule=None)

        self.assertEqual(sum(band["eligible_matches"] for band in bands), 5)
        self.assertEqual(
            [band["eligible_matches"] for band in bands], [1, 1, 1, 1, 1]
        )
        self.assertEqual(
            [band["price_band"] for band in bands],
            ["[0.55,0.60)", "[0.60,0.70)", "[0.70,0.80)",
             "[0.80,0.90)", "[0.90,1.00)"],
        )
        self.assertTrue(all(band["execution_cost_block"] == "BLOCK_DATA_FEE" for band in bands))
        self.assertTrue(all(band["economic_result_basis"] == "GROSS_REFERENCE_PROXY" for band in bands))

    def test_metrics_reconcile_to_trade_ledger(self) -> None:
        metrics = statistics.performance_metrics([0.25, -0.10, 0.05], [0.50, 0.50, 1.00])

        self.assertAlmostEqual(metrics["net_pnl"], 0.20)
        self.assertAlmostEqual(metrics["roi"], 0.20 / 2.00)
        self.assertAlmostEqual(metrics["max_drawdown_usd"], 0.10)
        self.assertAlmostEqual(metrics["max_drawdown_settlement_cashflow_usd"], 0.10)
        self.assertEqual(metrics["max_drawdown_basis"], "SETTLEMENT_CASHFLOW")
        self.assertAlmostEqual(metrics["return_mean"], 0.20 / 3.0)
        self.assertIsNotNone(metrics["sharpe_per_match"])
        self.assertNotIn("annualized_sharpe", metrics)

    def test_metrics_drawdown_follows_chronological_settlement_not_input_order(self) -> None:
        ledger = (
            {"event_id": "late-loss", "finish_ts": 30, "pnl": -0.70, "deployed_cost": 0.70},
            {"event_id": "early-loss", "finish_ts": 10, "pnl": -0.50, "deployed_cost": 0.50},
            {"event_id": "middle-win", "finish_ts": 20, "pnl": 0.60, "deployed_cost": 0.40},
        )

        metrics = statistics.performance_metrics(ledger_entries=ledger)

        self.assertAlmostEqual(metrics["net_pnl"], -0.60)
        self.assertAlmostEqual(metrics["max_drawdown_settlement_cashflow_usd"], 0.70)

    def test_favorite_uses_supplied_immutable_fee_schedule_and_records_provenance(self) -> None:
        rows = favorite_rows([0.60])
        schedule = research.FeeSchedule(
            rate=0.10,
            exponent=2.0,
            source="TEST_IMMUTABLE_FEE_SCHEDULE",
        )

        result = research.favorite_baseline(rows, fee_schedule=schedule)[1]

        ledger = result["trade_ledger"]
        self.assertEqual(result["fee_source"], "TEST_IMMUTABLE_FEE_SCHEDULE")
        self.assertEqual(result["economic_result_basis"], "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE")
        self.assertAlmostEqual(ledger[0]["fee_per_share"], 0.00576)
        self.assertAlmostEqual(ledger[0]["deployed_cost"], 0.60576)
        self.assertAlmostEqual(ledger[0]["pnl"], 0.39424)
        self.assertEqual(ledger[0]["fee_rate"], 0.10)
        self.assertEqual(ledger[0]["fee_exponent"], 2.0)
        self.assertEqual(ledger[0]["fee_source"], "TEST_IMMUTABLE_FEE_SCHEDULE")

    def test_fee_schedule_rejects_invalid_values_and_source(self) -> None:
        invalid_inputs = (
            (float("nan"), 1.0, "source"),
            (-0.01, 1.0, "source"),
            (0.01, float("inf"), "source"),
            (0.01, 0.0, "source"),
            (0.01, 1.0, ""),
        )

        for rate, exponent, source in invalid_inputs:
            with self.subTest(rate=rate, exponent=exponent, source=source), self.assertRaisesRegex(ValueError, "fee schedule"):
                research.FeeSchedule(rate=rate, exponent=exponent, source=source)

    def test_absent_fee_schedule_stays_gross_proxy_and_blocks_fee_claim(self) -> None:
        result = research.favorite_baseline(favorite_rows([0.60]), fee_schedule=None)[1]

        self.assertEqual(result["economic_result_basis"], "GROSS_REFERENCE_PROXY")
        self.assertEqual(result["execution_cost_block"], "BLOCK_DATA_FEE")
        self.assertIsNone(result["fee_source"])

    def test_binary_group_rejects_non_complementary_or_non_unique_outcomes(self) -> None:
        left, right = favorite_rows([0.60])
        malformed = (left, replace(right, token_id=left.token_id, won=True))

        with self.assertRaisesRegex(ValueError, "distinct token IDs and complementary winners"):
            research._binary_decision_groups(malformed)

    def test_random_baseline_retains_matches_times_and_selection_count(self) -> None:
        rows = favorite_rows([0.60, 0.70, 0.80, 0.90])

        result = research.random_baseline(rows, draws=10_000, seed=20260812, fee_schedule=None)

        self.assertEqual(result["eligible_matches"], 4)
        self.assertEqual(result["selections_per_draw"], 4)
        self.assertEqual(result["draws"], 10_000)
        self.assertEqual(result["seed"], 20260812)
        self.assertEqual(
            tuple(item["decision_ts"] for item in result["eligible_match_ledger"]),
            (100, 100, 100, 100),
        )
        self.assertEqual(len(result["net_pnl_distribution"]), 10_000)
        self.assertEqual(result["economic_result_basis"], "GROSS_REFERENCE_PROXY")
        repeated = research.random_baseline(rows, draws=10_000, seed=20260812, fee_schedule=None)
        self.assertEqual(result["net_pnl_distribution"], repeated["net_pnl_distribution"])


if __name__ == "__main__":
    unittest.main()
