from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import unittest

import numpy as np

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
    def test_split_handles_a_thousand_records_without_record_equality_partitioning(self) -> None:
        """A real-sized split must not repeatedly compare full path records."""
        matches = tuple(
            match_with_path(
                f"large-{index}", finish_ts=index * 100 + 80,
                start_ts=index * 100 + 20, pregame_ts=index * 100 + 10,
                path=[(index * 100 + 20, 0.60)],
            )
            for index in range(1_000)
        )

        split = research.chronological_split(matches, train_fraction=0.70)

        self.assertEqual(len(split.train) + len(split.test) + len(split.purged), len(matches))
        self.assertTrue(all(match.finish_ts < split.boundary_ts for match in split.train))
        self.assertTrue(all(match.pregame_ts > split.boundary_ts for match in split.test))
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

    def test_split_purges_boundary_decisions_and_keeps_test_strictly_after_cutoff(self) -> None:
        matches = (
            match_with_path("finish-100-a", finish_ts=100, start_ts=20, pregame_ts=10),
            match_with_path("finish-200-a", finish_ts=200, start_ts=120, pregame_ts=110),
            match_with_path("finish-200-b", finish_ts=200, start_ts=120, pregame_ts=110),
            match_with_path("finish-300-a", finish_ts=300, start_ts=230, pregame_ts=220),
        )

        split = research.chronological_split(matches, train_fraction=0.70)

        train_ids = {match.event_id for match in split.train}
        test_ids = {match.event_id for match in split.test}
        self.assertFalse(train_ids & test_ids)
        self.assertFalse({"finish-200-a", "finish-200-b"} & train_ids)
        self.assertFalse({"finish-200-a", "finish-200-b"} & test_ids)
        self.assertEqual({match.event_id for match in split.purged}, {"finish-200-a", "finish-200-b"})
        self.assertEqual(split.boundary_ts, 110)
        self.assertEqual(split.achieved_train_fraction, 0.50)
        self.assertTrue(all(match.finish_ts < split.boundary_ts for match in split.train))
        self.assertTrue(all(match.pregame_ts > split.boundary_ts for match in split.test))

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


def feature_rows(
    prices: list[float], *, levels: list[str | None] | None = None,
    volatility: list[float | None] | None = None,
) -> tuple[research.ResearchEvent, ...]:
    """Independent match rows with deliberately hand-checkable outcomes."""
    rows: list[research.ResearchEvent] = []
    for index, price in enumerate(prices):
        row = research.build_event(
            match_with_path(
                f"condition-{index}", finish_ts=1_000 + index,
                path=[(100, price)], won=index % 2 == 0,
            ), decision_ts=100, outcome_index=0,
        )
        rows.append(replace(
            row,
            current_price=price,
            reference_entry_price=price,
            level=(levels or ["ATP"] * len(prices))[index],
            realized_volatility=(volatility or [None] * len(prices))[index],
        ))
    return tuple(rows)


class ConditionDiscoveryTests(unittest.TestCase):
    def test_condition_results_use_the_same_supplied_fee_schedule_in_training_and_holdout(self) -> None:
        schedule = research.FeeSchedule(0.10, 2.0, "TEST_FEE")
        train = feature_rows([0.60, 0.70, 0.80, 0.90])
        manifest = research.freeze_training_manifest(train, alpha=0.05, fee_schedule=schedule)

        self.assertEqual(manifest.cost_specification["fee_schedule"]["source"], "TEST_FEE")
        self.assertEqual(manifest.cost_specification["economic_result_basis"], "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE")
        self.assertTrue(all(
            row["economic_result_basis"] == "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"
            for row in manifest.condition_ledger if not row["rule_id"].startswith("UNAVAILABLE_")
        ))

        selected = manifest.selected_rule_ids
        if selected:
            holdout = tuple(replace(event, event_id=f"holdout-{event.event_id}", decision_ts=10_000) for event in train)
            results = research.evaluate_holdout(manifest, holdout, fee_schedule=schedule)
            self.assertTrue(all(result.economic_result_basis == "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE" for result in results))

    def test_holdout_rejects_a_fee_schedule_that_differs_from_the_frozen_training_schedule(self) -> None:
        train = feature_rows([0.60, 0.70, 0.80, 0.90])
        manifest = research.freeze_training_manifest(
            train, alpha=0.05, fee_schedule=research.FeeSchedule(0.10, 2.0, "TRAIN"),
        )
        holdout = tuple(replace(event, event_id=f"holdout-{event.event_id}", decision_ts=10_000) for event in train)

        with self.assertRaisesRegex(ValueError, "fee schedule"):
            research.evaluate_holdout(
                manifest, holdout, fee_schedule=research.FeeSchedule(0.20, 2.0, "OTHER"),
            )
    def test_outcome_side_null_does_not_treat_fair_90_percent_favorites_as_edge(self) -> None:
        groups = []
        for index in range(10):
            match = match_with_path(
                f"calibration-{index}", finish_ts=1_000 + index,
                path=[(100, 0.90)], won=index < 9,
            )
            groups.append(tuple(
                research.build_event(match, decision_ts=100, outcome_index=outcome_index)
                for outcome_index in (0, 1)
            ))

        p_value = statistics.outcome_side_permutation_p_value(
            tuple((pair[0], pair[1]) for pair in groups), draws=1_000, seed=7,
        )

        self.assertGreater(p_value, 0.05)

    def test_outcome_side_permutation_uses_supplied_net_pair_pnls_without_price_recomputation(self) -> None:
        """The null must use the same already-costed returns as reported PnL."""
        selections = ((object(), object()), (object(), object()))
        net_pairs = ((0.30, -0.80), (0.30, -0.80))

        actual = statistics.outcome_side_permutation_p_value(
            selections, pnl_pairs=net_pairs, draws=4, seed=1,
        )

        choices = np.random.default_rng(1).integers(0, 2, size=(4, 2))
        null = np.sum(np.where(
            choices == 0,
            np.array([0.30, 0.30]),
            np.array([-0.80, -0.80]),
        ), axis=1)
        expected = (1 + np.count_nonzero(null >= 0.60)) / 5
        self.assertEqual(actual, expected)

    def test_condition_selection_uses_one_deterministic_outcome_per_match(self) -> None:
        match = match_with_path("condition-pair", finish_ts=1_000, path=[(100, 0.60)])
        events = tuple(
            research.build_event(match, decision_ts=100, outcome_index=outcome_index)
            for outcome_index in (0, 1)
        )
        condition = research.Condition(
            "all-outcomes", "market_level",
            (research.Clause("current_price", "GE", 0.0),), "TEST",
        )

        selected = research._condition_selection_groups(condition, events)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0][0].outcome_index, 0)
        self.assertEqual(selected[0][1].outcome_index, 1)

    def test_condition_selection_uses_one_fixed_position_across_decision_families(self) -> None:
        match = match_with_path("condition-across-families", finish_ts=1_000, path=[(100, 0.60), (400, 0.70)])
        events = tuple(
            research.build_event(
                match,
                decision_ts=decision_ts,
                outcome_index=outcome_index,
                decision_family=family,
            )
            for family, decision_ts in (
                ("PRE_MATCH_REFERENCE", match.pregame_ts),
                ("SCHEDULED_START_PROXY", 400),
            )
            for outcome_index in (0, 1)
        )
        condition = research.Condition(
            "all-outcomes", "market_level",
            (research.Clause("current_price", "GE", 0.0),), "TEST",
        )

        selected = research._condition_selection_groups(condition, events)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0][0].event_id, match.event_id)
        self.assertEqual(selected[0][0].decision_family, "PRE_MATCH_REFERENCE")

    def test_holdout_rejects_training_event_or_decision_at_cutoff(self) -> None:
        train = feature_rows([0.2, 0.4, 0.6, 0.8])
        manifest = research.freeze_training_manifest(train, alpha=0.05, split_cutoff_ts=2_000)

        with self.assertRaisesRegex(ValueError, "overlaps frozen training|strictly after"):
            research.evaluate_holdout(
                manifest,
                (replace(train[0], decision_ts=2_000),),
            )

    def test_holdout_applies_fdr_correction_to_every_frozen_rule(self) -> None:
        left = research.build_event(match_with_path("later-a", finish_ts=3_100, path=[(100, 0.60)]), decision_ts=2_100, outcome_index=0)
        right = research.build_event(match_with_path("later-a", finish_ts=3_100, path=[(100, 0.60)]), decision_ts=2_100, outcome_index=1)
        rule = research.Condition("all", "market_level", (research.Clause("current_price", "GE", 0.0),), "TEST")
        manifest = research.FrozenManifest(
            split_cutoff_ts=2_000, training_source_hashes_sha256="", training_source_hashes=(),
            source_event_ids=(), feature_definitions=(), empirical_cut_points={},
            selected_rule_ids=("all",), selected_rule_definitions={"all": rule.to_dict()},
            ranking_order=("all",), condition_ledger=(), cost_specification={},
            significance_alpha=0.05, significance_provenance="TEST", kelly_inputs={},
        )

        result = research.evaluate_holdout(manifest, (left, right))

        self.assertIsNotNone(result[0].q_value)
        self.assertIsNotNone(result[0].bonferroni_p_value)
        self.assertEqual(result[0].reject_reason, "FDR_NOT_SIGNIFICANT")
    def test_candidate_cut_points_come_only_from_training_values(self) -> None:
        train = feature_rows([0.20, 0.40, 0.60, 0.80])
        test = feature_rows([0.333, 0.777])
        candidates = research.generate_candidates(train)

        price_cuts = {
            clause.value for candidate in candidates for clause in candidate.clauses
            if clause.feature == "current_price"
        }
        self.assertIn(0.2, price_cuts)
        self.assertNotIn(test[0].current_price, price_cuts)
        self.assertNotIn(test[1].current_price, price_cuts)

    def test_missing_state_and_book_fields_do_not_generate_conditions(self) -> None:
        candidates = research.generate_candidates(feature_rows([0.2, 0.4]))

        self.assertFalse(any(candidate.family == "match_state" for candidate in candidates))
        self.assertFalse(any(candidate.family == "execution_feasibility" for candidate in candidates))

    def test_manifest_records_unavailable_state_and_execution_families(self) -> None:
        manifest = research.freeze_training_manifest(feature_rows([0.2, 0.4]), alpha=0.05)

        unavailable = {
            row["family"]: row
            for row in manifest.condition_ledger
            if row["rule_id"].startswith("UNAVAILABLE_")
        }
        self.assertEqual(unavailable["match_state"]["reject_reason"], "INADEQUATE_FIELD_COVERAGE")
        self.assertEqual(unavailable["execution_feasibility"]["reject_reason"], "INADEQUATE_FIELD_COVERAGE")
        self.assertEqual(unavailable["input_validation"]["reject_reason"], "INVALID_ARITHMETIC")
        self.assertFalse(set(unavailable) & {
            condition.family for condition in research.generate_candidates(feature_rows([0.2, 0.4]))
        })

    def test_condition_ledger_includes_concentration_diagnostics_for_every_row(self) -> None:
        events = feature_rows([0.2, 0.4, 0.6, 0.8])
        manifest = research.freeze_training_manifest(events, alpha=0.05)
        results = {
            result.rule_id: result
            for result in research._evaluate_conditions(
                research.generate_candidates(events), events, draws=10_000, seed=20260812,
            )
        }

        for row in manifest.condition_ledger:
            with self.subTest(rule_id=row["rule_id"]):
                self.assertIn("largest_contribution", row)
                self.assertIn("pnl_without_largest", row)
                if row["rule_id"].startswith("UNAVAILABLE_"):
                    self.assertIsNone(row["largest_contribution"])
                    self.assertEqual(row["pnl_without_largest"], 0.0)
                else:
                    result = results[row["rule_id"]]
                    self.assertEqual(row["largest_contribution"], result.largest_contribution)
                    self.assertEqual(row["pnl_without_largest"], result.pnl_without_largest)

    def test_benjamini_hochberg_is_monotone_and_matches_fixture(self) -> None:
        q_values = statistics.benjamini_hochberg([0.01, 0.04, 0.03, 0.20])

        np.testing.assert_allclose(q_values, [0.04, 0.0533333333, 0.0533333333, 0.20])

    def test_bonferroni_familywise_sensitivity_caps_at_one(self) -> None:
        adjusted = statistics.bonferroni([0.01, 0.40], tested_conditions=4)

        np.testing.assert_allclose(adjusted, [0.04, 1.00])

    def test_resampling_uses_event_blocks_not_outcome_rows(self) -> None:
        sampled = statistics.bootstrap_match_blocks(
            [("match-a", 0.1), ("match-a", 0.2), ("match-b", -0.1), ("match-b", -0.2)],
            draws=20, seed=7,
        )

        self.assertTrue(all(draw.count("match-a") in {0, 2, 4} for draw in sampled.match_ids))

    def test_contribution_diagnostic_removes_largest_absolute_event(self) -> None:
        diagnostic = statistics.contribution_diagnostics([0.10, 0.20, 5.00, -0.10])

        self.assertEqual(diagnostic["largest_contribution"], 5.00)
        self.assertAlmostEqual(diagnostic["pnl_without_largest"], 0.20)

    def test_holdout_evaluates_frozen_rules_without_reselection(self) -> None:
        train = feature_rows([0.2, 0.4, 0.6, 0.8], volatility=[0.01, 0.02, 0.03, 0.04])
        manifest = research.freeze_training_manifest(train, alpha=0.05)
        holdout = tuple(
            replace(
                row,
                event_id=f"holdout-{index}", market_id=f"holdout-market-{index}",
                decision_ts=2_000 + index, finish_ts=3_000 + index,
            )
            for index, row in enumerate(feature_rows([0.3, 0.7], volatility=[0.02, 0.05]))
        )

        result = research.evaluate_holdout(manifest, holdout)

        self.assertEqual([item.rule_id for item in result], list(manifest.selected_rule_ids))
        self.assertEqual([item.selection_rank for item in result], list(range(1, len(result) + 1)))
        self.assertTrue(all(item.economic_result_basis == "GROSS_REFERENCE_PROXY" for item in result))


if __name__ == "__main__":
    unittest.main()
