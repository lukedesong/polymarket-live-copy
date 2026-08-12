"""Tests for capital-aware hedge and bankroll research helpers."""
from __future__ import annotations

import unittest

import numpy as np

from sim.tennis_ev import bankroll


def trade(event_id: str, *, entry: int, settle: int, pnl_rate: float) -> bankroll.Trade:
    return bankroll.Trade(event_id=event_id, entry_ts=entry, settle_ts=settle, pnl_rate=pnl_rate)


class HedgeTests(unittest.TestCase):
    def test_static_hedge_payout_uses_complementary_share_counts(self) -> None:
        high_wins = bankroll.hedged_pnl(
            high_cost=0.70, low_cost=0.30, high_weight=0.80, high_won=True
        )
        low_wins = bankroll.hedged_pnl(
            high_cost=0.70, low_cost=0.30, high_weight=0.80, high_won=False
        )
        self.assertAlmostEqual(high_wins, 0.80 / 0.70 - 1.0)
        self.assertAlmostEqual(low_wins, 0.20 / 0.30 - 1.0)

    def test_complementary_cost_identity_is_reported(self) -> None:
        identity = bankroll.complementary_cost_check(0.52, 0.51)
        self.assertAlmostEqual(identity["combined_unit_cost"], 1.03)
        self.assertAlmostEqual(identity["locked_unit_loss"], 0.03)

    def test_paired_hedge_comparison_keeps_the_same_deployed_cash_denominator(self) -> None:
        observations = (
            bankroll.HedgeObservation("a", 100, 0.70, 0.30, True, 0.70, 0.30),
            bankroll.HedgeObservation("b", 200, 0.70, 0.30, False, 0.70, 0.30),
        )
        result = bankroll.compare_hedge(observations, high_weight=0.80)
        self.assertEqual(result["observations"], 2)
        self.assertAlmostEqual(result["unhedged_net_pnl"], 1.0 / 0.70 - 2.0)
        self.assertAlmostEqual(result["hedged_net_pnl"], 0.80 / 0.70 + 0.20 / 0.30 - 2.0)
        self.assertEqual(result["deployed_capital_per_observation"], 1.0)

    def test_hedge_observation_requires_the_high_side_to_be_market_favorite(self) -> None:
        with self.assertRaisesRegex(ValueError, "high_reference_price"):
            bankroll.HedgeObservation("tied", 100, 0.70, 0.30, True, 0.50, 0.50)
        with self.assertRaisesRegex(ValueError, "high_reference_price"):
            bankroll.HedgeObservation("underdog", 100, 0.70, 0.30, True, 0.30, 0.70)


class CapitalAndKellyTests(unittest.TestCase):
    def test_overlapping_matches_cannot_reuse_reserved_cash(self) -> None:
        trades = [
            trade("a", entry=100, settle=300, pnl_rate=0.10),
            trade("b", entry=200, settle=400, pnl_rate=0.10),
        ]
        ledger = bankroll.run_fixed_fraction(trades, initial_cash=10_000.0, fraction=0.10)
        self.assertAlmostEqual(ledger[0].stake, 1_000.0)
        self.assertAlmostEqual(ledger[1].stake, 900.0)
        self.assertTrue(ledger[0].accepted)
        self.assertTrue(ledger[1].accepted)

    def test_fractional_betting_ruin_means_equity_at_or_below_zero(self) -> None:
        summary = bankroll.summarize_paths(np.array([[10_000.0, 9_000.0, 8_100.0]]))
        self.assertEqual(summary["ruin_boundary_usd"], 0.0)
        self.assertEqual(summary["ruined_paths"], 0)
        self.assertEqual(summary["ruin_probability"], 0.0)
        self.assertEqual(summary["ruin_definition"], "EQUITY_AT_OR_BELOW_ZERO")

    def test_path_summary_reports_max_drawdown_distribution_without_an_implicit_threshold(self) -> None:
        summary = bankroll.summarize_paths(np.array([
            [100.0, 90.0, 100.0],
            [100.0, 50.0, 100.0],
            [100.0, 100.0, 100.0],
        ]))
        self.assertNotIn("max_drawdown_probability", summary)
        self.assertAlmostEqual(summary["max_drawdown_p05"], 0.01)
        self.assertAlmostEqual(summary["max_drawdown_median"], 0.10)
        self.assertAlmostEqual(summary["max_drawdown_p95"], 0.46)

    def test_executable_kelly_is_zero_without_positive_training_interval(self) -> None:
        self.assertEqual(bankroll.executable_kelly(0.80, 0.70, edge_interval_low=0.0), 0.0)
        self.assertAlmostEqual(bankroll.binary_kelly(0.80, 0.70), 1.0 / 3.0)
        self.assertAlmostEqual(bankroll.kelly_scenarios(0.80, 0.70, 0.01)["half_kelly"], 1.0 / 6.0)


class MonteCarloTests(unittest.TestCase):
    def test_monte_carlo_is_reproducible_and_preserves_day_blocks(self) -> None:
        blocks = {
            "2026-01-01": (trade("a", entry=100, settle=200, pnl_rate=0.10),),
            "2026-01-02": (trade("b", entry=100, settle=200, pnl_rate=-1.0),),
        }
        first = bankroll.monte_carlo(blocks, paths=10, initial_cash=10_000.0, fraction=0.02, seed=20260812)
        second = bankroll.monte_carlo(blocks, paths=10, initial_cash=10_000.0, fraction=0.02, seed=20260812)
        np.testing.assert_array_equal(first.equity_paths, second.equity_paths)
        self.assertEqual(first.sampled_unit, "UTC_DATE_BLOCK")
        self.assertEqual(first.sampled_block_ids, second.sampled_block_ids)

    def test_monte_carlo_retains_same_day_cash_reservation_for_overlapping_matches(self) -> None:
        blocks = {
            "2026-01-01": (
                trade("a", entry=100, settle=300, pnl_rate=0.10),
                trade("b", entry=200, settle=400, pnl_rate=0.10),
            ),
        }
        result = bankroll.monte_carlo(blocks, paths=1, initial_cash=10_000.0, fraction=0.10, seed=1)
        self.assertAlmostEqual(result.equity_paths[0, -1], 10_190.0)

    def test_monte_carlo_accepts_empty_and_uneven_date_blocks(self) -> None:
        blocks = {
            "2026-01-01": (),
            "2026-01-02": (trade("one", entry=100, settle=200, pnl_rate=0.10),),
            "2026-01-03": (
                trade("two-a", entry=100, settle=200, pnl_rate=0.10),
                trade("two-b", entry=300, settle=400, pnl_rate=-1.0),
            ),
        }
        result = bankroll.monte_carlo(blocks, paths=4, initial_cash=10_000.0, fraction=0.01, seed=3)
        self.assertEqual(result.equity_paths.shape[0], 4)
        self.assertTrue(all(len(path) == 3 for path in result.sampled_block_ids))

    def test_annualization_is_blocked_without_time_and_capital_coverage(self) -> None:
        result = bankroll.annualized_return(final_equity=11_000.0, initial_cash=10_000.0,
                                            covered_days=None, capital_coverage_complete=False)
        self.assertEqual(result["status"], "BLOCK_DATA")
        self.assertIsNone(result["annualized_return"])


if __name__ == "__main__":
    unittest.main()
