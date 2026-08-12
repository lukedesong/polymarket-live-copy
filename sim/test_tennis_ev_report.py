"""Tests for the immutable tennis EV report artifacts."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import tempfile
import unittest

from sim.tennis_ev import report
from sim.tennis_ev import research


def synthetic_run() -> report.ResearchRun:
    events = (
        research.ResearchEvent("m1", "market-1", "yes", 100, 200, 0, True, None,
                               0.60, 0.60, 0.0, 0.0, None, 0.60, 0.60, 0, "ATP",
                               None, None, None, ("REFERENCE_ENTRY_PRICE",), (0.60,)),
        research.ResearchEvent("m1", "market-1", "no", 100, 200, 1, False, None,
                               0.40, 0.40, 0.0, 0.0, None, 0.40, 0.40, 0, "ATP",
                               None, None, None, ("REFERENCE_ENTRY_PRICE",), (0.40,)),
    )
    result = research.ConditionResult("rule-1", 1, 1, 1, 1, 0, 0.40, 0.60,
                                      2 / 3, 0.10, 0.70, 0.01, 0.01, 0.01,
                                      0.40, 0.0, None)
    return report.ResearchRun(
        coverage={"raw_rows": 1, "usable_matches": 1, "excluded_matches": 0,
                  "execution_book_matches": 0},
        test_events=events,
        holdout_results=(result,),
        frozen_manifest={"selected_rule_ids": ["rule-1"]},
        random_baseline={"roi_samples": [0.1, -0.2, 0.0]},
        bankroll_results={"frozen": {"roi": 2 / 3}},
        monte_carlo_summary={"final_equity_samples": [9_000.0, 10_000.0, 11_000.0]},
    )


class VerdictTests(unittest.TestCase):
    def test_positive_proxy_with_missing_books_is_execution_blocked(self) -> None:
        verdict = report.decide_verdict(usable_test_matches=100, significant_positive_rules=1,
                                        executable_book_matches=0, net_after_verified_costs=True)
        self.assertEqual(verdict, "BLOCK_EXECUTION_DATA")

    def test_no_frozen_rule_passes_returns_required_chinese_conclusion(self) -> None:
        verdict = report.decide_verdict(usable_test_matches=100, significant_positive_rules=0,
                                        executable_book_matches=0, net_after_verified_costs=False)
        self.assertEqual(verdict, "NO_SIGNIFICANT_EDGE")
        self.assertIn("没有发现统计显著优势", report.verdict_text(verdict))

    def test_no_usable_holdout_is_block_data(self) -> None:
        self.assertEqual(report.decide_verdict(usable_test_matches=0, significant_positive_rules=0,
                                               executable_book_matches=0,
                                               net_after_verified_costs=False), "BLOCK_DATA")

    def test_verified_verdict_requires_all_explicit_evidence_criteria(self) -> None:
        self.assertEqual(report.decide_verdict(
            usable_test_matches=5, significant_positive_rules=1, executable_book_matches=5,
            net_after_verified_costs=True, corrected_significance_passed=True,
            net_ev_interval_lower_positive=True, concentration_check_passed=True,
            execution_coverage_complete=True,
        ), "VERIFIED_POSITIVE_EV")

    def test_global_book_coverage_cannot_verify_holdout_rows_without_row_evidence(self) -> None:
        run = synthetic_run()
        result = research.ConditionResult("rule-1", 1, 1, 1, 1, 0, 0.40, 0.60,
                                          2 / 3, 0.10, 0.70, 0.01, 0.01, 0.01,
                                          0.40, 0.1, None)
        run = report.ResearchRun(
            **{**run.__dict__, "coverage": {"execution_book_matches": 99},
               "frozen_manifest": {"verified_costs_captured": True, "significance_alpha": 0.05},
               "holdout_results": (result,),
               "holdout_ledger": ({"event_id": "m1", "selected": "true", "net_pnl": 0.4},)}
        )
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = report.build_report(run, Path(temporary) / "bundle")
            self.assertEqual(json.loads(artifacts.result_json.read_text())["verdict"], "BLOCK_EXECUTION_DATA")

    def test_manifest_alpha_controls_significance_not_a_hard_coded_value(self) -> None:
        run = synthetic_run()
        result = research.ConditionResult("rule-1", 1, 1, 1, 1, 0, 0.40, 0.60,
                                          2 / 3, 0.10, 0.70, 0.01, 0.04, 0.04,
                                          0.40, 0.1, None)
        evidence = {"event_id": "m1", "selected": "true", "net_pnl": 0.4,
                    "validated_executable_row": True, "cost_source": "official", "cost_fidelity": "verified"}
        run = report.ResearchRun(**{**run.__dict__, "holdout_results": (result,),
                                    "holdout_ledger": (evidence,),
                                    "frozen_manifest": {"verified_costs_captured": True, "significance_alpha": 0.03}})
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = report.build_report(run, Path(temporary) / "bundle")
            payload = json.loads(artifacts.result_json.read_text())
            self.assertEqual(payload["criteria"]["significance_alpha"], 0.03)
            self.assertEqual(payload["verdict"], "NO_SIGNIFICANT_EDGE")


class ArtifactTests(unittest.TestCase):
    def test_report_totals_recompute_from_event_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = report.build_report(synthetic_run(), output_dir=Path(temporary))
            with artifacts.event_ledger.open(newline="") as source:
                ledger = list(csv.DictReader(source))
            recomputed = sum(float(row["net_pnl"]) for row in ledger if row["selected"] == "true")
            payload = json.loads(artifacts.result_json.read_text())
            self.assertAlmostEqual(recomputed, payload["holdout"]["net_pnl"])

    def test_required_charts_are_nonempty_png_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary)
            report.build_report(synthetic_run(), output_dir=output_dir)
            for name in ("equity_curve.png", "drawdown_curve.png", "roi_distribution.png",
                         "strategy_comparison.png"):
                self.assertGreater((output_dir / name).stat().st_size, 0)

    def test_failed_build_preserves_the_previous_complete_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output_dir = Path(temporary) / "bundle"
            output_dir.mkdir()
            (output_dir / "old.txt").write_text("intact")
            original = report._write_charts
            report._write_charts = lambda *_args: (_ for _ in ()).throw(RuntimeError("injected chart failure"))
            try:
                with self.assertRaisesRegex(RuntimeError, "injected chart failure"):
                    report.build_report(synthetic_run(), output_dir)
            finally:
                report._write_charts = original
            self.assertEqual({path.name for path in output_dir.iterdir()}, {"old.txt"})
            self.assertEqual((output_dir / "old.txt").read_text(), "intact")


if __name__ == "__main__":
    unittest.main()
