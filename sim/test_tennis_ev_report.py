"""Tests for the immutable tennis EV report artifacts."""
from __future__ import annotations

import csv
import json
from pathlib import Path
import tempfile
import unittest

from sim import run_polymarket_tennis_ev

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
    def test_report_uses_only_the_displayed_frozen_rule_for_its_verdict(self) -> None:
        run = synthetic_run()
        displayed = research.ConditionResult("displayed", 1, 1, 1, 1, 0, 0.40, 0.60,
                                            2 / 3, 0.10, 0.70, 0.20, 0.20, 0.20,
                                            0.40, 0.10, "FDR_NOT_SIGNIFICANT")
        other = research.ConditionResult("other", 2, 1, 1, 1, 0, 0.40, 0.60,
                                         2 / 3, 0.10, 0.70, 0.01, 0.01, 0.01,
                                         0.40, 0.10, None)
        run = report.ResearchRun(**{
            **run.__dict__, "holdout_results": (displayed, other),
            "holdout_ledger": ({"event_id": "m1", "rule_id": "displayed", "selected": "true", "net_pnl": 0.4},),
            "frozen_manifest": {"significance_alpha": 0.05, "display_rule_id": "displayed"},
        })
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = report.build_report(run, Path(temporary) / "bundle")
            payload = json.loads(artifacts.result_json.read_text())
        self.assertEqual(payload["verdict"], "NO_SIGNIFICANT_EDGE")
        self.assertEqual(payload["criteria"]["display_rule_id"], "displayed")

    def test_no_ranked_rule_with_a_usable_holdout_is_no_significant_edge(self) -> None:
        run = synthetic_run()
        run = report.ResearchRun(**{
            **run.__dict__, "holdout_results": (), "holdout_ledger": (),
            "frozen_manifest": {"significance_alpha": 0.05, "display_rule_id": None},
        })
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = report.build_report(run, Path(temporary) / "bundle")
            payload = json.loads(artifacts.result_json.read_text())
        self.assertEqual(payload["verdict"], "NO_SIGNIFICANT_EDGE")
        self.assertEqual(payload["holdout"]["usable_test_matches"], 1)

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


class CliTests(unittest.TestCase):
    def test_cli_builds_reproducible_complete_artifact_set(self) -> None:
        """The entry point is local-only and publishes a self-verifying bundle."""
        rows = []
        for index in range(8):
            start = 1_000 + index * 1_000
            high_won = index % 2 == 0
            rows.append({
                "event_id": f"event-{index}", "market_id": f"market-{index}",
                "series": "ATP", "title": f"Player A vs Player B {index}",
                "start_ts": start, "actual_finish_ts": start + 700,
                "pregame_timestamp": start - 100,
                "outcomes": ["Player A", "Player B"], "pregame_prices": [0.65, 0.35],
                "high_outcome": "Player A", "low_outcome": "Player B",
                "high_token": f"yes-{index}", "low_token": f"no-{index}",
                "high_pregame_price": 0.65, "low_pregame_price": 0.35,
                "high_won": high_won, "low_won": not high_won,
                "high_path": [[0, 0.65], [300, 0.66]],
                "low_path": [[0, 0.35], [300, 0.34]],
            })
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / "paths.jsonl.gz"
            import gzip
            with gzip.open(fixture, "wt", encoding="utf-8") as output:
                for row in rows:
                    output.write(json.dumps(row) + "\n")
            first = root / "first"
            second = root / "second"
            args = ["--paths", str(fixture), "--output-dir", str(first),
                    "--simulation-paths", "2"]
            self.assertEqual(run_polymarket_tennis_ev.main(args), 0)
            args[3] = str(second)
            self.assertEqual(run_polymarket_tennis_ev.main(args), 0)
            self.assertEqual(json.loads((first / "result.json").read_text()),
                             json.loads((second / "result.json").read_text()))
            self.assertEqual(json.loads((first / "frozen_strategy_manifest.json").read_text()),
                             json.loads((second / "frozen_strategy_manifest.json").read_text()))
            manifest = json.loads((first / "artifact_manifest.json").read_text())["sha256"]
            import hashlib
            for name, digest in manifest.items():
                self.assertEqual(hashlib.sha256((first / name).read_bytes()).hexdigest(), digest)


if __name__ == "__main__":
    unittest.main()
