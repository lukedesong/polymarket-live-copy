from __future__ import annotations

import unittest
import gzip
import hashlib
import json
import tempfile
from pathlib import Path

from sim.tennis_ev import data


def path_row(*, event_id: str, high_price: float, high_won: bool) -> dict[str, object]:
    return {
        "event_id": event_id,
        "market_id": f"market-{event_id}",
        "series": "ATP",
        "title": "Player A vs. Player B",
        "start_ts": 1_000,
        "actual_finish_ts": 2_000,
        "pregame_timestamp": 990,
        "outcomes": '["Player A", "Player B"]',
        "high_outcome": "Player A",
        "low_outcome": "Player B",
        "high_token": "token-a",
        "low_token": "token-b",
        "high_pregame_price": high_price,
        "low_pregame_price": 1.0 - high_price,
        "high_won": high_won,
        "low_won": not high_won,
        "high_path": [[5, high_price], [60, high_price]],
        "low_path": [[5, 1.0 - high_price], [60, 1.0 - high_price]],
    }


class HistoricalAdapterTests(unittest.TestCase):
    def test_path_row_normalizes_without_inventing_execution_fields(self) -> None:
        match, exclusions = data.normalize_historical_row(
            path_row(event_id="e1", high_price=0.70, high_won=True), source_sha256="abc"
        )

        self.assertEqual(exclusions, [])
        assert match is not None
        self.assertEqual(match.event_id, "e1")
        self.assertEqual(match.level, "ATP")
        self.assertEqual(match.outcomes[0].pregame_price, 0.70)
        self.assertIsNone(match.outcomes[0].best_ask)
        self.assertIsNone(match.match_state)
        self.assertEqual(match.price_fidelity, "HISTORICAL_REFERENCE_PRICE")
        self.assertEqual(match.outcomes[0].path[0], (1_005, 0.70))

    def test_invalid_price_is_quarantined_with_denominator_preserved(self) -> None:
        match, exclusions = data.normalize_historical_row(
            path_row(event_id="bad", high_price=1.20, high_won=True), source_sha256="abc"
        )

        self.assertIsNone(match)
        self.assertEqual(exclusions[0].reason, "PRICE_OUT_OF_DOMAIN")
        self.assertEqual(exclusions[0].event_id, "bad")

    def test_player_names_follow_the_outcome_price_alignment(self) -> None:
        row = path_row(event_id="ordered", high_price=0.70, high_won=True)
        row["outcomes"] = '["Player B", "Player A"]'
        row["pregame_prices"] = "[0.3, 0.7]"
        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertEqual(exclusions, [])
        assert match is not None
        self.assertEqual(match.outcomes[0].name, "Player A")
        self.assertEqual(match.outcomes[1].name, "Player B")

    def test_missing_price_or_named_outcome_alignment_is_quarantined(self) -> None:
        row = path_row(event_id="unmapped", high_price=0.70, high_won=True)
        del row["high_outcome"]
        del row["low_outcome"]
        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertIsNone(match)
        self.assertEqual(exclusions[0].reason, "OUTCOME_PRICE_ALIGNMENT_MISSING")

    def test_historical_row_requires_strict_pregame_start_finish_ordering(self) -> None:
        row = path_row(event_id="bad-order", high_price=0.70, high_won=True)
        row["pregame_timestamp"] = row["start_ts"]

        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertIsNone(match)
        self.assertEqual(exclusions[0].reason, "INVALID_MATCH_CHRONOLOGY")

    def test_historical_row_rejects_path_point_at_or_after_settlement(self) -> None:
        row = path_row(event_id="late-point", high_price=0.70, high_won=True)
        row["high_path"] = [[1_000, 0.70]]

        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertIsNone(match)
        self.assertEqual(exclusions[0].reason, "PATH_POINT_AFTER_FINISH")

    def test_identical_path_timestamp_and_price_is_collapsed(self) -> None:
        row = path_row(event_id="same-point", high_price=0.70, high_won=True)
        row["high_path"] = [[5, 0.70], [5, 0.70], [60, 0.70]]

        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertEqual(exclusions, [])
        assert match is not None
        self.assertEqual(match.outcomes[0].path, ((1_005, 0.70), (1_060, 0.70)))

    def test_conflicting_path_prices_at_same_timestamp_are_quarantined(self) -> None:
        row = path_row(event_id="conflict-point", high_price=0.70, high_won=True)
        row["high_path"] = [[5, 0.70], [5, 0.71]]

        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertIsNone(match)
        self.assertEqual(exclusions[0].reason, "CONFLICTING_PATH_TIMESTAMP")


class ForwardAdapterTests(unittest.TestCase):
    def test_forward_snapshot_preserves_observed_book_and_state(self) -> None:
        snapshot = {
            "event_id": "e2",
            "observed_at": 200,
            "tokens": [{"token_id": "yes", "price": 0.61, "best_bid": 0.60,
                        "best_ask": 0.62, "visible_depth_usd": 125.0}],
            "match_state": {"sets": [1, 0], "server": "Player A"},
            "public_trades": [{"token_id": "yes", "timestamp": 199, "price": 0.61,
                               "size": 20.0, "side": "BUY", "transaction_id": "tx-1"}],
        }
        normalized = data.normalize_forward_snapshot(snapshot)

        self.assertEqual(normalized.snapshots[0].price_fidelity, "CONTEMPORANEOUS_BOOK")
        self.assertEqual(normalized.snapshots[0].best_ask, 0.62)
        self.assertEqual(normalized.snapshots[0].match_state["server"], "Player A")
        self.assertEqual(normalized.trades[0].transaction_id, "tx-1")
        self.assertEqual(normalized.trades[0].maker_taker_role, "UNKNOWN")

    def test_forward_trade_without_size_preserves_unknown_size(self) -> None:
        payload = data.normalize_forward_snapshot({
            "event_id": "e3", "observed_at": 200, "tokens": [],
            "public_trades": [{"token_id": "yes", "timestamp": 199, "price": 0.61}],
        })

        self.assertIsNone(payload.trades[0].size)

    def test_manifest_counts_all_raw_usable_and_excluded_rows(self) -> None:
        valid, valid_exclusions = data.normalize_historical_row(
            path_row(event_id="valid", high_price=0.70, high_won=True), source_sha256="abc"
        )
        _, excluded = data.normalize_historical_row(
            path_row(event_id="bad", high_price=1.20, high_won=True), source_sha256="abc"
        )
        assert valid is not None
        manifest = data.build_coverage_manifest(
            raw_rows=2, matches=[valid], exclusions=[*valid_exclusions, *excluded]
        )

        self.assertEqual(manifest["raw_rows"], 2)
        self.assertEqual(manifest["usable_matches"], 1)
        self.assertEqual(manifest["excluded_matches"], 1)
        self.assertEqual(sum(manifest["exclusions_by_reason"].values()), 1)

    def test_loader_hashes_compressed_source_and_reconciles_coverage(self) -> None:
        valid = path_row(event_id="valid", high_price=0.70, high_won=True)
        invalid = path_row(event_id="invalid", high_price=1.20, high_won=True)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "paths.jsonl.gz"
            with gzip.open(source, "wt", encoding="utf-8") as handle:
                handle.write(json.dumps(valid) + "\n")
                handle.write(json.dumps(invalid) + "\n")
            matches, snapshots, trades, states, exclusions, manifest = data.load_historical_matches(source)
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest()

        self.assertEqual(len(matches), 1)
        self.assertEqual((snapshots, trades, states), ((), (), ()))
        self.assertEqual(matches[0].source_sha256, source_hash)
        self.assertEqual(manifest["raw_rows"], manifest["usable_matches"] + manifest["excluded_matches"])
        self.assertEqual(len(exclusions), manifest["excluded_matches"])
        self.assertEqual(manifest["source_path"], str(source))
        self.assertEqual(manifest["source_sha256"], source_hash)
        self.assertEqual(manifest["unique_markets"], 1)
        self.assertEqual(manifest["settlement_coverage"], {
            "settled_matches": 1, "usable_matches": 1, "fraction": 1.0,
        })
        self.assertEqual(manifest["pagination_status"], "UNKNOWN_ARTIFACT")
        self.assertEqual(manifest["truncation_status"], "UNKNOWN_ARTIFACT")
        self.assertEqual(manifest["timestamp_coverage"], {
            "earliest_pregame_ts": 990, "latest_pregame_ts": 990,
            "earliest_start_ts": 1_000, "latest_start_ts": 1_000,
            "earliest_finish_ts": 2_000, "latest_finish_ts": 2_000,
        })

    def test_loader_quarantines_non_mapping_jsonl_row_and_reconciles_denominator(self) -> None:
        valid = path_row(event_id="valid", high_price=0.70, high_won=True)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "paths.jsonl"
            source.write_text(json.dumps(valid) + "\n" + json.dumps(["not", "a", "row"]) + "\n", encoding="utf-8")
            matches, _, _, _, exclusions, manifest = data.load_historical_matches(source)

        self.assertEqual(len(matches), 1)
        self.assertEqual(len(exclusions), 1)
        self.assertEqual(exclusions[0].reason, "INVALID_ROW_MAPPING")
        self.assertEqual(manifest["raw_rows"], manifest["usable_matches"] + manifest["excluded_matches"])

    def test_loader_quarantines_malformed_jsonl_line_and_preserves_source_hash(self) -> None:
        valid = path_row(event_id="valid", high_price=0.70, high_won=True)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "paths.jsonl"
            source.write_text(json.dumps(valid) + "\n{bad json}\n", encoding="utf-8")
            matches, _, _, _, exclusions, manifest = data.load_historical_matches(source)
            source_hash = hashlib.sha256(source.read_bytes()).hexdigest()

        self.assertEqual(len(matches), 1)
        self.assertEqual(len(exclusions), 1)
        self.assertEqual(exclusions[0].reason, "INVALID_JSON_ROW")
        self.assertEqual(manifest["raw_rows"], 2)
        self.assertEqual(manifest["usable_matches"], 1)
        self.assertEqual(manifest["excluded_matches"], 1)
        self.assertEqual(manifest["source_sha256"], source_hash)
        self.assertEqual(manifest["raw_rows"], manifest["usable_matches"] + manifest["excluded_matches"])


if __name__ == "__main__":
    unittest.main()
