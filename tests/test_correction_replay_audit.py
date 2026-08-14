import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(
    "/Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py"
)
REVIEWED_PATH = Path(
    "/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json"
)


def load_module():
    spec = importlib.util.spec_from_file_location("build_correction_audit", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CandidateExtractionTests(unittest.TestCase):
    def test_ignores_injected_context_and_collapses_fork_copy(self):
        audit = load_module()
        events = [
            {
                "source_id": "s1",
                "event_id": "e1",
                "role": "user",
                "text": "你又没有修完",
                "observed_at": "2026-08-10T12:00:00+08:00",
            },
            {
                "source_id": "fork",
                "event_id": "e1",
                "role": "user",
                "text": "你又没有修完",
                "observed_at": "2026-08-10T12:00:00+08:00",
            },
            {
                "source_id": "s1",
                "event_id": "e2",
                "role": "user",
                "text": "<heartbeat>不要补单</heartbeat>",
                "observed_at": "2026-08-10T12:01:00+08:00",
            },
            {
                "source_id": "s1",
                "event_id": "e3",
                "role": "user",
                "text": "# AGENTS.md instructions",
                "observed_at": "2026-08-10T12:02:00+08:00",
            },
            {
                "source_id": "s1",
                "event_id": "e4",
                "role": "user",
                "text": "<recommended_plugins>Slack</recommended_plugins>",
                "observed_at": "2026-08-10T12:03:00+08:00",
            },
        ]

        candidates = audit.extract_candidates(events)

        self.assertEqual([row["text"] for row in candidates], ["你又没有修完"])

    def test_later_recurrence_after_new_assistant_action_is_distinct(self):
        audit = load_module()
        events = [
            {
                "source_id": "s1",
                "event_id": "u1",
                "role": "user",
                "text": "账又算错了",
                "observed_at": "2026-08-11T10:00:00+08:00",
            },
            {
                "source_id": "s1",
                "event_id": "a1",
                "role": "assistant",
                "text": "已重新报账",
                "observed_at": "2026-08-11T10:01:00+08:00",
            },
            {
                "source_id": "s1",
                "event_id": "u2",
                "role": "user",
                "text": "账又算错了",
                "observed_at": "2026-08-11T10:02:00+08:00",
            },
        ]

        self.assertEqual(len(audit.extract_candidates(events)), 2)

    def test_session_decoder_only_emits_real_user_message_payloads(self):
        audit = load_module()
        rows = [
            {
                "timestamp": "2026-08-10T04:00:00.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "你没有完成"}],
                },
            },
            {
                "timestamp": "2026-08-10T04:00:01.000Z",
                "type": "turn_context",
                "payload": {"user_instructions": "你没有完成"},
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl"
            path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows))
            events = list(audit.iter_session_events(path))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["text"], "你没有完成")
        self.assertEqual(events[0]["role"], "user")


class ReviewedLedgerSchemaTests(unittest.TestCase):
    def test_reviewed_rows_require_complete_schema(self):
        audit = load_module()
        required = {
            "episode_id",
            "observed_at",
            "timezone",
            "timestamp_provenance",
            "minimal_quote",
            "source_path",
            "source_type",
            "project",
            "topic",
            "primary_category",
            "secondary_tags",
            "severity",
            "dedup_key",
            "required_future_behavior",
            "replay_check",
        }
        row = {field: "value" for field in required}
        row["primary_category"] = audit.PRIMARY_CATEGORIES[0]
        row["secondary_tags"] = []
        row["severity"] = "P0"

        audit.validate_episode(row)

        for missing in required:
            broken = dict(row)
            broken.pop(missing)
            with self.subTest(missing=missing):
                with self.assertRaises(ValueError):
                    audit.validate_episode(broken)

    def test_invalid_category_and_severity_are_rejected(self):
        audit = load_module()
        row = {
            "episode_id": "episode-1",
            "observed_at": "2026-08-10T12:00:00+08:00",
            "timezone": "Asia/Shanghai",
            "timestamp_provenance": "raw-session-event",
            "minimal_quote": "没有修完",
            "source_path": "/tmp/session.jsonl",
            "source_type": "raw-session",
            "project": "polymarket",
            "topic": "repair closure",
            "primary_category": "invalid",
            "secondary_tags": [],
            "severity": "P9",
            "dedup_key": "key",
            "required_future_behavior": "finish",
            "replay_check": "proof exists",
        }
        with self.assertRaises(ValueError):
            audit.validate_episode(row)


class ReviewedAuditTests(unittest.TestCase):
    def test_primary_counts_reconcile_to_episode_denominator(self):
        audit = load_module()
        rows = audit.load_reviewed_rows(REVIEWED_PATH)

        counts = audit.category_counts(rows)

        self.assertEqual(sum(counts.values()), len(rows))

    def test_priority_uses_declared_sort_order(self):
        audit = load_module()
        rows = audit.load_reviewed_rows(REVIEWED_PATH)

        register = audit.build_priority_register(rows)

        self.assertEqual(register, sorted(register, key=audit.priority_sort_key))

    def test_every_rough_language_occurrence_has_time_or_explicit_gap(self):
        audit = load_module()
        document = audit.load_reviewed_document(REVIEWED_PATH)

        for row in document["rough_language_occurrences"]:
            with self.subTest(quote=row["minimal_quote"]):
                self.assertTrue(
                    row.get("observed_at")
                    or row.get("timestamp_provenance") == "gap"
                )

    def test_episode_ids_and_dedup_keys_are_unique(self):
        audit = load_module()
        rows = audit.load_reviewed_rows(REVIEWED_PATH)

        self.assertEqual(len({row["episode_id"] for row in rows}), len(rows))
        self.assertEqual(len({row["dedup_key"] for row in rows}), len(rows))


if __name__ == "__main__":
    unittest.main()
