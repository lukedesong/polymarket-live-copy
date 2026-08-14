import importlib.util
import re
import sys
import unittest
from pathlib import Path


SKILL = Path("/Users/luke/.agents/skills/luke-correction-replay/SKILL.md")
BUILDER = Path(
    "/Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py"
)
GLOBAL_AGENTS = Path("/Users/luke/.codex/AGENTS.md")
PRIORITY = Path(
    "/Users/luke/.agents/skills/luke-correction-replay/references/priority-register.md"
)
EVALUATIONS = Path(
    "/Users/luke/.agents/skills/luke-correction-replay/references/evaluation-cases.md"
)


def load_runtime_helpers():
    spec = importlib.util.spec_from_file_location("correction_replay_runtime", BUILDER)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SkillContractTests(unittest.TestCase):
    def test_frontmatter_and_dynamic_trigger_coverage(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\nname: luke-correction-replay\n"))
        description = re.search(r"description: >\n((?:  .+\n)+)", text).group(1)
        for trigger in (
            "correction",
            "repair",
            "accounting",
            "deployment",
            "non-trivial",
            "wrong",
            "again",
        ):
            self.assertIn(trigger, description.lower())

    def test_workflow_has_required_gates(self):
        text = SKILL.read_text()
        for phrase in (
            "Detect Task and Sources",
            "Retrieve Relevant Corrections",
            "Build the Preflight",
            "Run the Completion Gate",
            "Capture a New Correction",
            "Respond to Luke",
        ):
            self.assertIn(phrase, text)
        self.assertIn("An applicable P0 FAIL blocks any completion claim", text)

    def test_skill_is_methodology_sized_and_has_defaults(self):
        text = SKILL.read_text()
        steps = re.findall(r"^## Step \d+:", text, flags=re.MULTILINE)
        self.assertGreaterEqual(len(steps), 5)
        self.assertLessEqual(len(steps), 9)
        self.assertIn("| Parameter | Default", text)
        self.assertLessEqual(len(text.splitlines()), 300)

    def test_references_are_declared(self):
        text = SKILL.read_text()
        for name in (
            "priority-register.md",
            "last-week-audit.md",
            "deduplication-and-severity.md",
            "evaluation-cases.md",
        ):
            self.assertIn(f"references/{name}", text)

    def test_global_hook_is_unique_compact_and_points_to_skill(self):
        text = GLOBAL_AGENTS.read_text()
        heading = "## P0：Luke 纠错回放 Hook"
        self.assertEqual(text.count(heading), 1)
        section = text.split(heading, 1)[1].split("\n## ", 1)[0]
        self.assertIn(str(SKILL), section)
        self.assertIn("非简单任务", section)
        self.assertIn("P0", section)
        self.assertIn("不得宣称完成", section)
        self.assertLessEqual(len([line for line in section.splitlines() if line.strip()]), 8)

    def test_evaluation_suite_covers_ten_declared_behaviors(self):
        text = EVALUATIONS.read_text()
        cases = re.findall(r"^\d+\. ", text, flags=re.MULTILINE)
        self.assertEqual(len(cases), 10)
        for phrase in (
            "current explicit instruction overrides",
            "one candidate for the incident",
            "severity-first ordering",
            "User output stays compact",
        ):
            self.assertIn(phrase, text)


class RuntimeBehaviorTests(unittest.TestCase):
    def test_p0_failure_blocks_completion_claim(self):
        runtime = load_runtime_helpers()
        result = runtime.completion_gate(
            [
                {"rule_id": "repair-closure", "severity": "P0", "status": "FAIL"},
                {"rule_id": "verbosity", "severity": "P2", "status": "PASS"},
            ]
        )
        self.assertFalse(result["may_claim_complete"])
        self.assertEqual(result["blocking_rule_ids"], ["repair-closure"])

    def test_trivial_translation_skips_cleanly(self):
        runtime = load_runtime_helpers()
        result = runtime.select_rule_ids("translate hello to Chinese")
        self.assertTrue(result["skip"])
        self.assertEqual(result["rule_ids"], [])

    def test_polymarket_repair_loads_global_and_domain_rules(self):
        runtime = load_runtime_helpers()
        result = runtime.select_rule_ids(
            "Repair and deploy the Polymarket live copy accounting bug"
        )
        self.assertFalse(result["skip"])
        self.assertTrue(
            {
                "repair-closure",
                "official-accounting",
                "version-identity",
                "unknown-no-repost",
                "memory-replay",
            }.issubset(result["rule_ids"])
        )

    def test_spreadsheet_task_does_not_load_trading_rules(self):
        runtime = load_runtime_helpers()
        result = runtime.select_rule_ids(
            "Update the De Beers spreadsheet with the new daily source rows"
        )
        self.assertTrue(
            {"history-lock", "source-verification", "memory-replay"}.issubset(
                result["rule_ids"]
            )
        )
        self.assertNotIn("unknown-no-repost", result["rule_ids"])


if __name__ == "__main__":
    unittest.main()
