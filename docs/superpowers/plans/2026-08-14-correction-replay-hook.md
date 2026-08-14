# Luke Correction Replay Hook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a global correction-replay Hook that turns Luke's deduplicated one-week correction history into task-specific preflight and completion checks, then makes those checks available in every non-trivial Codex task.

**Architecture:** A standard-library audit builder extracts candidate user corrections from raw Codex sessions and Obsidian candidate notes, while a reviewed JSON ledger remains the canonical denominator for the one-week audit. The builder renders the human-readable audit and frequency-ranked priority register. A compact global `AGENTS.md` trigger invokes a methodology skill that retrieves only applicable rules, delegates domain work to existing skills, and blocks false completion when an applicable P0 replay check fails.

**Tech Stack:** Python 3 standard library (`json`, `pathlib`, `re`, `datetime`, `zoneinfo`, `hashlib`, `unittest`), Markdown, Codex `AGENTS.md`, filesystem-backed Codex skill package.

---

## Task 1: Lock the audit data model and source filtering with failing tests

**Files:**
- Create: `/Users/luke/Documents/polymarket/tests/test_correction_replay_audit.py`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json`

- [ ] **Step 1: Create the skill directories without adding dependencies**

Run:

```bash
mkdir -p /Users/luke/.agents/skills/luke-correction-replay/{scripts,references}
```

Expected: the two directories exist; no package manager or virtual environment is created.

- [ ] **Step 2: Write the first failing parser tests**

Add fixtures inside `test_correction_replay_audit.py` rather than creating a separate fixture tree. Cover a genuine user message, a forked duplicate, injected `<heartbeat>`/`<recommended_plugins>`/`AGENTS.md` content, and a later recurrence after a new assistant action.

```python
import importlib.util
import unittest
from pathlib import Path

MODULE_PATH = Path("/Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py")


def load_module():
    spec = importlib.util.spec_from_file_location("build_correction_audit", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CandidateExtractionTests(unittest.TestCase):
    def test_ignores_injected_context_and_collapses_fork_copy(self):
        audit = load_module()
        events = [
            {"source_id": "s1", "event_id": "e1", "role": "user", "text": "你又没有修完", "observed_at": "2026-08-10T12:00:00+08:00"},
            {"source_id": "fork", "event_id": "e1", "role": "user", "text": "你又没有修完", "observed_at": "2026-08-10T12:00:00+08:00"},
            {"source_id": "s1", "event_id": "e2", "role": "user", "text": "<heartbeat>不要补单</heartbeat>", "observed_at": "2026-08-10T12:01:00+08:00"},
            {"source_id": "s1", "event_id": "e3", "role": "user", "text": "# AGENTS.md instructions", "observed_at": "2026-08-10T12:02:00+08:00"},
        ]
        candidates = audit.extract_candidates(events)
        self.assertEqual([row["text"] for row in candidates], ["你又没有修完"])

    def test_later_recurrence_after_new_assistant_action_is_distinct(self):
        audit = load_module()
        events = [
            {"source_id": "s1", "event_id": "u1", "role": "user", "text": "账又算错了", "observed_at": "2026-08-11T10:00:00+08:00"},
            {"source_id": "s1", "event_id": "a1", "role": "assistant", "text": "已重新报账", "observed_at": "2026-08-11T10:01:00+08:00"},
            {"source_id": "s1", "event_id": "u2", "role": "user", "text": "账又算错了", "observed_at": "2026-08-11T10:02:00+08:00"},
        ]
        self.assertEqual(len(audit.extract_candidates(events)), 2)
```

- [ ] **Step 3: Run the tests and verify RED**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_audit.py' -v
```

Expected: failure because `build_correction_audit.py` does not yet expose the required functions.

- [ ] **Step 4: Implement the smallest source-normalization and candidate-extraction core**

Use an explicit event shape internally. Session-specific decoding belongs in `iter_session_events`; counting logic must not depend on raw JSONL variants.

```python
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
WINDOW_START = datetime(2026, 8, 8, 0, 0, 0, tzinfo=SHANGHAI)
WINDOW_END = datetime(2026, 8, 14, 23, 59, 59, tzinfo=SHANGHAI)
INJECTED_PREFIXES = ("<heartbeat", "<recommended_plugins", "# AGENTS.md instructions")


def normalized_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def exact_event_key(event: dict) -> str:
    stable = "|".join((event.get("event_id", ""), event["observed_at"], normalized_text(event["text"])))
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def extract_candidates(events: list[dict]) -> list[dict]:
    seen: set[str] = set()
    output: list[dict] = []
    assistant_generation = 0
    semantic_seen: set[tuple[str, int]] = set()
    for event in events:
        if event["role"] == "assistant":
            assistant_generation += 1
            continue
        text = normalized_text(event["text"])
        if not text or text.startswith(INJECTED_PREFIXES):
            continue
        exact_key = exact_event_key({**event, "text": text})
        semantic_key = (text, assistant_generation)
        if exact_key in seen or semantic_key in semantic_seen:
            continue
        seen.add(exact_key)
        semantic_seen.add(semantic_key)
        output.append({**event, "text": text, "dedup_key": exact_key})
    return output
```

Implement `iter_session_events(path)`, `iter_obsidian_candidates(path)`, and ISO timestamp normalization next to this core. Only accept actual user-role message payloads; never scan arbitrary serialized context blobs for profanity.

- [ ] **Step 5: Add the reviewed-ledger schema test**

Require each reviewed row to contain:

```python
REQUIRED_FIELDS = {
    "episode_id", "observed_at", "timezone", "timestamp_provenance",
    "minimal_quote", "source_path", "source_type", "project", "topic",
    "primary_category", "secondary_tags", "severity", "dedup_key",
    "required_future_behavior", "replay_check",
}
```

Also assert that `primary_category` is one of the ten design categories and `severity` is exactly one of `P0`, `P1`, or `P2`.

- [ ] **Step 6: Run the focused tests and verify GREEN**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_audit.py' -v
```

Expected: all candidate extraction, filtering, recurrence, timestamp, and schema tests pass.

- [ ] **Step 7: Commit the parser and tests**

Only the project test file is in the current repository. Record the external skill-script hash in the commit message body.

```bash
SCRIPT_SHA=$(shasum -a 256 /Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py | awk '{print $1}')
git add tests/test_correction_replay_audit.py
git commit -m "test: define correction audit contract" -m "External audit builder SHA-256: ${SCRIPT_SHA}"
```

Expected: one commit containing only `tests/test_correction_replay_audit.py`.

## Task 2: Build and reconcile the reviewed one-week audit

**Files:**
- Modify: `/Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py`
- Modify: `/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.md`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/references/priority-register.md`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/references/deduplication-and-severity.md`
- Modify: `/Users/luke/Documents/polymarket/tests/test_correction_replay_audit.py`

- [ ] **Step 1: Add failing reconciliation and ordering tests**

```python
class ReviewedAuditTests(unittest.TestCase):
    def test_primary_counts_reconcile_to_episode_denominator(self):
        audit = load_module()
        rows = audit.load_reviewed_rows(Path("/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json"))
        counts = audit.category_counts(rows)
        self.assertEqual(sum(counts.values()), len(rows))

    def test_priority_is_severity_then_frequency_then_recency_then_breadth(self):
        audit = load_module()
        rows = audit.load_reviewed_rows(Path("/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json"))
        register = audit.build_priority_register(rows)
        self.assertEqual(register, sorted(register, key=audit.priority_sort_key))

    def test_every_rough_language_episode_has_time_or_provenance_gap(self):
        audit = load_module()
        rows = audit.load_reviewed_rows(Path("/Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json"))
        for row in rows:
            if "rough-language-trigger" in row["secondary_tags"]:
                self.assertTrue(row["observed_at"] or row["timestamp_provenance"] == "gap")
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_audit.py' -v
```

Expected: failures for the missing reviewed rows, category aggregation, and priority renderer.

- [ ] **Step 3: Extract candidates from the user-specified window**

Run the builder in candidate-only mode over the explicit Asia/Shanghai window and both evidence roots:

```bash
python3 /Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py candidates \
  --sessions-root /Users/luke/.codex/sessions/2026/08 \
  --obsidian-inbox "/Users/luke/Documents/lukedesong/lukedesong/AI Memory/Inbox" \
  --start 2026-08-08T00:00:00+08:00 \
  --end 2026-08-14T23:59:59+08:00 \
  --output /tmp/luke-correction-candidates.json
```

Expected: a JSON candidate list plus a source manifest that reports files read, files skipped, raw user messages considered, exact duplicates collapsed, and timestamp provenance gaps. These are **实证值** generated from the named sources and window, not assumed counts.

- [ ] **Step 4: Review every candidate into exactly one primary category**

Populate `last-week-audit.json` only after opening the cited raw event or Obsidian note. Use this exact top-level structure:

```json
{
  "window": {
    "timezone": "Asia/Shanghai",
    "start": "2026-08-08T00:00:00+08:00",
    "end": "2026-08-14T23:59:59+08:00",
    "source": "user-specified"
  },
  "source_manifest": {},
  "episodes": []
}
```

For fork/import timestamps that cannot be proven as original event time, keep the available source timestamp, set `timestamp_provenance` to `gap`, and explain the limitation. Do not turn the preliminary exact-quote count into the distinct-episode denominator.

- [ ] **Step 5: Implement deterministic rendering**

Add these pure functions and CLI modes:

```python
SEVERITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}


def category_counts(rows: list[dict]) -> dict[str, int]: ...
def build_priority_register(rows: list[dict]) -> list[dict]: ...
def priority_sort_key(item: dict) -> tuple: ...
def render_audit_markdown(document: dict) -> str: ...
def render_priority_markdown(rows: list[dict]) -> str: ...
```

The priority item must expose `severity`, `episode_count`, `latest_recurrence`, and `project_breadth`. Sort ascending by severity rank, then descending by episode count, latest recurrence, and breadth. Render the denominator and source manifest before conclusions.

- [ ] **Step 6: Write the stable counting reference**

`deduplication-and-severity.md` must contain the design's exact episode unit, duplicate-collapse rule, recurrence rule, ten primary categories, P0/P1/P2 definitions, and conflict precedence. It must state that secondary tags never inflate category frequency.

- [ ] **Step 7: Render and verify the audit artifacts**

Run:

```bash
python3 /Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py render \
  --reviewed /Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json \
  --audit-output /Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.md \
  --priority-output /Users/luke/.agents/skills/luke-correction-replay/references/priority-register.md
python3 -m unittest discover -s tests -p 'test_correction_replay_audit.py' -v
```

Expected: renderer exits zero; category counts sum exactly to the distinct episode denominator; priority order matches the defined tuple; every rough-language row has a proven time or explicit provenance gap.

- [ ] **Step 8: Preserve an auditable project receipt**

Create `/Users/luke/Documents/polymarket/docs/2026-08-14-correction-replay-audit-receipt.md` containing the user-specified window, source manifest totals, distinct episode denominator, category counts, and SHA-256 values for the reviewed JSON and two rendered Markdown files. Do not copy all quotes into the receipt.

```bash
git add tests/test_correction_replay_audit.py docs/2026-08-14-correction-replay-audit-receipt.md
git commit -m "docs: record correction audit evidence"
```

Expected: the commit includes only the updated audit test and the receipt.

## Task 3: Create the correction-replay methodology skill

**Files:**
- Create: `/Users/luke/.agents/skills/luke-correction-replay/SKILL.md`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/README.md`
- Create: `/Users/luke/.agents/skills/luke-correction-replay/references/evaluation-cases.md`
- Create: `/Users/luke/Documents/polymarket/tests/test_correction_replay_skill.py`

- [ ] **Step 1: Write failing static-contract tests**

```python
import re
import unittest
from pathlib import Path

SKILL = Path("/Users/luke/.agents/skills/luke-correction-replay/SKILL.md")


class SkillContractTests(unittest.TestCase):
    def test_frontmatter_and_dynamic_trigger_coverage(self):
        text = SKILL.read_text()
        self.assertTrue(text.startswith("---\nname: luke-correction-replay\n"))
        description = re.search(r"description: (.+)", text).group(1)
        for trigger in ("correction", "repair", "accounting", "deployment", "non-trivial"):
            self.assertIn(trigger, description.lower())

    def test_workflow_has_preflight_and_completion_gate(self):
        text = SKILL.read_text()
        for phrase in ("Detect task", "Retrieve relevant corrections", "Preflight", "Completion gate", "Capture new correction"):
            self.assertIn(phrase, text)

    def test_p0_failure_blocks_completion_claim(self):
        text = SKILL.read_text()
        self.assertIn("An applicable P0 FAIL blocks any completion claim", text)
```

- [ ] **Step 2: Run the skill tests and verify RED**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_skill.py' -v
```

Expected: failure because `SKILL.md` does not exist.

- [ ] **Step 3: Write the compact skill frontmatter and dynamic workflow**

Start `SKILL.md` with:

```markdown
---
name: luke-correction-replay
description: Use before and after any non-trivial Luke task involving correction history, repair, accounting, deployment, automation, deletion, financial claims, or completion claims; also use when Luke says the result is wrong, repeats a prior correction, identifies a bad case, or uses strong language tied to a concrete failure.
---
```

Keep the body between five and nine numbered workflow steps. The workflow must:

1. detect the task project, object, mutation, live authority, and available evidence sources;
2. retrieve all global P0 rules plus only task-matching priority items;
3. create an internal table of rule ID, reason, required behavior, forbidden regression, and completion proof;
4. run the existing domain skill or direct workflow without creating a second domain process;
5. replay each applicable check as `PASS`, `FAIL`, or `N/A` against actual files/tool evidence and the draft answer;
6. block completion on any applicable P0 failure;
7. capture one deduplicated Obsidian Inbox candidate for a new explicit correction or strong-language trigger;
8. expose only failures, conflicts, coverage gaps, or a user-requested audit.

Use a defaults table for source precedence, current-live-fact refresh, missing-source behavior, and current-instruction conflict resolution. Do not paste the complete historical audit into `SKILL.md`.

- [ ] **Step 4: Write evaluation cases and the minimal README**

`evaluation-cases.md` must give inputs, expected loaded rule IDs, forbidden behavior, and observable pass evidence for all ten validation cases in the design. `README.md` should contain only purpose, installed path, reference map, deterministic regeneration command, and verification command.

- [ ] **Step 5: Run static skill tests and line-budget checks**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_skill.py' -v
test "$(wc -l < /Users/luke/.agents/skills/luke-correction-replay/SKILL.md)" -le 300
```

Expected: all tests pass and the skill stays at or below the **外部约束值** of 300 lines from the selected skill-authoring guidance.

- [ ] **Step 6: Commit the skill contract tests**

```bash
SKILL_SHA=$(shasum -a 256 /Users/luke/.agents/skills/luke-correction-replay/SKILL.md | awk '{print $1}')
git add tests/test_correction_replay_skill.py
git commit -m "test: enforce correction replay skill" -m "Installed skill SHA-256: ${SKILL_SHA}"
```

Expected: one commit containing only the project test file.

## Task 4: Install the compact global trigger without duplicating the existing memory Hook

**Files:**
- Modify: `/Users/luke/.codex/AGENTS.md`
- Modify: `/Users/luke/Documents/polymarket/tests/test_correction_replay_skill.py`

- [ ] **Step 1: Add failing global-trigger tests**

```python
GLOBAL_AGENTS = Path("/Users/luke/.codex/AGENTS.md")


class GlobalHookTests(unittest.TestCase):
    def test_global_hook_is_unique_and_points_to_skill(self):
        text = GLOBAL_AGENTS.read_text()
        self.assertEqual(text.count("## P0：Luke 纠错回放 Hook"), 1)
        self.assertIn("/Users/luke/.agents/skills/luke-correction-replay/SKILL.md", text)
        self.assertIn("非简单任务", text)
        self.assertIn("P0", text)
        self.assertIn("不得宣称完成", text)
```

Also assert that the global section does not contain copied historical quotes or the full priority table.

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_skill.py' -v
```

Expected: the unique global Hook heading or skill path is missing.

- [ ] **Step 3: Patch the existing Obsidian memory section in place**

Add one compact section immediately after the current Obsidian cross-session memory Hook. Do not create a second competing memory policy.

```markdown
## P0：Luke 纠错回放 Hook

- 每个非简单任务开始实质工作前，必须完整读取 `/Users/luke/.agents/skills/luke-correction-replay/SKILL.md`，检索当前任务适用的已确认规则与 Inbox 纠错候选，并形成内部防复发检查表。
- 当前用户指令优先于历史候选；会变化的事实必须重新读取当前权威来源，禁止用记忆代替实时证据。
- 最终回复前逐条回放适用检查。任一适用 P0 为 `FAIL` 时，不得宣称完成；继续安全修正，或明确报告外部、权限或数据阻断。
- 简单翻译、单行格式化等完全自包含且历史不可能改变答案的任务可跳过；不确定时执行 Hook。
```

- [ ] **Step 4: Run uniqueness and compactness checks**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_skill.py' -v
python3 - <<'PY'
from pathlib import Path
text = Path('/Users/luke/.codex/AGENTS.md').read_text()
section = text.split('## P0：Luke 纠错回放 Hook', 1)[1].split('\n## ', 1)[0]
assert len(section.splitlines()) <= 8
print('GLOBAL_HOOK_COMPACT_PASS')
PY
```

Expected: tests pass and output is `GLOBAL_HOOK_COMPACT_PASS`.

- [ ] **Step 5: Commit only the updated project-side test**

```bash
AGENTS_SHA=$(shasum -a 256 /Users/luke/.codex/AGENTS.md | awk '{print $1}')
git add tests/test_correction_replay_skill.py
git commit -m "test: require global correction replay hook" -m "Global AGENTS SHA-256: ${AGENTS_SHA}"
```

Expected: the external global file is installed, while the repository commit contains only its executable contract test.

## Task 5: Prove behavior with table-driven evaluation cases

**Files:**
- Modify: `/Users/luke/Documents/polymarket/tests/test_correction_replay_skill.py`
- Modify: `/Users/luke/.agents/skills/luke-correction-replay/references/evaluation-cases.md`

- [ ] **Step 1: Add failing table-driven evaluation tests**

Represent the ten design cases as data, not ten separate custom frameworks:

```python
CASES = [
    ("polymarket repair", {"repair-closure", "official-accounting", "version-identity", "unknown-no-repost"}, False),
    ("spreadsheet historical update", {"history-lock", "source-verification"}, False),
    ("translate hello to Chinese", set(), True),
]


class EvaluationCaseTests(unittest.TestCase):
    def test_evaluation_case_matrix(self):
        skill = load_runtime_helpers()
        for prompt, required_ids, may_skip in CASES:
            result = skill.select_rules(prompt)
            self.assertEqual(result["skip"], may_skip)
            self.assertTrue(required_ids.issubset(set(result["rule_ids"])))
```

Extend the table to duplicated session history, true recurrence, P0 completion block, stale-candidate override, one strong-language capture, priority regeneration, and compact user output.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_skill.py' -v
```

Expected: failures identify missing selection or replay behavior; no live system is touched.

- [ ] **Step 3: Add only the pure helpers needed by the evaluations**

Place deterministic selection and completion helpers in `build_correction_audit.py`; keep task execution in `SKILL.md`.

```python
def select_rules(task: dict, priority_rows: list[dict]) -> list[dict]:
    """Return all global P0 rows plus rows matching project, object, or requested action."""


def completion_gate(checks: list[dict]) -> dict:
    blocking = [row for row in checks if row["severity"] == "P0" and row["status"] == "FAIL"]
    return {"may_claim_complete": not blocking, "blocking_rule_ids": [row["rule_id"] for row in blocking]}
```

The selector may use transparent keyword sets from the priority register. It must not use an opaque score or infer live state from memory.

- [ ] **Step 4: Run all correction-replay tests**

Run:

```bash
python3 -m unittest discover -s tests -p 'test_correction_replay_*.py' -v
```

Expected: every audit, skill, trigger, selection, and completion-gate test passes.

- [ ] **Step 5: Commit the behavior evaluations**

```bash
git add tests/test_correction_replay_skill.py
git commit -m "test: cover correction replay behavior"
```

Expected: one commit containing the evaluation test expansion.

## Task 6: Run final quality review and freeze installation evidence

**Files:**
- Create: `/Users/luke/Documents/polymarket/docs/2026-08-14-correction-replay-installation-receipt.md`
- Verify: `/Users/luke/.agents/skills/luke-correction-replay/SKILL.md`
- Verify: `/Users/luke/.codex/AGENTS.md`
- Verify: `/Users/luke/.agents/skills/luke-correction-replay/references/*.md`

- [ ] **Step 1: Run syntax, unit, and deterministic-render verification**

```bash
python3 -m py_compile /Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py
python3 -m unittest discover -s tests -p 'test_correction_replay_*.py' -v
python3 /Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py check \
  --reviewed /Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json \
  --audit /Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.md \
  --priority /Users/luke/.agents/skills/luke-correction-replay/references/priority-register.md
```

Expected: compilation succeeds, all tests pass, and `check` reports `AUDIT_RECONCILED`, `PRIORITY_ORDER_PASS`, and `RENDER_DETERMINISTIC_PASS`.

- [ ] **Step 2: Self-review against the approved design and skill quality rubric**

Confirm all of the following with direct file evidence:

- every design validation case and acceptance criterion is represented by a test or deterministic check;
- all ten primary categories are defined and every episode has exactly one;
- no `TODO`, `TBD`, placeholder, invented timestamp, or unexplained count remains;
- the skill has exhaustive trigger wording, dynamic source detection, five-to-nine workflow steps, a defaults table, error behavior, and a compact output contract;
- no command in the audit builder or skill can submit an order, mutate a ledger, change an automation, or modify production state;
- the global Hook is unique and does not embed the full history;
- external installed-file hashes match the receipt.

Run:

```bash
rg -n 'TODO|TBD|PLACEHOLDER' \
  /Users/luke/.agents/skills/luke-correction-replay \
  /Users/luke/Documents/polymarket/tests/test_correction_replay_audit.py \
  /Users/luke/Documents/polymarket/tests/test_correction_replay_skill.py
```

Expected: no matches.

- [ ] **Step 3: Write the installation receipt with classified numbers**

Record:

- the one-week window as a **用户指定值**;
- files and episode counts as **实证值** from the rendered source manifest;
- category totals and priority ordering as **公式推导值** from reviewed rows;
- the skill line cap as an **外部约束值** from the skill-authoring guidance;
- SHA-256 values for `SKILL.md`, the builder, all references, and global `AGENTS.md`;
- exact verification commands and their fresh results.

- [ ] **Step 4: Commit the final receipt only after every check is green**

```bash
git add docs/2026-08-14-correction-replay-installation-receipt.md
git commit -m "docs: verify correction replay installation"
```

Expected: final commit contains only the installation receipt. Do not claim completion if any applicable P0 evaluation remains red.

- [ ] **Step 5: Report the installed outcome compactly**

The final user-facing report should give: the installed Hook path, installed skill path, reviewed distinct-episode denominator and top priorities with source classifications, fresh verification result, and any timestamp-provenance gaps. Do not expose the internal checklist unless a check failed or Luke asks for it.
