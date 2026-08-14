# Luke Correction Replay Hook Design

## 1. Goal

Create one global, repeatable correction-replay mechanism so that previously
recorded mistakes change the next task's behavior. The mechanism must not stop
at memory capture. It must:

1. retrieve task-relevant corrections before work starts;
2. convert them into a small, observable anti-regression checklist;
3. replay that checklist against the actual result before claiming completion;
4. capture new corrections without double-counting copied conversation history;
5. prioritize recurrent mistakes by observed frequency without hiding severe
   low-frequency safety failures.

The first evidence window is Asia/Shanghai calendar time from 2026-08-08
00:00:00 through 2026-08-14 23:59:59. This is a user-requested one-week review
window, not a permanent statistical threshold.

## 2. Confirmed Scope

The Hook applies globally to all non-trivial Luke tasks, not only Polymarket.
It remains task-selective: a spreadsheet task does not load Polymarket trading
details unless the current request makes them relevant.

A trivial task is a self-contained request whose answer cannot be changed by
project history or prior corrections, such as a direct translation or a
one-line formatting operation. When uncertain, the Hook runs.

## 3. Evidence Sources and Provenance

The audit and future retrieval use the following sources in precedence order:

1. current user messages in the active task;
2. raw Codex session events under `~/.codex/sessions`;
3. confirmed Obsidian P0, preference, project, and decision pages;
4. relevant Obsidian `AI Memory/Inbox` correction candidates;
5. Codex `MEMORY.md` and directly linked rollout summaries;
6. project `AGENTS.md` and applicable skills.

Current runtime facts, account values, deployment state, prices, and service
health are never accepted from memory. They must still be refreshed from their
live authority.

Every audit row records:

- observed time and timezone;
- smallest necessary user quote;
- source file and source type;
- project and topic;
- correction category;
- severity;
- deduplication key;
- required future behavior;
- a concrete replay check.

## 4. Counting and Priority Rules

### 4.1 Counting unit

The frequency unit is one distinct correction episode, not one file, one quote
fragment, or one occurrence copied into a forked task.

Exact duplicates copied across session forks are one episode. Punctuation and
spacing variants with the same topic and same underlying assistant action are
one episode. A later restatement counts as a new episode only when it follows a
new assistant action or demonstrates recurrence in another task.

One episode may carry more than one theme tag, but it has exactly one primary
category. Primary-category counts therefore sum to the total number of audited
episodes. Secondary tags are searchable but do not inflate the ranking.

### 4.2 Priority ordering

No opaque weighted score is used. Priority is ordered by:

1. severity band;
2. distinct episode frequency within that band;
3. most recent recurrence;
4. breadth across independent projects or task types.

Severity bands are:

- `P0`: could cause unauthorized action, duplicate or wrong trading action,
  financial/accounting misstatement, destructive deletion, false completion,
  or a direct violation of an explicit user stop/delete/authority boundary;
- `P1`: materially wrong result, repeated unfinished work, missed requested
  scope, stale automation behavior, or preventable excessive delay;
- `P2`: presentation, terminology, or verbosity defect that does not change the
  underlying result.

A low-frequency P0 remains above a high-frequency P1. Within a severity band,
frequency is the first ordering key as requested.

## 5. Initial Error Taxonomy

The first audit must classify every episode into one primary category from this
set:

1. repair did not reach an actual verified terminal result;
2. simple work was over-engineered or allowed to expand indefinitely;
3. accounting object, authority, time window, or realized/unrealized boundary
   was wrong;
4. copy-action fidelity failed through missed action, missed leg, unsafe retry,
   wrong skip, or treating unknown state as completion;
5. stop, pause, delete, fresh-start, active topology, or automation authority
   was misunderstood;
6. process activity, scan volume, candidate count, or partial evidence was
   reported as a completed result;
7. recorded memory was not retrieved or not translated into behavior;
8. repair version identity was missing, stale, or inconsistent across tasks;
9. risk-reduction execution was delayed after authority and evidence were
   already sufficient;
10. other, with a required explanation and a proposal for whether it should
    become a stable category.

The taxonomy is a starting structure, not a quota. Categories are merged when
the audit shows the same root cause and split only when replay checks differ.

## 6. Architecture

Use the approved hybrid design.

### 6.1 Global trigger

Add a compact mandatory Hook to `~/.codex/AGENTS.md`. It requires every
non-trivial task to invoke the correction replay skill before material work and
to run its completion gate before the final answer.

The global file contains only the trigger, authority order, and fail-closed
rule. Detailed history does not accumulate there.

### 6.2 Skill

Create `~/.agents/skills/luke-correction-replay/SKILL.md` as a methodology
skill. It dynamically detects available session logs, Obsidian memory, Codex
memory, project rules, and current task context. Missing optional sources reduce
coverage and are reported; they do not cause the skill to invent history.

The skill workflow is:

1. detect sources and current task identity;
2. retrieve relevant correction rules;
3. select applicable checks by project, object, requested action, and risk;
4. produce a compact internal preflight checklist;
5. let the task proceed using the applicable domain skill;
6. replay the checklist against files, tool results, and the draft answer;
7. capture a new candidate event when the current user explicitly corrects the
   assistant or triggers the existing strong-emotion rule;
8. return a short completion result.

### 6.3 Reference files

The skill uses these references:

- `references/priority-register.md`: frequency-ranked categories, counts,
  evidence coverage, and replay rules;
- `references/last-week-audit.md`: the full deduplicated 2026-08-08 through
  2026-08-14 audit, including profanity-trigger timestamps;
- `references/deduplication-and-severity.md`: stable counting, severity, and
  conflict rules;
- `references/evaluation-cases.md`: positive and negative behavior examples.

Historical quotes remain in the audit reference, not in the global prompt.

## 7. Runtime Behavior

### 7.1 Before work

The Hook derives task keywords, project, object, requested mutation, and live
authority. It loads only matching rules plus all globally applicable P0 rules.
The result is an internal table with:

- rule ID;
- why it applies;
- required action;
- forbidden regression;
- proof needed before completion.

### 7.2 During work

Domain skills remain responsible for the task itself. The Hook does not create
a second trading, accounting, spreadsheet, release, or research workflow. It
only injects applicable constraints and checks.

If a current instruction conflicts with an older candidate, the current user
instruction wins and the conflict is reported. Confirmed memory outranks an
Inbox candidate, but neither can replace current live evidence.

### 7.3 Completion gate

Before the final answer, each applicable rule becomes `PASS`, `FAIL`, or `N/A`
with evidence. Any `FAIL` on an applicable P0 blocks a completion claim. The
agent must continue safely or report the exact external, authority, or data
blocker.

The checklist stays internal by default. The user sees it only when a rule
fails, when a conflict exists, or when the user asks for the audit.

## 8. New Correction Capture

The existing Obsidian strong-emotion capture remains the source-of-record
workflow for new candidate notes. The new skill adds structured fields needed
for replay and frequency counting.

A new event is captured when Luke explicitly says the result is wrong,
repeats a prior correction, identifies a bad case, or uses strong language tied
to a concrete task failure. Profanity alone is not treated as a personality
fact or psychological diagnosis.

The capture stores the minimum necessary quote. It must label any emotional
interpretation as unconfirmed inference. Exact repeats from imported history do
not create new events.

## 9. Error Handling

- No session-log access: use structured memory sources and mark raw-timestamp
  coverage incomplete.
- No Obsidian access: use Codex memory and project rules; do not silently claim
  full correction coverage.
- Conflicting timestamps: preserve both source timestamps and identify the
  canonical event time only when provenance proves it.
- Candidate conflicts with current instruction: current instruction wins;
  preserve the conflict for review.
- Current live fact conflicts with memory: live authority wins; memory is
  corrected through the approved candidate workflow.
- Hook not invoked on a non-trivial task: completion gate fails by definition.

## 10. Validation

Evaluation cases must prove at least these behaviors:

- a Polymarket repair task loads repair closure, official accounting, version,
  live-evidence, and no-unsafe-retry rules;
- a spreadsheet task loads historical lock and source-verification rules but
  not trading details;
- a trivial translation skips the Hook cleanly;
- duplicated session history does not inflate frequency;
- a repeated correction after a new failed action does increase frequency;
- a P0 replay failure prevents the phrase "completed" or equivalent;
- current instructions override stale candidates without deleting history;
- a new rough-language correction creates one candidate, not multiple copies;
- the priority register can be regenerated from the audit rows;
- global trigger and skill output remain compact enough not to turn every task
  into a process report.

## 11. Acceptance Criteria

The design is complete when:

- the one-week audit has a reproducible denominator and source manifest;
- every profanity-trigger row has a time or an explicit timestamp-provenance
  gap;
- category counts exactly reconcile to the distinct episode total;
- the priority register is sorted by the defined rules;
- the global Hook forces invocation for non-trivial tasks;
- the skill passes its evaluation cases;
- applicable P0 checks can block a false completion claim;
- no live trading, ledger, automation, or production state is mutated by the
  audit or Hook itself.

## 12. Non-Goals

- psychological profiling or sentiment scoring of Luke;
- showing historical profanity in ordinary task responses;
- loading the entire memory vault into every task;
- replacing domain-specific safety skills;
- treating frequency as proof that a low-severity issue outranks financial or
  authorization safety;
- silently promoting Inbox candidates into confirmed long-term facts.
