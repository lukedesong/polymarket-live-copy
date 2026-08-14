# Correction Replay Audit Receipt

## Scope

- Review window: `2026-08-08T00:00:00+08:00` through `2026-08-14T23:59:59+08:00` in Asia/Shanghai. This is a **用户指定值**.
- Frozen source snapshot: `2026-08-14T23:45:48+08:00`.
- Source snapshot SHA-256: `8adc223b194cc02c04262e849d888d04a20fa718c1f92185b6b6596dca383de5`.

## Source Manifest

The following are **实证值** from the frozen source scan:

- Raw session files read: `160 / 160`; read failures: `0`.
- Obsidian Inbox Markdown files discovered: `37`.
- Decoded in-window message events before correction filtering: `8,319`.
- Machine correction candidates before manual review: `302`.
- Human-reviewed distinct correction episodes after removing ordinary requests, assistant-authored subagent prompts, injected context, exact fork/import copies, and repeated quotes from one episode: `31`.
- Rough-language occurrences retained in the dedicated timeline: `23`. This occurrence count is not the episode denominator.

## Category Reconciliation

The following are **公式推导值** from the 31 reviewed rows. Primary categories are mutually exclusive, so the counts sum to the episode denominator.

| Priority band | Primary category | Episodes |
|---|---|---:|
| P0 | `stop_delete_topology_authority_wrong` | 7 |
| P0 | `repair_not_verified_terminal` | 6 |
| P0 | `accounting_authority_or_boundary_wrong` | 4 |
| P0 | `copy_fidelity_or_unsafe_retry` | 3 |
| P0 | `repair_version_identity_inconsistent` | 2 |
| P0 | `memory_not_retrieved_or_applied` | 1 |
| P0 | `risk_reduction_delayed` | 1 |
| P1 | `partial_evidence_called_complete` | 4 |
| P1 | `overengineered_or_unbounded` | 3 |
|  | **Total** | **31** |

Priority ordering is severity, then distinct episode frequency within severity, then latest evidenced recurrence, then independent project breadth. A low-frequency P0 remains above every P1.

## Provenance Boundary

- Exact times marked `raw-session-event` are raw event timestamps.
- Date-only Obsidian notes are recorded as `TIME_GAP`; no hour or minute was invented.
- Imported/forked session rows retain their visible import timestamp but are labeled `raw-import-timestamp-not-original`.
- Rough wording is stored as an observable correction trigger, not as a psychological diagnosis.

## Artifact Hashes

| Artifact | SHA-256 |
|---|---|
| `last-week-audit.json` | `651cefcd893e27fb6f41c34690954bb8dba8526be9c72b5fa059219dadce7764` |
| `last-week-audit.md` | `c5fad3dd27d4e651b4922bae330c8bb94f3724af4d4de0a08ef1a6c9678684ee` |
| `priority-register.md` | `dbc13cef6fd69fbbca20d9556aba4dbd6619f0b90cae201c42995dd167f997ca` |
| `deduplication-and-severity.md` | `b16587881f47ec653c5de9ac7ea749670ffe4a8c9e4adb28c9820bc4ae6de030` |
| `build_correction_audit.py` | `fa4119dd00d8595ce422064d714f7a80d5eb3f16bde8c2e4e3c86314ec2dc5a1` |

## Fresh Verification

Executed:

```bash
python3 /Users/luke/.agents/skills/luke-correction-replay/scripts/build_correction_audit.py check \
  --reviewed /Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.json \
  --audit /Users/luke/.agents/skills/luke-correction-replay/references/last-week-audit.md \
  --priority /Users/luke/.agents/skills/luke-correction-replay/references/priority-register.md
python3 -m unittest discover -s tests -p 'test_correction_replay_audit.py' -v
```

Observed results: `AUDIT_RECONCILED`, `PRIORITY_ORDER_PASS`, `RENDER_DETERMINISTIC_PASS`, and `9 / 9` tests passed with `0` failures.
