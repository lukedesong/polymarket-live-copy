# Luke Correction Replay Hook Installation Receipt

## Result

`INSTALLED_AND_VERIFIED`

The global Hook now requires every non-trivial Codex task to load `/Users/luke/.agents/skills/luke-correction-replay/SKILL.md`, retrieve only applicable correction rules, and block a completion claim when an applicable P0 check fails.

## Reviewed evidence

All values below are **empirical values** from the frozen reviewed artifact unless stated otherwise.

- User-specified review window: `2026-08-08T00:00:00+08:00` through `2026-08-14T23:59:59+08:00`.
- Frozen snapshot: `2026-08-14T23:45:48+08:00`.
- Raw session files read: `160 / 160`; skipped: `0 / 160`.
- Obsidian candidate files discovered: `37`.
- Decoded events in the window: `8,319`.
- Machine candidates before manual review: `302`.
- Reviewed distinct correction episodes: `31`.
- Rough-language occurrences retained with timestamp or explicit provenance gap: `23`.
- Source snapshot SHA-256: `8adc223b194cc02c04262e849d888d04a20fa718c1f92185b6b6596dca383de5`.

## Priority result

The ordering is a **formula-derived value**: severity, then distinct episode frequency, then latest evidenced recurrence, then project breadth.

| Priority | Category | Episodes |
|---|---|---:|
| P0 | stop/delete/topology authority wrong | 7 |
| P0 | repair not verified terminal | 6 |
| P0 | accounting authority or boundary wrong | 4 |
| P0 | copy fidelity or unsafe retry | 3 |
| P0 | repair version identity inconsistent | 2 |
| P0 | risk reduction delayed | 1 |
| P0 | memory not retrieved or applied | 1 |
| P1 | partial evidence called complete | 4 |
| P1 | overengineered or unbounded | 3 |

The mutually exclusive primary-category total is `31 / 31` reviewed episodes.

## Installed artifacts

| Artifact | SHA-256 |
|---|---|
| Global Hook `/Users/luke/.codex/AGENTS.md` | `19e23af77e009fd979bf26e542c219ab5adc71d6cc764ec562ed9b923e77fccf` |
| `SKILL.md` | `37ca270d3d4ebba17d5ddc6d1bcef97a988de7d66b3b91d86670dd9269c8362a` |
| `README.md` | `d351655dd9c190a31731825468af8f267bf6e837ca78a8f3edc00288e0fd09ce` |
| Audit/runtime builder | `61b93fcd7dc15ffeee43d10bb7466829a3677d65baa9d001e9489dbb31f25509` |
| Reviewed JSON | `651cefcd893e27fb6f41c34690954bb8dba8526be9c72b5fa059219dadce7764` |
| Reviewed Markdown | `c5fad3dd27d4e651b4922bae330c8bb94f3724af4d4de0a08ef1a6c9678684ee` |
| Priority register | `dbc13cef6fd69fbbca20d9556aba4dbd6619f0b90cae201c42995dd167f997ca` |
| Deduplication method | `b16587881f47ec653c5de9ac7ea749670ffe4a8c9e4adb28c9820bc4ae6de030` |
| Evaluation cases | `0a46ff4b8930b3b113b83da01a6271c9ab60adad6f55d78542a877d69056f183` |

## Verification

- Python compilation: PASS.
- Reviewed audit reconciliation: PASS.
- Severity-first priority ordering: PASS.
- Deterministic Markdown rendering: PASS.
- Automated tests: `19 / 19` passed, `0` failed.
- Declared behavior cases: `10 / 10` covered by the evaluation reference and automated contract tests.
- Main Skill length: `70` lines, an empirical file count.

## Quality rubric

The following is a **formula-derived review score** using the installed skill-creator rubric: sum of ten reviewer-assigned dimensions, each out of ten.

| Dimension | Score |
|---|---:|
| Trigger quality | 9 |
| Defaults coverage | 8 |
| Step architecture | 8 |
| Reference strategy | 9 |
| Runtime adaptation | 8 |
| Output template | 8 |
| Missing-data handling | 8 |
| Code/formula quality | 8 |
| Conciseness | 10 |
| Domain accuracy | 9 |
| **Total** | **85 / 100** |

The score is a structured reviewer judgment, not an external benchmark result. The executable tests and hashes above are the completion evidence.
