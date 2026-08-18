---
name: polymarket-closed-loop-recovery
description: >
  Run and enforce a Polymarket copy-trading inspection-to-repair loop. Use for
  hourly copy-trading patrols, heartbeat inspections, release failures,
  service health errors, action-conservation failures, stale cursors, mixed
  release versions, retry-state faults, or when the user says repair it,
  do not only report, keep fixing, release it, or verify the server.
---

# Polymarket Closed-Loop Recovery

Use this for the active Polymarket production-copy patrol. The only successful
outcome is a verified server state. Detecting a fault, editing local code,
passing a unit test, restarting a process, or attempting a release is not a
repair.

## Step 1: Detect Authority and Runtime State

Read the current project `AGENTS.md`, applicable copy contract, current
automation prompt, and installed server release/receipt. Detect tools with:

```sh
command -v ssh || echo SSH_MISSING
ssh -G polymarket-hk >/dev/null 2>&1 || echo SSH_ALIAS_UNAVAILABLE
```

Use SSH if available. If it is unavailable, use only non-mutating local
analysis and report `BLOCK_SERVER_ACCESS`; do not claim a repair.

Before any other Polymarket statement, read the fixed server path
`/opt/polymarket-live/CURRENT_REPAIR_VERSION.json`.
Treat only its `semantic_repair_version` as the current repair version. Verify
that its `release` resolves to the same target as
`/opt/polymarket-live/current`, and verify the SHA-256 of its declared source
commit receipt. A one-time `CANONICAL_BACKFILLED` record may explicitly
supersede under-counted historical receipt versions only when it lists every
deduplicated `VERIFIED_FIXED` receipt used in the formula. The
release-directory timestamp, change ID, local notes, chat history, and
uncommitted candidates are never version numbers. If the fixed index is
unavailable or inconsistent, report
`BLOCK_VERSION_AUTHORITY` or `BLOCK_VERSION_IDENTITY` instead of guessing.

Before diagnosis, also read the Obsidian `AI Memory/Memory Index.md` and search
the active project's confirmed P0/correction pages **and relevant Inbox
candidates** using the current profile, wallet, fault, accounting, release, and
execution terms.  Convert every applicable prior correction into a short
anti-regression checklist for this run.  Candidate notes remain unconfirmed
warnings rather than facts, but they must still trigger the corresponding
verification.  Merely recording a correction without replaying this search is
an incomplete preflight.

Before every mutation, dynamically obtain (do not assume): active profile/unit
mapping, current release, all active-process CWD/exe, action waterlines,
latest source actions, active reservations, latest UNKNOWN/SUBMITTED states,
and authenticated official open orders.

## Step 2: Choose the Mode

| Gate result | Mode | Permitted work |
|---|---|---|
| Any active action, reservation, unsafe submission, or official open order | `FOLLOW_ONLY` | Follow, read-only reconciliation, non-disruptive diagnosis only. |
| No such gate and no severe execution risk | `MAINTENANCE_ALLOWED` | Continue this loop to completion. |
| Duplicate, reverse, wrong token/amount, cursor jump, or unsafe submission | `EMERGENCY_FAIL_CLOSED` | Pause only the affected submission capability, preserve other profiles and read-only reconciliation, then continue the loop. |

Never finish a run merely because a fault was found. Do not make an ordinary
restart during `FOLLOW_ONLY`. An external endpoint failure alone is not a
reason to restart.

## Step 3: Preserve Exact Evidence and Classify

For every fault, create an immutable evidence bundle before any change:

- precise action IDs, source timestamps, token, side, source amount/notional;
- local terminal/attempt states, reservation and unknown state;
- official order/trade/chain result or exact external endpoint error;
- current release hash, service PID/CWD/exe, and time of observation.

Classify one of:

- `INTERNAL_SAFE_TO_REPAIR`: code, service, cursor, health, topology, mapping,
  permission, or known-safe state interpretation defect.
- `EXTERNAL_RETRYABLE`: official/RPC/CLOB/Data API/orderbook constraint linked
  to a precise action. Keep collection running and use the authorized bounded
  retry state machine; do not call it fixed.
- `BLOCK_UNSAFE`: repair would repost UNKNOWN, chase historical actions,
  overwrite ledger/receipt, create duplicate/reverse/wrong-size orders, or
  exceed authority. Preserve evidence and escalate.

## Step 4: Repair Until a Terminal Outcome

For `INTERNAL_SAFE_TO_REPAIR`, repeat this loop without ending the patrol:

1. Write a failing regression test reproducing the exact fault.
2. Fix the shared root cause and all comparable active-profile paths.
3. Run the affected full tests and static checks.
4. Re-run the real-time gate immediately before mutation.
5. If still `MAINTENANCE_ALLOWED`, use the existing closed-loop release
   transaction or a minimal exact service repair. Never manually alter live
   SQLite, action receipts, holdings, settlements, or historical waterlines.
6. Re-check the real server: release symlink, manifest, committed receipt,
   code identity, unit policy/PID/CWD/exe, SQLite integrity, locks, cursor,
   action conservation, official orders/trades/receipts, and the original
   fault's exact invariant.
7. If any verification fails, treat it as the next fault and return to item 1.

Stop only at a terminal result:

- `VERIFIED_FIXED`: code/change plus test, deployed receipt, and immediate
  server evidence all pass.
- `EXTERNAL_RETRYABLE`: exact external action remains on its authorized retry
  path and collection is running.
- `BLOCK_UNSAFE`: one of the explicit unsafe conditions in Step 3.

Mixed release versions are an internal topology failure. First prove the gate
is empty and code identities are equal; only then perform the smallest
identity-alignment restart. Call that `PROCESS_ALIGNMENT`, never `FIXED`, and
continue the loop through release and server verification.

## Step 5: Enforce Bounded Retry Safety

Apply only the currently authorized retry policy and activation waterline.
For every retryable action, prove official zero fill before retrying; never
repost UNKNOWN or timeout results. Preserve cumulative fill, retry only the
remainder, enforce price/time/minimum/inventory/opposite-action terminal gates,
and never chase historical failures.

## Step 6: Report Only Terminal Evidence

Every Polymarket operational response, not only a repair handoff, starts with
`Current version: <semantic_repair_version>` (rendered in the user's language)
from the fixed server receipt. This makes separate conversations use the same
live authority.

For every `VERIFIED_FIXED` repair, the first user-visible line must be
`版本: <previous> → <current>`, and both values must match the server repair
timeline and committed release receipt.  Omitting this line is an incomplete
repair handoff even when the server was correctly versioned.

Before writing the final response, replay the Step 1 anti-regression checklist
against the actual change and server evidence.  If any applicable prior
correction is violated or not checked, return to the repair loop; do not claim
`VERIFIED_FIXED`.  This is the completion gate that turns memory retrieval into
observable behavior rather than another note.

Use these labels exactly:

1. `发现`: what was observed, with action/profile and evidence.
2. `实际变更`: code/deployment/restart actually completed; say `无` if none.
3. `测试`: exact tests/checks and result.
4. `服务器复核`: immediate, observable post-change evidence.
5. `结论`: only `VERIFIED_FIXED`, `EXTERNAL_RETRYABLE`, `BLOCK_UNSAFE`, or
   `STILL_REPAIRING`.

Forbidden wording: “已修复” for diagnosis, a local diff, a passing test, a
release attempt, a failed deployment, or process alignment alone.

## Step 7: Version and External-Unfilled Timeline

Every verified repair has one version number and an immutable timing boundary.
Read the current version number from the fixed server receipt before preparing
a candidate. After any discovered problem is actually repaired—by the
automatic patrol or on user request—and reaches `VERIFIED_FIXED`, increment the
minor component of the current `major.minor` version exactly once. The new
release `COMMITTED.json`, the latest `verified_repair_release` record in the
server timeline, and the fixed server receipt must agree on that version and
release identity before completion can be reported.
Do not alter an already committed receipt merely to add a later version label;
append a separate timeline record instead.

Before deployment, snapshot every externally unfilled action by its immutable
source identity. For every item preserve source official time, first discovery,
first official zero-fill/constraint evidence, current terminal reason, and
the prior release/version. After the committed cutover, report three disjoint
sets:

- `BEFORE_VERSION_CARRIED_FORWARD`: first seen before the cutover and still
  unresolved; never call it a new post-repair failure.
- `AT_CUTOVER_CARRIED`: unresolved at the exact cutover snapshot.
- `AFTER_VERSION_NEW`: source time and first discovery are both after the
  committed cutover; this is the only set that can demonstrate a post-repair
  recurrence.

For each set, report exact action IDs and timestamps as well as counts. A
count without its action/time evidence is not an acceptable external-error
report. This timeline is operational evidence only: it must be append-only,
must not rewrite a live trading SQLite ledger, and must not authorize a
historical retry or repost.

## Default Parameters

| Parameter | Default | Source |
|---|---|---|
| Mutation gate | Dynamic live evidence | Safety requirement |
| Repair scope | Shared root cause plus active peers | User instruction |
| Completion standard | Test + deployed receipt + live verification | User instruction |
| Unknown submission | Read-only reconciliation, no repost | Safety contract |
| Historical action | Never chase/recreate | Safety contract |

## Reference

Current trading policy and numeric limits come from the automation prompt and
project `AGENTS.md`; they must be re-read each run rather than copied into this
skill.
