# 0x44b0 Full-Wallet Live Copy Design

Date: 2026-08-09

## Objective

Add one independent real-money live-copy sleeve for source wallet
`0x44b0a564260008b65a111286e45079f2cf360822`.  The sleeve follows every
future source action from a newly frozen chain watermark.  It must not replay
or fill any action observed before that watermark.

The copy objective is action fidelity.  BUY, SELL, losses, exits, strategy
changes, and multi-leg decisions are all source actions.  No category,
`Netflix`, or `this week` filter is allowed after launch.

## Approved Inputs and Provenance

- Source wallet: user-specified value.
- Live sleeve capital: `200 USD`, user-specified value.
- Fixed share scale:
  `((5 shares / 15.25773 source shares) / 10) =
  0.03277027447726496667590788407`, formula-derived value.  The `5`-share
  input was the observed official market minimum across the historical market
  sample; `15.25773` was the smallest complete source action in that sample;
  division by `10` was user-specified.  The scale is frozen and may not change
  when the source wallet, wallet balance, or later action sizes change.
- Below-minimum BUY policy: user-approved `UPSCALE_TO_CURRENT_MARKET_MINIMUM`.
  The current market's official minimum is read afresh for each action and
  persisted with the execution receipt.  No historical minimum is assumed to
  be a platform-wide constant.
- SELL policy: only sell this sleeve's actual inventory.  Never borrow another
  sleeve's position and never sell short.
- Scope: all future source-wallet actions, user-confirmed on 2026-08-09.
- Topic-change notification: every first-seen source action whose frozen
  official title and event identity are not classified as Netflix must create
  one deduplicated user alert.  This is a notification rule only; the action
  remains eligible and execution must not pause or filter it.
- Historical catch-up: forbidden.  This is a forward-only live launch.

At the read-only pre-design snapshot at 2026-08-09 19:50:36 Asia/Shanghai,
the authenticated CLOB collateral was `845.483579 USD`, an empirical value.
The coordinator attributed `146.683486 USD` to CD90 and `698.800092 USD` to
tennis, with a `0.000001 USD` reconciliation delta.  Those figures are not a
deployment constant.  They must be re-read while all participating services
are frozen.  The transfer is blocked unless tennis still has at least the
user-authorized `200 USD` available after active reservations and unresolved
submissions.

## Architecture

### Independent profile and ledger

Create a new profile key that names the source rather than the former paper
strategy, for example `wallet_44b0_full_wallet`.  It receives:

- a new empty SQLite ledger;
- a dedicated runtime directory;
- a dedicated systemd service and status output;
- the existing shared authenticated wallet lock;
- the existing shared-wallet coordinator;
- `FullWalletEventScope`, used only to freeze official immutable market and
  event identity, not to filter categories;
- the full-wallet source-action detection contract already used for an
  address whose maker fills and verified public-wallet actions must both be
  conserved.

The new ledger starts with no positions, no historical action receipts, and no
settlement history.  Its initial chain cursor is the processable head captured
during the stopped deployment transaction.

### Capital transfer

The shared wallet currently has no material unallocated cash.  Therefore the
new sleeve cannot be initialized by duplicating `200 USD` in a third ledger.
Doing so would overstate physical collateral.

The safe operation is an explicit capital transfer from the tennis sleeve to
the new sleeve:

1. Stop and freeze all registered live sleeves under the existing closed-loop
   release transaction.
2. Prove SQLite integrity, zero active BUY reservations, and no unsafe local or
   shared redemption transition.
3. Re-read authenticated collateral and coordinator cash attribution.
4. Append one immutable transfer identity to the coordinator and both affected
   ledgers.
5. Decrease tennis cash by exactly `200 USD` without changing tennis realized
   PnL, fees, positions, fixed scale, or source cursor.
6. Initialize the new empty ledger with exactly `200 USD` initial capital.
7. Extend the coordinator with the new ledger as a `RESERVED` sleeve and prove
   that adjusted sleeve cash sums to authenticated physical collateral within
   the already defined reconciliation precision.

The transfer must be idempotent by a unique transfer ID.  A restart at any
phase resumes from a persisted transaction manifest.  A pre-commit failure
restores all frozen databases and service files from verified backups.  A
post-commit restart may not apply the cash transfer again.

The rest of the wallet is not authorized to this new profile.  Existing CD90
and tennis scales stay unchanged.  The new profile cannot infer a larger
budget from later wallet deposits, realized profits, or the physical wallet's
remaining cash.

### Execution state machine

For each source action identified by
`transaction_hash + token_id + side + order_hash` and causally ordered by
`block_number + source_log_index`:

1. Persist the immutable source receipt and official market metadata.
2. Compute target shares using the frozen scale.
3. For a BUY below the market's current official minimum, raise only that BUY
   to the current minimum and record the proportional deviation.
4. Prepare and locally hash the signed FAK order before network submission.
5. Persist the predicted order hash and submission intent, then submit once.
6. A definite zero fill becomes recoverable external-liquidity pending and is
   retried only under the existing new-head rules.
7. An uncertain submission is read-only reconciled on later processable heads
   and is never reposted.
8. A SELL is capped at this sleeve's attributable inventory.  Missing
   inventory is preserved as a physical constraint; no historical BUY is
   created to make the SELL possible.

Multi-leg source decisions are tracked as complete decision units.  A filled
first leg and temporarily unavailable later leg remains recoverable pending;
the later leg is not silently dropped and no profitability filter is applied.

### Non-Netflix notification

Each newly observed action is classified from the same official metadata
frozen for execution evidence.  A non-Netflix action appends an immutable alert
identified by the source action identity, with market/event, side, source time,
discovery time, and our current processing state.  Replays cannot create a
second alert for the same action.

The alert must be surfaced in the current Codex task without opening a new
chat.  A no-change check remains quiet.  Alert delivery is not allowed to sit
in the order-submission path: notification failure cannot block or duplicate
the authorized trade, and order failure cannot be reported as successful just
because an alert was delivered.

### Shared-wallet safety

The existing coordinator remains the single authority for:

- physical cash reservations across every live sleeve;
- token and condition ownership;
- shared-condition inventory attribution;
- single-submit redemption and per-sleeve payout distribution;
- collision handling when three or more profiles touch the same condition.

The new service must use the same submission lock path as CD90 and tennis.
No service may obtain credentials through a new secret file or copy a private
key into its runtime directory.

## Server Capacity Gate

The pre-design snapshot showed only `274 MiB` free on the server root
filesystem, an empirical value, while one fresh rollback snapshot of the two
current ledgers is larger than that.  Deployment is therefore blocked until a
retention manifest identifies obsolete repair snapshots, records their paths,
sizes, timestamps, and hashes, and preserves:

- both current live ledgers and the coordinator;
- the current release;
- the latest verified rollback snapshot needed by the deployment;
- all immutable live receipts in active ledgers.

Only superseded snapshot copies may be removed.  The release gate compares
free bytes with the measured bytes required for the new verified backup,
candidate release, new ledger, and measured temporary files.  No guessed disk
margin is used.

## Tests Required Before Deployment

Write failing tests first for:

- full-wallet scope accepting future non-Netflix actions;
- exact frozen scale and per-action official minimum BUY upsize;
- SELL inventory isolation;
- a new ledger containing no pre-watermark action;
- the `200 USD` tennis-to-new-sleeve transfer preserving aggregate cash and
  leaving both existing PnL values, positions, scales, and cursors unchanged;
- interruption and idempotent resume at every transfer-manifest phase;
- additive third-sleeve coordinator migration;
- same-token and same-condition collisions across three and four sleeves;
- single physical redemption with exact per-sleeve allocation;
- predicted order identity, uncertain-submit read-only reconciliation, and
  zero repost;
- health reporting that discovers all coordinator-registered services rather
  than assuming exactly two profiles;
- non-Netflix action classification, one alert per immutable action, quiet
  replay deduplication, and execution continuing regardless of alert state;
- closed-loop deployment rollback after service-start or verification failure.

Run the full live-copy, profile, sizing, coordinator, release-transaction, and
server-health test suites.  A candidate is not deployable while any related
test is red.

## Closed-Loop Deployment and Acceptance

The deployment transaction must:

1. create and verify a retention manifest and sufficient disk space;
2. freeze both existing services and prove frozen cursors and zero active
   reservations;
3. back up and integrity-check both live ledgers and the coordinator;
4. create the empty new ledger and apply the idempotent capital transfer;
5. extend the coordinator atomically;
6. capture the new profile's forward watermark at the current processable
   head;
7. install the candidate release and all three service definitions;
8. start exactly one instance of each service;
9. verify code hashes, SQLite integrity, cash conservation, fixed configuration,
   process count, heartbeat freshness, head/cursor alignment, action
   conservation, shared lock identity, and zero historical catch-up;
10. register the current-task, quiet-unless-triggered non-Netflix alert check;
11. roll back automatically if any execution verification fails.

Successful launch means only that the authorized forward live-copy service is
running safely.  It does not claim that the source wallet has future edge or
that every below-minimum upsize has identical risk to the source action.
