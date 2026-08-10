# Repair-window delayed source-action recovery

## Status and objective

This design implements the user-approved exception to the normal forward-only
copy rule:

- Delayed recovery is allowed only for source actions missed while an
  internally controlled repair stop was active.
- The objective is to restore the source wallet's still-current scaled
  exposure as faithfully as the venue permits.
- Recovery is not a per-trade-profit filter.  BUY, SELL, losses, exits, and
  basket legs remain part of the source action sequence.
- No historical gap, ordinary network outage, external API outage, or generic
  restart gap becomes recoverable under this design.

The initial production scope is the immutable CD90 runtime-gap receipt with
database id `2903`.  The id, block interval, action count, and action ids below
are **observed values** from
`/srv/polymarket-live/runtime/cd90_live/live.sqlite3` on `polymarket-hk`:

- previous processed block: `91761070`
- resume head: `91761298`
- unavailable interval: blocks `91761071` through `91761298`
- skipped block count: `228`
- source action count: `3`
- receipt reason: `PRE_REPAIR_INTERNAL_UNPRICED_GAP_NO_ACTION_TIME_CLOB`
- pricing status: `PRE_REPAIR_INTERNAL_UNPRICED_NO_ACTION_TIME_CLOB`

The three immutable action ids are:

1. `768dc414da874507553a74fc7be25ee077b6c402bc8ddadd20a2564067e46d03`
2. `9b6f67ebdc599a6b6423fed6e33fcc6653542bbfccf22ffe9a7d89113b9dd1ba`
3. `bc606045e7270a9091737a39351fa82dcd4bfba61ec6adc0532b028a1cc4afd8`

No other existing receipt is authorized by this document.

## Price rule

Recovery uses a formula-derived zero-additional-loss boundary.  It does not
embed an estimated percentage.

For a source action with source quantity `Q_s` and source notional `N_s`, the
source unit price is:

```text
P_s = N_s / Q_s
```

For a delayed BUY with proposed executable quantity `Q_f`, worst executable
notional `N_f`, and the official conservative fee bound `F_f`, recovery is
price-eligible only when:

```text
(N_f + F_f) / Q_f <= P_s
```

For a delayed SELL with proposed executable quantity `Q_f`, worst executable
gross proceeds `N_f`, and the official conservative fee bound `F_f`, recovery
is price-eligible only when:

```text
(N_f - F_f) / Q_f >= P_s
```

The source price inputs are **formula-derived values** from immutable source
quantity and notional.  The follower notional, depth, minimum order size, tick,
and fee-curve inputs are **observed/external-constraint values** frozen from the
current official CLOB snapshot immediately before each submission attempt.

The price rule is a safety floor, not a claim that zero additional loss is the
optimal long-run CD90 cap.  A positive adverse-drift allowance remains blocked
until a separate receipt freezes complete source-history inputs, the current
regime, forward execution evidence, cutoff, raw-file hashes, formula, and
sensitivity results.  Previously discussed percentages without those inputs
must remain monitor-only and cannot authorize an order.

## Eligibility boundary

A source action may enter delayed recovery only if all conditions below hold:

1. Its action id is listed in a canonical JSON operator recovery manifest whose
   SHA-256 is locked into the profile database before activation.
2. The manifest names one existing `runtime_gap_receipts` row.
3. That row has the exact internal-repair reason and pricing status in this
   design.
4. The action id is contained in the row's immutable `details_json`.
5. The action's block lies strictly after `previous_processed_block` and no
   later than `resume_head`.
6. The action still has zero submission attempts and zero active reservation.
7. Its latest pre-recovery state is the exact internal repair-gap terminal
   state, not an UNKNOWN or an ordinary historical miss.
8. Its current economic effect has not been cancelled by a later canonical
   source action.

Failure of any condition is an internal invariant error and produces no order.
The manifest is single-use and profile-specific.  A future repair window needs
its own manifest and explicit operator activation; the daemon never scans all
historical gaps for candidates.

## Current-effect reconstruction

Recovery processes manifest actions in canonical source order:

```text
(block_number, source_log_index, source_timestamp,
 transaction_hash, token_id, side, order_hash, action_id)
```

Before reading a book or reserving funds, it reconstructs the current scaled
open target for the token through the current processed head.  This prevents a
stale BUY from being placed after a later source SELL has already cancelled it.

BUY recovery uses the deployed cumulative same-token sizing contract:

- the fixed share scale effective at the source block is immutable;
- confirmed minimum-upscale surplus offsets later source BUY fragments;
- the market minimum may be applied only after that cumulative credit;
- the same missed fragment cannot create repeated minimum-sized submissions.

SELL recovery is bounded by this sleeve's currently available token inventory.
It cannot use another profile's inventory and cannot sell short.

## Persistence and state machine

The existing source action receipt and original `ERROR_INTERNAL` transition are
never edited or deleted.  Add a dedicated recovery receipt keyed by
`(profile_key, gap_receipt_id, action_id)` with:

- manifest hash and policy hash;
- source price inputs and source-order identity;
- recovery state and immutable state transitions;
- current-effect reconstruction hash;
- each frozen CLOB snapshot hash;
- requested quantity, filled quantity, fee, order id, and reconciliation
  evidence;
- last evaluated processable block.

Recovery states are:

```text
AUTHORIZED
CURRENT_EFFECT_RECONSTRUCTED
NO_ORDER_COVERED_BY_CUMULATIVE_SURPLUS
PENDING_PRICE
PENDING_EXTERNAL_LIQUIDITY
SUBMIT_STARTED
SUBMITTED_UNRECONCILED
UNKNOWN_SUBMISSION
PARTIAL_PENDING
FILLED
SUPERSEDED_BY_LATER_SOURCE_ACTION
EXTERNAL_UNFILLABLE
ERROR_INTERNAL
```

`UNKNOWN_SUBMISSION` is read-only forever: the predicted order hash is queried
against official order/on-chain evidence on a new processable block, but the
signed order is never posted again.

`PENDING_PRICE`, `PENDING_EXTERNAL_LIQUIDITY`, and `PARTIAL_PENDING` may be
re-evaluated only on a new processable block and only while the market still
accepts orders.  Every re-evaluation freezes a new current snapshot; it never
pretends that the snapshot existed at source-action time.

## Execution integration

The normal forward source-action path remains unchanged.  A separate
repair-recovery worker runs once per new processable block while an active
manifest exists.  A pending recovery must not prevent the same cycle from
processing newly observed forward source actions.  The worker holds the
existing shared-wallet lock only for its bounded plan/reserve/submit or
reconcile transaction and uses coordinator cash and inventory authority before
any reservation.

The worker reuses the deployed signed-order safety path:

- prepare and hash the order before the network POST;
- persist only the safe prepared-order whitelist;
- require the response order id to match the predicted hash;
- reconcile uncertain outcomes without reposting;
- keep SELL inventory sleeve-local;
- route shared cash and redemption attribution through the coordinator.

Recovery may use FAK and may therefore partially fill.  A partial fill is not
reported as complete; the remaining current target stays pending and must pass
the same price rule on every later attempt.

## Expected treatment of the current manifest

These are design-time expectations, not orders and not frozen future prices:

- The two Paris BUY actions have immutable source unit price `0.64`, a
  **formula-derived value** from each action's notional divided by quantity.
  Applying the fixed scale to the earlier normal BUY plus both repair-window
  BUYs gives a cumulative target of
  `5.768049838642480527076141534` shares.  This is a
  **formula-derived value**.  The live sleeve held `5` shares at design time,
  an **observed value**, so only the cumulative deficit remains relevant.  A
  recovery BUY still must satisfy the official current minimum and the
  fee-adjusted price rule.
- The Milan SELL has immutable source unit price `0.26`, a
  **formula-derived value**.  The live sleeve held `38.560644` shares at design
  time, an **observed value**.  Recovery may sell no more than the refreshed
  sleeve inventory and may only do so when net proceeds per share are at least
  `0.26`.

Because books and balances move, deployment must refresh all current inputs.
No design-time quote is an execution authorization.

## Tests

Implementation begins with failing tests that prove:

1. An ordinary restart/network gap and a historical action cannot enter
   recovery.
2. Only exact action ids in the exact repair manifest are eligible.
3. A manifest/action mismatch, nonzero prior submission, active reservation,
   wrong profile, changed receipt, or changed policy hash fails closed.
4. Canonical order and later-source-action supersession prevent stale recovery.
5. BUY cumulative minimum surplus is consumed before a new minimum-sized order.
6. BUY at a fee-adjusted worse price stays `PENDING_PRICE` with zero submission.
7. SELL at a fee-adjusted worse price stays `PENDING_PRICE` with zero
   submission.
8. Equal-or-better BUY/SELL prices may submit through the existing presigned
   path.
9. Partial FAK fill persists the remainder and rechecks price on a later block.
10. UNKNOWN reconciliation performs zero reposts, including after restart.
11. SELL cannot exceed sleeve inventory and cannot consume another sleeve's
    position.
12. The original gap transition remains immutable and action/recovery
    conservation is separately reportable.
13. Two services sharing the wallet cannot both claim the same recovery.
14. Re-running an already terminal manifest is idempotent and submits nothing.

The complete related suite, release-manifest verification, database migration
tests, and rollback tests must pass before deployment.

## Deployment and verification

Use the existing closed-loop release process.  Before stopping services,
freeze authenticated open-order, reservation, cursor, cash, inventory, and
three-ledger integrity evidence.  Stop only the affected shared-core services,
back up databases, migrate schema, switch the tested release, and restore all
services.  Roll back automatically on migration, start, or verification
failure.

After deployment, verify:

- tested and deployed core hashes match;
- each service has exactly one running instance and no crash loop;
- three sleeve databases plus coordinator pass `integrity_check`;
- head/cursor catch up and normal forward actions remain conserved;
- the recovery manifest contains exactly its immutable actions;
- each recovery action has exactly one current state;
- active reservations, predicted order hashes, submissions, fills, cash, and
  positions reconcile;
- no action outside the repair manifest was replayed;
- any actual order/fill is reported using its fresh official evidence.

Only fresh post-deployment evidence may support the words "deployed",
"submitted", "filled", or "fixed".
