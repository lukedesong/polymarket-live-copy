# Copyability-First Wallet Screening Design

**Date:** 2026-07-25
**Status:** Approved by user delegation: Codex must apply independent judgment
**Scope:** Public, read-only Polymarket wallet discovery and research classification

## Decision

Wallet strategy shape and copyability are separate questions.

A source wallet is not rejected merely because it:

- buys and later sells;
- takes profit, stops loss, or reverses an opinion;
- trades multiple outcomes;
- hedges;
- appears to make markets;
- sends several transactions close together.

Those behaviors describe how the source trades. They do not prove that a
delayed follower cannot reproduce the strategy or that the delayed strategy
has negative expectation.

The only economically valid exclusion is evidence that our implementation
cannot reproduce the relevant source actions at executable prices, or that the
reproduced actions lose their positive expectation after delay, spread, fees,
depth, partial fills, and settlement are included.

## Speed Observation

The user proposed repeated buys and sells within a rolling one-minute window as
the intuitive meaning of high frequency. The one-minute duration is a
**user-specified value** and is retained as an observation window, not promoted
to a universal rejection threshold.

The scanner records continuous execution-shape evidence:

- unique source transactions per rolling observation window;
- buy-to-sell, sell-to-buy, and position-reversal intervals per token;
- exact-timestamp transaction bursts;
- repeated same-token inventory recycling;
- public-page coverage and truncation.

No fixed number of transactions, round trips, or saturated pages automatically
rejects a wallet. A short interval raises `SPEED_REVIEW`; it does not establish
`UNFOLLOWABLE`.

## Strategy Lifecycle

Lifecycle labels remain descriptive:

- `HOLD_TO_RESOLUTION`;
- `OPEN_OR_UNRESOLVED`;
- `ACTIVE_EXIT`;
- `BASKET_OR_HEDGE`;
- `SELL_ONLY_INCOMPLETE_HISTORY`;
- `UNRESOLVED`.

`ACTIVE_EXIT` is especially important because it may reveal systematic profit
taking, stop loss, information updating, or position management. It must pass
from the light screen into full-history analysis.

`BASKET_OR_HEDGE` and observable inventory recycling also pass into
full-history analysis. They may be difficult to copy, but difficulty is a
research question rather than a source-history verdict.

## Separate Classification Axes

The scanner keeps two independent axes.

### Source strategy description

This describes observed behavior such as directional holding, active exits,
baskets, hedging, inventory recycling, or speed-shaped execution. It must not
control deep-scan eligibility by itself.

### Copyability state

- `NEEDS_FORWARD_PAPER`: no conclusive delayed-execution evidence yet;
- `SPEED_REVIEW`: timing evidence suggests execution risk, but no failed-copy
  evidence exists;
- `FORWARD_COPYABLE`: forward paper evidence supports action coverage and
  positive delayed expectation;
- `UNFOLLOWABLE_EXECUTION`: forward paper evidence shows that material actions
  cannot be reproduced or delayed expectation is not positive;
- `NO_NONCRYPTO_SLEEVE`: no relevant activity exists after the approved domain
  filter;
- `BLOCK_DATA`: source coverage is insufficient or contradictory.

Only `NO_NONCRYPTO_SLEEVE` and `BLOCK_DATA` prevent further research before
forward paper. Only `UNFOLLOWABLE_EXECUTION` is an execution-based rejection.
`FORWARD_COPYABLE` and `UNFOLLOWABLE_EXECUTION` cannot be awarded from source
history alone.

## Light-Screen Behavior

The light screen:

- removes crypto rows under the existing user-approved research boundary;
- reconstructs lifecycle and speed observations;
- assigns `SPEED_REVIEW` when speed risk is observed;
- otherwise assigns `NEEDS_FORWARD_PAPER`;
- allows every wallet with relevant observable activity into the deep scan.

Recent-page saturation remains coverage metadata. It no longer rewrites a
wallet into a market-maker/high-frequency rejection state.

## Deep Analysis

The deep scan preserves active exits and reconstructs chronological position
changes. It reports:

- holding and reversal intervals;
- event and token-level lifecycle;
- whether sells reduce existing inventory or create an unexplained
  incomplete-history state;
- repeated inventory recycling;
- strategy phase changes;
- historical PnL evidence with concentration and coverage caveats.

These source-history results rank research priority. They do not authorize live
copying.

## Forward Copyability Test

For a promoted wallet, the follower records every newly observed source action
and attempts the proportional paper action using the delayed executable book:

- buys use the delayed executable Ask;
- sells use the delayed executable Bid;
- visible depth, partial fills, fees, minimum order constraints, cash usage,
  and settlement remain in the ledger;
- skipped, stale, reversed-before-observation, and under-minimum actions remain
  explicit rather than being silently removed.

Copyability is judged from action coverage and delayed paper economics over
observable forward data. Any future qualification threshold must be supported
by empirical out-of-sample evidence or an external execution constraint. An
estimated threshold may prioritize research but may not authorize real money.

## Required Code Changes

- Stop using `ACTIVE_EXIT` or `BASKET_OR_HEDGE` to block full-history analysis.
- Stop using recent-page saturation to force
  `OBSERVABLE_MM_OR_SPEED` or block full-history analysis.
- Preserve same-timestamp and page-saturation fields as evidence.
- Add the rolling one-minute speed observation as a continuous metric and mark
  its duration as user-specified.
- Add a copyability state independent from the source strategy state.
- Keep existing output fields where compatibility is useful, but make
  `deep_scan_eligible` depend on relevant data availability rather than trade
  style.
- Add regression tests covering active exits, baskets, same-token opposite-side
  activity, exact-timestamp bursts, and saturated recent pages.

## Safety

This change is research-only. It does not place orders, access private wallet
data, or promote any wallet directly to live trading. Source-wallet historical
profit remains evidence about the source, not proof of follower profit.
