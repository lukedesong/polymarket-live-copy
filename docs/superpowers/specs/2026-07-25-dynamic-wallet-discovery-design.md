# Dynamic Polymarket Wallet Discovery Design

**Date:** 2026-07-25
**Status:** User-approved design, pending implementation
**Scope:** Public, read-only wallet research. No order placement and no private account access.

## Goal

Replace the current fixed-wallet rescan with a resumable discovery pipeline that:

- continually finds wallets beyond the legacy seed list;
- removes crypto-market activity from the research universe;
- preserves the non-crypto expert sleeve of a mixed wallet;
- separates directional experts from observable market-making, speed, and inventory-recycling behavior;
- analyzes whether a strategy holds to resolution or depends on intermediate buys and sells;
- publishes the strategy evidence and Polymarket profile URL for every promoted candidate.

The legacy wallets remain ordinary seeds for continuity. They do not define the universe, receive priority, or satisfy discovery coverage by themselves.

## Source Contract

The discovery source is Polymarket's public Data API:

- `GET /v1/leaderboard` supplies `proxyWallet`, username, rank, volume, and PnL.
- Discovery uses the official categories, excluding the `CRYPTO` category as a direct source. `OVERALL` remains included so a mixed wallet can contribute a valid non-crypto sleeve.
- `MONTH` and `ALL` are used together because the approved research question needs both recent and long-cycle evidence.
- `orderBy=PNL` is the primary discovery order. A volume-only ranking would systematically over-represent the speed and market-making behavior the user asked to exclude.
- Leaderboard requests use the documented maximum page size and paginate until an empty/short page or the documented offset boundary. Any boundary hit is recorded as truncated rather than described as exhaustive.
- `GET /trades` must set `takerOnly=false`. User-scoped full-history reads use the documented `start`/`end` windows when the per-window offset boundary is reached.
- Closed and current positions are read from the existing public position endpoints.

Official references:

- <https://docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings>
- <https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets>
- <https://docs.polymarket.com/api-reference/core/get-closed-positions-for-a-user>

These are extensions of the already registered Polymarket public Data API asset. No new credential, order-capable API, or ARMORY asset is introduced.

## Persistent Candidate Pool

Wallet identity is the lower-cased `proxyWallet`, never the display name.

Each candidate record stores:

- wallet address and latest observed username;
- discovery origins: category, period, rank, and observation time;
- first-seen and last-seen timestamps;
- legacy-seed status as provenance only;
- latest source trade timestamp;
- light-screen and deep-screen status;
- last successful scan time, last failure, and truncation flags;
- promotion, deferral, or exclusion reasons.

The pool is written atomically. A failed or interrupted run must leave the last valid state readable.

## Pipeline

### Discovery

Read every configured non-crypto leaderboard/period pair, normalize addresses, and merge duplicate appearances into one candidate. Merge the legacy seeds afterward with `legacy_seed` provenance.

The discovery report must state:

- which category/period pages succeeded;
- which failed or reached an API boundary;
- how many rows were returned and how many unique wallets were produced;
- whether the result is complete for the requested API coverage.

### Incremental queue

Queue order is deterministic:

1. wallets never analyzed;
2. wallets whose latest source trade is newer than the last successful analysis;
3. oldest successfully analyzed wallets.

There is no hidden fixed wallet count. A run continues through the queue and checkpoints after each wallet. If interrupted, the next run resumes from persisted state instead of starting again from the legacy seeds.

### Light screen

The light screen reads public recent activity and partitions it by domain and market lifecycle.

- Crypto rows are removed from analysis rather than used to reject the entire wallet.
- A wallet with no observable non-crypto sleeve is excluded with that explicit reason.
- Observable two-sided inventory recycling, repeated rapid round trips, or systematic same-market buy/sell behavior is labelled as market-making/speed risk.
- Ambiguous behavior is not silently rejected. It is labelled `FORMULA_RESEARCH` and preserved for deeper inspection.
- Fill rows are not treated as independent decisions; grouped event, condition, outcome, direction, and time structure are retained.

No fixed domain-share, win-rate, fill-count, or frequency threshold is introduced. Those quantities remain continuous evidence, because the user explicitly rejected a universal concentration cutoff.

### Deep strategy analysis

For each non-crypto expert sleeve, the deep screen reconstructs:

- event and condition groupings;
- one-sided versus two-sided outcome activity;
- BUY-only, SELL-only, and mixed lifecycle;
- hold-to-resolution, active exit, basket/hedge, and unresolved classifications;
- historical coverage and any API truncation;
- event-level and time-segment PnL;
- winning/losing event counts without treating them alone as qualification;
- largest-event and concentrated-event contribution;
- early/later performance and detectable regime changes;
- trade cadence and whether execution depends on speed or intermediate prices;
- delayed-copy risks that source-wallet PnL cannot prove.

“Expert” is evaluated by the coherence and performance of a domain sleeve, not by a fixed share of the whole wallet. Personal or unrelated bets stay outside the sleeve.

### Promotion states

The scanner may emit:

- `DIRECTIONAL_RESEARCH_CANDIDATE`: coherent non-crypto direction with lifecycle evidence;
- `FORMULA_RESEARCH`: potentially systematic but price-path or formula dependence remains;
- `OBSERVABLE_MM_OR_SPEED`: behavior appears dependent on two-sided inventory, rapid cycling, or execution speed;
- `INSUFFICIENT_HISTORY`: public coverage cannot support the claim;
- `NO_NONCRYPTO_SLEEVE`: no relevant activity after crypto removal;
- `BLOCK_DATA`: a source failure, unresolved pagination boundary, or contradictory field blocks judgment.

No wallet becomes `COPYABLE_EVIDENCE` from source history alone. That requires separate forward paper evidence using our delayed executable Ask/Bid, fees, depth, partial fills, and settlement.

## Outputs

Each run writes:

- a machine-readable discovery snapshot;
- the persistent candidate pool/checkpoint;
- a human-readable shortlist showing wallet, Polymarket profile URL, expert sleeve, lifecycle, strategy classification, evidence, exclusions, and unresolved questions;
- a coverage section that distinguishes fetched rows, analyzed wallets, deferred wallets, failed wallets, and truncated histories.

The existing report fields remain available where possible, but fixed-list language such as `wallet_count` as universe coverage and `not_an_exhaustive_universe_scan` tied to the legacy list is removed or replaced with dynamic coverage metadata.

## Failure and Safety Rules

- All calls are public and read-only.
- API failures use bounded retries and are then recorded; retry count and backoff are operational estimates, never research qualification thresholds.
- Partial leaderboard coverage does not overwrite a larger valid candidate pool.
- Partial wallet history cannot authorize promotion.
- Unknown domain classification is preserved as unknown and routed for review.
- No live-order module, credential, private key, paper-trading mutation, or automation schedule is added by this change.

## Numeric Provenance

- Leaderboard page-size and offset boundaries are **external constraint values** from the official API documentation.
- Trade page-size/window behavior is an **external constraint value** from the official API documentation.
- `MONTH` plus `ALL`, crypto removal, legacy-seed demotion, and preservation of non-crypto sleeves are **user-approved research rules**.
- Any observed ranks, activity counts, PnL, timing, or concentration values are **empirical values** and must carry source coverage.
- No estimated threshold may promote or reject a wallet. Any later runtime cap must be visibly marked as an operational estimate and may only defer work, never change a candidate's research result.

## Test and Acceptance Contract

Implementation starts with failing tests proving:

- a new leaderboard wallet enters the pool without appearing in the legacy seed map;
- repeated addresses across category/period pages deduplicate while retaining every origin;
- the `CRYPTO` source category is not queried;
- crypto rows are removed while a mixed wallet's non-crypto sleeve remains;
- queue ordering selects unseen and changed wallets before unchanged legacy seeds;
- interruption-safe state resumes without restarting at the seed list;
- `takerOnly=false` is present in wallet trade reads;
- a per-window trade offset boundary triggers time-window continuation or `BLOCK_DATA`;
- source failures and truncation cannot be reported as complete coverage;
- no hardcoded eight-wallet universe remains in the main scan path.

Acceptance requires:

- the focused test suite to pass;
- a live public, read-only discovery run to produce at least one snapshot;
- inspection of the snapshot proving dynamic addresses, provenance, exclusions, and coverage fields;
- verification that no order-capable code path or paper ledger was changed.
