# Polymarket Tennis Positive-EV Research Design

## Objective and decision boundary

Build a reproducible historical research framework that tests whether Polymarket tennis markets contain repeatable positive expected value. The framework does not assume an edge exists and does not predict match winners as its objective.

The first version reuses the repository's existing tennis match paths, public price history, market metadata, and official settlement labels. Historical order-book depth, executable Bid/Ask snapshots, point-by-point scores, and serving-player indicators are used only when contemporaneous records exist. Missing fields remain missing and are reported as evidence gaps; they are never reconstructed from future prices or current books.

The framework may conclude only:

- `VERIFIED_POSITIVE_EV`: a frozen rule remains statistically positive on the untouched chronological test set after every verifiable cost, with adequate execution evidence.
- `NO_SIGNIFICANT_EDGE`: no frozen candidate has statistically significant positive net performance on the test set.
- `BLOCK_EXECUTION_DATA`: a statistical signal survives proxy-price testing, but historical spread, depth, slippage, fill, or fee evidence is insufficient to establish executable positive EV.
- `BLOCK_DATA`: coverage or required research fields are insufficient for the requested test.

No result authorizes real-money orders.

## Numeric provenance

The requested `70% / 30%` split, favorite-price bands `0.55-0.60`, `0.60-0.70`, `0.70-0.80`, `0.80-0.90`, and `0.90+`, hedge allocations `90/10`, `80/20`, `70/30`, and `60/40`, fixed bankroll fractions `1%`, `2%`, `5%`, and `10%`, initial bankroll `$10,000`, Monte Carlo replication count `10,000`, and requested display limit `Top 50` are **user-specified values**. They are scenario definitions, not evidence that the values are optimal.

All other numerical thresholds used for candidate generation must be either:

- observed values or training-set quantiles, labeled **empirical values**;
- exact calculations from disclosed inputs, labeled **formula-derived values**; or
- current market/API rules supported by captured metadata, labeled **external constraint values**.

Unsupported constants may appear only as explicitly labeled estimates in sensitivity analysis and may not support `VERIFIED_POSITIVE_EV`.

## Scope and non-scope

The first complete version includes:

- canonical data schemas and coverage manifests;
- adapters for existing historical artifacts and future Polymarket API snapshots;
- pre-match favorite and random baselines;
- training-only condition discovery across every available feature family;
- chronological holdout validation and multiple-testing controls;
- hedge and bankroll simulations;
- tables, audit ledgers, charts, and a final research report;
- automated tests for causality, leakage prevention, accounting, and reproducibility.

It does not include:

- real-money order submission;
- fabricated historical books or scores;
- a new prediction model based on external player statistics;
- continuous production scheduling;
- claims about maker fills, queue position, or rebates without account-specific evidence.

## Research unit and data model

The closed research universe is decomposed into five tables. Together they cover static market identity, outcomes, time-varying prices, optional match state, and optional execution state.

### `matches`

One row per tennis match market:

- market and event identifiers;
- scheduled start, observed first market timestamp, observed finish, and settlement timestamps;
- competition name and normalized ATP/WTA/ITF level when recoverable;
- player names and outcome-token mapping;
- official winning outcome and resolution source;
- source hashes and extraction timestamp.

### `market_snapshots`

One row per market outcome and observation timestamp:

- public reference price;
- best Bid, best Ask, spread, and visible depth when contemporaneously captured;
- cumulative volume and reported liquidity when available;
- observation source and fidelity classification.

Public `prices-history` values are labeled historical reference prices. They are not labeled executable Bid or Ask.

### `trades`

One row per deduplicated public trade or fill fragment:

- token, side, price, size, timestamp, transaction identifier, and source;
- role remains unknown unless independent maker/taker evidence exists;
- logical-order identity remains unknown when no order identifier is available.

### `match_state`

Optional point-in-time records for sets, games, score, server, and state-source timestamp. Strategies that require unavailable state fields are reported as `BLOCK_DATA`; price timestamps are not used to infer the missing score.

### `research_events`

One immutable row per match, selected outcome, and decision timestamp:

- features available strictly at the decision time;
- entry-price specification and fidelity;
- frozen strategy identifier;
- selected/not-selected status and reason;
- gross and net settlement PnL;
- capital exposure interval and coverage flags.

Every generated dataset includes a coverage manifest: endpoints or files, earliest and latest timestamps, raw rows, unique markets, settled markets, usable events, exclusion counts by reason, hashes, pagination status, and known truncation.

## Data ingestion and validation

The historical adapter reads the existing tennis path artifact and its validation sidecars. It preserves the raw artifacts and writes normalized derived tables separately. Source hashes make every report reproducible.

The forward adapter uses the same schema for future Gamma, CLOB, public trade, price-history, and optional sports-state snapshots. It is an interface boundary for later real-time collection, not a production daemon in this version.

Validation rejects or quarantines:

- ambiguous outcome-token mappings;
- missing or conflicting official settlements;
- timestamps after settlement used as features;
- prices outside the binary-contract domain;
- duplicate records not resolvable by stable keys;
- markets lacking enough point-in-time evidence for a requested strategy.

No current order book may backfill a historical timestamp.

## Execution and PnL specifications

Each result is calculated under separately labeled price specifications:

1. Historical reference-price gross result: useful for market-efficiency research, not executable PnL.
2. Historical reference price plus verifiable fee schedule: a cost sensitivity result, still not proof of spread, depth, or fill.
3. Contemporaneous executable Bid/Ask and depth result: calculated only for rows that actually contain that evidence.

For a binary share purchased at all-in unit cost `c`, settlement PnL per share is `1 - c` on a win and `-c` on a loss. These are **formula-derived values**. Fee formulas and minimum-order rules are read from captured market metadata and labeled **external constraint values**.

Missing spread, slippage, depth, or fill evidence prevents proxy-price results from becoming `VERIFIED_POSITIVE_EV`. Results must disclose filled/usable observations over all eligible observations rather than silently dropping unavailable rows.

## Baselines

### Favorite baseline

At the frozen pre-match decision timestamp, choose the higher-priced outcome. Report the user-specified price bands and an unbinned calibration curve. Each band reports:

- eligible and traded matches;
- wins and losses;
- observed win rate and market-implied probability;
- gross and cost-adjusted ROI;
- cumulative PnL, maximum drawdown, volatility, Sharpe definition, and return distribution;
- coverage period and price-fidelity class.

### Random baseline

Random selection retains the strategy's eligible matches, decision timestamps, number of selections, and sizing. Repeated label permutations or outcome selections generate a null distribution for total PnL, ROI, drawdown, and Sharpe. The random generator seed and inputs are stored so results are reproducible.

## Feature universe and anti-anchoring coverage

The feature universe is closed by source class rather than by the examples in the request:

- market level: current price, opening price, complementary price, overround proxy, volume, liquidity, spread, depth, and market age;
- path behavior: absolute and relative returns over available lookbacks, acceleration, realized volatility, drawdown, rebound, jump size, direction, and stale-price duration;
- match timing: pre-start, elapsed market time, and verified set/game/server state when available;
- competition and participants: ATP/WTA/ITF level, competition, and player identifiers when coverage supports out-of-sample use;
- execution feasibility: Bid/Ask, spread, visible depth, fee, minimum size, and fill fidelity;
- interaction terms: only bounded, predeclared combinations of the preceding families selected on training data.

The list is closed for the first version because every usable field belongs to identity, time, market state, match state, or execution state. New external player-fundamental data is outside this version and would require a separately approved point-in-time data contract.

Continuous cut points are derived solely from training observations or enumerated from distinct training values. Missing features do not produce a favorable condition.

## Discovery, ranking, and statistical tests

The chronological split uses the first `70%` of settled matches for training and the final `30%` for testing, ordered by observed finish time. This is a **user-specified split**. Simultaneous or overlapping matches are assigned without allowing later-settled outcomes into earlier decisions.

Training performs condition generation, threshold selection, interaction selection, edge estimation, Kelly estimation, and ranking. The frozen strategy manifest records every selected rule before the test set is evaluated.

Candidate conditions are ranked on training data by net expected value with uncertainty and coverage. The displayed `Top 50` is a **user-specified output limit**, not fifty independent discoveries. The report includes all tested-condition counts and applies a false-discovery-rate correction plus a family-wise robustness sensitivity. Bootstrap confidence intervals resample at the match/event level, not the fill-row level. A permutation test compares each frozen strategy with the matched random baseline.

The untouched test set is evaluated once. Test results may invalidate candidates but may not alter thresholds, feature definitions, strategy ordering, Kelly inputs, or cost assumptions. Any post-test idea is versioned as a new research hypothesis requiring a new future holdout.

An edge claim must report the number of independent matches, coverage period, net EV interval, corrected significance, maximum-event contribution, result without the largest contributor, and execution-evidence coverage.

## Hedge strategies

For each qualified high-probability entry, the framework compares the unhedged position with the user-specified high/low allocations `90/10`, `80/20`, `70/30`, and `60/40`.

Before simulation, an accounting identity checks the combined acquisition cost of complementary outcomes. Because the two outcomes have a fixed combined settlement payoff, a static two-sided purchase can reduce variance while locking in transaction costs or overpayment. The framework therefore does not describe lower volatility as improved risk-adjusted performance unless net return metrics also improve under the same capital denominator.

Reported metrics are total return, return on deployed capital, volatility, maximum drawdown, loss probability, ruin probability under the defined bankroll rule, and paired differences from the unhedged baseline.

## Bankroll and Kelly analysis

The fixed-fraction scenarios use the user-specified bankroll fractions `1%`, `2%`, `5%`, and `10%`. The initial bankroll is the user-specified `$10,000`. Each bet is sized from capital available immediately before the bet; overlapping matches reserve capital until settlement.

Kelly inputs are estimated from training data only. Full Kelly is reported as a mathematical reference, accompanied by shrinkage and estimation-error sensitivity. If the training edge interval does not have a positive lower bound, the executable Kelly allocation is zero. No Kelly result overrides liquidity or maximum-loss constraints.

The requested `10,000` Monte Carlo paths are a **user-specified simulation count**. Simulations use frozen test-eligible outcome distributions and preserve dependence through date/event blocks where data supports it. They report terminal-capital distribution, drawdown distribution, probability of crossing the explicitly defined ruin boundary, and path coverage. The report must state the ruin boundary rather than equating any loss with bankruptcy.

Annualized return is reported only when timestamp coverage supports elapsed-year calculation and capital occupancy. Otherwise it is `BLOCK_DATA` rather than extrapolated from an arbitrary match frequency.

## Metrics

Metrics use disclosed denominators:

- win rate: winning settled positions / settled positions;
- ROI: net PnL / deployed acquisition cost;
- maximum drawdown: maximum peak-to-trough decline on the chronological equity curve, reported in dollars and as a fraction of the prior peak;
- volatility: standard deviation of match- or capital-period returns with the unit stated;
- Sharpe: mean excess return divided by return standard deviation, with period and annualization disclosed; no annualization when frequency is not defensible;
- loss or ruin probability: qualifying simulation paths / all valid simulation paths.

Skipped, unavailable, and unresolved matches remain in the intention-to-test ledger and coverage denominators.

## Components and data flow

The minimum implementation consists of one focused Python package under `sim/tennis_ev/`, one command entry point, tests beside the existing simulation tests, and generated artifacts under `outputs/tennis_ev/`.

Data flow:

1. Read immutable historical sources and compute hashes.
2. Normalize and validate canonical tables.
3. Build causal research events using only information available at each timestamp.
4. Freeze the chronological split and training-selected strategy manifest.
5. Evaluate baselines and frozen candidates on the untouched test set.
6. Run hedge, sizing, and Monte Carlo analyses from frozen inputs.
7. Generate machine-readable ledgers, tables, charts, coverage reports, and the final verdict.

Existing tennis fee and path helpers are reused where their semantics match this contract. Live-copy code and authenticated order submission are not dependencies.

## Error handling and reproducibility

Data errors are accumulated into a structured exclusion ledger instead of disappearing in logs. Fatal integrity errors stop the run before strategy evaluation. Recoverable row-level gaps remain visible in denominators.

Every run records:

- source paths and hashes;
- configuration and numeric provenance;
- code version when available;
- random seeds;
- train/test cutoff and frozen rule manifest;
- coverage and exclusions;
- artifact hashes.

Running the same code with the same inputs and seed must reproduce the same tables and charts.

## Tests and verification

Implementation follows test-first development. Required tests cover:

- schema validation and source hashing;
- strict point-in-time feature construction;
- chronological split boundaries and overlapping events;
- proof that test labels cannot affect training rule selection;
- favorite and random baseline accounting;
- fee and complementary-outcome hedge identities;
- drawdown, ROI, volatility, Sharpe, Kelly, capital reservation, and ruin calculations;
- multiple-testing correction and match-level resampling;
- missing execution or match-state data producing the correct block verdict;
- deterministic output with a fixed seed;
- end-to-end generation from a small synthetic fixture;
- reconciliation of report totals to the immutable event ledger.

Historical execution is verified separately from unit tests. Completion requires a fresh full test run, a successful artifact build, source/coverage inspection, and independent recomputation of key report totals from the event ledger.

## Deliverables

- Modular Python source code and command-line entry point.
- Canonical schema documentation and normalized-data manifest.
- Frozen strategy manifest and complete tested-condition ledger.
- Baseline, condition-search, hedge, sizing, and Monte Carlo result tables.
- Equity curves, drawdown curves, ROI distributions, and strategy-comparison charts.
- A final Markdown report answering whether a statistically verified and executable long-run positive EV was found, which feature family explains it, whether it exceeds verifiable costs, and which evidence gaps limit the conclusion.

If no candidate passes the frozen holdout and statistical requirements, the final report must state: `没有发现统计显著优势`.
