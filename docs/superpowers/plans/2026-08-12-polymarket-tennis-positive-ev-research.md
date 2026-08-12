# Polymarket Tennis Positive-EV Research Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible, chronological Polymarket tennis research pipeline that searches training data for repeatable statistical advantages, evaluates frozen rules once on a held-out test set, and reports either verified net positive EV, no significant edge, or a quantified data/execution block.

**Architecture:** Reuse the immutable tennis path artifact and existing fee semantics through a small `sim/tennis_ev` package. Normalize source rows into causal match records, keep discovery and holdout evaluation separate through a serialized frozen manifest, then feed the same immutable event ledger into metrics, hedge, bankroll, Monte Carlo, charts, and verdict generation. Missing historical Bid/Ask, depth, score-state, or server-state evidence remains explicit in coverage fields and can block an executable conclusion.

**Tech Stack:** Python standard library, NumPy, Matplotlib, `unittest`, gzip JSONL/CSV/JSON/Markdown artifacts.

---

## File map

- Create `sim/tennis_ev/__init__.py`: public package exports only.
- Create `sim/tennis_ev/data.py`: canonical dataclasses, historical path adapter, forward-snapshot adapter, validation, hashes, and coverage manifest.
- Create `sim/tennis_ev/research.py`: causal event construction, chronological split, baselines, training-only condition generation, frozen-rule selection, and holdout evaluation.
- Create `sim/tennis_ev/statistics.py`: PnL metrics, drawdown, bootstrap, permutation baseline, Benjamini-Hochberg correction, and contribution diagnostics.
- Create `sim/tennis_ev/bankroll.py`: complementary-outcome hedge accounting, fixed-fraction/Kelly sizing, capital reservation, and Monte Carlo paths.
- Create `sim/tennis_ev/report.py`: result tables, charts, Markdown report, verdict, and artifact hashes.
- Create `sim/run_polymarket_tennis_ev.py`: one command-line orchestration entry point.
- Create `sim/test_tennis_ev_data.py`: data, fidelity, normalization, and coverage tests.
- Create `sim/test_tennis_ev_research.py`: causality, split, baselines, discovery, leakage, and statistics tests.
- Create `sim/test_tennis_ev_bankroll.py`: hedge, sizing, capital, Kelly, ruin, and simulation tests.
- Create `sim/test_tennis_ev_report.py`: verdict, reconciliation, deterministic artifacts, and end-to-end fixture tests.
- Generate `outputs/tennis_ev/`: normalized ledgers, manifests, tables, charts, and final report. Generated artifacts are not hand-edited.
- Modify `sim/backtest_log.md`: append the completed run's immutable evidence and verdict.

The package is deliberately limited to these responsibilities. It does not depend on authenticated live-copy code and does not add a scheduler, database, web UI, or order-submission path.

All literal values used only inside synthetic tests are **estimated structural fixtures** chosen to make expected arithmetic hand-checkable; they are not research thresholds and never enter the historical run. Values that do enter the historical run retain the provenance stated in the design and in the tasks below.

### Task 1: Canonical records, historical adapter, and forward adapter

**Files:**
- Create: `sim/tennis_ev/__init__.py`
- Create: `sim/tennis_ev/data.py`
- Create: `sim/test_tennis_ev_data.py`

- [ ] **Step 1: Write failing canonical-record and historical-adapter tests**

Create fixture rows with two outcomes, pre-match prices, minute paths, an official winner, absent books, and stable source identifiers. Require valid rows to normalize and invalid prices or ambiguous winners to enter the exclusion ledger.

```python
class HistoricalAdapterTests(unittest.TestCase):
    def test_path_row_normalizes_without_inventing_execution_fields(self) -> None:
        row = path_row(event_id="e1", high_price=0.70, high_won=True)
        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertEqual(exclusions, [])
        self.assertEqual(match.event_id, "e1")
        self.assertEqual(match.level, "ATP")
        self.assertEqual(match.outcomes[0].pregame_price, 0.70)
        self.assertIsNone(match.outcomes[0].best_ask)
        self.assertIsNone(match.match_state)
        self.assertEqual(match.price_fidelity, "HISTORICAL_REFERENCE_PRICE")

    def test_invalid_price_is_quarantined_with_denominator_preserved(self) -> None:
        row = path_row(event_id="bad", high_price=1.20, high_won=True)
        match, exclusions = data.normalize_historical_row(row, source_sha256="abc")

        self.assertIsNone(match)
        self.assertEqual(exclusions[0].reason, "PRICE_OUT_OF_DOMAIN")
        self.assertEqual(exclusions[0].event_id, "bad")
```

- [ ] **Step 2: Run the focused tests and verify the expected failure**

Run: `python3 -m unittest sim/test_tennis_ev_data.py -v`

Expected: import failure because `sim.tennis_ev.data` does not exist.

- [ ] **Step 3: Implement immutable canonical records and validation**

Use frozen dataclasses and reject non-binary prices without filling optional evidence.

```python
@dataclass(frozen=True)
class OutcomeRecord:
    token_id: str
    name: str
    pregame_price: float
    opening_price: float | None
    won: bool
    path: tuple[tuple[int, float], ...]
    best_bid: float | None = None
    best_ask: float | None = None
    visible_depth_usd: float | None = None

@dataclass(frozen=True)
class MatchRecord:
    event_id: str
    market_id: str
    level: str | None
    title: str
    start_ts: int
    finish_ts: int
    pregame_ts: int
    outcomes: tuple[OutcomeRecord, OutcomeRecord]
    price_fidelity: str
    match_state: Mapping[str, object] | None
    source_sha256: str

@dataclass(frozen=True)
class TradeRecord:
    event_id: str
    token_id: str
    timestamp: int
    price: float
    size: float
    side: str | None
    transaction_id: str | None
    maker_taker_role: str = "UNKNOWN"

def validate_binary_price(value: object, field: str) -> float:
    price = float(value)
    if not math.isfinite(price) or not 0.0 < price < 1.0:
        raise DataValidationError("PRICE_OUT_OF_DOMAIN", field)
    return price
```

Normalize string-encoded JSON fields from the existing gzip artifact, preserve `series` as the recoverable competition level, retain player names from `outcomes`, and map paths to absolute timestamps using `start_ts + elapsed_seconds`. The existing artifact's `pregame_price` remains a pre-match reference price; `opening_price` stays absent because `open_ts` is not an opening-price observation. Define `TradeRecord`, but do not turn `prices-history` samples into trades. Hash the compressed source bytes with SHA-256.

- [ ] **Step 4: Add failing forward-snapshot and coverage-manifest tests**

```python
def test_forward_snapshot_preserves_observed_book_and_state(self) -> None:
    snapshot = {
        "event_id": "e2", "observed_at": 200,
        "tokens": [{"token_id": "yes", "price": 0.61,
                    "best_bid": 0.60, "best_ask": 0.62,
                    "visible_depth_usd": 125.0}],
        "match_state": {"sets": [1, 0], "server": "Player A"},
    }
    normalized = data.normalize_forward_snapshot(snapshot)
    self.assertEqual(normalized[0].price_fidelity, "CONTEMPORANEOUS_BOOK")
    self.assertEqual(normalized[0].best_ask, 0.62)
    self.assertEqual(normalized[0].match_state["server"], "Player A")

def test_manifest_counts_all_raw_usable_and_excluded_rows(self) -> None:
    manifest = data.build_coverage_manifest(raw_rows=3, matches=[valid], exclusions=[bad1, bad2])
    self.assertEqual(manifest["raw_rows"], 3)
    self.assertEqual(manifest["usable_matches"], 1)
    self.assertEqual(manifest["excluded_matches"], 2)
    self.assertEqual(sum(manifest["exclusions_by_reason"].values()), 2)
```

- [ ] **Step 5: Run the focused tests and verify the new failures**

Run: `python3 -m unittest sim/test_tennis_ev_data.py -v`

Expected: historical normalization tests pass; forward normalization and coverage functions fail because they are absent.

- [ ] **Step 6: Implement forward normalization, source loading, and manifest reconciliation**

`load_historical_matches(path)` must return `(matches, snapshots, trades, states, exclusions, manifest)`, where `raw_rows == usable_matches + excluded_matches`. Historical `trades` and point-by-point `states` are empty unless the source actually provides them. `normalize_forward_snapshot` accepts supplied payloads only; it performs no network calls and establishes the schema boundary for later API collection, including optional public trades and contemporaneous state.

```python
def build_coverage_manifest(raw_rows, matches, exclusions):
    reasons = Counter(item.reason for item in exclusions)
    if raw_rows != len(matches) + len(exclusions):
        raise ValueError("coverage denominator does not reconcile")
    return {
        "raw_rows": raw_rows,
        "usable_matches": len(matches),
        "excluded_matches": len(exclusions),
        "exclusions_by_reason": dict(sorted(reasons.items())),
        "earliest_finish_ts": min((m.finish_ts for m in matches), default=None),
        "latest_finish_ts": max((m.finish_ts for m in matches), default=None),
        "execution_book_matches": sum(
            all(o.best_bid is not None and o.best_ask is not None for o in m.outcomes)
            for m in matches
        ),
        "match_state_matches": sum(m.match_state is not None for m in matches),
    }
```

- [ ] **Step 7: Run the data tests and commit**

Run: `python3 -m unittest sim/test_tennis_ev_data.py -v`

Expected: all data tests pass with no network access.

Commit:

```bash
git add sim/tennis_ev/__init__.py sim/tennis_ev/data.py sim/test_tennis_ev_data.py
git commit -m "feat: normalize tennis EV research data"
```

### Task 2: Causal research events and chronological holdout

**Files:**
- Create: `sim/tennis_ev/research.py`
- Create: `sim/test_tennis_ev_research.py`

- [ ] **Step 1: Write failing point-in-time and split tests**

The fixture must include a pre-match observation, a later in-play observation, two matches tied at the cutoff timestamp, and one match whose result becomes known only after the cutoff.

```python
class CausalEventTests(unittest.TestCase):
    def test_feature_builder_never_reads_points_after_decision(self) -> None:
        match = match_with_path([(100, 0.60), (160, 0.65), (220, 0.20)], won=True)
        event = research.build_event(match, decision_ts=160, outcome_index=0)
        self.assertEqual(event.current_price, 0.65)
        self.assertEqual(event.path_high, 0.65)
        self.assertNotIn(0.20, event.observed_prices)

    def test_split_keeps_equal_finish_timestamps_on_one_side(self) -> None:
        matches = matches_finishing_at([100, 200, 200, 300])
        split = research.chronological_split(matches, train_fraction=0.70)
        train_ids = {m.event_id for m in split.train}
        test_ids = {m.event_id for m in split.test}
        self.assertFalse(train_ids & test_ids)
        self.assertTrue({"finish-200-a", "finish-200-b"} <= train_ids or
                        {"finish-200-a", "finish-200-b"} <= test_ids)
```

`0.70` is the user-specified training fraction. Tied finish timestamps move together; the manifest reports the achieved fraction instead of splitting a tie to manufacture the exact percentage.

- [ ] **Step 2: Run the tests and verify failure**

Run: `python3 -m unittest sim/test_tennis_ev_research.py -v`

Expected: import failure because `sim.tennis_ev.research` does not exist.

- [ ] **Step 3: Implement causal features and deterministic split**

```python
@dataclass(frozen=True)
class ResearchEvent:
    event_id: str
    decision_ts: int
    finish_ts: int
    outcome_index: int
    won: bool
    opening_price: float | None
    reference_entry_price: float
    current_price: float
    absolute_change: float
    relative_change: float
    realized_volatility: float | None
    path_high: float
    path_low: float
    elapsed_seconds: int
    level: str | None
    best_bid: float | None
    best_ask: float | None
    visible_depth_usd: float | None
    feature_availability: tuple[str, ...]

def chronological_split(matches, train_fraction):
    ordered = sorted(matches, key=lambda m: (m.finish_ts, m.event_id))
    target = len(ordered) * train_fraction
    boundary_index = min(range(1, len(ordered)), key=lambda i: abs(i - target))
    boundary_ts = ordered[boundary_index - 1].finish_ts
    while boundary_index < len(ordered) and ordered[boundary_index].finish_ts == boundary_ts:
        boundary_index += 1
    return Split(tuple(ordered[:boundary_index]), tuple(ordered[boundary_index:]), boundary_ts)
```

Feature windows use only samples whose absolute timestamp is at or before `decision_ts`. Competition, match-state, and execution features are included only if present at that timestamp.

Build the requested decision-point families explicitly: the recorded pre-match reference timestamp; scheduled-start plus the user-specified five minutes, labeled `SCHEDULED_START_PROXY`; and first-set end, second-set end, deciding-set, score lead, and server state only when a contemporaneous `match_state` record proves them. Missing state checkpoints produce quantified `BLOCK_DATA` coverage rows rather than inferred observations.

- [ ] **Step 4: Add a leakage regression test for the frozen manifest**

```python
def test_test_labels_cannot_change_frozen_training_manifest(self) -> None:
    train, test = discovery_fixture()
    original = research.freeze_training_manifest(train, alpha=0.05)
    flipped_test = [replace(row, won=not row.won) for row in test]
    repeated = research.freeze_training_manifest(train, alpha=0.05)
    self.assertEqual(original.to_json(), repeated.to_json())
    self.assertEqual(original.source_event_ids, repeated.source_event_ids)
    self.assertFalse(set(original.source_event_ids) & {r.event_id for r in flipped_test})
```

`0.05` is an explicitly labeled conventional research setting, not a proven economic threshold. The report must expose raw p-values and adjusted q-values so sensitivity can be inspected.

- [ ] **Step 5: Run tests, implement manifest serialization, and rerun**

`FrozenManifest` stores split cutoff, training source hash, feature definitions, empirical cut points, selected rule IDs, ranking order, cost specification, significance setting and its provenance, and Kelly inputs. JSON serialization uses sorted keys.

Run: `python3 -m unittest sim/test_tennis_ev_research.py -v`

Expected: all causal construction, split, and leakage tests pass.

- [ ] **Step 6: Commit**

```bash
git add sim/tennis_ev/research.py sim/test_tennis_ev_research.py
git commit -m "feat: add causal tennis research split"
```

### Task 3: Baselines and core performance metrics

**Files:**
- Create: `sim/tennis_ev/statistics.py`
- Modify: `sim/tennis_ev/research.py`
- Modify: `sim/test_tennis_ev_research.py`

- [ ] **Step 1: Write failing favorite-band, PnL, ROI, drawdown, and Sharpe tests**

Use exact hand-computable fixtures. Price-band intervals are `[0.55, 0.60)`, `[0.60, 0.70)`, `[0.70, 0.80)`, `[0.80, 0.90)`, and `[0.90, 1.00)`; these are the user-specified bands with a deterministic non-overlap convention.

```python
def test_favorite_bands_have_no_boundary_double_count(self) -> None:
    rows = favorite_rows(prices=[0.55, 0.60, 0.70, 0.80, 0.90])
    bands = research.favorite_baseline(rows, fee_rate=None)
    self.assertEqual(sum(b["eligible_matches"] for b in bands), 5)
    self.assertEqual([b["eligible_matches"] for b in bands], [1, 1, 1, 1, 1])

def test_metrics_reconcile_to_trade_ledger(self) -> None:
    returns = [0.25, -0.10, 0.05]
    costs = [0.50, 0.50, 1.00]
    metrics = statistics.performance_metrics(returns, costs)
    self.assertAlmostEqual(metrics["net_pnl"], 0.20)
    self.assertAlmostEqual(metrics["roi"], 0.20 / 2.00)
    self.assertAlmostEqual(metrics["max_drawdown_usd"], 0.10)
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `python3 -m unittest sim.test_tennis_ev_research -v`

Expected: failures for missing baseline and statistics functions.

- [ ] **Step 3: Implement formula-derived settlement accounting and metrics**

```python
def share_pnl(price, won, fee_per_share=0.0):
    cost = price + fee_per_share
    return (1.0 - cost if won else -cost), cost

def performance_metrics(pnls, deployed_costs):
    pnl = np.asarray(pnls, dtype=float)
    costs = np.asarray(deployed_costs, dtype=float)
    equity = np.concatenate(([0.0], np.cumsum(pnl)))
    peaks = np.maximum.accumulate(equity)
    drawdowns = peaks - equity
    return {
        "observations": int(pnl.size),
        "net_pnl": float(pnl.sum()),
        "deployed_cost": float(costs.sum()),
        "roi": float(pnl.sum() / costs.sum()) if costs.sum() else None,
        "max_drawdown_usd": float(drawdowns.max(initial=0.0)),
        "return_mean": float(pnl.mean()) if pnl.size else None,
        "return_volatility": float(pnl.std(ddof=1)) if pnl.size > 1 else None,
        "sharpe_per_match": (
            float(pnl.mean() / pnl.std(ddof=1)) if pnl.size > 1 and pnl.std(ddof=1) else None
        ),
    }
```

Do not annualize per-match Sharpe. Fee rate is optional: absent captured fee evidence produces gross proxy metrics plus an execution-cost coverage block rather than silently applying a constant.

- [ ] **Step 4: Write a failing matched-random baseline test**

```python
def test_random_baseline_retains_matches_times_and_selection_count(self) -> None:
    rows = favorite_rows(prices=[0.60, 0.70, 0.80, 0.90])
    result = research.random_baseline(rows, draws=10_000, seed=20260812)
    self.assertEqual(result["eligible_matches"], len(rows))
    self.assertEqual(result["selections_per_draw"], len(rows))
    self.assertEqual(result["draws"], 10_000)
    self.assertEqual(result["seed"], 20260812)
```

The draw count reuses the user's specified `10,000` simulation scenario. The seed is a reproducibility identifier derived from the research date, not evidence of optimality and has no effect on expected returns.

- [ ] **Step 5: Implement vectorized random selection and rerun tests**

For every draw, choose one of the two outcomes for every eligible match while retaining the same entry timestamp and capital convention. Store the null distributions for PnL, ROI, drawdown, and per-match Sharpe.

Run: `python3 -m unittest sim.test_tennis_ev_research -v`

Expected: all baseline and metric tests pass.

- [ ] **Step 6: Commit**

```bash
git add sim/tennis_ev/statistics.py sim/tennis_ev/research.py sim/test_tennis_ev_research.py
git commit -m "feat: add tennis EV baselines and metrics"
```

### Task 4: Training-only condition discovery and multiple-testing controls

**Files:**
- Modify: `sim/tennis_ev/research.py`
- Modify: `sim/tennis_ev/statistics.py`
- Modify: `sim/test_tennis_ev_research.py`

- [ ] **Step 1: Write failing empirical-cut, feature-coverage, and FDR tests**

```python
def test_candidate_cut_points_come_only_from_training_values(self) -> None:
    train = feature_rows(current_price=[0.20, 0.40, 0.60, 0.80])
    test = feature_rows(current_price=[0.333, 0.777])
    candidates = research.generate_candidates(train)
    serialized = json.dumps([c.to_dict() for c in candidates])
    self.assertNotIn("0.333", serialized)
    self.assertNotIn("0.777", serialized)

def test_missing_score_does_not_generate_score_condition(self) -> None:
    candidates = research.generate_candidates(feature_rows(match_state=[None, None]))
    self.assertFalse(any(c.family == "match_state" for c in candidates))

def test_benjamini_hochberg_is_monotone_and_matches_fixture(self) -> None:
    q = statistics.benjamini_hochberg([0.01, 0.04, 0.03, 0.20])
    np.testing.assert_allclose(q, [0.04, 0.0533333333, 0.0533333333, 0.20])

def test_bonferroni_familywise_sensitivity_caps_at_one(self) -> None:
    adjusted = statistics.bonferroni([0.01, 0.40], tested_conditions=4)
    np.testing.assert_allclose(adjusted, [0.04, 1.00])
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `python3 -m unittest sim.test_tennis_ev_research -v`

Expected: failures for candidate generation and Benjamini-Hochberg correction.

- [ ] **Step 3: Implement the bounded feature-family search**

Generate one-dimensional threshold and category rules for every available family: market level, path behavior, timing, competition, match state, and execution feasibility. Empirical cut points are distinct training values compressed to training quantiles only when the distinct set would duplicate identical partitions. Add only pairwise interactions whose component rules individually have usable training coverage; do not create higher-order interactions.

```python
@dataclass(frozen=True)
class Condition:
    rule_id: str
    family: str
    clauses: tuple[Clause, ...]
    numeric_provenance: str

def evaluate_condition(condition, row):
    for clause in condition.clauses:
        value = getattr(row, clause.feature)
        if value is None or not clause.compare(value):
            return False
    return True
```

Record all evaluated conditions, including those rejected for zero matches, inadequate fields, non-positive training EV, or invalid arithmetic.

- [ ] **Step 4: Add failing bootstrap, permutation, and concentration tests**

```python
def test_resampling_uses_match_ids_not_outcome_rows(self) -> None:
    rows = paired_outcome_rows_for_two_matches()
    sampled = statistics.bootstrap_match_blocks(rows, draws=10_000, seed=20260812)
    self.assertTrue(all(draw.count("match-a") in {0, 2, 4} for draw in sampled.match_ids))

def test_contribution_diagnostic_removes_largest_absolute_event(self) -> None:
    diag = statistics.contribution_diagnostics([0.10, 0.20, 5.00, -0.10])
    self.assertEqual(diag["largest_contribution"], 5.00)
    self.assertAlmostEqual(diag["pnl_without_largest"], 0.20)
```

- [ ] **Step 5: Implement training statistics and frozen ranking**

For each condition calculate match count, wins/losses, net EV, bootstrap interval, matched-random permutation p-value, Benjamini-Hochberg adjusted q-value, Bonferroni family-wise sensitivity, largest-event contribution, and result without that event. Rank the user-requested `Top 50` training candidates by positive net EV lower bound, then q-value, then rule ID. `Top 50` limits display only; the tested-condition denominator includes the whole generated set.

Use the user-specified `10,000` draws for bootstrap and permutation runs and label that provenance in the manifest. Use `alpha=0.05` only as an explicitly labeled conventional research setting and include q-value sensitivity in output.

- [ ] **Step 6: Add a failing frozen-holdout test, implement evaluation, and rerun**

```python
def test_holdout_evaluates_every_frozen_rule_without_reselection(self) -> None:
    manifest = frozen_manifest(rule_ids=["price_ge_0.60", "volatility_ge_train_q75"])
    result = research.evaluate_holdout(manifest, test_rows())
    self.assertEqual([r.rule_id for r in result], list(manifest.rule_ids))
    self.assertEqual(result[0].selection_rank, 1)
    self.assertEqual(result[1].selection_rank, 2)
```

Run: `python3 -m unittest sim.test_tennis_ev_research -v`

Expected: all discovery, statistical-control, and frozen-holdout tests pass.

- [ ] **Step 7: Commit**

```bash
git add sim/tennis_ev/research.py sim/tennis_ev/statistics.py sim/test_tennis_ev_research.py
git commit -m "feat: discover and freeze tennis EV conditions"
```

### Task 5: Hedge accounting, bankroll sizing, Kelly, and Monte Carlo

**Files:**
- Create: `sim/tennis_ev/bankroll.py`
- Create: `sim/test_tennis_ev_bankroll.py`

- [ ] **Step 1: Write failing complementary-outcome hedge tests**

Treat each allocation as a fraction of one unit of deployed cash, purchasing shares on both sides at their respective all-in costs.

```python
def test_static_hedge_payout_uses_complementary_share_counts(self) -> None:
    high_wins = bankroll.hedged_pnl(
        high_cost=0.70, low_cost=0.30, high_weight=0.80, high_won=True
    )
    low_wins = bankroll.hedged_pnl(
        high_cost=0.70, low_cost=0.30, high_weight=0.80, high_won=False
    )
    self.assertAlmostEqual(high_wins, 0.80 / 0.70 - 1.0)
    self.assertAlmostEqual(low_wins, 0.20 / 0.30 - 1.0)

def test_complementary_cost_identity_is_reported(self) -> None:
    identity = bankroll.complementary_cost_check(0.52, 0.51)
    self.assertAlmostEqual(identity["combined_unit_cost"], 1.03)
    self.assertAlmostEqual(identity["locked_unit_loss"], 0.03)
```

- [ ] **Step 2: Run the bankroll tests and verify failure**

Run: `python3 -m unittest sim/test_tennis_ev_bankroll.py -v`

Expected: import failure because `sim.tennis_ev.bankroll` does not exist.

- [ ] **Step 3: Implement hedge scenarios and paired comparison**

Implement the user-specified allocations `90/10`, `80/20`, `70/30`, and `60/40`. Use identical matches, timestamps, fee specification, and deployed-capital denominator for hedged/unhedged pairs. Report return, volatility, drawdown, loss probability, and paired metric differences.

- [ ] **Step 4: Write failing capital-reservation and fixed-fraction tests**

```python
def test_overlapping_matches_cannot_reuse_reserved_cash(self) -> None:
    trades = [
        trade("a", entry=100, settle=300, pnl_rate=0.10),
        trade("b", entry=200, settle=400, pnl_rate=0.10),
    ]
    ledger = bankroll.run_fixed_fraction(trades, initial_cash=10_000.0, fraction=0.10)
    self.assertAlmostEqual(ledger[0].stake, 1_000.0)
    self.assertAlmostEqual(ledger[1].stake, 900.0)

def test_fractional_betting_ruin_means_equity_at_or_below_zero(self) -> None:
    summary = bankroll.summarize_paths(np.array([[10_000.0, 9_000.0, 8_100.0]]))
    self.assertEqual(summary["ruin_boundary_usd"], 0.0)
    self.assertEqual(summary["ruined_paths"], 0)
    self.assertEqual(summary["ruin_probability"], 0.0)
```

The initial `$10,000` and fractions `1%`, `2%`, `5%`, and `10%` are user-specified values. Mathematical ruin is equity at or below zero. Because fractional sizing below total capital can make this structurally unreachable in finite paths, the report must state that limitation; operational inability to meet a market minimum is `BLOCK_DATA` unless contemporaneous minimum-order evidence exists.

- [ ] **Step 5: Implement fixed-fraction replay and training-only Kelly**

```python
def binary_kelly(win_probability, all_in_cost):
    if not 0.0 < all_in_cost < 1.0:
        raise ValueError("all_in_cost must be in (0, 1)")
    net_odds = (1.0 - all_in_cost) / all_in_cost
    return max(0.0, min(1.0, (net_odds * win_probability - (1.0 - win_probability)) / net_odds))

def executable_kelly(train_win_rate, cost, edge_interval_low):
    return 0.0 if edge_interval_low <= 0.0 else binary_kelly(train_win_rate, cost)
```

Full Kelly is a formula-derived reference. Add half- and quarter-Kelly as explicitly labeled heuristic sensitivity scenarios derived arithmetically from full Kelly, not claimed optima. Reserve stakes from entry through settlement and record skipped trades caused by unavailable cash.

- [ ] **Step 6: Write failing deterministic block-bootstrap Monte Carlo tests**

```python
def test_monte_carlo_is_reproducible_and_preserves_day_blocks(self) -> None:
    blocks = dated_trade_blocks()
    first = bankroll.monte_carlo(blocks, paths=10_000, initial_cash=10_000.0,
                                 fraction=0.02, seed=20260812)
    second = bankroll.monte_carlo(blocks, paths=10_000, initial_cash=10_000.0,
                                  fraction=0.02, seed=20260812)
    np.testing.assert_array_equal(first.equity_paths, second.equity_paths)
    self.assertEqual(first.sampled_unit, "UTC_DATE_BLOCK")
```

- [ ] **Step 7: Implement simulation, rerun all bankroll tests, and commit**

Resample UTC-date blocks with replacement so same-day dependence is not broken into apparently independent fills. Preserve within-block chronological order and capital occupancy. Use exactly the user-specified `10,000` paths.

Run: `python3 -m unittest sim/test_tennis_ev_bankroll.py -v`

Expected: all hedge, sizing, Kelly, capital, and Monte Carlo tests pass.

Commit:

```bash
git add sim/tennis_ev/bankroll.py sim/test_tennis_ev_bankroll.py
git commit -m "feat: simulate tennis hedge and bankroll rules"
```

### Task 6: Verdict, tables, charts, and artifact reconciliation

**Files:**
- Create: `sim/tennis_ev/report.py`
- Create: `sim/test_tennis_ev_report.py`

- [ ] **Step 1: Write failing verdict tests for every allowed terminal state**

```python
def test_positive_proxy_with_missing_books_is_execution_blocked(self) -> None:
    verdict = report.decide_verdict(
        usable_test_matches=100,
        significant_positive_rules=1,
        executable_book_matches=0,
        net_after_verified_costs=True,
    )
    self.assertEqual(verdict, "BLOCK_EXECUTION_DATA")

def test_no_frozen_rule_passes_returns_required_chinese_conclusion(self) -> None:
    verdict = report.decide_verdict(
        usable_test_matches=100,
        significant_positive_rules=0,
        executable_book_matches=0,
        net_after_verified_costs=False,
    )
    self.assertEqual(verdict, "NO_SIGNIFICANT_EDGE")
    self.assertIn("没有发现统计显著优势", report.verdict_text(verdict))
```

Also cover `BLOCK_DATA` when no usable test matches exist and `VERIFIED_POSITIVE_EV` only when a frozen rule passes corrected significance, its net-EV interval lower bound is positive after captured costs, concentration checks survive, and execution coverage is adequate for every claimed observation.

- [ ] **Step 2: Run the report tests and verify failure**

Run: `python3 -m unittest sim/test_tennis_ev_report.py -v`

Expected: import failure because `sim.tennis_ev.report` does not exist.

- [ ] **Step 3: Implement verdict logic and machine-readable artifacts**

Write atomically through a temporary file and rename. Produce:

- `coverage_manifest.json`;
- `normalized_matches.csv`;
- `market_snapshots.csv`;
- `trades.csv`;
- `match_state.csv`;
- `exclusions.csv`;
- `research_event_ledger.csv`;
- `frozen_strategy_manifest.json`;
- `favorite_baseline.csv`;
- `favorite_calibration.csv`;
- `random_baseline.json`;
- `all_training_conditions.csv`;
- `top_50_training_conditions.csv`;
- `frozen_holdout_results.csv`;
- `hedge_results.csv`;
- `bankroll_results.json`;
- `monte_carlo_summary.json`;
- `result.json`;
- `report.md`;
- `artifact_manifest.json`.

Each result row contains object, unit, numerator/denominator or sample count, time interval, data source, and numeric provenance.

- [ ] **Step 4: Write failing chart and reconciliation tests**

```python
def test_report_totals_recompute_from_event_ledger(self) -> None:
    artifacts = report.build_report(synthetic_run(), output_dir=self.output_dir)
    ledger = list(csv.DictReader(open(artifacts.event_ledger, newline="")))
    recomputed = sum(float(row["net_pnl"]) for row in ledger if row["selected"] == "true")
    payload = json.loads(Path(artifacts.result_json).read_text())
    self.assertAlmostEqual(recomputed, payload["holdout"]["net_pnl"])

def test_required_charts_are_nonempty_png_files(self) -> None:
    artifacts = report.build_report(synthetic_run(), output_dir=self.output_dir)
    for name in ("equity_curve.png", "drawdown_curve.png", "roi_distribution.png",
                 "strategy_comparison.png"):
        self.assertGreater((self.output_dir / name).stat().st_size, 0)
```

- [ ] **Step 5: Implement the four required charts and Markdown answers**

Use Matplotlib's non-interactive `Agg` backend. Plot chronological equity, chronological drawdown, Monte Carlo/test ROI distribution, and side-by-side baseline/frozen/hedge comparisons. Write the unbinned favorite calibration table using every distinct training price and a held-out calibration evaluation; do not choose calibration bins from test outcomes. Titles and captions state price fidelity and whether costs are proxy, verified, or unavailable.

The Markdown report directly answers:

1. whether verified long-run positive EV was found;
2. whether evidence points to player fundamentals, market odds, liquidity, or trading behavior;
3. whether it exceeds fees, slippage, and liquidity limits;
4. the exact no-edge sentence when applicable.

Player-fundamental attribution is `BLOCK_DATA` in this version because no external point-in-time player model is introduced.

- [ ] **Step 6: Run report tests and commit**

Run: `MPLBACKEND=Agg python3 -m unittest sim/test_tennis_ev_report.py -v`

Expected: every verdict, artifact, chart, and reconciliation test passes.

Commit:

```bash
git add sim/tennis_ev/report.py sim/test_tennis_ev_report.py
git commit -m "feat: report tennis EV evidence and verdict"
```

### Task 7: One-command pipeline and synthetic end-to-end run

**Files:**
- Create: `sim/run_polymarket_tennis_ev.py`
- Modify: `sim/test_tennis_ev_report.py`

- [ ] **Step 1: Write a failing end-to-end CLI test**

The test uses a temporary gzip JSONL fixture, invokes `main()` without network access, and checks the split, frozen manifest, report, charts, and artifact hashes.

```python
def test_cli_builds_reproducible_complete_artifact_set(self) -> None:
    first = run_cli(self.fixture_path, self.output_a)
    second = run_cli(self.fixture_path, self.output_b)
    self.assertEqual(first, 0)
    self.assertEqual(second, 0)
    self.assertEqual(read_json(self.output_a / "result.json"),
                     read_json(self.output_b / "result.json"))
    self.assertEqual(read_json(self.output_a / "frozen_strategy_manifest.json"),
                     read_json(self.output_b / "frozen_strategy_manifest.json"))
    self.assert_manifest_hashes_match(self.output_a / "artifact_manifest.json")
```

- [ ] **Step 2: Run the CLI test and verify failure**

Run: `MPLBACKEND=Agg python3 -m unittest sim.test_tennis_ev_report -v`

Expected: failure because the CLI module and orchestration function are absent.

- [ ] **Step 3: Implement the minimal orchestration entry point**

```python
def run(args):
    matches, snapshots, trades, states, exclusions, coverage = load_historical_matches(Path(args.paths))
    split = chronological_split(matches, train_fraction=args.train_fraction)
    train_events, test_events = build_research_universe(split)
    manifest = freeze_training_manifest(train_events, alpha=args.alpha)
    holdout = evaluate_holdout(manifest, test_events)
    analyses = run_baselines_discovery_hedges_and_bankroll(
        train_events, test_events, manifest, args
    )
    return build_report(
        ResearchRun(coverage, exclusions, split, manifest, holdout, analyses),
        Path(args.output_dir),
    )
```

CLI defaults are the user-specified split, bands, allocations, fixed fractions, initial bankroll, simulation paths, and display count. `--fee-rate` defaults to absent, not to an unverified current constant. When captured fee metadata is unavailable, the run emits gross proxy and fee-sensitivity status but cannot produce `VERIFIED_POSITIVE_EV`.

- [ ] **Step 4: Run the end-to-end test and the complete new suite**

Run:

```bash
MPLBACKEND=Agg python3 -m unittest \
  sim.test_tennis_ev_data \
  sim.test_tennis_ev_research \
  sim.test_tennis_ev_bankroll \
  sim.test_tennis_ev_report -v
```

Expected: all new tests pass and the synthetic artifact manifests reconcile.

- [ ] **Step 5: Commit**

```bash
git add sim/run_polymarket_tennis_ev.py sim/test_tennis_ev_report.py
git commit -m "feat: run tennis EV research end to end"
```

### Task 8: Historical execution, evidence audit, and final research report

**Files:**
- Generate: `outputs/tennis_ev/*`
- Modify: `sim/backtest_log.md`

- [ ] **Step 1: Run the full historical pipeline on the immutable source**

Run:

```bash
MPLBACKEND=Agg python3 sim/run_polymarket_tennis_ev.py \
  --paths outputs/polymarket_tennis_exit_paths_primary.jsonl.gz \
  --output-dir outputs/tennis_ev \
  --train-fraction 0.70 \
  --alpha 0.05 \
  --simulation-paths 10000 \
  --initial-bankroll-usd 10000
```

Numeric provenance: `0.70`, `10,000` paths, and `$10,000` are user-specified values; `0.05` is an explicitly labeled conventional research setting. This command submits no orders and performs no authenticated calls.

Expected: exit code zero and a JSON summary naming every artifact and the terminal verdict. A successful process is not itself proof of positive EV.

- [ ] **Step 2: Inspect coverage before interpreting performance**

Run:

```bash
python3 - <<'PY'
import json
from pathlib import Path
p = json.loads(Path('outputs/tennis_ev/coverage_manifest.json').read_text())
assert p['raw_rows'] == p['usable_matches'] + p['excluded_matches']
assert sum(p['exclusions_by_reason'].values()) == p['excluded_matches']
print(json.dumps(p, ensure_ascii=False, indent=2, sort_keys=True))
PY
```

Expected: denominators reconcile and the printed manifest quantifies usable match, book, liquidity, match-state, and fee coverage. If usable test matches are zero, stop performance interpretation and retain `BLOCK_DATA`.

- [ ] **Step 3: Independently recompute holdout totals from the immutable ledger**

Run:

```bash
python3 - <<'PY'
import csv, json
from pathlib import Path
rows = list(csv.DictReader(Path('outputs/tennis_ev/research_event_ledger.csv').open()))
selected = [r for r in rows if r['partition'] == 'test' and r['selected'] == 'true']
net = sum(float(r['net_pnl']) for r in selected)
cost = sum(float(r['deployed_cost']) for r in selected)
result = json.loads(Path('outputs/tennis_ev/result.json').read_text())
assert len(selected) == result['holdout']['selected_matches']
assert abs(net - result['holdout']['net_pnl']) < 1e-10
assert abs(cost - result['holdout']['deployed_cost']) < 1e-10
print({'selected_matches': len(selected), 'net_pnl': net, 'deployed_cost': cost})
PY
```

Expected: selected-match, PnL, and deployed-cost totals match within the estimated floating-point tolerance `1e-10`, used only for arithmetic verification and not for strategy selection.

- [ ] **Step 4: Verify freeze integrity and artifact hashes**

Run:

```bash
python3 - <<'PY'
import hashlib, json
from pathlib import Path
base = Path('outputs/tennis_ev')
manifest = json.loads((base / 'artifact_manifest.json').read_text())
for name, expected in manifest['sha256'].items():
    actual = hashlib.sha256((base / name).read_bytes()).hexdigest()
    assert actual == expected, name
frozen = json.loads((base / 'frozen_strategy_manifest.json').read_text())
assert not set(frozen['source_event_ids']) & set(frozen['test_event_ids'])
print({'artifact_hashes': len(manifest['sha256']), 'frozen_rules': len(frozen['rule_ids'])})
PY
```

Expected: all artifact hashes match and no held-out event ID appears in training provenance.

- [ ] **Step 5: Run old and new regression suites**

Run:

```bash
MPLBACKEND=Agg python3 -m unittest discover -s sim -p 'test_bt_polymarket_tennis*.py' -v
MPLBACKEND=Agg python3 -m unittest discover -s sim -p 'test_tennis_ev*.py' -v
```

Expected: all existing tennis research tests and all new tennis-EV tests pass. Any failure is resolved before a completion claim.

- [ ] **Step 6: Append the evidence-backed run to the durable backtest log**

Append one entry to `sim/backtest_log.md` containing the run timestamp, input hash, raw/usable/excluded match counts with denominators, train/test match counts and achieved split, number of tested/frozen/significant conditions, price and execution fidelity, verified-cost coverage, verdict, artifact-manifest hash, and the exact final conclusion. Do not write “positive EV” when the verdict is blocked or non-significant.

- [ ] **Step 7: Review the final report against the four required questions**

Read `outputs/tennis_ev/report.md` and confirm it explicitly answers long-run EV, attribution family, fees/slippage/liquidity, and the no-edge wording. Confirm the four PNG charts exist and correspond to ledger data.

- [ ] **Step 8: Commit code-independent historical artifacts and log**

```bash
git add outputs/tennis_ev sim/backtest_log.md
git commit -m "research: report Polymarket tennis EV holdout"
```

## Final verification gate

Before declaring completion, rerun the complete new suite, existing tennis suite, historical pipeline, coverage reconciliation, ledger recomputation, freeze-integrity check, and artifact-hash check from Task 8. Report exact pass/total counts, match denominators, coverage period, frozen-rule count, held-out sample size, and terminal verdict. A positive proxy result with absent contemporaneous book/depth evidence is reported as `BLOCK_EXECUTION_DATA`, never as verified executable positive EV.
