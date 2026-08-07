# Source Action Fidelity and Basket Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every profile-eligible source action persistently retryable and event-linked so internally avoidable minimum-size and FAK-zero-fill outcomes cannot become permanent missing basket legs.

**Architecture:** Add a small pure fidelity module for states, targets, hashes, and monitoring math; extend the existing SQLite ledger additively with frozen market metadata, action targets, decision units, and submission attempts; then integrate those records into the current chain follower and authenticated FAK reconciliation loop. Drift remains report-only, while all eligible actions continue through the retry state machine until filled, causally superseded, externally unfillable, capital constrained, or stopped by an internal invariant.

**Tech Stack:** Python 3.12, `Decimal`, SQLite WAL transactions, pytest, official Polymarket CLOB/Gamma/Data APIs, Polygon new-head WebSocket, systemd, POSIX file locks.

---

## File structure

- Create `work/wallet_copy_paper/live_action_fidelity.py`: pure statuses, frozen metadata and target dataclasses, canonical hashing, remaining-quantity and monitoring calculations.
- Create `work/wallet_copy_paper/tests/test_live_action_fidelity.py`: unit tests for the pure fidelity contract.
- Modify `work/wallet_copy_paper/live_copy_profiles.py`: shared official token-to-condition-to-event resolver and full-wallet CD90 eligibility scope.
- Modify `work/wallet_copy_paper/cd90_live_sizing.py`: return retryable planning states instead of terminal skips for eligible BUY/SELL resource constraints.
- Modify `work/wallet_copy_paper/cd90_live_copy.py`: additive schema, persistent attempts/targets/decision units, retry scheduler, causal supersession, status reporting, CD90 metadata scope wiring.
- Modify `work/wallet_copy_paper/live_wallet_coordinator.py`: expose coordinator collisions as internal blocking invariants rather than economic skips.
- Modify `work/wallet_copy_paper/server_health_heartbeat.py`: audit pending legs, action conservation, decision-unit completeness, and release-local internal errors.
- Modify existing tests under `work/wallet_copy_paper/tests/`: replace old terminal-skip expectations and add migration, restart, profile-isolation, reporting, and release tests.
- Modify `work/wallet_copy_paper/tools/deploy_n_wallet_closed_loop_release.sh`: validate the new tables and pending-attempt invariants before committing the release.

### Task 1: Pure fidelity state and target model

**Files:**
- Create: `work/wallet_copy_paper/live_action_fidelity.py`
- Create: `work/wallet_copy_paper/tests/test_live_action_fidelity.py`

- [ ] **Step 1: Write failing tests for canonical metadata, target remainder, and report-only drift**

```python
from decimal import Decimal

from live_action_fidelity import (
    FrozenActionMetadata,
    ActionTarget,
    REPORT_ONLY_DRIFT_MODE,
    remaining_quantity,
)


def test_frozen_metadata_hash_is_order_independent():
    left = FrozenActionMetadata.from_mapping({
        "condition_id": "0xabc",
        "market_slug": "high-temperature-in-paris-on-august-8",
        "event_slug": "temperature-in-paris-on-august-8",
        "profile_follow": True,
        "profile_reason": "FULL_WALLET_ACTION_ELIGIBLE",
    })
    right = FrozenActionMetadata.from_mapping(dict(reversed(list(left.payload.items()))))
    assert left.evidence_hash == right.evidence_hash


def test_remaining_quantity_subtracts_only_authoritative_fills():
    target = ActionTarget(
        proportional_quantity=Decimal("4"),
        target_quantity=Decimal("5"),
        cumulative_filled_quantity=Decimal("1.25"),
    )
    assert remaining_quantity(target) == Decimal("3.75")


def test_drift_mode_is_report_only_and_cannot_gate_actions():
    assert REPORT_ONLY_DRIFT_MODE == "MONITOR_ONLY_NO_EXECUTION_GATE"
```

- [ ] **Step 2: Run the new tests and confirm import failure**

Run:

```bash
cd /Users/luke/Documents/polymarket/work/wallet_copy_paper
pytest -q tests/test_live_action_fidelity.py
```

Expected: FAIL because `live_action_fidelity` does not exist.

- [ ] **Step 3: Implement the pure module**

```python
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

ZERO = Decimal("0")
REPORT_ONLY_DRIFT_MODE = "MONITOR_ONLY_NO_EXECUTION_GATE"
RETRYABLE_ACTION_STATES = frozenset({
    "PENDING_METADATA",
    "PENDING_LIQUIDITY",
    "PARTIAL_PENDING",
    "PENDING_MINIMUM_UNWIND",
    "PENDING_CAPITAL",
})
BUSINESS_TERMINAL_STATES = frozenset({
    "FILLED",
    "EXTERNAL_UNFILLABLE",
    "SUPERSEDED_UNFILLED",
    "ERROR_INTERNAL",
    "SKIPPED_PROFILE_EXCLUDED",
})


def canonical_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class FrozenActionMetadata:
    payload: Mapping[str, Any]
    evidence_hash: str

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "FrozenActionMetadata":
        frozen = dict(payload)
        return cls(payload=frozen, evidence_hash=canonical_hash(frozen))


@dataclass(frozen=True)
class ActionTarget:
    proportional_quantity: Decimal
    target_quantity: Decimal
    cumulative_filled_quantity: Decimal = ZERO


def remaining_quantity(target: ActionTarget) -> Decimal:
    remaining = target.target_quantity - target.cumulative_filled_quantity
    if remaining < ZERO:
        raise ValueError("CUMULATIVE_FILL_EXCEEDS_ACTION_TARGET")
    return remaining
```

- [ ] **Step 4: Run the pure tests**

Run: `pytest -q tests/test_live_action_fidelity.py`

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add work/wallet_copy_paper/live_action_fidelity.py \
  work/wallet_copy_paper/tests/test_live_action_fidelity.py
git commit -m "feat: add live action fidelity model"
```

### Task 2: Freeze official event metadata for every eligible action

**Files:**
- Modify: `work/wallet_copy_paper/live_copy_profiles.py:123-228`
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:3599-3884,6459-6558`
- Test: `work/wallet_copy_paper/tests/test_live_copy_profiles.py`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_copy.py`

- [ ] **Step 1: Add failing full-wallet metadata tests**

```python
def test_full_wallet_scope_freezes_condition_market_and_event_identity():
    scope = FullWalletEventScope(public_get_json)
    decision = scope.resolve("token-a")
    assert decision.follow is True
    assert decision.reason == "FULL_WALLET_ACTION_ELIGIBLE"
    assert decision.metadata["condition_id"] == CONDITION
    assert decision.metadata["market_slug"] == "high-temperature-in-paris-on-august-8"
    assert decision.metadata["event_slug"] == "temperature-in-paris-on-august-8"


def test_full_wallet_scope_treats_missing_gamma_event_as_retryable_metadata():
    scope = FullWalletEventScope(lambda url: mapping() if "markets-by-token" in url else [])
    with pytest.raises(LiveProfileMetadataUnavailable):
        scope.resolve("token-a")
```

- [ ] **Step 2: Run the focused tests and confirm failure**

Run:

```bash
pytest -q tests/test_live_copy_profiles.py \
  -k 'full_wallet_scope'
```

Expected: FAIL because `FullWalletEventScope` is undefined.

- [ ] **Step 3: Extract one shared official metadata resolver and add full-wallet scope**

Move the complete token-validation block currently implemented in
`ATPWTAMainlineScope.resolve()` into
`OfficialEventMetadataResolver.resolve()`: validate the non-empty token, fetch
`markets-by-token`, require membership in the official primary/secondary token
pair, fetch exactly one Gamma market by condition id, require the same
condition id and token membership, then require exactly one Gamma event.
Return the existing evidence fields (`condition_id`, `market_id`,
`market_slug`, `event_slug`, open/closed/order-book flags, start time, and
token ids). `ATPWTAMainlineScope` applies its existing tennis filters to that
evidence; `FullWalletEventScope.resolve_action()` returns
`ScopeDecision(True, "FULL_WALLET_ACTION_ELIGIBLE", evidence)` without a
market-category filter. Preserve every existing configuration-versus-metadata
exception branch and add a focused test for each one.

- [ ] **Step 4: Wire CD90 to `FullWalletEventScope` and freeze eligibility before execution**

```python
if action_scope is None and profile_key == LIVE_PROFILE_CD90:
    from live_copy_profiles import FullWalletEventScope
    action_scope = FullWalletEventScope(_bounded_public_json)
```

Run: `pytest -q tests/test_live_copy_profiles.py tests/test_cd90_live_copy.py -k 'scope or metadata'`

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add work/wallet_copy_paper/live_copy_profiles.py \
  work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_live_copy_profiles.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "feat: freeze live action event metadata"
```

### Task 3: Additive ledger migration for targets, attempts, and decision units

**Files:**
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:326-570,2309-3165`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_copy.py`

- [ ] **Step 1: Add a failing migration-preservation test**

```python
def test_fidelity_schema_migration_preserves_existing_ledger_values(tmp_path):
    store = initialized_store(tmp_path)
    before = immutable_ledger_snapshot(store)
    store.initialize()
    after = immutable_ledger_snapshot(store)
    assert after == before
    with store.connect() as connection:
        tables = {row["name"] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
    assert {
        "action_market_metadata",
        "action_targets",
        "submission_attempts",
        "decision_units",
    } <= tables
```

- [ ] **Step 2: Run the migration test and confirm missing tables**

Run: `pytest -q tests/test_cd90_live_copy.py -k fidelity_schema_migration`

Expected: FAIL listing the four missing tables.

- [ ] **Step 3: Add the schema in `LiveStore.initialize()`**

```sql
CREATE TABLE IF NOT EXISTS action_market_metadata (
    action_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    event_slug TEXT NOT NULL,
    profile_follow INTEGER NOT NULL,
    profile_reason TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    frozen_at_ms INTEGER NOT NULL,
    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
);

CREATE TABLE IF NOT EXISTS decision_units (
    event_slug TEXT PRIMARY KEY,
    metadata_hash TEXT NOT NULL,
    first_source_timestamp INTEGER NOT NULL,
    last_source_timestamp INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS action_targets (
    action_id TEXT PRIMARY KEY,
    proportional_quantity TEXT NOT NULL,
    target_quantity TEXT NOT NULL,
    cumulative_filled_quantity TEXT NOT NULL DEFAULT '0',
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
);

CREATE TABLE IF NOT EXISTS submission_attempts (
    attempt_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    order_id TEXT,
    state TEXT NOT NULL,
    requested_quantity TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id),
    UNIQUE(action_id, attempt_number),
    UNIQUE(order_id)
);
```

- [ ] **Step 4: Add transactional store APIs and prove idempotency**

Implement and test these exact interfaces:

```python
store.freeze_action_metadata(source, metadata, frozen_at_ms) -> bool
store.ensure_action_target(source, proportional_quantity, target_quantity, state, reason, updated_at_ms) -> dict
store.next_submission_attempt(source, requested_quantity, snapshot, created_at_ms) -> dict
store.set_attempt_order_id(attempt_id, order_id, response, updated_at_ms) -> None
store.update_action_progress(source, filled_delta, state, reason, updated_at_ms, details) -> None
store.retryable_actions() -> list[tuple[SourceAction, dict, dict]]
store.decision_unit_summary() -> list[dict]
```

Run: `pytest -q tests/test_cd90_live_copy.py -k 'fidelity_schema or action_target or submission_attempt'`

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

```bash
git add work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "feat: persist live action targets and attempts"
```

### Task 4: Replace eligible-action minimum and resource skips with explicit plans

**Files:**
- Modify: `work/wallet_copy_paper/cd90_live_sizing.py:23-255`
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:61-81,4069-4355`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_sizing.py`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_copy.py:788-899`

- [ ] **Step 1: Replace old CD90 strict-skip tests with failing fidelity tests**

```python
def test_cd90_below_minimum_buy_uses_current_market_minimum():
    plan = plan_action(
        side="BUY", source_quantity=D("2"), scale=D("1"), held_quantity=D("0"),
        minimum_order_size=D("5"), minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.25"), visible_best_level_size=D("20"),
        taker_fee_bps=D("0"), available_cash=D("20"),
        allow_minimum_upscale=True,
    )
    assert plan.terminal_status == "READY"
    assert plan.proportional_quantity == D("2")
    assert plan.requested_quantity == D("5")
    assert plan.reason == "MINIMUM_ORDER_SIZE_UPSCALE"


def test_cash_shortage_is_pending_not_terminal_skip():
    plan = plan_action(
        side="BUY", source_quantity=D("2"), scale=D("1"),
        held_quantity=D("0"), minimum_order_size=D("5"),
        minimum_marketable_buy_notional_usd=D("1"),
        best_price=D("0.25"), visible_best_level_size=D("20"),
        taker_fee_bps=D("0"), available_cash=D("0.50"),
        allow_minimum_upscale=True,
    )
    assert plan.terminal_status == "PENDING_CAPITAL"
    assert plan.reason == "INSUFFICIENT_AVAILABLE_CASH"
```

- [ ] **Step 2: Run focused sizing tests and confirm old behavior fails**

Run: `pytest -q tests/test_cd90_live_sizing.py tests/test_cd90_live_copy.py -k 'minimum or cash_shortage'`

Expected: FAIL on the CD90 skip policy and cash terminal status.

- [ ] **Step 3: Make minimum upscaling the eligible BUY policy and return resource states**

```python
LIVE_PROFILE_MINIMUM_SIZE_POLICIES = {
    LIVE_PROFILE_CD90: MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
    LIVE_PROFILE_BDDC_WEATHER: MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
    LIVE_PROFILE_TENNIS_MAINLINE: MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
}
```

Change `plan_action()` so BUY cash shortage returns `PENDING_CAPITAL`; SELL inventory below minimum returns `PENDING_MINIMUM_UNWIND`; SELL zero inventory returns `EXTERNAL_UNFILLABLE_NO_LOCAL_INVENTORY`. Preserve proportional and requested quantities in every plan.

- [ ] **Step 4: Run sizing and execution tests**

Run: `pytest -q tests/test_cd90_live_sizing.py tests/test_cd90_live_copy.py -k 'minimum or inventory or cash'`

Expected: PASS with no below-minimum eligible BUY becoming `SKIPPED`.

- [ ] **Step 5: Commit Task 4**

```bash
git add work/wallet_copy_paper/cd90_live_sizing.py \
  work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_sizing.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "fix: keep eligible minimum actions followable"
```

### Task 5: Turn FAK outcomes into a crash-safe retry lifecycle

**Files:**
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:2309-2625,4034-4650,5307-5395`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_copy.py:904-1285,1516-1615`

- [ ] **Step 1: Add failing zero-fill, partial-remainder, and restart tests**

```python
def test_fak_zero_fill_becomes_pending_and_retries_on_a_new_head(tmp_path):
    store, source = pending_test_store(tmp_path)
    execute_source_action(
        store=store, source=source, execution=ZeroFillAdapter(),
        allocated_cash=D("20"), live_enabled=True,
    )
    reconcile_submitted_actions(store=store, execution=ZeroFillAdapter())
    assert store.latest_transition(source)["terminal_status"] == "PENDING_LIQUIDITY"
    retry_pending_actions(
        store=store, execution=FillOnSecondAttemptAdapter(),
        allocated_cash=D("20"), wallet_lock_path=tmp_path / "wallet.lock",
        coordinator=None, profile_key="cd90",
    )
    assert store.latest_transition(source)["terminal_status"] == "SUBMITTED_UNRECONCILED"
    assert store.submission_attempt_count(source.action_id) == 2


def test_partial_fill_retries_only_the_exact_remainder(tmp_path):
    store, source = target_store(tmp_path, target="5")
    reconcile_attempt(store, source, matched="2")
    pending = store.action_target(source.action_id)
    assert pending["state"] == "PARTIAL_PENDING"
    assert pending["remaining_quantity"] == "3"
```

- [ ] **Step 2: Run focused tests and confirm current terminal `SKIPPED/PARTIAL` behavior**

Run: `pytest -q tests/test_cd90_live_copy.py -k 'zero_fill or partial_fill_retries or pending_restart'`

Expected: FAIL because canceled FAK is terminal `SKIPPED` and partial is terminal `PARTIAL`.

- [ ] **Step 3: Implement attempt-aware reconciliation**

Implement these rules in one SQLite transaction per attempt:

```python
if authoritative_quantity > ZERO:
    store.apply_attempt_fill_and_update_target(
        source=source,
        attempt_id=attempt_id,
        authoritative_quantity=authoritative_quantity,
        authoritative_price=authoritative_price,
        fee_usd=fee_usd,
        updated_at_ms=now_ms(),
    )
elif official_status in DEFINITIVE_NO_FILL_STATUSES:
    store.release_attempt_and_set_target(
        source=source,
        state="PENDING_LIQUIDITY",
        reason="FAK_ZERO_FILL_RETRYABLE",
    )
else:
    keep SUBMITTED_UNRECONCILED
```

`PARTIAL_PENDING` must store cumulative authoritative quantity and calculate the next request from the exact remainder.

- [ ] **Step 4: Add `retry_pending_actions()` to every processed head**

```python
def retry_pending_actions(*, store, execution, allocated_cash, wallet_lock_path,
                          coordinator, profile_key):
    for source, metadata, target in store.retryable_actions():
        if store.has_unreconciled_attempt(source.action_id):
            continue
        execute_source_action(
            store=store,
            source=source,
            execution=execution,
            allocated_cash=allocated_cash,
            live_enabled=True,
            coordinator=coordinator,
            profile_key=profile_key,
            retry_existing_target=True,
        )
```

Call it after ingesting the current head's source actions and after the first reconciliation, then reconcile newly accepted attempts once. Do not retry `UNKNOWN_SUBMISSION`.

- [ ] **Step 5: Run retry and accounting tests**

Run: `pytest -q tests/test_cd90_live_copy.py -k 'fak or retry or partial or reservation or unknown_submission'`

Expected: PASS; cash and inventory change only from authoritative fills.

- [ ] **Step 6: Commit Task 5**

```bash
git add work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "fix: retry unfilled live source actions"
```

### Task 6: Preserve source causality and basket action conservation

**Files:**
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:3599-3884,4034-4355`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_copy.py`

- [ ] **Step 1: Add failing late-B and opposite-action tests**

```python
def test_late_second_leg_stays_in_same_event_until_filled(tmp_path):
    # A is observed and filled on one head. B arrives on a later head,
    # receives a zero-fill, then fills on the following head.
    assert unit["event_slug"] == "temperature-in-paris-on-august-8"
    assert unit["eligible_observed"] == 2
    assert unit["filled"] == 2
    assert unit["pending"] == 0


def test_unfilled_buy_is_not_posted_after_a_later_source_sell(tmp_path):
    # BUY never filled; a later source SELL for the same token is frozen.
    retry_pending_actions(
        store=store,
        execution=execution,
        allocated_cash=D("20"),
        wallet_lock_path=tmp_path / "wallet.lock",
        coordinator=None,
        profile_key="cd90",
    )
    assert store.action_target(buy.action_id)["state"] == "SUPERSEDED_UNFILLED"
    assert execution.submitted_tokens == []
```

- [ ] **Step 2: Run the causal tests and confirm failure**

Run: `pytest -q tests/test_cd90_live_copy.py -k 'late_second_leg or not_posted_after'`

Expected: FAIL because there is no event target ledger or supersession logic.

- [ ] **Step 3: Ingest metadata and targets before attempting any action**

For each decoded block range:

```python
actions = self._new_source_actions(from_block=from_block, to_block=to_block)
for action in actions:
    self._freeze_scope_and_target(action)
self._supersede_fully_unfilled_targets(actions)
for action in actions:
    self._execute_frozen_action(
        action,
        allocated_cash=allocated_cash,
        live_enabled=live_enabled,
    )
```

Supersession may affect only an older action with cumulative authoritative fill equal to zero. Partial fills must remain booked and the later SELL must use actual local inventory.

- [ ] **Step 4: Add and enforce the conservation query**

```sql
SELECT
  COUNT(*) AS eligible_observed,
  SUM(state = 'FILLED') AS filled,
  SUM(state IN ('PENDING_METADATA','READY','SUBMITTED_UNRECONCILED',
                'PARTIAL_PENDING','PENDING_LIQUIDITY',
                'PENDING_MINIMUM_UNWIND','PENDING_CAPITAL')) AS pending,
  SUM(state IN ('EXTERNAL_UNFILLABLE','SUPERSEDED_UNFILLED')) AS external_or_causal,
  SUM(state = 'ERROR_INTERNAL') AS internal_error
FROM action_targets;
```

Assert `eligible_observed == filled + pending + external_or_causal + internal_error` in the status payload and server health audit.

- [ ] **Step 5: Run follower, cursor, gap, and causality tests**

Run: `pytest -q tests/test_cd90_live_copy.py -k 'source_follower or cursor or gap or late_second_leg or superseded'`

Expected: PASS.

- [ ] **Step 6: Commit Task 6**

```bash
git add work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "feat: preserve basket action causality"
```

### Task 7: Make fidelity and drift visible without gating execution

**Files:**
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:5863-6117`
- Modify: `work/wallet_copy_paper/server_health_heartbeat.py`
- Test: `work/wallet_copy_paper/tests/test_cd90_live_copy.py:2940-3113`
- Test: `work/wallet_copy_paper/tests/test_server_health_heartbeat.py`

- [ ] **Step 1: Add failing status contract tests**

```python
def test_status_reports_action_conservation_and_decision_units(tmp_path):
    payload = write_status_files(store, tmp_path)
    assert payload["action_fidelity"]["conservation_passed"] is True
    assert payload["action_fidelity"]["eligible_observed"] == 2
    assert payload["decision_units"][0]["event_slug"] == "temperature-in-paris-on-august-8"
    assert payload["execution_drag"]["mode"] == "MONITOR_ONLY_NO_EXECUTION_GATE"
    assert payload["execution_drag"]["historical_reference_evidence"] == (
        "UNREPRODUCED_MISSING_RAW_CUTOFF_AND_HASH"
    )
```

- [ ] **Step 2: Run status tests and confirm missing fields**

Run: `pytest -q tests/test_cd90_live_copy.py tests/test_server_health_heartbeat.py -k 'fidelity or decision_unit or execution_drag'`

Expected: FAIL because the new status sections do not exist.

- [ ] **Step 3: Add JSON and HTML sections**

The payload must include exact counts, denominators, oldest pending timestamp, per-event rows, and dollar-weighted source turnover/price loss/actual fee. The historical `27.8533%` reference must be labeled `UNREPRODUCED_MISSING_RAW_CUTOFF_AND_HASH` and never passed to sizing or execution functions.

- [ ] **Step 4: Make server health fail on internal missing legs, not external pending**

```python
fidelity_passed = (
    conservation_passed
    and internal_error_count == 0
    and terminal_internal_skip_count == 0
)
```

External pending and `N=0` remain visible but do not masquerade as internal health success or failure.

- [ ] **Step 5: Run status and health tests**

Run: `pytest -q tests/test_cd90_live_copy.py tests/test_server_health_heartbeat.py -k 'status or health or fidelity'`

Expected: PASS.

- [ ] **Step 6: Commit Task 7**

```bash
git add work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/server_health_heartbeat.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_server_health_heartbeat.py
git commit -m "feat: report live copy fidelity and basket completeness"
```

### Task 8: Shared-wallet/profile safety and transactional release

**Files:**
- Modify: `work/wallet_copy_paper/live_wallet_coordinator.py`
- Modify: `work/wallet_copy_paper/cd90_live_copy.py:4138-4175`
- Modify: `work/wallet_copy_paper/tools/deploy_n_wallet_closed_loop_release.sh`
- Test: `work/wallet_copy_paper/tests/test_live_wallet_coordinator.py`
- Test: `work/wallet_copy_paper/tests/test_live_release_transaction.py`

- [ ] **Step 1: Add failing collision and release-validation tests**

```python
def test_cross_sleeve_collision_is_internal_pending_not_profile_skip(tmp_path):
    result = execute_source_action(
        store=store,
        source=source,
        execution=execution,
        allocated_cash=D("20"),
        live_enabled=True,
        coordinator=coordinator,
        profile_key="cd90",
    )
    assert result["terminal_status"] == "PENDING_INTERNAL_INVARIANT"
    assert store.latest_transition(source)["reason"] == "CONDITION_OWNED_BY_ANOTHER_PROFILE"


def test_release_script_checks_new_fidelity_tables_before_symlink_commit():
    script = RELEASE_SCRIPT.read_text()
    assert "action_targets" in script
    assert "submission_attempts" in script
    assert "pragma integrity_check" in script.lower()
```

- [ ] **Step 2: Run collision/release tests and confirm failure**

Run: `pytest -q tests/test_live_wallet_coordinator.py tests/test_live_release_transaction.py -k 'collision or fidelity_tables'`

Expected: FAIL because collision currently writes `SKIPPED` and release does not validate new tables.

- [ ] **Step 3: Route ownership collisions to the internal invariant path**

The coordinator continues to block ambiguous physical ownership; the caller records `PENDING_INTERNAL_INVARIANT`, appends an immutable runtime error, leaves the action unresolved, and does not submit an order. It must never count the collision as a rational source-action skip.

- [ ] **Step 4: Extend release preflight and rollback checks**

Before switching `/opt/polymarket-live/current`, validate both CD90 and Tennis backups and active databases:

```sql
PRAGMA integrity_check;
SELECT COUNT(*) FROM order_reservations WHERE active=1;
SELECT COUNT(*) FROM submission_attempts WHERE state='SUBMITTED_UNRECONCILED';
SELECT COUNT(*) FROM action_targets
WHERE state='ERROR_INTERNAL';
```

An active/unknown real submission blocks the release. Existing positions and ledger values are compared before and after migration; rollback restores both databases and the prior release symlink.

- [ ] **Step 5: Run coordinator and release tests**

Run: `pytest -q tests/test_live_wallet_coordinator.py tests/test_live_wallet_coordinator_cli.py tests/test_live_release_transaction.py`

Expected: PASS.

- [ ] **Step 6: Commit Task 8**

```bash
git add work/wallet_copy_paper/live_wallet_coordinator.py \
  work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tools/deploy_n_wallet_closed_loop_release.sh \
  work/wallet_copy_paper/tests/test_live_wallet_coordinator.py \
  work/wallet_copy_paper/tests/test_live_release_transaction.py
git commit -m "fix: preserve fidelity across shared wallet releases"
```

### Task 9: Full verification and atomic server deployment

**Files:**
- Verify all files above.
- Deploy to `/opt/polymarket-live/releases/<timestamp>-action-fidelity`.
- Preserve `/srv/polymarket-live/runtime/cd90_live/live.sqlite3`.
- Preserve `/srv/polymarket-live/runtime/tennis_live/live.sqlite3`.
- Preserve `/srv/polymarket-live/runtime/shared_wallet/coordinator.sqlite3`.

- [ ] **Step 1: Run syntax and full local tests**

```bash
cd /Users/luke/Documents/polymarket/work/wallet_copy_paper
python -m py_compile live_action_fidelity.py live_copy_profiles.py \
  cd90_live_sizing.py cd90_live_copy.py live_wallet_coordinator.py \
  tennis_live_copy.py server_health_heartbeat.py
pytest -q
```

Expected: all tests PASS; no warnings indicating ledger mutation or network-side live submission.

- [ ] **Step 2: Build a release directory locally and verify hashes**

Copy only the audited application modules, tests needed for server smoke verification, and unit files. Generate SHA256 for every release file and compare with the local working files before upload.

- [ ] **Step 3: Upload without activating and run server-side offline tests**

Use `ssh polymarket-hk` and `/opt/polymarket-live/venv/bin/python` to compile and run the focused tests against temporary SQLite copies. Do not source live credentials and do not start a service during this step.

- [ ] **Step 4: Capture the exact pre-release snapshot**

Record, for both active profiles:

```text
systemd active state and MainPID
current release target and file hashes
SQLite integrity_check
action receipt/transition/target/attempt counts
cash, realized PnL, fees, position quantity/cost rows
active reservation and unreconciled attempt counts
last_processed_block/current_head
coordinator registered sleeves and physical-cash interval
```

Expected: both services active before the controlled stop; no active or unknown submission.

- [ ] **Step 5: Execute the transactional release**

Run the audited deployment script with an explicit release path, change id, and new snapshot directory. The script stops both current services, backs up all three databases, arms planned cursor resume, switches the symlink, starts CD90 and Tennis, and rolls back automatically on any failure.

- [ ] **Step 6: Verify the live release immediately**

Prove:

```text
one CD90 daemon and one Tennis daemon
NRestarts unchanged after the release start
fresh heartbeats
last_processed_block equals processable chain head
SQLite integrity_check = ok for both ledgers and coordinator
pre-existing cash/PnL/fees/positions/scale unchanged
action conservation passes
no post-release eligible action terminally skipped for drift, minimum BUY, or FAK zero-fill
paper_only=false and real_order_submission_enabled=true only for the already-authorized profiles
```

If the source wallet produces no post-release action, report `N=0`; do not claim the real retry path was empirically exercised.

- [ ] **Step 7: Commit verification receipts**

```bash
git add docs/superpowers/plans/2026-08-07-source-action-fidelity-and-basket-completeness.md \
  work/wallet_copy_paper
git commit -m "chore: record source fidelity release verification"
```
