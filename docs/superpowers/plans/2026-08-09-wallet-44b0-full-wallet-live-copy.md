# 0x44b0 Full-Wallet Live Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Launch a forward-only real-money follower for `0x44b0a564260008b65a111286e45079f2cf360822` with a frozen share scale, an isolated `200 USD` sleeve funded by an auditable tennis capital transfer, and a quiet non-Netflix alert.

**Architecture:** Reuse the audited live-copy execution core and shared-wallet coordinator, but give the source its own wrapper, SQLite ledger, systemd unit, immutable profile configuration, and current-head watermark. Add an idempotent cross-ledger capital-transfer state machine before the coordinator's additive third-sleeve migration, then generalize release and health tooling from two hard-coded services to the coordinator-registered fleet.

**Tech Stack:** Python 3.12, SQLite, pytest, py-clob-client-v2, Polygon JSON-RPC/WebSocket, Polymarket CLOB/Gamma/Data APIs, POSIX shell, systemd.

---

## File Structure

- Create `work/wallet_copy_paper/wallet44b0_live_copy.py`: immutable source/profile/allocation/scale wrapper and full-wallet service entry point.
- Create `work/wallet_copy_paper/tests/test_wallet44b0_live_copy.py`: wrapper, scale, scope, watermark, and collateral tests.
- Modify `work/wallet_copy_paper/cd90_live_copy.py`: register the profile, add explicit fixed-scale initialization, persist topic alerts, and expose alerts in status.
- Modify `work/wallet_copy_paper/live_copy_profiles.py`: include official question/title fields in frozen metadata and classify Netflix without filtering.
- Modify `work/wallet_copy_paper/live_wallet_coordinator.py`: persist and resume one frozen capital transfer before additive sleeve migration.
- Modify `work/wallet_copy_paper/tests/test_cd90_live_copy.py`: shared-core profile, explicit-scale, alert, minimum-upscale, and no-repost regressions.
- Modify `work/wallet_copy_paper/tests/test_live_copy_profiles.py`: metadata/title and full-wallet non-filtering tests.
- Modify `work/wallet_copy_paper/tests/test_live_wallet_coordinator.py`: debit/credit conservation, interruption, and third/fourth sleeve tests.
- Create `work/wallet_copy_paper/systemd/com.luke.polymarket.wallet-44b0-live.service`: dedicated live daemon.
- Modify `work/wallet_copy_paper/systemd/com.luke.polymarket.live-health.service`: permit/read the third runtime.
- Modify `work/wallet_copy_paper/systemd/polymarket-server-health`: register the third service and runtime.
- Modify `work/wallet_copy_paper/server_health_heartbeat.py`: report pending topic alerts and prove monitored/registered fleet equality.
- Modify `work/wallet_copy_paper/tests/test_server_health_heartbeat.py`: three-profile health and alert coverage.
- Create `work/wallet_copy_paper/tools/prepare_live_snapshot_retention.py`: hash-bound dry-run/apply cleanup for obsolete rollback copies.
- Create `work/wallet_copy_paper/tests/test_live_snapshot_retention.py`: retention safety tests.
- Modify `work/wallet_copy_paper/tools/deploy_n_wallet_closed_loop_release.sh`: manifest-driven three-profile backup, transfer, migration, install, rollback, and acceptance.
- Modify `work/wallet_copy_paper/tests/test_live_release_transaction.py`: third-service and transfer-phase ordering tests.
- Create `work/wallet_copy_paper/README_wallet44b0_live.md`: user-visible immutable live contract and status paths.

### Task 1: Register the profile and explicit fixed scale

**Files:**
- Modify: `work/wallet_copy_paper/tests/test_cd90_live_copy.py`
- Modify: `work/wallet_copy_paper/cd90_live_copy.py`

- [ ] **Step 1: Write failing profile and scale tests**

```python
def test_wallet44b0_profile_uses_full_wallet_detection_and_minimum_buy_upscale():
    assert live.minimum_size_policy_for_profile(
        live.LIVE_PROFILE_WALLET_44B0_FULL
    ) == live.MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM
    assert live.source_action_detection_contract_for_profile(
        live.LIVE_PROFILE_WALLET_44B0_FULL
    ) == live.SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET


def test_explicit_user_scale_initializes_once_without_inventing_source_value(tmp_path):
    store = LiveStore(tmp_path / "live.sqlite3")
    scale = D("0.03277027447726496667590788407")
    receipt = store.initialize_explicit_fixed_scale_once(
        allocation_usd=D("200"),
        fixed_share_scale=scale,
        scale_basis="USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
        observed_at_ms=100,
    )
    assert receipt["fixed_share_scale"] == str(scale)
    assert store.fixed_share_scale() == scale
    assert store.config("source_open_position_value_usd") is None
    assert store.account_snapshot()["cash_usd"] == D("200")
```

- [ ] **Step 2: Run the tests and verify the missing API/profile failures**

Run:

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py::test_wallet44b0_profile_uses_full_wallet_detection_and_minimum_buy_upscale \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py::test_explicit_user_scale_initializes_once_without_inventing_source_value
```

Expected: FAIL because `LIVE_PROFILE_WALLET_44B0_FULL` and `initialize_explicit_fixed_scale_once` do not exist.

- [ ] **Step 3: Add the profile and idempotent explicit-scale initializer**

```python
LIVE_PROFILE_WALLET_44B0_FULL = "wallet_44b0_full_wallet"
LIVE_PROFILE_MINIMUM_SIZE_POLICIES = {
    LIVE_PROFILE_CD90: MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
    LIVE_PROFILE_BDDC_WEATHER: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_TENNIS_MAINLINE: MINIMUM_SIZE_POLICY_SKIP_BELOW_MINIMUM,
    LIVE_PROFILE_WALLET_44B0_FULL: MINIMUM_SIZE_POLICY_UPSCALE_TO_MINIMUM,
}
```

Extend `source_action_detection_contract_for_profile` so CD90 and the new
profile both return `SOURCE_ACTION_DETECTION_CONTRACT_FULL_WALLET`.  Add a
`LiveStore.initialize_explicit_fixed_scale_once` transaction that validates
finite positive decimals, writes `allocation_usd`, `fixed_share_scale`,
`scale_basis`, `source_sleeve_observed_at_ms`, and `account_state` exactly
once, and on restart requires every stored value to match the requested value.
It must never write a fabricated `source_open_position_value_usd`.

- [ ] **Step 4: Run the focused tests**

Run the command from Step 2. Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "feat: register 44b0 full-wallet live profile"
```

### Task 2: Build the independent forward-only wrapper

**Files:**
- Create: `work/wallet_copy_paper/wallet44b0_live_copy.py`
- Create: `work/wallet_copy_paper/tests/test_wallet44b0_live_copy.py`

- [ ] **Step 1: Write failing wrapper tests**

```python
def test_wrapper_freezes_user_contract_and_uses_full_wallet_scope(tmp_path):
    store = LiveStore(tmp_path / "live.sqlite3")
    receipt = wallet44b0.prepare_store(store=store, observed_at_ms=100)
    assert receipt["profile_key"] == "wallet_44b0_full_wallet"
    assert receipt["source_wallet"] == (
        "0x44b0a564260008b65a111286e45079f2cf360822"
    )
    assert receipt["allocation_usd"] == "200"
    assert receipt["fixed_share_scale"] == (
        "0.03277027447726496667590788407"
    )
    assert receipt["profile_scope"] == "FULL_WALLET"
    assert isinstance(wallet44b0.build_action_scope(lambda _url: []), FullWalletEventScope)


def test_wrapper_rejects_any_environment_allocation_or_source_override():
    with pytest.raises(wallet44b0.Wallet44B0ConfigurationError):
        wallet44b0.build_core_env({
            **VALID_ENV,
            "WALLET_44B0_ALLOCATION_USD": "201",
        })
```

- [ ] **Step 2: Run and verify the import failure**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q work/wallet_copy_paper/tests/test_wallet44b0_live_copy.py
```

Expected: FAIL because `wallet44b0_live_copy.py` is absent.

- [ ] **Step 3: Implement the wrapper**

Define immutable constants for the source, profile, `200` allocation, exact
scale, and `FULL_WALLET` scope.  `build_core_env` maps the profile variables to
the audited core environment and rejects any allocation mismatch.  `prepare_store`
locks `profile_key`, `profile_scope`, `source_wallet`, calls the explicit-scale
initializer, and writes one canonical launch receipt.  `run_service` calls
`core.run_live_service` with `FullWalletEventScope`; `--establish-forward-watermark`
uses the existing core forward-only path and cannot scan older blocks.

- [ ] **Step 4: Run the wrapper tests**

Expected: all tests in `test_wallet44b0_live_copy.py` pass.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/wallet44b0_live_copy.py \
  work/wallet_copy_paper/tests/test_wallet44b0_live_copy.py
git commit -m "feat: add forward-only 44b0 live wrapper"
```

### Task 3: Persist non-Netflix alerts without filtering execution

**Files:**
- Modify: `work/wallet_copy_paper/live_copy_profiles.py`
- Modify: `work/wallet_copy_paper/cd90_live_copy.py`
- Modify: `work/wallet_copy_paper/tests/test_live_copy_profiles.py`
- Modify: `work/wallet_copy_paper/tests/test_cd90_live_copy.py`

- [ ] **Step 1: Write failing metadata and alert tests**

```python
def test_full_wallet_scope_keeps_question_and_classifies_without_filtering():
    decision = FullWalletEventScope(PublicMetadata([
        market(question="Will Trump say tariff?")
    ])).resolve_action(SimpleNamespace(token_id=TOKEN, source_timestamp=1))
    assert decision.follow is True
    assert decision.metadata["question"] == "Will Trump say tariff?"
    assert decision.metadata["topic_classification"] == "NON_NETFLIX"


def test_non_netflix_alert_is_deduplicated_and_does_not_skip_action(tmp_path):
    store = LiveStore(tmp_path / "live.sqlite3")
    source = action(side="BUY")
    first = store.record_source_topic_alert(
        source=source,
        metadata={"question": "Will Trump say tariff?", "event_slug": "event"},
        processing_state="SCOPE_ELIGIBLE",
        created_at_ms=10,
    )
    second = store.record_source_topic_alert(
        source=source,
        metadata={"question": "Will Trump say tariff?", "event_slug": "event"},
        processing_state="SCOPE_ELIGIBLE",
        created_at_ms=11,
    )
    assert first is True
    assert second is False
    assert store.source_topic_alerts(unacknowledged_only=True)[0]["action_id"] == source.action_id
```

- [ ] **Step 2: Run and verify red tests**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q \
  work/wallet_copy_paper/tests/test_live_copy_profiles.py::test_full_wallet_scope_keeps_question_and_classifies_without_filtering \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py::test_non_netflix_alert_is_deduplicated_and_does_not_skip_action
```

Expected: FAIL for absent metadata and alert storage.

- [ ] **Step 3: Add classification and persistence**

Extend `OfficialEventMetadataResolver` to freeze the official market question
or event title.  Add `topic_classification = "NETFLIX"` only when normalized
official metadata contains `netflix`; otherwise use `NON_NETFLIX`.  Do not
change `FullWalletEventScope.follow`.

Add:

```sql
CREATE TABLE IF NOT EXISTS source_topic_alerts (
    action_id TEXT PRIMARY KEY,
    topic_classification TEXT NOT NULL,
    event_slug TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    side TEXT NOT NULL,
    source_timestamp INTEGER NOT NULL,
    discovered_at_ms INTEGER NOT NULL,
    processing_state TEXT NOT NULL,
    details_json TEXT NOT NULL,
    acknowledged_at_ms INTEGER,
    FOREIGN KEY(action_id) REFERENCES action_receipts(action_id)
);
```

After immutable metadata is frozen and before order planning, insert one alert
for the new profile only when classification is `NON_NETFLIX`.  Continue to
`execute_source_action` regardless of insert or notification state.  Add alert
count and rows to `status.json`.

- [ ] **Step 4: Run profile/core tests**

Run the command from Step 2 plus the entire two test files. Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/live_copy_profiles.py \
  work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_live_copy_profiles.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "feat: record non-netflix source alerts"
```

### Task 4: Add append-only capital-transfer legs

**Files:**
- Modify: `work/wallet_copy_paper/cd90_live_copy.py`
- Modify: `work/wallet_copy_paper/tests/test_cd90_live_copy.py`

- [ ] **Step 1: Write debit and credit failure tests**

```python
def test_capital_debit_changes_cash_only_and_is_idempotent(tmp_path):
    store = LiveStore(tmp_path / "live.sqlite3")
    store.initialize_explicit_fixed_scale_once(
        allocation_usd=D("698.800092"),
        fixed_share_scale=D("0.1"),
        scale_basis="TEST_FIXED_SCALE",
        observed_at_ms=1,
    )
    before = store.account_snapshot()
    result = store.apply_capital_transfer_leg(
        transfer_id="wallet44b0-launch",
        role="SOURCE_DEBIT",
        counterparty_profile="wallet_44b0_full_wallet",
        amount_usd=D("200"),
        created_at_ms=10,
    )
    replay = store.apply_capital_transfer_leg(
        transfer_id="wallet44b0-launch",
        role="SOURCE_DEBIT",
        counterparty_profile="wallet_44b0_full_wallet",
        amount_usd=D("200"),
        created_at_ms=10,
    )
    assert result == replay
    after = store.account_snapshot()
    assert after["cash_usd"] == D("498.800092")
    assert after["realized_pnl_usd"] == before["realized_pnl_usd"]
    assert after["fees_usd"] == before["fees_usd"]
```

Also test insufficient cash, active reservation, unresolved submission,
non-matching replay, and a destination receipt that accepts only a newly
initialized empty ledger whose initial cash already equals the transfer.

- [ ] **Step 2: Run and verify the missing method failure**

Run the new tests. Expected: FAIL with missing `apply_capital_transfer_leg`.

- [ ] **Step 3: Implement the local transfer receipt**

Add a `capital_transfer_receipts` table keyed by `(transfer_id, role)`.  In one
`BEGIN IMMEDIATE` transaction validate zero active reservations, no unresolved
submission/redemption state, and exact prior cash.  `SOURCE_DEBIT` subtracts
cash and appends its receipt atomically.  `DESTINATION_ALLOCATION` requires a
new ledger with initial and current cash equal to the transfer and with no
actions, positions, submissions, redemptions, or prior transfers; it appends
the funding-provenance receipt without crediting cash a second time.  Replays
return the identical receipt; any identity or amount mismatch raises
`CAPITAL_TRANSFER_REPLAY_MISMATCH`.

- [ ] **Step 4: Run the focused and complete core tests**

Expected: focused tests and `test_cd90_live_copy.py` pass.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py
git commit -m "feat: add immutable sleeve capital transfer legs"
```

### Task 5: Coordinate and resume the frozen 200 USD transfer

**Files:**
- Modify: `work/wallet_copy_paper/live_wallet_coordinator.py`
- Modify: `work/wallet_copy_paper/tests/test_live_wallet_coordinator.py`

- [ ] **Step 1: Write failing coordinator state-machine tests**

```python
def test_frozen_transfer_and_additive_migration_conserve_physical_cash(tmp_path):
    cd90 = _store(tmp_path / "cd90.sqlite3", "457.906884")
    tennis = _store(tmp_path / "tennis.sqlite3", "698.800092")
    coordinator = SharedWalletCoordinator(tmp_path / "coordinator.sqlite3")
    coordinator.initialize_from_frozen_ledgers(
        sleeves=(
            SleeveSpec("cd90", cd90.path, "RESIDUAL"),
            SleeveSpec("tennis_atp_wta_mainline", tennis.path, "RESERVED"),
        ),
        authenticated_collateral_usd=D("845.483579"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=10,
    )
    new = LiveStore(tmp_path / "wallet44b0.sqlite3")
    new.initialize_explicit_fixed_scale_once(
        allocation_usd=D("200"),
        fixed_share_scale=D("0.03277027447726496667590788407"),
        scale_basis="USER_AUTHORIZED_HISTORICAL_MINIMUM_DIVIDED_BY_TEN",
        observed_at_ms=19,
    )
    result = coordinator.apply_frozen_capital_transfer(
        transfer_id="wallet44b0-launch",
        source_profile_key="tennis_atp_wta_mainline",
        destination=SleeveSpec("wallet_44b0_full_wallet", new.path, "RESERVED"),
        amount_usd=D("200"),
        authenticated_collateral_usd=D("845.483579"),
        funder_address="0x" + "a" * 40,
        observed_at_ms=20,
    )
    assert result["state"] == "COMMITTED"
    assert tennis.account_snapshot()["cash_usd"] == D("498.800092")
    assert new.account_snapshot()["cash_usd"] == D("200")
    assert coordinator.cash_snapshot(
        profile_key="wallet_44b0_full_wallet",
        authenticated_collateral_usd=D("845.483579"),
    ).profile_available_cash_usd == D("200")
```

Parametrize crash points after PREPARED, SOURCE_APPLIED, DESTINATION_APPLIED,
and MIGRATED.  Re-running must reach COMMITTED once with one receipt per leg.

- [ ] **Step 2: Run and verify red tests**

Expected: FAIL for absent state machine/table.

- [ ] **Step 3: Implement coordinator transfer states**

Add `sleeve_capital_transfers` with states `PREPARED`, `SOURCE_APPLIED`,
`DESTINATION_APPLIED`, `MIGRATED`, `COMMITTED`, plus immutable transitions.
`apply_frozen_capital_transfer` verifies the source and destination ledger
hashes and cash before each phase, invokes each idempotent local leg, then
calls `extend_from_frozen_ledgers`.  It must reject a physical-cash change,
source cash below `200`, active reservations, and any different replay input.

- [ ] **Step 4: Run coordinator tests**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q work/wallet_copy_paper/tests/test_live_wallet_coordinator.py
```

Expected: all tests pass, including existing three/four sleeve and shared
condition redemption tests.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/live_wallet_coordinator.py \
  work/wallet_copy_paper/tests/test_live_wallet_coordinator.py
git commit -m "feat: coordinate frozen sleeve capital transfer"
```

### Task 6: Add third service and fleet health coverage

**Files:**
- Create: `work/wallet_copy_paper/systemd/com.luke.polymarket.wallet-44b0-live.service`
- Modify: `work/wallet_copy_paper/systemd/com.luke.polymarket.live-health.service`
- Modify: `work/wallet_copy_paper/systemd/polymarket-server-health`
- Modify: `work/wallet_copy_paper/server_health_heartbeat.py`
- Modify: `work/wallet_copy_paper/tests/test_server_health_heartbeat.py`

- [ ] **Step 1: Write failing three-profile health tests**

Assert the root bridge contains:

```text
--live-profile wallet_44b0_full_wallet=com.luke.polymarket.wallet-44b0-live.service=/srv/polymarket-live/runtime/wallet_44b0_live
```

and the health payload reports all coordinator profiles, SQLite integrity for
all three, and unacknowledged non-Netflix alerts.

- [ ] **Step 2: Run and verify the bridge/health failures**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q work/wallet_copy_paper/tests/test_server_health_heartbeat.py
```

Expected: new assertions fail while existing tests stay green.

- [ ] **Step 3: Add the service and dynamic alert health output**

The new service loads its own root-owned environment file, executes
`wallet44b0_live_copy.py --run`, uses `/srv/polymarket-live/runtime/wallet_44b0_live`,
and orders after network availability.  Extend health unit read/write paths and
bridge registration.  Health output includes the exact new alert count and
latest immutable alert identity; it never acknowledges or mutates alerts.

- [ ] **Step 4: Run health tests**

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/systemd \
  work/wallet_copy_paper/server_health_heartbeat.py \
  work/wallet_copy_paper/tests/test_server_health_heartbeat.py
git commit -m "feat: monitor third live wallet sleeve"
```

### Task 7: Build hash-bound snapshot retention

**Files:**
- Create: `work/wallet_copy_paper/tools/prepare_live_snapshot_retention.py`
- Create: `work/wallet_copy_paper/tests/test_live_snapshot_retention.py`

- [ ] **Step 1: Write failing retention tests**

Test that a dry run retains live directories, coordinator, current release,
and newest complete rollback snapshot; lists only `repair_snapshot_*` or
explicit pre-release snapshot directories; records each path, byte size,
mtime, and tree hash; and refuses `--apply` when any identity changes.

- [ ] **Step 2: Run and verify the missing tool failure**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q work/wallet_copy_paper/tests/test_live_snapshot_retention.py
```

Expected: FAIL because the tool is absent.

- [ ] **Step 3: Implement dry-run and hash-confirmed apply**

The default command writes JSON only.  `--apply --manifest PATH
--expected-manifest-hash HASH` re-stats and re-hashes every candidate, refuses
symlinks and paths outside the exact runtime root, then removes only candidates
committed by the manifest.  It prints deleted and retained byte totals and
does not touch any active ledger or release.

- [ ] **Step 4: Run retention tests**

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/tools/prepare_live_snapshot_retention.py \
  work/wallet_copy_paper/tests/test_live_snapshot_retention.py
git commit -m "feat: add verified live snapshot retention"
```

### Task 8: Generalize the closed-loop release transaction

**Files:**
- Modify: `work/wallet_copy_paper/tools/deploy_n_wallet_closed_loop_release.sh`
- Modify: `work/wallet_copy_paper/tests/test_live_release_transaction.py`

- [ ] **Step 1: Write failing release-order tests**

Assert, in order: candidate manifest verification; retention/free-byte gate;
snapshot of both old ledgers/coordinator/service files; stop both existing
services; exact physical cash read; empty new ledger creation; forward
watermark; capital transfer; coordinator generation increment; install three
units and bridge; start three services; acceptance; commit.  Assert rollback
restores old DBs and two services only when the commit boundary was not crossed.

- [ ] **Step 2: Run and verify ordering failures**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q work/wallet_copy_paper/tests/test_live_release_transaction.py
```

Expected: new third-profile and transfer assertions fail.

- [ ] **Step 3: Implement manifest-driven profile loops and transfer phases**

Replace paired `cd90_unit`/`tennis_unit` start-stop-install checks with an
explicit profile manifest containing profile key, unit, env, runtime, ledger,
entry point, and role.  Keep the existing release symlink and rollback guards.
The new-ledger/transfer phase runs only for the unique launch change ID and is
idempotent.  Existing releases without a requested capital transfer must not
move cash.

- [ ] **Step 4: Run release tests**

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add work/wallet_copy_paper/tools/deploy_n_wallet_closed_loop_release.sh \
  work/wallet_copy_paper/tests/test_live_release_transaction.py
git commit -m "feat: deploy three-wallet live fleet atomically"
```

### Task 9: Document and run the complete local verification

**Files:**
- Create: `work/wallet_copy_paper/README_wallet44b0_live.md`

- [ ] **Step 1: Write the immutable operator contract**

Document source address, exact scale formula and provenance, user-authorized
capital, full-wallet scope, per-market minimum BUY rule, SELL inventory rule,
forward watermark, no history catch-up, non-Netflix notification, status path,
service name, and rollback behavior.

- [ ] **Step 2: Run static checks and all related tests**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python -m py_compile \
  work/wallet_copy_paper/cd90_live_copy.py \
  work/wallet_copy_paper/wallet44b0_live_copy.py \
  work/wallet_copy_paper/live_copy_profiles.py \
  work/wallet_copy_paper/live_wallet_coordinator.py \
  work/wallet_copy_paper/server_health_heartbeat.py
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/pytest -q \
  work/wallet_copy_paper/tests/test_wallet44b0_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_copy.py \
  work/wallet_copy_paper/tests/test_cd90_live_sizing.py \
  work/wallet_copy_paper/tests/test_live_copy_profiles.py \
  work/wallet_copy_paper/tests/test_live_wallet_coordinator.py \
  work/wallet_copy_paper/tests/test_live_wallet_coordinator_cli.py \
  work/wallet_copy_paper/tests/test_server_health_heartbeat.py \
  work/wallet_copy_paper/tests/test_live_release_transaction.py \
  work/wallet_copy_paper/tests/test_live_snapshot_retention.py
```

Expected: zero failures.  Record the exact test numerator/denominator and
composite SHA-256 of every deployed source and test file.

- [ ] **Step 3: Commit documentation**

```bash
git add work/wallet_copy_paper/README_wallet44b0_live.md
git commit -m "docs: record 44b0 live operating contract"
```

### Task 10: Deploy and verify the real service

**Files:**
- Server runtime: `/srv/polymarket-live/runtime/wallet_44b0_live`
- Server coordinator: `/srv/polymarket-live/runtime/shared_wallet/coordinator.sqlite3`
- Server service: `/etc/systemd/system/com.luke.polymarket.wallet-44b0-live.service`

- [ ] **Step 1: Create and inspect the retention dry-run manifest**

Run the tool on `polymarket-hk`, retain its JSON and hash, and verify the
measured free bytes after planned deletion exceed the measured candidate
backup/release/temp bytes.  Apply only the exact reviewed manifest.

- [ ] **Step 2: Build the candidate release and manifest**

Copy only the committed files, generate `MANIFEST.sha256`, verify it locally
and on the server, and record the candidate release path and composite hash.

- [ ] **Step 3: Run preflight with real authenticated collateral**

Require current tennis coordinated available cash at least `200 USD`, zero
active reservations, zero unsafe unresolved submissions, three SQLite
`integrity_check=ok`, and source address/config exact match.  If any condition
fails, stop before cash mutation.

- [ ] **Step 4: Execute the closed-loop release**

Run the release with one unique launch/transfer change ID.  Do not invoke any
manual order method and do not replay source history.

- [ ] **Step 5: Perform immediate acceptance**

Verify exactly one process for each of the three services, `NRestarts` without
a crash loop, fresh heartbeat, current head equal to or ahead of each
`last_processed_block`, new profile watermark equals its launch boundary,
new action receipts before the boundary equal zero, all SQLite integrity
checks pass, coordinator cash sums to authenticated collateral, tennis cash
fell exactly `200`, new cash equals `200`, existing PnL/fees/positions/scales
and cursors are unchanged, shared lock paths match, and real-order capability
is enabled only in the three authorized daemons.

- [ ] **Step 6: Commit the deployment evidence receipt**

Store a local JSON/Markdown receipt containing timestamps, source hashes,
service evidence, cursor evidence, cash transfer identity, coordinator receipt
hash, SQLite results, and action-conservation denominators.  Commit only this
receipt.

### Task 11: Register the current-task non-Netflix notification

**Files:**
- Codex automation state for the current task

- [ ] **Step 1: Create a quiet current-task heartbeat after service acceptance**

Use the Codex automation API, not a new chat.  The check reads the server's
unacknowledged `source_topic_alerts` and health status.  It returns no user
notification when the count is zero.  For each unseen action it reports source
market/event, side, source time, discovery time, and our processing state,
then records the alert identity as delivered without changing the trade ledger.

- [ ] **Step 2: Verify notification deduplication**

Run one fixture alert through two checks.  Expected: first check notifies once;
second check is quiet; the source action and execution transitions remain
unchanged.

- [ ] **Step 3: Hand off the live status**

Report profile PASS/FAIL, launch watermark, source actions observed after
launch, complete/partial/pending/external/error numerator and denominator,
cash allocation, available cash, positions, realized PnL, and non-Netflix
alert count.  Do not claim success until this evidence is current.
