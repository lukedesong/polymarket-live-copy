# World Cup Paper Live Google Sheet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run conservative real-time paper market making on selected Polymarket World Cup markets and publish cost, proceeds, inventory, fills, and profit/loss to a simple Google Sheet.

**Architecture:** Extend the existing SQLite-backed WebSocket collector with an isolated paper execution engine. Official CLOB parameters supply minimum order size and maker fee; official trade events supply conservative trade-through fill evidence. SQLite remains the source of truth, while a stable export is copied into a two-tab Google Sheet by a recurring Codex automation.

**Tech Stack:** Python 3.12, `Decimal`, SQLite, `websockets`, official Polymarket Gamma/CLOB APIs, pytest, native Google Sheets connector, Codex automation.

---

### Task 1: Persist paper orders, fills, positions, and account snapshots

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/storage.py`
- Test: `work/world_cup_market_maker/tests/test_paper_storage.py`

- [ ] **Step 1: Write failing storage tests**

Create tests proving an open paper order can be inserted once, the same official trigger cannot produce a duplicate fill, position and account changes commit atomically, and the latest account summary can be read after restart.

```python
def test_fill_is_idempotent_and_updates_position_atomically(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    order_id = store.open_paper_order(
        condition_id="condition-a",
        asset_id="asset-a",
        side="BUY",
        price=Decimal("0.49"),
        quantity=Decimal("5"),
        quote_book_timestamp="1000",
        created_at=NOW,
    )
    first = store.apply_paper_fill(
        order_id,
        trigger_event_hash="trade-a",
        trigger_price=Decimal("0.48"),
        filled_at=NOW,
        best_bid=Decimal("0.48"),
    )
    second = store.apply_paper_fill(
        order_id,
        trigger_event_hash="trade-a",
        trigger_price=Decimal("0.48"),
        filled_at=NOW,
        best_bid=Decimal("0.48"),
    )
    assert first is True
    assert second is False
    assert store.paper_position("asset-a").quantity == Decimal("5")
```

- [ ] **Step 2: Run the new tests and verify failure**

Run: `work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests/test_paper_storage.py -q`

Expected: failure because paper tables and methods do not exist.

- [ ] **Step 3: Add schema and transactional storage methods**

Add `paper_orders`, `paper_fills`, `paper_positions`, `paper_account_snapshots`, and `paper_report_sync`. Store all prices, sizes, amounts, and profit/loss as decimal text. Enforce unique `trigger_event_hash` per filled order and update fill, position, and snapshot inside one SQLite transaction.

- [ ] **Step 4: Run storage tests**

Expected: all new storage tests pass.

- [ ] **Step 5: Commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/storage.py work/world_cup_market_maker/tests/test_paper_storage.py
git commit -m "feat: persist paper market-making ledger"
```

### Task 2: Fetch official minimum order size and maker fee

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/market_params.py`
- Test: `work/world_cup_market_maker/tests/test_market_params.py`

- [ ] **Step 1: Write failing response-parsing tests**

```python
def test_parse_clob_market_info_uses_official_minimum_and_maker_fee():
    result = parse_clob_market_info({"mos": 5, "mbf": 0, "mts": 0.01})
    assert result.minimum_order_size == Decimal("5")
    assert result.maker_fee_bps == 0
    assert result.tick_size == Decimal("0.01")
```

- [ ] **Step 2: Verify failure**

Run the targeted test and expect the parser module to be missing.

- [ ] **Step 3: Implement the public CLOB client**

Fetch `GET https://clob.polymarket.com/clob-markets/{condition_id}` with an explicit application user agent, parse `mos`, `mbf`, and `mts` into immutable typed values, reject missing or non-positive order sizes/ticks, and never substitute a hard-coded size.

- [ ] **Step 4: Run tests and commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/market_params.py work/world_cup_market_maker/tests/test_market_params.py
git commit -m "feat: load official CLOB market parameters"
```

### Task 3: Implement conservative paper quoting and fills

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/paper_engine.py`
- Test: `work/world_cup_market_maker/tests/test_paper_engine.py`

- [ ] **Step 1: Write failing engine tests**

Cover buy fill only when trade price is strictly below the paper bid, sell fill only when trade price is strictly above the paper ask, equality does not fill, repeated trade events are idempotent, shorts are rejected, weighted average cost is correct, and unrealized profit uses best bid.

```python
def test_touch_does_not_fill_but_trade_through_does():
    engine.quote(asset_id="asset-a", best_bid=Decimal("0.49"), best_ask=Decimal("0.51"))
    assert engine.on_trade(asset_id="asset-a", price=Decimal("0.49"), event_hash="touch") == []
    fills = engine.on_trade(asset_id="asset-a", price=Decimal("0.48"), event_hash="through")
    assert fills[0].price == Decimal("0.49")
```

- [ ] **Step 2: Verify the tests fail**

Run the targeted engine file and expect missing engine symbols.

- [ ] **Step 3: Implement the engine**

Maintain one live paper buy order per eligible asset in `PREMATCH_OPEN`. Place paper sells only for non-zero inventory. Requote after book changes, preserve order audit history, process only `last_trade_price` events received while the book is ready, and compute all cash and inventory values with `Decimal`.

- [ ] **Step 4: Add risk-state behavior**

At the user-specified `NO_NEW_INVENTORY` boundary cancel buys; at `REDUCE_ONLY` quote sells up to inventory; at cancel/in-play/data-blocked states cancel all paper orders and disable fill processing.

- [ ] **Step 5: Run engine and existing risk tests, then commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/paper_engine.py work/world_cup_market_maker/tests/test_paper_engine.py
git commit -m "feat: simulate conservative paper maker fills"
```

### Task 4: Integrate paper live runtime and commands

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/cli.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/collector.py`
- Create: `work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py`
- Test: `work/world_cup_market_maker/tests/test_paper_runtime.py`
- Test: `work/world_cup_market_maker/tests/test_cli.py`

- [ ] **Step 1: Write failing runtime tests**

Prove WebSocket book events create/requote orders, official trade events route to the engine, disconnect cancels paper orders and prevents fills, restart recovers positions, and no authenticated order client is constructed.

- [ ] **Step 2: Verify failure**

Run the targeted runtime and CLI tests.

- [ ] **Step 3: Implement `paper-run`, `paper-status`, and `paper-export`**

`paper-run` selects the latest frontier manifest, fetches official parameters, starts the market and sports WebSockets, and writes a PID/health record. `paper-status` returns connection, orders, fills, inventory, cost, proceeds, realized, unrealized, and total profit/loss. `paper-export` emits a stable JSON object with `overview` and `fills` arrays.

- [ ] **Step 4: Prove the paper command cannot place real orders**

Add a test that patches `ClobClient` to raise if constructed with credentials and run `paper-run` through a bounded fake collector. The command must pass without touching the authenticated order path.

- [ ] **Step 5: Run tests and commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/cli.py work/world_cup_market_maker/src/world_cup_mm/collector.py work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py work/world_cup_market_maker/tests
git commit -m "feat: run World Cup paper market making"
```

### Task 5: Create the simple Google Sheet and sync contract

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/sheet_export.py`
- Test: `work/world_cup_market_maker/tests/test_sheet_export.py`
- Modify: `work/world_cup_market_maker/README.md`

- [ ] **Step 1: Write failing export tests**

Prove the overview contains update time, status, cost, proceeds, realized, unrealized, total profit/loss and inventory; prove each fill row contains time, market, outcome, side, price, quantity, amount, resulting inventory, resulting realized profit/loss, and evidence.

- [ ] **Step 2: Implement stable sheet rows and verify tests**

Return values without locale-specific parsing and keep the local database as source of truth.

- [ ] **Step 3: Create a new native Google Sheet**

Create `Polymarket 世界杯赛前做市纸面盈亏` with only `总览` and `成交明细`. Verify its spreadsheet ID differs from the existing BTC workbook. Populate it from `paper-export` and verify written cells by connector readback.

- [ ] **Step 4: Create recurring refresh automation**

Create a local Codex automation that reads `data/world_cup_mm.sqlite3`, runs `paper-export`, updates only the two owned tabs, and records sync success/failure. Use the shortest stable recurrence supported by the automation API; this cadence is a reporting constraint, not a trading parameter.

- [ ] **Step 5: Commit local code and documentation**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/sheet_export.py work/world_cup_market_maker/tests/test_sheet_export.py work/world_cup_market_maker/README.md
git commit -m "feat: export paper profit and fills to Google Sheets"
```

### Task 6: Start paper live and verify end to end

**Files:**
- Runtime: `data/world_cup_mm.sqlite3`

- [ ] **Step 1: Run the complete deterministic suite**

Run: `work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests -q -m 'not live'`

Expected: all deterministic tests pass.

- [ ] **Step 2: Refresh the official market scan**

Run `world-cup-mm scan`, verify at least one eligible future World Cup market, and persist the manifest.

- [ ] **Step 3: Start `paper-run` in a durable local process**

Verify one collector process is running, WebSocket events advance, paper orders exist, and authenticated order submission count remains zero.

- [ ] **Step 4: Verify accounting and Google Sheet**

Recompute positions and profit/loss from fills, compare to `paper-status`, compare `paper-export` to both Google Sheet tabs, and confirm any zero-fill result is caused by absence of valid trade-through rather than a dead collector.

- [ ] **Step 5: Report the paper-live URL and current status**

Return the verified Google Sheet URL, running state, current fills, cost, proceeds, realized, unrealized, total profit/loss, and the independent real-order gate status.
