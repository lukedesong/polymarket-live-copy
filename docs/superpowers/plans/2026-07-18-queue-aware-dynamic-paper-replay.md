# Queue-Aware Dynamic Paper Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the comparison-only strict/touch paper result with one authoritative queue-aware paper mode that uses official trade quantity, dynamic books, partial fills, and executable-depth liquidation without ever constructing a real order client.

**Architecture:** Keep the official WebSocket collector and order-book reconstruction as the fact source. Extend the SQLite ledger so queue state, partial fills, inventory marks, and liquidations are durable; keep queue matching in `PaperEngine`, and keep per-market fee parameters and dynamic book routing in `PaperRuntimeSink`. Expose the result through the existing status/export commands while retaining strict/touch as explicitly comparison-only modes.

**Tech Stack:** Python 3.12, `Decimal`, SQLite, pytest, official Polymarket Market WebSocket and CLOB market-info responses.

---

## File map

- Modify `work/world_cup_market_maker/src/world_cup_mm/market_params.py`: retain official fee-curve parameters needed for taker liquidation.
- Modify `work/world_cup_market_maker/src/world_cup_mm/storage.py`: migrate the paper schema, persist queue events/partial fills/dynamic marks/liquidations, and calculate depth-adjusted accounts.
- Modify `work/world_cup_market_maker/src/world_cup_mm/paper_engine.py`: maintain quote queues, match direction-compatible official trades, reprice only when best price moves, and force depth liquidation at blocking risk states.
- Modify `work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py`: pass full dynamic books, official trade side/size/fee evidence, and stable event identities into the engine.
- Modify `work/world_cup_market_maker/src/world_cup_mm/cli.py`: add authoritative `queue` mode and stable queue/depth/drift report fields.
- Modify `work/world_cup_market_maker/README.md`: document the single paper result, observable limitations, and no-real-order safety boundary.
- Modify the existing paper/storage/runtime/CLI/market-parameter tests and add focused queue/liquidation regression cases.

### Task 1: Persist official fee curves and queue-capable paper schema

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/market_params.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/storage.py`
- Test: `work/world_cup_market_maker/tests/test_market_params.py`
- Test: `work/world_cup_market_maker/tests/test_paper_storage.py`

- [ ] **Step 1: Write failing fee-curve and migration tests**

Add tests that require `ClobMarketParams` to expose `taker_fee_rate` and `fee_exponent`, parsed from official `fd.r` and `fd.e`, and require reopening a legacy paper database to add the new columns/tables without losing existing fills.

```python
def test_parse_clob_market_info_retains_official_fee_curve():
    result = parse_clob_market_info({
        "mos": 5, "mts": 0.01, "mbf": 0,
        "fd": {"r": 0.03, "e": 1, "to": True}, "t": [],
    })
    assert result.taker_fee_rate == Decimal("0.03")
    assert result.fee_exponent == 1

def test_store_migrates_existing_paper_schema(tmp_path):
    path = make_legacy_database(tmp_path)
    store = Store(path)
    assert store.connection.execute(
        "SELECT remaining_quantity_text FROM paper_orders"
    ).fetchall() == []
    assert store.connection.execute(
        "SELECT COUNT(*) FROM paper_queue_events"
    ).fetchone()[0] == 0
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cd work/world_cup_market_maker && pytest tests/test_market_params.py tests/test_paper_storage.py -q`

Expected: failures because fee curve fields, schema migration, and queue tables do not exist.

- [ ] **Step 3: Implement fee parsing and idempotent schema migration**

Extend `ClobMarketParams` with:

```python
taker_fee_rate: Decimal
fee_exponent: int
taker_only: bool
```

Add an idempotent `_migrate_schema()` called after `executescript(SCHEMA)`. It adds order fields including `original_quantity_text`, `remaining_quantity_text`, `queue_ahead_initial_text`, `queue_ahead_remaining_text`, `last_book_timestamp`, and `requeue_reason`; it creates `paper_queue_events`, `paper_inventory_marks`, and `paper_liquidations`; and it rebuilds `paper_fills` so multiple partial fills may reference one order while each official event remains uniquely consumed.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `cd work/world_cup_market_maker && pytest tests/test_market_params.py tests/test_paper_storage.py -q`

Expected: all focused tests pass.

- [ ] **Step 5: Commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/market_params.py \
        work/world_cup_market_maker/src/world_cup_mm/storage.py \
        work/world_cup_market_maker/tests/test_market_params.py \
        work/world_cup_market_maker/tests/test_paper_storage.py
git commit -m "feat: persist queue paper ledger fields"
```

### Task 2: Implement stable queue lifecycle and partial fills

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/storage.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_engine.py`
- Test: `work/world_cup_market_maker/tests/test_paper_storage.py`
- Test: `work/world_cup_market_maker/tests/test_paper_engine.py`

- [ ] **Step 1: Write failing queue behavior tests**

Add separate tests for these observable rules:

```python
def test_same_price_book_update_keeps_order_and_queue(tmp_path):
    store, paper = queue_engine(tmp_path)
    quote(paper, bid_size="10")
    first = store.open_paper_orders("asset-a")[0]
    quote(paper, bid_size="20")
    second = store.open_paper_orders("asset-a")[0]
    assert second.order_id == first.order_id
    assert second.queue_ahead_remaining == Decimal("10")

def test_at_price_trade_consumes_queue_then_partially_fills(tmp_path):
    store, paper = queue_engine(tmp_path)
    quote(paper, bid_size="10")
    paper.on_trade(
        asset_id="asset-a", trade_price=Decimal("0.49"),
        trade_side="SELL", trade_quantity=Decimal("12"),
        trigger_event_hash="trade-a", best_bid=Decimal("0.49"),
        risk_state=RiskState.PREMATCH_OPEN, now=NOW,
    )
    order = store.open_paper_orders("asset-a")[0]
    assert order.queue_ahead_remaining == Decimal("0")
    assert order.remaining_quantity == Decimal("3")
    assert store.paper_position("asset-a").quantity == Decimal("2")

def test_wrong_trade_side_cannot_consume_buy_queue(tmp_path):
    store, paper = queue_engine(tmp_path)
    quote(paper, bid_size="10")
    paper.on_trade(
        asset_id="asset-a", trade_price=Decimal("0.49"),
        trade_side="BUY", trade_quantity=Decimal("20"),
        trigger_event_hash="trade-b", best_bid=Decimal("0.49"),
        risk_state=RiskState.PREMATCH_OPEN, now=NOW,
    )
    assert store.open_paper_orders("asset-a")[0].queue_ahead_remaining == Decimal("10")
    assert store.paper_fill_count() == 0
```

Add mirror assertions showing that a size decrease without a compatible trade leaves queue ahead unchanged, a same-price size increase stays behind our order, and a trade-through fills the valid remaining quantity exactly once.

The synthetic fixtures use explicit `Decimal` values only to prove arithmetic; they are not trading thresholds.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_engine.py tests/test_paper_storage.py -q`

Expected: failures because orders are currently requoted on every book event and fills are whole-order only.

- [ ] **Step 3: Implement minimal queue APIs and matching**

Change order opening to accept the displayed quantity already at the quote:

```python
def open_paper_order(
    self,
    *,
    condition_id: str,
    market_id: str,
    asset_id: str,
    outcome: str,
    side: str,
    price: Decimal,
    quantity: Decimal,
    queue_ahead: Decimal,
    maker_fee_bps: int,
    quote_book_timestamp: str | None,
    created_at: datetime,
) -> int:
    """Open or retain one queue-aware paper order."""
```

Return the existing order unchanged when side and price match. When the best price changes, cancel with `best_price_changed`, create a new order, and reset queue ahead to the displayed quantity. Add:

```python
def consume_paper_trade(
    order_id: int,
    *,
    trigger_event_hash: str,
    trigger_price: Decimal,
    official_trade_quantity: Decimal,
    proof_type: str,
    filled_at: datetime,
    best_bid: Decimal,
) -> Decimal:
    """Consume observable queue and return paper-filled quantity."""
```

For `AT_PRICE_QUEUE`, consume queue ahead first and fill only the excess official quantity. For `TRADE_THROUGH`, fill the valid remainder. Update position, remaining order quantity, fill row, queue event, and account snapshot in one transaction.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_engine.py tests/test_paper_storage.py -q`

Expected: all queue and partial-fill tests pass.

- [ ] **Step 5: Commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/storage.py \
        work/world_cup_market_maker/src/world_cup_mm/paper_engine.py \
        work/world_cup_market_maker/tests/test_paper_storage.py \
        work/world_cup_market_maker/tests/test_paper_engine.py
git commit -m "feat: model observable FIFO paper fills"
```

### Task 3: Add dynamic executable-depth marks and forced liquidation

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/storage.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_engine.py`
- Test: `work/world_cup_market_maker/tests/test_paper_storage.py`
- Test: `work/world_cup_market_maker/tests/test_paper_engine.py`

- [ ] **Step 1: Write failing depth and drift tests**

```python
def test_mark_uses_all_bid_levels_and_reports_unliquidated_quantity(tmp_path):
    store = position_store(tmp_path, quantity="5", average_cost="0.50")
    mark = store.record_inventory_mark(
        asset_id="asset-a",
        bids=((Decimal("0.49"), Decimal("2")), (Decimal("0.48"), Decimal("1"))),
        best_ask=Decimal("0.51"),
        marked_at=NOW,
    )
    assert mark.liquidatable_quantity == Decimal("3")
    assert mark.liquidation_proceeds == Decimal("1.46")
    assert mark.unliquidated_quantity == Decimal("2")

def test_forced_liquidation_walks_depth_and_charges_official_taker_fee(tmp_path):
    sale = executable_sale(
        Decimal("3"),
        ((Decimal("0.60"), Decimal("1")), (Decimal("0.50"), Decimal("2"))),
        taker_fee_rate=Decimal("0.03"),
    )
    expected_fee = (
        Decimal("1") * Decimal("0.03") * Decimal("0.60") * Decimal("0.40")
        + Decimal("2") * Decimal("0.03") * Decimal("0.50") * Decimal("0.50")
    )
    assert sale.gross_proceeds == Decimal("1.60")
    assert sale.fee == expected_fee
    assert sale.unliquidated_quantity == Decimal("0")
```

Add a markout assertion after moving the bids below the fill price, and a liquidation assertion proving that depth shortage leaves the uncovered quantity in inventory.

The fee assertion must use the official formula `shares * fee_rate * price * (1 - price)` and official rounding behavior, with the fee-rate input supplied by the synthetic market fixture.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_engine.py tests/test_paper_storage.py -q`

Expected: failures because current marks use only best bid and no liquidation ledger exists.

- [ ] **Step 3: Implement reusable depth calculations and durable marks**

Add a pure calculation helper in `storage.py` (or a focused local dataclass in `paper_engine.py`) with this interface:

```python
def executable_sale(
    quantity: Decimal,
    bid_levels: Sequence[tuple[Decimal, Decimal]],
    *,
    taker_fee_rate: Decimal,
) -> DepthSale:
    """Calculate an executable sale without fabricating missing depth."""
```

It returns sold quantity, proceeds, VWAP, fee, and unliquidated quantity. `PaperEngine.on_book()` records a mark after every valid book event. Blocking risk states cancel quotes and call the same depth calculation to persist each executed liquidation layer; any uncovered remainder stays in the position.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_engine.py tests/test_paper_storage.py -q`

Expected: all dynamic-mark, drift, fee, and depth-shortfall tests pass.

- [ ] **Step 5: Commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/storage.py \
        work/world_cup_market_maker/src/world_cup_mm/paper_engine.py \
        work/world_cup_market_maker/tests/test_paper_storage.py \
        work/world_cup_market_maker/tests/test_paper_engine.py
git commit -m "feat: mark and liquidate paper inventory by depth"
```

### Task 4: Route complete official events into authoritative queue mode

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/cli.py`
- Test: `work/world_cup_market_maker/tests/test_paper_runtime.py`
- Test: `work/world_cup_market_maker/tests/test_cli.py`

- [ ] **Step 1: Write failing runtime and CLI tests**

Require runtime to pass `side`, `size`, current full bid/ask levels, timestamp, and event hash; require missing or invalid trade evidence to create no fill; require parser default `queue`; and require strict/touch status to say `comparison_only`.

```python
def test_parser_defaults_paper_run_to_authoritative_queue_mode():
    args = build_parser().parse_args(["paper-run"])
    assert args.fill_mode == "queue"

def test_status_marks_legacy_modes_comparison_only(tmp_path):
    store = Store(tmp_path / "paper.sqlite3")
    store.start_session("session-a", selection_mode="paper_frontier_touch", started_at=NOW)
    status = build_paper_status(store)
    assert status["authoritative"] is False
    assert status["result_role"] == "comparison_only"

def test_queue_runtime_uses_trade_side_and_size_for_partial_fill(tmp_path):
    store, sink = queue_sink(tmp_path)
    asyncio.run(feed_book_and_trade(sink, side="SELL", size="12"))
    assert store.paper_fill_rows()[0]["quantity"] == "2"

def test_invalid_trade_is_recorded_without_fill(tmp_path):
    store, sink = queue_sink(tmp_path)
    asyncio.run(feed_book_and_trade(sink, side="", size="not-a-number"))
    assert store.paper_fill_count() == 0
    assert store.paper_anomaly_count() == 1
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_runtime.py tests/test_cli.py -q`

Expected: failures because runtime drops trade side/size and CLI has no queue mode.

- [ ] **Step 3: Implement runtime routing and reporting**

Add `queue` to `--fill-mode`, make it the default, and include `authoritative` in status. Add a store method returning sorted Decimal book levels. Pass the raw official event hash to the engine and preserve anomaly counts instead of silently filling. Export stable fields for official trade quantity, queue ahead, partial/full fill counts, liquidation proceeds, fees, executable drift, and unliquidated quantity.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_runtime.py tests/test_cli.py -q`

Expected: all runtime and reporting tests pass.

- [ ] **Step 5: Commit**

```bash
git add work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py \
        work/world_cup_market_maker/src/world_cup_mm/cli.py \
        work/world_cup_market_maker/tests/test_paper_runtime.py \
        work/world_cup_market_maker/tests/test_cli.py
git commit -m "feat: expose authoritative queue paper mode"
```

### Task 5: Prove end-to-end safety and replay consistency

**Files:**
- Modify: `work/world_cup_market_maker/tests/test_paper_runtime.py`
- Modify: `work/world_cup_market_maker/tests/test_cli.py`
- Modify: `work/world_cup_market_maker/README.md`

- [ ] **Step 1: Write the failing end-to-end replay test**

Feed a full book, an at-price partial official trade, a top-price move, a trade-through event, and a blocking risk transition through `PaperRuntimeSink`. Assert that exported totals equal independent sums of fill and liquidation rows, price changes requeue rather than reuse old queue state, and the paper path never instantiates `AuthenticatedOrderControl` or calls any order-submission method.

- [ ] **Step 2: Run the end-to-end test and verify RED**

Run: `cd work/world_cup_market_maker && pytest tests/test_paper_runtime.py tests/test_cli.py -q`

Expected: the new integration assertion fails until all report/safety wiring is complete.

- [ ] **Step 3: Complete reporting and documentation**

Update README commands to run a dedicated queue database, explain that all prices and sizes are official empirical inputs, label the prematch risk windows as user-specified rather than optimized, and state that public data cannot reveal cancellation position within a same-price queue.

- [ ] **Step 4: Run all automated tests**

Run: `cd work/world_cup_market_maker && pytest -q`

Expected: all tests pass with no warnings or network-dependent live tests unless explicitly selected.

- [ ] **Step 5: Run a bounded official-data smoke collection**

Run the CLI against a new queue-only SQLite file, then inspect session mode, raw event count, book readiness, queue orders, fills, marks, anomaly rows, and confirm no authenticated trading client was created. The bound is a test-operation parameter, not a trading threshold, and must be stated when the smoke run is launched.

- [ ] **Step 6: Commit**

```bash
git add work/world_cup_market_maker/tests/test_paper_runtime.py \
        work/world_cup_market_maker/tests/test_cli.py \
        work/world_cup_market_maker/README.md
git commit -m "test: verify queue paper replay end to end"
```

## Numeric provenance and live gate

- Official book prices/sizes and official trade price/side/size are **empirical values** from stored WebSocket events.
- Minimum order size, tick size, and fee curve are **external constraint values** queried per market from CLOB market info.
- Prematch risk boundaries remain **user-specified values** and are not presented as optimized.
- All Decimal values inside unit tests are **synthetic test fixtures**, not deployable thresholds.
- No real order quantity, capital scale, profit hurdle, or live eligibility threshold is introduced by this plan. Any such number remains `BLOCK_DATA` until separately evidenced and authorized.
