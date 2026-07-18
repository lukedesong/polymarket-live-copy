# Dual Paper Fill Modes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run independent touch-fill and strict-trade-through paper simulations against official Polymarket data, without enabling real orders.

**Architecture:** Add an explicit fill-mode value to the paper engine and thread it through the runtime and CLI. Launch each mode as a separate process with a separate SQLite database, so inventory and profit/loss remain isolated while market selection, official feeds, and prematch risk rules remain identical.

**Tech Stack:** Python, argparse, asyncio, SQLite, pytest, official Polymarket Gamma/CLOB/WebSocket clients.

---

### Task 1: Define and test fill-mode behavior

**Files:**
- Modify: `work/world_cup_market_maker/tests/test_paper_engine.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_engine.py`

- [x] **Step 1: Write failing engine tests**

Add tests that construct `PaperEngine(..., fill_mode="touch")` and prove an equal-price BUY or SELL fills, while `fill_mode="strict"` still requires a trade through the quote. Also prove an unsupported mode raises `ValueError("unsupported_fill_mode:...")`.

- [x] **Step 2: Run the focused tests and verify RED**

Run: `PYTHONPATH=work/world_cup_market_maker/src work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests/test_paper_engine.py -q`

Expected: FAIL because `PaperEngine` does not accept `fill_mode`.

- [x] **Step 3: Implement the minimal engine mode switch**

Add `fill_mode: Literal["strict", "touch"] = "strict"` to `PaperEngine.__init__`, validate it, and use `<=`/`>=` only in touch mode while retaining `</>` in strict mode.

- [x] **Step 4: Run the focused tests and verify GREEN**

Run the same focused pytest command. Expected: PASS.

### Task 2: Thread fill mode through runtime and CLI

**Files:**
- Modify: `work/world_cup_market_maker/tests/test_cli.py`
- Modify: `work/world_cup_market_maker/tests/test_paper_runtime.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/cli.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py`

- [x] **Step 1: Write failing parser and runtime tests**

Assert `paper-run --fill-mode touch` parses the mode and that a `PaperRuntimeSink(..., fill_mode="touch")` fills an equal-price official trade. Assert the default remains strict for backward compatibility.

- [x] **Step 2: Run focused tests and verify RED**

Run: `PYTHONPATH=work/world_cup_market_maker/src work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests/test_cli.py work/world_cup_market_maker/tests/test_paper_runtime.py -q`

Expected: FAIL because the CLI and runtime do not accept the new argument.

- [x] **Step 3: Implement minimal argument propagation**

Add `--fill-mode` with choices `strict` and `touch`, default `strict`; pass it through `run_paper_collection` and `PaperRuntimeSink`; include `fill_mode` in status/export output so ledgers are self-describing.

- [x] **Step 4: Run focused tests and verify GREEN**

Run the same focused pytest command. Expected: PASS.

### Task 3: Verify and launch independent paper processes

**Files:**
- Preserve: `data/world_cup_paper_live.sqlite3`
- Create at runtime: `data/world_cup_paper_strict.sqlite3`
- Create at runtime: `data/world_cup_paper_touch.sqlite3`

- [x] **Step 1: Run the full automated test suite**

Run: `PYTHONPATH=work/world_cup_market_maker/src work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests -q`

Expected: all tests pass.

- [x] **Step 2: Stop only the superseded strict paper process after confirming its exact PID**

Resolve the current `paper-run` command and terminate that PID gracefully. Do not touch other processes and do not delete its database.

- [x] **Step 3: Seed both new ledgers with the same selected-market manifest**

Copy the existing paper database to each new ledger before starting. This preserves the same market selection while each new process starts independent paper orders, fills, positions, and profit/loss.

- [x] **Step 4: Start both paper-only processes**

Launch one process with `--fill-mode strict` and one with `--fill-mode touch`, each using the user-specified one-hour duration converted to `3600` seconds. Do not load real trading credentials and do not invoke any real-order command.

- [x] **Step 5: Verify live process and ledger isolation**

Read both process commands and run `paper-export` against both databases. Confirm each reports its own fill mode, connected session, official events, paper orders, fills, inventory, and profit/loss.

- [x] **Step 6: Commit the tested code and plan**

Commit only the plan, relevant tests, and source files; exclude runtime databases and logs.
