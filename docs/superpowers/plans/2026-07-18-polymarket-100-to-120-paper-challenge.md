# Polymarket $100 to $120 Paper Challenge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one auditable, paper-only Polymarket account with user-specified $100 starting cash for the user-specified 12-hour window and truthfully evaluate whether depth-liquidated equity reaches the user-specified $120 target.

**Architecture:** Extend the existing public-data queue paper engine with universal active-market discovery, a persisted cash/reservation ledger, capital-aware quoting, and challenge status/deadline reporting. Reuse the existing official CLOB WebSocket collector, queue model, partial fills, official fee curve, and depth liquidation; do not add any authenticated order submission path.

**Tech Stack:** Python 3.12, asyncio, SQLite, Decimal, official Gamma REST API, official CLOB REST/WebSocket, pytest.

---

### Task 1: Universal active-market discovery

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/challenge_discovery.py`
- Test: `work/world_cup_market_maker/tests/test_challenge_discovery.py`

- [ ] **Step 1: Write failing tests** covering active/order-accepting binary markets, invalid token IDs, expired markets, and deterministic ranking by observed volume, liquidity, and quoted spread.
- [ ] **Step 2: Run** `.venv/bin/pytest tests/test_challenge_discovery.py -q` and verify failure because the module is absent.
- [ ] **Step 3: Implement** a paginated Gamma events client and pure parsing/ranking functions. Use event/market end time as the risk deadline for non-sports markets and game start time when a future official sports start exists.
- [ ] **Step 4: Run the focused tests** and verify all pass.
- [ ] **Step 5: Commit** the discovery module and tests.

### Task 2: Persisted challenge account

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/storage.py`
- Create: `work/world_cup_market_maker/src/world_cup_mm/challenge_account.py`
- Test: `work/world_cup_market_maker/tests/test_challenge_account.py`

- [ ] **Step 1: Write failing tests** for one-time challenge creation, restart-stable start/deadline, available cash after open-buy reservations, cash after partial/full fills, depth-liquidated equity, and rejection of orders exceeding cash.
- [ ] **Step 2: Run** `.venv/bin/pytest tests/test_challenge_account.py -q` and verify the missing API fails.
- [ ] **Step 3: Add SQLite tables** for challenge metadata and equity snapshots, plus Decimal-based account reconciliation derived from existing paper orders, fills, positions, liquidation marks, and fees.
- [ ] **Step 4: Implement** an account policy that returns the maximum affordable quantity at a quoted price while reserving all other open buys; never allow negative available cash.
- [ ] **Step 5: Run focused tests** and verify all pass.
- [ ] **Step 6: Commit** the ledger implementation and tests.

### Task 3: Capital-aware queue paper engine

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_engine.py`
- Modify: `work/world_cup_market_maker/src/world_cup_mm/paper_runtime.py`
- Test: `work/world_cup_market_maker/tests/test_paper_engine.py`
- Test: `work/world_cup_market_maker/tests/test_paper_runtime.py`

- [ ] **Step 1: Write failing tests** showing that a buy quote uses an affordable quantity, preserves queue position at an unchanged price, cancels/requotes on price change, and stops quoting when cash is fully reserved.
- [ ] **Step 2: Run the focused tests** and verify the new account-policy cases fail.
- [ ] **Step 3: Inject** the challenge account policy into the paper engine. Continue using official minimum order size as the floor; skip the quote when the affordable quantity is below that external constraint.
- [ ] **Step 4: Record** an equity snapshot after every book event and fill using executable liquidation value rather than midpoint.
- [ ] **Step 5: Run focused tests** and verify all pass.
- [ ] **Step 6: Commit** the engine integration and tests.

### Task 4: Challenge CLI and reporting

**Files:**
- Modify: `work/world_cup_market_maker/src/world_cup_mm/cli.py`
- Modify: `work/world_cup_market_maker/README.md`
- Test: `work/world_cup_market_maker/tests/test_cli.py`

- [ ] **Step 1: Write failing CLI tests** for `challenge-scan`, `challenge-run`, and `challenge-status`, including paper-only output and restart-stable deadline.
- [ ] **Step 2: Run** `.venv/bin/pytest tests/test_cli.py -q` and verify the new commands are absent.
- [ ] **Step 3: Implement** `challenge-scan` using the universal discovery module and official CLOB market constraints, `challenge-run` using queue mode and the persisted account, and `challenge-status` showing cash, reserved cash, inventory cost, realized profit, executable liquidation value, equity, return, target gap, official trade count, paper fill count, start, and deadline.
- [ ] **Step 4: Ensure** these commands never read private keys, API credentials, wallet addresses, or instantiate the authenticated CLOB client.
- [ ] **Step 5: Run focused tests** and verify all pass.
- [ ] **Step 6: Commit** CLI, documentation, and tests.

### Task 5: Full verification and launch

**Files:**
- Create at runtime: `data/polymarket_100_to_120_challenge.sqlite3`
- Create at runtime: `data/polymarket_100_to_120_challenge.log`

- [ ] **Step 1: Run** `.venv/bin/pytest tests -m "not live" -q` and require a clean pass.
- [ ] **Step 2: Run the official-data smoke tests** for Gamma discovery, CLOB constraints, and public WebSocket connectivity.
- [ ] **Step 3: Create a fresh database** with starting cash `100`, target equity `120`, and duration derived as `12 * 60 * 60` seconds; store all three as user-specified experiment inputs.
- [ ] **Step 4: Start one detached paper-only runner** and record its process identity and log path.
- [ ] **Step 5: Verify** process health, public-data connection, selected markets, account reconciliation, zero authenticated orders, and a fixed deadline.
- [ ] **Step 6: Report** the actual starting status and continue monitoring until the deadline; at deadline cancel paper quotes and evaluate executable liquidation equity without altering recorded fills.
