# Three-Wallet Minimum-Size Copy Paper Tracker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Launch the smallest reliable local paper tracker for the three approved wallets, with an independent user-specified 100 USD ledger for each account.

**Architecture:** One standard-library Python script reads public Data/CLOB APIs, applies minimum-size BUY/SELL actions to one SQLite database, and rewrites one local HTML/JSON status report. Small shell wrappers install and control one macOS LaunchAgent. No authenticated client or trading endpoint exists in the program.

**Tech Stack:** Python 3.12 standard library, SQLite, `Decimal`, `urllib`, pytest, macOS launchd.

---

### Task 1: Failing behavior tests

**Files:**
- Create: `work/wallet_copy_paper/tests/test_wallet_copy_paper.py`
- Create: `work/wallet_copy_paper/wallet_copy_paper.py`

- [ ] **Step 1: Write failing tests for the complete core contract**

Tests cover: three isolated 100 USD accounts; `takerOnly=false`; GET-only host/path allowlist; startup seeding without historical fills; source grouping by wallet/transaction/asset/side; BUY at current `min_order_size` through asks; SELL at current `min_order_size` through bids; cash/depth/position/stale skips; duplicate suppression; and safety markers.

```python
def test_buy_uses_current_minimum_and_only_matching_account(tmp_path):
    tracker = Tracker(tmp_path / "paper.sqlite3", ACCOUNTS)
    tracker.initialize()
    result = tracker.apply(ACTION_BUY, BOOK)
    assert result.quantity == BOOK.min_order_size
    assert tracker.cash("russell") < Decimal("100")
    assert tracker.cash("zorro") == Decimal("100")

def test_transport_is_public_get_only():
    with pytest.raises(ReadOnlyViolation):
        validate_public_request("POST", "https://clob.polymarket.com/order")
```

- [ ] **Step 2: Run tests and confirm RED**

Run: `cd work/wallet_copy_paper && python3 -m pytest tests/test_wallet_copy_paper.py -q`

Expected: collection fails because production symbols do not exist.

### Task 2: Minimal tracker and report

**Files:**
- Modify: `work/wallet_copy_paper/wallet_copy_paper.py`
- Modify: `work/wallet_copy_paper/tests/test_wallet_copy_paper.py`

- [ ] **Step 1: Implement only enough code to pass core tests**

The single script contains immutable account configuration, strict decimal/time parsing, an allowlisted GET transport, source grouping, depth walking, SQLite schema/transactions, startup watermark, one polling cycle, status calculation, and atomic HTML/JSON/CSV writes.

```python
ALLOWED = {
    "data-api.polymarket.com": {"/trades"},
    "clob.polymarket.com": {"/book"},
    "gamma-api.polymarket.com": {"/markets"},
}

def source_identity(row):
    raw = "|".join((row.wallet.lower(), row.transaction_hash.lower(), row.asset, row.side))
    return hashlib.sha256(raw.encode()).hexdigest()
```

- [ ] **Step 2: Run tests until GREEN, then add focused restart and report tests**

Run: `cd work/wallet_copy_paper && python3 -m pytest tests/test_wallet_copy_paper.py -q`

Expected: all tests pass, including restart idempotency and `paper_only: true` / `real_order_submitted: false`.

- [ ] **Step 3: Commit core implementation**

Commit: `git add work/wallet_copy_paper && git commit -m "feat: add minimal three-wallet paper tracker"`

### Task 3: Start, verify, and keep running

**Files:**
- Create: `work/wallet_copy_paper/start.sh`
- Create: `work/wallet_copy_paper/stop.sh`
- Create: `work/wallet_copy_paper/com.luke.polymarket.wallet-copy-paper.plist`
- Create: `work/wallet_copy_paper/README.md`

- [ ] **Step 1: Add a live read-only smoke test**

The test reads each approved wallet with `takerOnly=false`; when a recent trade exists, it reads that token's public order book and checks `min_order_size` is present.

- [ ] **Step 2: Run full verification**

Run: `cd work/wallet_copy_paper && python3 -m pytest -q`

Run: `cd work/wallet_copy_paper && ! rg -n "POLYMARKET_(PRIVATE_KEY|API_KEY|API_SECRET|API_PASSPHRASE)|post_order|create_order|cancel_order" wallet_copy_paper.py start.sh stop.sh com.luke.polymarket.wallet-copy-paper.plist`

Expected: tests pass and the safety scan finds no secret or order-submission symbols.

- [ ] **Step 3: Install and start one LaunchAgent**

`start.sh` initializes the database, seeds historical actions without copying them, installs the plist, and starts one tracker process. The process rewrites `runtime/status.html`, `runtime/status.json`, and `runtime/ledger.csv` after each successful cycle.

- [ ] **Step 4: Verify runtime evidence**

Check LaunchAgent state, unique process count, advancing heartbeat, SQLite `integrity_check`, three account balances, local status files, and safety markers. Commit operations files, merge locally, and start from the main project path.
