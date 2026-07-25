# Paper Dashboard Realized Trade Detail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add clear open-position and closed-record sections with formula-derived per-record realized profit and loss to both running paper dashboards.

**Architecture:** A small shared read-only ledger replay module reconstructs weighted-average cost by asset from executed paper ledger rows. Each tracker enriches its existing status payload with occupied cost, closed records, and a reconciliation flag, then renders the same approved account-statement layout without changing SQLite or execution logic.

**Tech Stack:** Python standard library, `Decimal`, SQLite, static HTML, pytest, macOS LaunchAgents.

---

## File Structure

- Create `work/wallet_copy_paper/paper_dashboard_statement.py`: shared ledger replay and Shanghai-time formatting.
- Create `work/wallet_copy_paper/tests/test_paper_dashboard_statement.py`: synthetic ledger replay tests.
- Modify `work/wallet_copy_paper/weather_proportional_paper.py`: add read-only statement fields and the new page sections.
- Modify `work/wallet_copy_paper/tests/test_weather_proportional_paper.py`: weather integration and preservation tests.
- Modify `work/wallet_copy_paper/tian_wen_speech_paper.py`: add read-only statement fields and the new page sections.
- Modify `work/wallet_copy_paper/tests/test_tian_wen_speech_paper.py`: Tian-Wen integration and preservation tests.

No database schema, runtime database, balance, position, source state, sizing
rule, or simulated execution function is modified.

### Task 1: Shared read-only ledger replay

**Files:**
- Create: `work/wallet_copy_paper/paper_dashboard_statement.py`
- Create: `work/wallet_copy_paper/tests/test_paper_dashboard_statement.py`

- [ ] **Step 1: Write failing replay tests**

Use synthetic test fixtures only; their amounts are test data, not strategy
parameters.

```python
from decimal import Decimal

from paper_dashboard_statement import build_account_statement


D = Decimal


def executed_row(
    row_id,
    *,
    asset,
    side="",
    kind="REBALANCE",
    status="FILLED",
    quantity,
    notional,
    fee="0",
    reason="",
):
    return {
        "id": row_id,
        "observed_at": 1784950000 + row_id,
        "asset": asset,
        "side": side,
        "kind": kind,
        "status": status,
        "quantity": quantity,
        "notional": notional,
        "fee": fee,
        "reason": reason,
    }


def test_replay_separates_open_cost_and_realized_sell_pnl():
    rows = [
        executed_row(
            1, asset="a", side="BUY", quantity="10", notional="3", fee="0.1"
        ),
        executed_row(
            2, asset="a", side="SELL", quantity="4", notional="2", fee="0.05"
        ),
    ]

    statement = build_account_statement(
        rows,
        metadata_by_asset={
            "a": {"title": "Market A", "outcome": "Yes"}
        },
    )

    assert statement["closed_records"][0]["cost_basis"] == "1.24"
    assert statement["closed_records"][0]["net_recovered"] == "1.95"
    assert statement["closed_records"][0]["realized_pnl"] == "0.71"
    assert statement["positions_by_asset"]["a"]["quantity"] == "6"
    assert statement["positions_by_asset"]["a"]["average_cost"] == "0.31"


def test_replay_calculates_winning_and_losing_settlements():
    rows = [
        executed_row(
            1, asset="win", side="BUY", quantity="5", notional="2", fee="0"
        ),
        executed_row(
            2, asset="lose", side="BUY", quantity="4", notional="1", fee="0"
        ),
        executed_row(
            3,
            asset="win",
            kind="SETTLEMENT",
            status="SETTLED",
            quantity="5",
            notional="5",
            reason="WINNER",
        ),
        executed_row(
            4,
            asset="lose",
            kind="SETTLEMENT",
            status="SETTLED",
            quantity="4",
            notional="0",
            reason="LOSER",
        ),
    ]

    statement = build_account_statement(rows, metadata_by_asset={})
    records = {row["asset"]: row for row in statement["closed_records"]}

    assert records["win"]["close_type"] == "结算盈利"
    assert records["win"]["realized_pnl"] == "3"
    assert records["lose"]["close_type"] == "结算亏损"
    assert records["lose"]["realized_pnl"] == "-1"


def test_skipped_rows_do_not_change_statement():
    rows = [
        executed_row(
            1,
            asset="a",
            side="BUY",
            status="SKIPPED",
            quantity="0",
            notional="0",
        )
    ]

    statement = build_account_statement(rows, metadata_by_asset={})

    assert statement["closed_records"] == []
    assert statement["reconstructed_realized_pnl"] == "0"
    assert statement["replay_errors"] == []
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
cd /Users/luke/Documents/polymarket/work/wallet_copy_paper
uv run --with pytest python -m pytest -q \
  tests/test_paper_dashboard_statement.py
```

Expected: collection fails because `paper_dashboard_statement` does not exist.

- [ ] **Step 3: Implement the minimal replay module**

Implement:

```python
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


D = Decimal
ZERO = D("0")
SHANGHAI = ZoneInfo("Asia/Shanghai")
EXECUTED_STATUSES = {"FILLED", "PARTIAL", "SETTLED"}


def format_observed_at_shanghai(value: Any) -> str:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return "未知"
    return datetime.fromtimestamp(timestamp, SHANGHAI).strftime(
        "%Y-%m-%d %H:%M:%S（上海时间）"
    )


def build_account_statement(
    ledger_rows: Iterable[Mapping[str, Any]],
    *,
    metadata_by_asset: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    states: dict[str, tuple[Decimal, Decimal]] = defaultdict(
        lambda: (ZERO, ZERO)
    )
    closed_records: list[dict[str, str]] = []
    errors: list[str] = []
    reconstructed = ZERO

    for row in ledger_rows:
        if str(row.get("status", "")) not in EXECUTED_STATUSES:
            continue
        asset = str(row.get("asset", ""))
        quantity = D(str(row.get("quantity", "0")))
        notional = D(str(row.get("notional", "0")))
        fee = D(str(row.get("fee", "0")))
        held, average_cost = states[asset]
        side = str(row.get("side", ""))
        kind = str(row.get("kind", ""))

        if side == "BUY":
            new_quantity = held + quantity
            new_average = (
                (held * average_cost + notional + fee) / new_quantity
                if new_quantity > ZERO
                else ZERO
            )
            states[asset] = (new_quantity, new_average)
            continue

        if side != "SELL" and kind != "SETTLEMENT":
            continue
        if quantity <= ZERO or quantity > held:
            errors.append(f"ledger:{row.get('id', '')}:invalid_close_quantity")
            continue

        cost_basis = average_cost * quantity
        net_recovered = notional - fee
        realized_pnl = net_recovered - cost_basis
        reconstructed += realized_pnl
        metadata = metadata_by_asset.get(asset, {})
        reason = str(row.get("reason", ""))
        close_type = (
            "结算盈利"
            if kind == "SETTLEMENT" and reason == "WINNER"
            else "结算亏损"
            if kind == "SETTLEMENT"
            else "卖出"
        )
        closed_records.append(
            {
                "ledger_id": str(row.get("id", "")),
                "observed_at": str(row.get("observed_at", "")),
                "close_time_shanghai": format_observed_at_shanghai(
                    row.get("observed_at")
                ),
                "asset": asset,
                "title": str(metadata.get("title", asset)),
                "outcome": str(metadata.get("outcome", "")),
                "close_type": close_type,
                "quantity": str(quantity),
                "cost_basis": str(cost_basis),
                "net_recovered": str(net_recovered),
                "fee": str(fee),
                "realized_pnl": str(realized_pnl),
            }
        )
        remaining = held - quantity
        states[asset] = (
            remaining,
            average_cost if remaining > ZERO else ZERO,
        )

    return {
        "closed_records": list(reversed(closed_records)),
        "positions_by_asset": {
            asset: {
                "quantity": str(quantity),
                "average_cost": str(average_cost),
            }
            for asset, (quantity, average_cost) in states.items()
            if quantity > ZERO
        },
        "reconstructed_realized_pnl": str(reconstructed),
        "replay_errors": errors,
    }
```

- [ ] **Step 4: Run the replay tests and verify GREEN**

Run the same pytest command. Expected: all replay tests pass with no warnings.

- [ ] **Step 5: Commit the shared replay**

```bash
git add \
  work/wallet_copy_paper/paper_dashboard_statement.py \
  work/wallet_copy_paper/tests/test_paper_dashboard_statement.py
git commit -m "feat: reconstruct paper trade pnl"
```

### Task 2: Integrate the weather dashboard

**Files:**
- Modify: `work/wallet_copy_paper/weather_proportional_paper.py`
- Modify: `work/wallet_copy_paper/tests/test_weather_proportional_paper.py`

- [ ] **Step 1: Write failing weather status and HTML tests**

Create a weather paper buy followed by a simulated sell, then assert:

```python
status = store.status()
closed = status["closed_records"][0]
assert closed["close_type"] == "卖出"
assert D(closed["realized_pnl"]) == (
    D(closed["net_recovered"]) - D(closed["cost_basis"])
)
assert status["pnl_reconciliation_ok"] is True

render_status_files(store, runtime_dir, poll_seconds=1)
html = (runtime_dir / "status.html").read_text()
assert "<h2>持仓中</h2>" in html
assert "<h2>已结束</h2>" in html
assert "单笔盈亏" in html
assert "尚未实现" in html
```

Snapshot cash, positions, source state, and ledger before rendering; assert all
snapshots remain identical after rendering.

- [ ] **Step 2: Run the focused weather tests and verify RED**

```bash
uv run --with pytest python -m pytest -q \
  tests/test_weather_proportional_paper.py \
  -k 'closed_record or account_statement'
```

Expected: failure because `closed_records`, reconciliation status, and the new
headings are absent.

- [ ] **Step 3: Add weather read-only statement data**

Import `build_account_statement`. In `PaperStore.status()`:

- read the complete ledger in ledger-id order;
- read title/outcome metadata for all paper-position assets;
- replay the ledger;
- add `occupied_cost`, `position_status="持仓中"`, and
  `pnl_status="尚未实现"` to each positive position;
- add `closed_records`, `reconstructed_realized_pnl`, `replay_errors`, and
  `pnl_reconciliation_ok`.

Use USD cent precision only for the reconciliation display check:

```python
USD_CENT = Decimal("0.01")  # External constraint: USD display precision.
pnl_reconciliation_ok = (
    not statement["replay_errors"]
    and _decimal(statement["reconstructed_realized_pnl"]).quantize(USD_CENT)
    == _decimal(account["realized_pnl"]).quantize(USD_CENT)
)
```

- [ ] **Step 4: Render weather `持仓中` and `已结束` sections**

Add the approved columns, `profit`, `loss`, and `neutral` styles, plus a visible
warning when reconciliation fails. Keep the existing recent-action table.

- [ ] **Step 5: Run weather tests and verify GREEN**

```bash
uv run --with pytest python -m pytest -q \
  tests/test_weather_proportional_paper.py
```

Expected: all weather tests pass.

- [ ] **Step 6: Commit the weather integration**

```bash
git add \
  work/wallet_copy_paper/weather_proportional_paper.py \
  work/wallet_copy_paper/tests/test_weather_proportional_paper.py
git commit -m "feat: show weather paper trade pnl"
```

### Task 3: Integrate the Tian-Wen dashboard

**Files:**
- Modify: `work/wallet_copy_paper/tian_wen_speech_paper.py`
- Modify: `work/wallet_copy_paper/tests/test_tian_wen_speech_paper.py`

- [ ] **Step 1: Write failing Tian-Wen status and HTML tests**

Extend the existing buy/sell and settlement fixtures to assert:

```python
status = store.status()
assert status["closed_records"][0]["close_type"] in {
    "卖出", "结算盈利", "结算亏损"
}
assert status["pnl_reconciliation_ok"] is True

tracker.render_status_files(store, runtime_dir, poll_seconds=1)
html = (runtime_dir / "status.html").read_text()
assert "<h2>持仓中</h2>" in html
assert "<h2>已结束</h2>" in html
assert "单笔盈亏" in html
assert "尚未实现" in html
```

Assert rendering does not change cash, paper positions, processed-source rows,
source state, or ledger rows.

- [ ] **Step 2: Run focused Tian-Wen tests and verify RED**

```bash
uv run --with pytest python -m pytest -q \
  tests/test_tian_wen_speech_paper.py \
  -k 'closed_record or account_statement'
```

Expected: failure because the statement fields and sections are absent.

- [ ] **Step 3: Add Tian-Wen read-only statement data**

Use the same shared replay and reconciliation semantics as the weather tracker.
Preserve all Tian-Wen-specific status fields, source-action handling, and
cursor safety state.

- [ ] **Step 4: Render Tian-Wen `持仓中` and `已结束` sections**

Use the same approved labels, columns, and styles. Retain the current sizing
note, account details, and recent-action table.

- [ ] **Step 5: Run Tian-Wen tests and verify GREEN**

```bash
uv run --with pytest python -m pytest -q \
  tests/test_tian_wen_speech_paper.py
```

Expected: all Tian-Wen tests pass.

- [ ] **Step 6: Commit the Tian-Wen integration**

```bash
git add \
  work/wallet_copy_paper/tian_wen_speech_paper.py \
  work/wallet_copy_paper/tests/test_tian_wen_speech_paper.py
git commit -m "feat: show Tian-Wen paper trade pnl"
```

### Task 4: Full verification and live paper deployment

**Files:**
- Verify: all files above
- Runtime outputs:
  `work/wallet_copy_paper/weather_proportional_runtime/status.html`
  and
  `work/wallet_copy_paper/tian_wen_speech_runtime/status.html`

- [ ] **Step 1: Run the complete relevant test suite**

```bash
cd /Users/luke/Documents/polymarket/work/wallet_copy_paper
uv run --with pytest python -m pytest -q \
  tests/test_paper_dashboard_statement.py \
  tests/test_weather_proportional_paper.py \
  tests/test_tian_wen_speech_paper.py
```

Expected: all tests pass with no warnings.

- [ ] **Step 2: Verify no execution-path changes**

Review the diff and confirm only status reads, statement replay, HTML rendering,
and tests changed. Confirm there is no modification to order simulation,
position sizing, source cursors, settlement writes, or request methods.

- [ ] **Step 3: Integrate the feature branch locally**

Merge the verified feature branch into the main checkout. Re-run the complete
relevant test suite on the merged result before touching the running services.

- [ ] **Step 4: Restart both paper LaunchAgents**

```bash
cd /Users/luke/Documents/polymarket
work/wallet_copy_paper/start_weather_proportional.sh
work/wallet_copy_paper/start_tian_wen_speech_paper.sh
```

If a bootstrap race occurs, wait until `launchctl print` confirms the old
service is absent, then bootstrap the already-copied plist once.

- [ ] **Step 5: Verify both live pages and databases**

For both runtimes:

- `launchctl print` reports one running service;
- heartbeat advances;
- `last_error` is empty;
- SQLite `PRAGMA integrity_check` returns `ok`;
- the page contains `持仓中`, `已结束`, `单笔盈亏`, and `尚未实现`;
- status JSON `pnl_reconciliation_ok` is true;
- the sum of displayed single-record results reconciles with account realized
  profit and loss at displayed currency precision;
- page cash and occupied capital match the read-only SQLite snapshot;
- `paper_only=true`;
- `real_order_submitted=false`.

- [ ] **Step 6: Report the deployed result**

Give the user both local page links, explain the open-versus-closed profit and
loss scope in one sentence, and report only freshly verified runtime facts.
