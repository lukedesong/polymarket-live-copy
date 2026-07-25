# Paper Dashboard Realized Trade Detail Design

## Goal

Make both running paper dashboards understandable as account statements without
changing paper balances, positions, source tracking, sizing, order simulation, or
settlement logic.

The dashboard must clearly separate:

- account-level realized profit and loss;
- available cash;
- capital still occupied by open positions;
- open positions whose profit and loss is not yet realized;
- closed quantities with a separately calculated realized profit or loss.

This remains paper-only. No private endpoint, order submission, cancellation,
transfer, or wallet credential is introduced.

## Approved Profit-and-Loss Scope

Open positions do not receive a live or estimated profit-and-loss value. Their
profit-and-loss cell reads `尚未实现`, while their cost basis remains visible.

Only a simulated sell or a market settlement creates a single-record realized
profit or loss:

- sell realized profit and loss =
  sell notional - sell fee - closed quantity cost basis;
- settlement realized profit and loss =
  settlement payout - settled quantity cost basis.

The inputs are empirical paper-ledger values. Each displayed single-record
profit or loss is formula-derived from those values. No current midpoint, Bid,
Ask, or mark-to-market estimate is mixed into realized results.

## Ledger Replay

The status renderer reconstructs cost basis from the complete paper ledger in
ascending ledger order. It keeps quantity and weighted average cost separately
for each asset.

For an executed buy:

```text
new average cost =
  (old quantity × old average cost + buy notional + buy fee)
  ÷ (old quantity + bought quantity)
```

For an executed sell, the renderer calculates realized profit or loss before
reducing the held quantity. For a settlement, it calculates realized profit or
loss before clearing the position.

Skipped and otherwise non-executed ledger records do not alter reconstructed
positions or realized results. The sum of reconstructed closed-record profit
and loss must reconcile with the account `realized_pnl` at the displayed
currency precision. A failed reconciliation is shown as `账本待核对`; the page
must not invent or silently adjust a value to force a match.

The replay is read-only and does not add or modify database columns.

## Page Structure

### Account overview

Keep the current headline cards:

- `已实现盈亏`;
- `可用现金`;
- `占用资金`.

The safety markers remain visible.

### 持仓中

Render one row per positive paper position with:

- market;
- outcome/direction;
- quantity;
- fee-inclusive average cost;
- occupied cost;
- market end time in Shanghai time;
- status `持仓中`;
- profit and loss `尚未实现`.

Occupied cost is quantity multiplied by fee-inclusive average cost.

### 已结束

Render one row per executed sell or settlement, newest first, with:

- close time in Shanghai time;
- market;
- outcome/direction when available;
- close type (`卖出`, `结算盈利`, or `结算亏损`);
- closed quantity;
- cost basis;
- net recovered amount;
- fee;
- single-record realized profit or loss.

Positive values use a profit style, negative values use a loss style, and zero
uses a neutral style.

### 最近动作

Retain the existing recent-action table below the account and position views.
It remains an execution audit trail and is not used as the user-facing
profit-and-loss statement.

## Status Data

The generated status JSON receives additive display fields:

- each open position receives its occupied cost and explicit unrealized status;
- `closed_records` contains the replayed sell and settlement details;
- `pnl_reconciliation_ok` reports whether reconstructed and account realized
  profit and loss agree at display precision.

Existing fields and meanings remain unchanged.

## Error Handling

- Missing market metadata falls back to the existing unknown/end-time display.
- A ledger record that cannot be replayed is not assigned a fabricated
  profit-and-loss value.
- Reconciliation failure produces a visible warning while preserving the raw
  account and ledger values.
- The renderer must not write to SQLite.

## Tests

Automated tests must prove:

- a buy appears under `持仓中` with occupied cost and `尚未实现`;
- a sell appears under `已结束` with the correct net recovered amount, cost
  basis, fee, and single-record realized profit or loss;
- a winning settlement and a losing settlement each produce the correct
  single-record result;
- skipped actions never create closed profit-and-loss rows;
- reconstructed realized profit and loss reconciles with the account value;
- rendering leaves cash, positions, ledger rows, source state, and paper safety
  markers unchanged;
- both dashboard variants render the same approved headings and semantics.

Deployment verification must also check the running service, heartbeat,
database integrity, page-to-database values, `paper_only=true`, and
`real_order_submitted=false`.
