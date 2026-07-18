# Polymarket $100 to $120 Paper Challenge Design

## Objective and provenance

- Starting paper cash: **$100** (`用户指定值`).
- Target liquidated equity: **$120** (`用户指定值`).
- Evaluation horizon: **12 hours** (`用户指定值`).
- These values define the experiment. They are not evidence that the return is attainable.
- The system remains paper-only and must never load signing credentials or submit a real order.

## Honest success definition

Success is measured at the deadline as:

`cash + executable liquidation value of inventory - outstanding liabilities`

The liquidation value walks the current official CLOB depth and applies the official market fee curve. Open maker quotes, midpoint marks, unfilled intentions, and synthetic fills do not count as profit. The challenge succeeds only if this liquidated equity is at least the user-specified target.

## Market and strategy selection

The runner discovers active, order-accepting Polymarket markets in bulk, then obtains official CLOB constraints and books for candidates. It ranks candidates using observed executable spread, recent official trade activity, available depth, queue size, fee burden, and short-horizon price movement. It does not rely on a fixed category such as the World Cup or LeBron.

Permitted paper actions are:

1. queue-aware passive maker orders;
2. executable taker trades only when both entry and planned exit are evaluated against official depth and fees;
3. complementary-outcome opportunities whose complete executable cost and payout are internally consistent;
4. inventory reduction or liquidation using the official book.

No action may be created from a fabricated fill, a midpoint-only price, or an assumed queue jump.

## Account controls

The account maintains cash, reserved cash, inventory cost, open-order commitments, realized profit, and depth-liquidated equity. It rejects any order that would make cash or collateral negative. Order quantity is bounded by the official minimum, available paper cash, and observed book capacity. Unsupported sizing constants remain experiment parameters and cannot authorize later live trading.

## Audit trail

A new SQLite database records the challenge start and deadline, every official book/trade event used, every paper order and queue state, every fill proof, fees, inventory marks, liquidation paths, account equity, and strategy decision. The old LeBron database remains read-only evidence and is excluded from challenge results.

## Failure handling

On stale or disconnected official data, the system cancels paper orders and stops creating fills. Missing official market constraints disqualify a market. At the deadline it cancels all paper orders, values inventory through executable depth, and reports the actual result whether or not the target was reached.

## Acceptance checks

- Starting cash reconciles to the user-specified amount.
- No authenticated Polymarket trading client is instantiated.
- Cash, reservations, inventory, and equity reconcile after every fill.
- Queue-aware maker fills and executable taker fills have distinct proof types.
- Deadline reporting uses depth-liquidated equity.
- Restarting the runner does not duplicate fills or reset the clock.
