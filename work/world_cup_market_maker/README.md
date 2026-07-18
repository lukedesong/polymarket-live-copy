# Polymarket World Cup Prematch Queue Paper Market Maker

This package discovers active FIFA World Cup markets from Polymarket's official
Gamma API, selects a liquidity/volume Pareto frontier without inventing a fixed
cutoff, records the official CLOB market WebSocket and sports WebSocket, rebuilds
order books locally, evaluates prematch risk states, and runs a queue-aware
paper market maker.

It does **not** submit real quotes or real trades. `paper-run` never loads
trading credentials and writes only to its SQLite database. The separate
authenticated collection mode exposes only market-scoped cancellation; there
is deliberately no real order-submission path in this package.

## Requirements

- Python `3.12` is a project runtime constraint from `pyproject.toml`.
- The dependencies are pinned in `pyproject.toml`.
- Runtime data is written to `data/world_cup_mm.sqlite3` and ignored by Git.

From the repository root:

```bash
work/polymarket-api-py312-venv/bin/pip install -e work/world_cup_market_maker
```

## Run

Discover and persist the current eligible World Cup market manifest:

```bash
work/polymarket-api-py312-venv/bin/world-cup-mm scan
```

Collect the Pareto-frontier order books and sports feed in public data mode:

```bash
work/polymarket-api-py312-venv/bin/world-cup-mm collect
```

Inspect current safety state or replay the latest recorded session:

```bash
work/polymarket-api-py312-venv/bin/world-cup-mm status
work/polymarket-api-py312-venv/bin/world-cup-mm replay
```

Run the authoritative paper model in an independent database:

```bash
work/polymarket-api-py312-venv/bin/world-cup-mm \
  --database data/world_cup_paper_queue.sqlite3 scan
work/polymarket-api-py312-venv/bin/world-cup-mm \
  --database data/world_cup_paper_queue.sqlite3 paper-run --fill-mode queue
```

Inspect or export the paper result:

```bash
work/polymarket-api-py312-venv/bin/world-cup-mm \
  --database data/world_cup_paper_queue.sqlite3 paper-status
work/polymarket-api-py312-venv/bin/world-cup-mm \
  --database data/world_cup_paper_queue.sqlite3 paper-export
```

`queue` is the authoritative paper mode. The retained `strict` and `touch`
modes are explicitly comparison-only.

## Paper execution model

- A new quote joins the current best price behind all displayed quantity at
  that price.
- Same-price size changes keep the existing order and queue timestamp. Size
  added later is behind the paper order; size removed without a compatible
  official trade does not guess which queue position was cancelled.
- An official at-price trade consumes observable queue-ahead quantity before
  producing a partial paper fill. A direction-compatible trade through the
  quote proves that the valid remaining paper order was crossed.
- Each official trade event is consumed at most once. Missing, invalid, or
  out-of-order price/side/quantity evidence creates an anomaly instead of a
  fill.
- Every valid book update recalculates executable inventory value from all bid
  levels and records best-bid and depth-VWAP drift from the paper fill price.
- The forced prematch exit walks actual displayed bid depth. Uncovered inventory
  remains explicitly unliquidated and is not assigned an invented exit price.
- Maker fees are not assumed. Per-market taker fee rate and exponent come from
  official CLOB market info, and liquidation uses the official fee curve.

The public market channel exposes aggregated price levels rather than external
order identities. Therefore this model is reproducible from public data, but it
cannot know whether an unrelated same-price cancellation occurred ahead of or
behind the paper order. That uncertainty must be calibrated later with a real
small order and the private user channel; paper output alone cannot authorize
capital scaling.

`collect` runs until interrupted. A positive `--max-messages` value is available
only to bound diagnostics; it is not a strategy parameter.

## Risk contract

The timing values below are **user-specified values**, not backtested optima:

- More than 30 minutes before scheduled start: prematch-open state, subject to
  healthy market data and cancellation capability.
- At 30 minutes: no new inventory.
- At 15 minutes: reduce-only state.
- At 5 minutes: block trading and request cancellation of orders for that exact
  market condition.
- At scheduled start, or whenever the official sports feed reports the game as
  live: block trading immediately and request market-scoped cancellation.
- On stale/missing order books, market WebSocket disconnect, or a non-tradable
  market: fail closed.

These gates are enforced in the paper engine. `NO_NEW_INVENTORY` and
`REDUCE_ONLY` cancel inventory-increasing paper buys and retain only
inventory-reducing paper sells. The final prematch block cancels all paper
quotes and simulates a taker exit against current displayed depth. During a
market-data disconnect or live match the engine cancels paper quotes but does
not invent an executable exit from stale data.

## Existing API configuration

Authenticated cancellation accepts the repository's existing address naming:
`POLYMARKET_WALLET_ADDRESS`, then `DEPOSIT_WALLET_ADDRESS`, then
`POLYMARKET_FUNDER_ADDRESS`. It also requires the existing private key, API key,
secret, passphrase, and signature type variables at process runtime.

To arm cancellation, explicitly load the existing local environment and add
`--cancel-enabled`. This is intentionally not the default and should only be run
after confirming that the selected manifest contains the intended markets.

## Verification

Deterministic tests:

```bash
work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests -m "not live"
```

Current official Gamma response smoke test:

```bash
work/polymarket-api-py312-venv/bin/pytest work/world_cup_market_maker/tests/test_live_official.py -m live
```

Official references:

- https://docs.polymarket.com/market-data/fetching-markets
- https://docs.polymarket.com/market-data/websocket/market-channel
- https://docs.polymarket.com/market-data/websocket/overview
- https://docs.polymarket.com/trading/fees
- https://docs.polymarket.com/api-reference/markets/get-clob-market-info
