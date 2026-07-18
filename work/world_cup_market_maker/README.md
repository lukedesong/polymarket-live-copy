# Polymarket World Cup Prematch Market Maker — Phase 1

This package discovers active FIFA World Cup markets from Polymarket's official
Gamma API, selects a liquidity/volume Pareto frontier without inventing a fixed
cutoff, records the official CLOB market WebSocket and sports WebSocket, rebuilds
order books locally, and evaluates prematch risk states.

It does **not** place quotes or increase positions. The default mode is public
data collection only. Authenticated mode exposes only market-scoped order
cancellation; there is deliberately no account-wide cancel path.

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

Phase 1 has no quoting or position engine, so `NO_NEW_INVENTORY` and
`REDUCE_ONLY` are enforceable gates for the next execution phase rather than
claims that a live position has already been changed. The cancellation action is
implemented and can be armed separately.

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
- https://docs.polymarket.com/developers/CLOB/websocket/market-channel
- https://docs.polymarket.com/developers/sports-websocket/overview
- https://docs.polymarket.com/developers/CLOB/orders/cancel-orders
