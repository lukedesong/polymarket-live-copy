# Polymarket World Cup Prematch Market-Making Data System

Date: 2026-07-18

## Purpose

Build the first executable phase of a Polymarket World Cup prematch market-making system. This phase discovers eligible markets, captures official real-time order-book and trade data, stores replayable local records, and enforces a fail-closed prematch risk state machine.

This phase does not place orders or claim that a market-making edge exists. Its output is the evidence base required to measure spread capture, fill likelihood, adverse selection, inventory behavior, and exit feasibility before live quoting is considered.

## Verified official interfaces

The design uses only documented Polymarket public interfaces for discovery and data collection:

- Gamma `events` API for active-event discovery and associated market metadata: <https://docs.polymarket.com/market-data/fetching-markets>
- CLOB Market WebSocket for order-book snapshots, price-level changes, best quotes, and matched-trade messages: <https://docs.polymarket.com/market-data/websocket/market-channel>
- Sports WebSocket for live game state as a secondary safety signal: <https://docs.polymarket.com/market-data/websocket/sports>
- Public CLOB market metadata for official game start time and market constraints: <https://docs.polymarket.com/trading/clients/public>

The Market WebSocket is the primary market-data source. Gamma metadata is discovery data, not proof of executable depth. Sports status is a safety input, not a trading signal; the official documentation warns that it may be delayed or incomplete.

## Scope

### Included

- Scan all active, open Gamma events through pagination.
- Identify FIFA World Cup events using official tag slugs rather than hard-coded tag identifiers.
- Select direct match markets that have an official game start time, accept orders, expose valid outcome token identifiers, and are not closed.
- Rank eligible markets using Gamma market-level liquidity and rolling daily volume without inventing a dollar cutoff.
- Capture every supported public Market WebSocket message for selected outcome tokens.
- Maintain normalized order-book state from snapshots and price-level changes.
- Store raw and normalized data in a local SQLite database.
- Maintain a deterministic prematch risk state for every market.
- Produce machine-readable scanner and health summaries.
- Provide a cancel-only adapter boundary for a later authenticated execution layer.

### Excluded

- Live or paper order placement.
- Position sizing, quote width, expected edge, or profitability claims.
- Tournament futures without an official per-match start-time mapping.
- In-play market making.
- Automatic liquidation of residual inventory.
- Copying credentials or local environment files into the repository.

## Repository layout

The implementation will live in a focused package under the existing workspace:

```text
work/world_cup_market_maker/
  pyproject.toml
  README.md
  src/world_cup_mm/
    cli.py
    config.py
    discovery.py
    models.py
    orderbook.py
    risk.py
    storage.py
    websocket_collector.py
  tests/
  scripts/
data/                         # ignored local runtime database
docs/superpowers/specs/
```

Existing BTC strategy code remains untouched unless a small, clearly reusable safety abstraction is later extracted with its own tests.

## Architecture and data flow

```text
Gamma active events
        |
        v
World Cup tag and structural filters
        |
        v
Ranked eligible market manifest
        |
        +--------------------------+
        |                          |
        v                          v
CLOB Market WebSocket       Sports WebSocket
        |                          |
        v                          v
Raw event journal ------> Prematch risk controller
        |                          |
        v                          v
Normalized books/trades     Risk decisions/actions
        |                          |
        +------------+-------------+
                     v
                 SQLite
```

The scanner and collector are separable. A discovery run writes a versioned market manifest. Each eligible market is marked as either part of the non-dominated liquidity-volume frontier or as an eligible non-frontier market. The collector subscribes to the frontier by default. An explicit research-only `--all-eligible` option may broaden collection without changing market eligibility. Every collector session records the manifest identity and selection mode, making later replay attributable to the exact selection input.

## Market discovery

The scanner requests active, non-closed events and follows offset pagination until the API returns no new event identifiers. It then applies these gates:

- Event tags contain `fifa-world-cup` or `2026-fifa-world-cup`.
- Market is active and not closed.
- Market accepts orders.
- Market has a parseable official `gameStartTime`.
- Market has a condition identifier and parseable CLOB token identifiers.
- Game start time is still in the future according to UTC.

Markets with missing or contradictory metadata are rejected with an explicit reason. Tournament futures are retained in the raw scan results but are ineligible for automated prematch control until an independently verified match schedule mapping exists.

The scanner reports Gamma liquidity and rolling daily volume as source fields. It sorts candidates by those fields and identifies the non-dominated frontier across both measures. This avoids presenting an arbitrary dollar threshold as proven. The frontier is the default collection set, but it remains only a discovery priority; actual order-book depth and spread must come from the CLOB feed.

## WebSocket collection

The collector subscribes to both outcome tokens for each selected market with the documented custom market feature enabled. It records:

- `book`
- `price_change`
- `last_trade_price`
- `best_bid_ask`
- `tick_size_change`
- `new_market`
- `market_resolved`

Each received message is written to the raw journal before normalized state is updated. Raw records include local receive time, server time when present, session identity, asset identity, condition identity when known, event type, and the complete payload.

An initial `book` message replaces the normalized book for its asset. A `price_change` message changes or removes the specified price level. Decimal values are preserved as decimal strings at the storage boundary so binary floating-point conversion cannot silently change prices or sizes.

On reconnect, the collector starts a new session and requires fresh `book` snapshots before normalized books are considered ready. It does not apply deltas to stale state.

## Local storage

SQLite is used because it is local, transactional, queryable, and available without an external service. The database uses these logical tables:

- `scan_runs`: request time, completion state, source parameters, and errors.
- `events`: raw Gamma event metadata by scan.
- `markets`: normalized market metadata and eligibility decision.
- `assets`: outcome token mapping.
- `collector_sessions`: connection and reconnect lifecycle.
- `raw_ws_events`: append-only official WebSocket payloads.
- `book_levels`: latest normalized price levels by asset and side.
- `book_snapshots`: snapshot boundaries and source hashes.
- `trades`: normalized matched-trade messages.
- `sports_status`: received game-state messages.
- `risk_decisions`: deterministic state and its evidence.
- `risk_actions`: cancel, block, and reduce-only intents plus delivery status.

Writes are transactional. Raw-event insertion and normalized-state mutation for a message occur in the same transaction. Duplicate server messages must be harmless; stable source fields and payload hashes provide idempotency.

## Prematch risk controller

The following timing boundaries are user-specified values. They are operational requirements, not validated optimal parameters:

| Time to official start | State | Permitted behavior |
|---|---|---|
| More than 30 minutes | `PREMATCH_OPEN` | Data collection and future quoting eligibility |
| At most 30 and more than 15 minutes | `NO_NEW_INVENTORY` | No action may increase absolute inventory |
| At most 15 and more than 5 minutes | `REDUCE_ONLY` | Only inventory-reducing actions are eligible |
| At most 5 minutes | `CANCELLED_BLOCKED` | Cancel all resting orders and prohibit trading |
| Game live or already started | `IN_PLAY_BLOCKED` | Cancel all resting orders and prohibit trading |

Boundary comparisons are conservative: equality enters the safer state.

Additional fail-closed transitions apply when:

- The official game start time is missing or becomes unparseable.
- Gamma reports that the market is closed or no longer accepts orders.
- The Market WebSocket disconnects while an execution adapter is armed.
- A fresh post-reconnect order-book snapshot has not yet arrived.
- The Sports WebSocket reports a live or in-progress game.
- Market and sports metadata materially disagree.

`REDUCE_ONLY` does not invent a liquidation price or force a taker trade. It exposes constraints to a later quoting engine and raises an unresolved-inventory alert. `CANCELLED_BLOCKED` cancels resting orders but does not claim that inventory has been flattened.

## Cancellation boundary

The core risk controller emits idempotent action intents through an `OrderControl` interface. The first implementation includes:

- A recording adapter used by tests and data-only runs.
- An authenticated cancel-only adapter isolated from the collector.

No order-placement method exists in this phase. The authenticated adapter is disabled by default and can be armed only when credentials are supplied at runtime and the user explicitly selects cancel-enabled mode. Credentials are never read from or copied out of existing local environment files during development.

If cancellation is unavailable, the system cannot report itself as quote-capable. It remains a data collector and records the unmet cancel requirement.

## Command-line behavior

The package will expose commands with these responsibilities:

- `scan`: discover and persist eligible markets, then print a ranked JSON summary.
- `collect`: start Market and Sports WebSocket collection from the latest successful manifest.
- `status`: report database freshness, connections, selected markets, normalized-book readiness, and current risk states.
- `replay`: rebuild normalized order-book state from stored raw events without network access.

All commands return a non-success exit status for incomplete or unsafe states. Human-readable output is secondary to stable JSON fields that can be tested and monitored.

## Error handling

- HTTP failures retain the last successful manifest but do not silently present it as current.
- Pagination repetition is treated as an API failure, preventing an infinite scan.
- Malformed markets are stored with rejection reasons instead of being dropped silently.
- WebSocket disconnects close the current collector session and invalidate normalized-book readiness.
- Unknown WebSocket event types are preserved raw and reported, not discarded.
- SQLite errors abort the current transaction so raw and normalized state cannot diverge.
- Cancellation failures remain pending, keep trading blocked, and surface prominently in status output.

## Testing strategy

Implementation follows test-driven development. Every new behavior begins with a failing test. Required coverage includes:

- Gamma pagination, repeated-page detection, tag filtering, and rejection reasons.
- CLOB token identifier parsing and UTC game-time parsing.
- Liquidity ranking and non-dominated frontier selection.
- Order-book snapshot replacement, delta application, level deletion, and reconnect invalidation.
- Raw-message idempotency and transactional normalized updates.
- Every risk-state boundary using an injected clock.
- Immediate blocking on live sports status, market closure, and WebSocket disconnect.
- Idempotent cancel intents and fail-closed behavior when cancellation is unavailable.
- Offline replay equivalence with live normalization.
- Public official-API smoke tests that do not require credentials or submit orders.

Network tests are separate from deterministic unit tests. A live smoke test may prove that the current official endpoints are reachable and that messages match the documented envelope, but it cannot prove future availability or strategy profitability.

## Numeric provenance

| Value | Classification | Allowed use |
|---|---|---|
| 30-minute no-new-inventory boundary | User-specified value | Implement exactly; not presented as optimal |
| 15-minute reduce-only boundary | User-specified value | Implement exactly; not presented as optimal |
| 5-minute cancel boundary | User-specified value | Implement exactly; not presented as optimal |
| Gamma liquidity and volume fields | Empirical source values | Discovery ranking for the timestamped scan only |
| Official game start time | External constraint value | Prematch clock input, subject to conflict checks |

No quote size, inventory cap, spread threshold, liquidity dollar floor, reconnect delay, or profitability target is authorized by this design. Such parameters require external constraints, empirical evidence, an explicit user instruction, or an estimate label that cannot authorize live trading.

## Acceptance criteria

The first phase is accepted only when:

- A real official Gamma scan persists active World Cup direct-match candidates and explicit rejections.
- A public WebSocket session stores raw order-book and trade-related messages for the default frontier tokens.
- Offline replay reconstructs the same normalized state as the online processing path for the captured messages.
- Risk-state tests pass at every user-specified boundary and for disconnect/live-game fail-closed cases.
- Status output distinguishes data-only, cancel-capable, book-ready, stale, and blocked states.
- No real order is submitted.
- Fresh verification output documents the tests and live smoke checks.
