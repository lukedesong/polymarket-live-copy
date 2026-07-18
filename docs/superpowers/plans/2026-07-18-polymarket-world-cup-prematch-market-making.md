# Polymarket World Cup Prematch Market-Making Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable, data-only World Cup prematch market-making foundation that discovers eligible Polymarket markets, captures official order-book and trade events, stores replayable local data, and enforces user-specified prematch risk states without placing orders.

**Architecture:** A focused Python package under `work/world_cup_market_maker` separates discovery, risk, order-book normalization, persistence, WebSocket transport, and market-scoped cancellation. Pure functions and injected clocks/transports keep unit tests deterministic; official network access is limited to separate smoke checks. SQLite stores both raw source messages and normalized state so offline replay uses the same processing path as live collection.

**Tech Stack:** Locally verified Python 3.12.13, pytest 9.1.1, websockets 15.0.1, py-clob-client-v2 1.0.2, standard-library urllib/asyncio/sqlite3/decimal/dataclasses.

The runtime and library versions above are empirical local-environment values, not strategy parameters. The prematch 30-minute, 15-minute, and 5-minute boundaries are user-specified values and are not presented as optimal.

---

## File map

- `work/world_cup_market_maker/pyproject.toml`: package metadata and locally verified runtime dependencies.
- `work/world_cup_market_maker/README.md`: exact setup, scan, collect, status, replay, and safety instructions.
- `work/world_cup_market_maker/src/world_cup_mm/models.py`: immutable domain objects and source parsing.
- `work/world_cup_market_maker/src/world_cup_mm/discovery.py`: Gamma pagination, eligibility, ranking, and manifest construction.
- `work/world_cup_market_maker/src/world_cup_mm/risk.py`: deterministic prematch state machine and action intents.
- `work/world_cup_market_maker/src/world_cup_mm/orderbook.py`: snapshot/delta normalization using decimal strings.
- `work/world_cup_market_maker/src/world_cup_mm/storage.py`: SQLite schema, transactional writes, status queries, and replay inputs.
- `work/world_cup_market_maker/src/world_cup_mm/collector.py`: Market and Sports WebSocket transports and reconnect invalidation.
- `work/world_cup_market_maker/src/world_cup_mm/order_control.py`: recording adapter and authenticated market-scoped cancellation only.
- `work/world_cup_market_maker/src/world_cup_mm/cli.py`: `scan`, `collect`, `status`, and `replay` orchestration.
- `work/world_cup_market_maker/tests/`: deterministic unit and integration tests.

### Task 1: Package bootstrap and source models

**Files:**
- Create: `work/world_cup_market_maker/pyproject.toml`
- Create: `work/world_cup_market_maker/src/world_cup_mm/__init__.py`
- Create: `work/world_cup_market_maker/src/world_cup_mm/models.py`
- Create: `work/world_cup_market_maker/tests/test_models.py`

- [ ] **Step 1: Create package metadata and write the failing model tests**

```toml
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "world-cup-mm"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = ["websockets==15.0.1", "py-clob-client-v2==1.0.2"]

[project.scripts]
world-cup-mm = "world_cup_mm.cli:main"

[tool.pytest.ini_options]
pythonpath = ["src"]
testpaths = ["tests"]
markers = ["live: reaches current official Polymarket public endpoints"]
```

```python
from datetime import datetime, timezone
from world_cup_mm.models import parse_clob_token_ids, parse_utc_datetime

def test_parse_clob_token_ids_accepts_gamma_json_string():
    assert parse_clob_token_ids('["yes-token", "no-token"]') == ("yes-token", "no-token")

def test_parse_utc_datetime_normalizes_gamma_offset():
    assert parse_utc_datetime("2026-07-18 21:00:00+00") == datetime(2026, 7, 18, 21, tzinfo=timezone.utc)
```

- [ ] **Step 2: Run the tests and verify the import fails for the missing model module**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_models.py -v`

Expected: FAIL because `world_cup_mm.models` does not exist.

- [ ] **Step 3: Implement strict token and UTC parsing**

```python
def parse_clob_token_ids(value: object) -> tuple[str, ...]:
    payload = json.loads(value) if isinstance(value, str) else value
    if not isinstance(payload, list) or not payload or not all(isinstance(item, str) and item for item in payload):
        raise ValueError("invalid_clob_token_ids")
    return tuple(payload)

def parse_utc_datetime(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing_datetime")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("datetime_missing_timezone")
    return parsed.astimezone(timezone.utc)
```

Add immutable `EligibleMarket` and `RejectedMarket` dataclasses containing event identity, market identity, condition identity, token identifiers, start time, source liquidity/volume decimal strings, and rejection reasons.

- [ ] **Step 4: Run the model tests and verify they pass**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_models.py -v`

Expected: all model tests PASS.

- [ ] **Step 5: Commit the bootstrap**

```bash
git add work/world_cup_market_maker/pyproject.toml work/world_cup_market_maker/src work/world_cup_market_maker/tests/test_models.py
git commit -m "feat: bootstrap World Cup market data package"
```

### Task 2: Gamma discovery, eligibility, and liquidity frontier

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/discovery.py`
- Create: `work/world_cup_market_maker/tests/test_discovery.py`

- [ ] **Step 1: Write failing tests for pagination, eligibility, and frontier selection**

```python
def test_fetch_all_events_stops_on_empty_page_and_advances_by_received_count():
    pages = {0: [{"id": "event-a"}], 1: [{"id": "event-b"}], 2: []}
    assert [item["id"] for item in fetch_all_events(lambda offset: pages[offset])] == ["event-a", "event-b"]

def test_repeated_gamma_page_is_an_error():
    with pytest.raises(DiscoveryError, match="repeated_gamma_page"):
        fetch_all_events(lambda offset: [{"id": "same-event"}])

def test_market_without_game_start_is_rejected_as_tournament_future():
    eligible, rejected = classify_events([world_cup_event(game_start=None)], now=NOW)
    assert eligible == []
    assert rejected[0].reasons == ("missing_game_start_time",)

def test_frontier_excludes_market_dominated_in_liquidity_and_volume():
    markets = [candidate("a", "100", "80"), candidate("b", "90", "70"), candidate("c", "80", "100")]
    assert {m.market_id for m in liquidity_volume_frontier(markets)} == {"a", "c"}
```

- [ ] **Step 2: Run discovery tests and verify RED**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_discovery.py -v`

Expected: FAIL because discovery functions do not exist.

- [ ] **Step 3: Implement injectable pagination and strict eligibility**

```python
WORLD_CUP_TAG_SLUGS = frozenset({"fifa-world-cup", "2026-fifa-world-cup"})

def fetch_all_events(fetch_page: Callable[[int], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    offset = 0
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    while True:
        page = fetch_page(offset)
        if not page:
            return result
        page_ids = [str(event.get("id") or "") for event in page]
        if not all(page_ids) or any(event_id in seen for event_id in page_ids):
            raise DiscoveryError("repeated_gamma_page")
        seen.update(page_ids)
        result.extend(page)
        offset += len(page)
```

`classify_events` must retain every rejected market with exact reason codes, require an official World Cup tag slug, future timezone-aware `gameStartTime`, active/open/accepting state, condition ID, and valid tokens. `liquidity_volume_frontier` must use `Decimal` and mark a market dominated only when another market is no worse on both source fields and strictly better on at least one.

- [ ] **Step 4: Add the real Gamma transport without a fabricated page-size limit**

```python
class GammaClient:
    def fetch_page(self, offset: int) -> list[dict[str, Any]]:
        query = urlencode({"active": "true", "closed": "false", "order": "liquidity", "ascending": "false", "offset": offset})
        with urlopen(f"https://gamma-api.polymarket.com/events?{query}") as response:
            payload = json.load(response)
        if not isinstance(payload, list):
            raise DiscoveryError("gamma_response_not_list")
        return payload
```

- [ ] **Step 5: Run discovery tests and commit**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_discovery.py -v`

Expected: all discovery tests PASS.

```bash
git add work/world_cup_market_maker/src/world_cup_mm/discovery.py work/world_cup_market_maker/tests/test_discovery.py
git commit -m "feat: discover eligible World Cup markets"
```

### Task 3: Prematch risk state machine and market-scoped intents

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/risk.py`
- Create: `work/world_cup_market_maker/tests/test_risk.py`

- [ ] **Step 1: Write failing boundary and fail-closed tests**

```python
@pytest.mark.parametrize(("minutes", "expected"), [
    (31, RiskState.PREMATCH_OPEN),
    (30, RiskState.NO_NEW_INVENTORY),
    (15, RiskState.REDUCE_ONLY),
    (5, RiskState.CANCELLED_BLOCKED),
])
def test_user_specified_boundaries_enter_conservative_state(minutes, expected):
    decision = evaluate_risk(start=NOW + timedelta(minutes=minutes), now=NOW, context=healthy_context())
    assert decision.state is expected

def test_live_sports_status_overrides_scheduled_start():
    decision = evaluate_risk(start=NOW + timedelta(hours=1), now=NOW, context=healthy_context(sports_live=True))
    assert decision.state is RiskState.IN_PLAY_BLOCKED
    assert decision.actions == (RiskAction.CANCEL_MARKET_ORDERS, RiskAction.BLOCK_TRADING)

def test_armed_disconnect_cancels_only_affected_market():
    decision = evaluate_risk(start=NOW + timedelta(hours=1), now=NOW, context=healthy_context(market_ws_connected=False, cancel_armed=True))
    assert decision.market_condition_id == "condition-a"
    assert RiskAction.CANCEL_MARKET_ORDERS in decision.actions
```

- [ ] **Step 2: Run risk tests and verify RED**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_risk.py -v`

Expected: FAIL because risk types and evaluator do not exist.

- [ ] **Step 3: Implement exact user-specified thresholds and conservative equality**

```python
NO_NEW_INVENTORY = timedelta(minutes=30)  # user-specified
REDUCE_ONLY = timedelta(minutes=15)       # user-specified
CANCEL_ALL = timedelta(minutes=5)         # user-specified

def evaluate_risk(*, start: datetime, now: datetime, context: RiskContext) -> RiskDecision:
    if context.sports_live or now >= start:
        return blocked(RiskState.IN_PLAY_BLOCKED, context, "game_live_or_started")
    if not context.market_open or not context.accepting_orders:
        return blocked(RiskState.MARKET_BLOCKED, context, "market_not_tradeable")
    if not context.market_ws_connected or not context.book_ready:
        return blocked(RiskState.DATA_BLOCKED, context, "market_data_not_ready")
    remaining = start - now
    if remaining <= CANCEL_ALL:
        return blocked(RiskState.CANCELLED_BLOCKED, context, "inside_cancel_window")
    if remaining <= REDUCE_ONLY:
        return RiskDecision(RiskState.REDUCE_ONLY, (RiskAction.REDUCE_ONLY,), context.condition_id, "inside_reduce_window")
    if remaining <= NO_NEW_INVENTORY:
        return RiskDecision(RiskState.NO_NEW_INVENTORY, (RiskAction.NO_NEW_INVENTORY,), context.condition_id, "inside_no_new_inventory_window")
    return RiskDecision(RiskState.PREMATCH_OPEN, (), context.condition_id, "prematch_open")
```

The `blocked` helper emits market-scoped cancellation only when the execution adapter is armed; blocking is always emitted. No account-wide action exists in the enum or interface.

- [ ] **Step 4: Run risk tests and commit**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_risk.py -v`

Expected: all risk tests PASS.

```bash
git add work/world_cup_market_maker/src/world_cup_mm/risk.py work/world_cup_market_maker/tests/test_risk.py
git commit -m "feat: enforce World Cup prematch risk states"
```

### Task 4: Decimal-safe order-book normalization

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/orderbook.py`
- Create: `work/world_cup_market_maker/tests/test_orderbook.py`

- [ ] **Step 1: Write failing snapshot, delta, and reconnect tests**

```python
def test_snapshot_replaces_book_and_sorts_best_prices():
    book = OrderBookState("asset-a")
    book.apply(snapshot(bids=[("0.48", "2"), ("0.49", "1")], asks=[("0.52", "3")]))
    assert book.best_bid == Decimal("0.49")
    assert book.best_ask == Decimal("0.52")
    assert book.ready is True

def test_zero_size_delta_removes_price_level():
    book = seeded_book()
    book.apply(price_change(side="BUY", price="0.49", size="0"))
    assert Decimal("0.49") not in book.bids

def test_reconnect_invalidates_book_until_fresh_snapshot():
    book = seeded_book()
    book.invalidate()
    with pytest.raises(BookNotReady):
        book.apply(price_change(side="BUY", price="0.50", size="1"))
```

- [ ] **Step 2: Run order-book tests and verify RED**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_orderbook.py -v`

Expected: FAIL because the normalizer does not exist.

- [ ] **Step 3: Implement one normalization path for live and replay**

```python
class OrderBookState:
    def apply(self, payload: Mapping[str, Any]) -> None:
        event_type = payload.get("event_type")
        if event_type == "book":
            self.bids = parse_levels(payload.get("bids"))
            self.asks = parse_levels(payload.get("asks"))
            self.ready = True
            return
        if event_type == "price_change":
            if not self.ready:
                raise BookNotReady(self.asset_id)
            for change in payload.get("price_changes") or ():
                if str(change.get("asset_id")) == self.asset_id:
                    self._apply_change(change)
```

Use `Decimal` for comparisons and persist canonical strings. Unknown event types must return an explicit `ignored_for_book=True` result while remaining eligible for raw storage.

- [ ] **Step 4: Run tests and commit**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_orderbook.py -v`

Expected: all order-book tests PASS.

```bash
git add work/world_cup_market_maker/src/world_cup_mm/orderbook.py work/world_cup_market_maker/tests/test_orderbook.py
git commit -m "feat: normalize replayable CLOB order books"
```

### Task 5: Transactional SQLite storage and offline replay

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/storage.py`
- Create: `work/world_cup_market_maker/tests/test_storage.py`

- [ ] **Step 1: Write failing persistence and idempotency tests**

```python
def test_raw_event_and_book_update_commit_together(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    store.record_market_event(SESSION, snapshot_payload())
    assert store.raw_event_count() == 1
    assert store.best_quotes("asset-a") == ("0.49", "0.52")

def test_duplicate_payload_hash_is_idempotent(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    assert store.record_market_event(SESSION, snapshot_payload()) is True
    assert store.record_market_event(SESSION, snapshot_payload()) is False
    assert store.raw_event_count() == 1

def test_replay_reconstructs_same_quotes(tmp_path):
    store = Store(tmp_path / "market.sqlite3")
    for payload in recorded_sequence():
        store.record_market_event(SESSION, payload)
    assert replay_events(store.raw_events(SESSION))["asset-a"].best_bid == Decimal("0.50")
```

- [ ] **Step 2: Run storage tests and verify RED**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_storage.py -v`

Expected: FAIL because `Store` does not exist.

- [ ] **Step 3: Implement schema migration and atomic event processing**

```python
def record_market_event(self, session_id: str, payload: Mapping[str, Any]) -> bool:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    event_hash = hashlib.sha256(canonical.encode()).hexdigest()
    with self.connection:
        inserted = self.connection.execute(
            "INSERT OR IGNORE INTO raw_ws_events(session_id,event_hash,event_type,payload_json) VALUES(?,?,?,?)",
            (session_id, event_hash, str(payload.get("event_type") or "unknown"), canonical),
        ).rowcount
        if not inserted:
            return False
        self._apply_normalized_event(payload)
    return True
```

Create all tables from the approved design, store manifest selection mode, and expose focused queries used by `status` and `replay`. Use SQLite foreign keys and transactional context managers; do not add retention or rotation thresholds.

- [ ] **Step 4: Run storage tests and commit**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_storage.py -v`

Expected: all storage tests PASS.

```bash
git add work/world_cup_market_maker/src/world_cup_mm/storage.py work/world_cup_market_maker/tests/test_storage.py
git commit -m "feat: persist and replay World Cup market data"
```

### Task 6: Official WebSocket collectors and disconnect invalidation

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/collector.py`
- Create: `work/world_cup_market_maker/tests/test_collector.py`

- [ ] **Step 1: Write failing transport-envelope and disconnect tests**

```python
def test_market_subscription_uses_documented_asset_envelope():
    assert market_subscription(["a", "b"]) == {
        "assets_ids": ["a", "b"], "type": "market", "custom_feature_enabled": True
    }

def test_disconnect_invalidates_all_session_books():
    store = FakeStore(assets=["a", "b"])
    asyncio.run(handle_market_disconnect(store, "session-a"))
    assert store.invalidated == ["a", "b"]

def test_sports_ping_receives_immediate_pong_action():
    assert parse_sports_frame("ping") == SportsFrame(pong_required=True, payload=None)
```

- [ ] **Step 2: Run collector tests and verify RED**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_collector.py -v`

Expected: FAIL because collector functions do not exist.

- [ ] **Step 3: Implement separate Market and Sports collectors**

```python
async def collect_market(uri: str, asset_ids: Sequence[str], sink: EventSink) -> None:
    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps(market_subscription(asset_ids)))
        await sink.connected()
        try:
            async for frame in websocket:
                for payload in decode_market_frame(frame):
                    await sink.market_event(payload)
        finally:
            await sink.disconnected()
```

The Market collector sends the documented heartbeat and the Sports collector immediately answers server `ping` with `pong`. Reconnect orchestration must create a new collector session and invalidate old normalized books before any delta is accepted. No fabricated reconnect delay is introduced; the command exits non-success after a disconnect so an external supervisor can decide restart policy from evidence.

- [ ] **Step 4: Run collector tests and commit**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_collector.py -v`

Expected: all collector tests PASS.

```bash
git add work/world_cup_market_maker/src/world_cup_mm/collector.py work/world_cup_market_maker/tests/test_collector.py
git commit -m "feat: collect official Polymarket WebSocket data"
```

### Task 7: Market-scoped cancellation and command-line orchestration

**Files:**
- Create: `work/world_cup_market_maker/src/world_cup_mm/order_control.py`
- Create: `work/world_cup_market_maker/src/world_cup_mm/cli.py`
- Create: `work/world_cup_market_maker/tests/test_order_control.py`
- Create: `work/world_cup_market_maker/tests/test_cli.py`

- [ ] **Step 1: Write failing tests proving cancellation cannot become account-wide**

```python
def test_recording_adapter_is_idempotent_by_condition():
    adapter = RecordingOrderControl()
    adapter.cancel_market_orders("condition-a")
    adapter.cancel_market_orders("condition-a")
    assert adapter.calls == ["condition-a"]

def test_authenticated_adapter_uses_cancel_market_orders_only():
    client = FakeClient()
    AuthenticatedOrderControl(client).cancel_market_orders("condition-a")
    assert client.market_payload.market == "condition-a"
    assert client.cancel_all_called is False

def test_data_only_status_is_never_quote_capable():
    assert build_status(cancel_capable=False, book_ready=True)["quote_capable"] is False
```

- [ ] **Step 2: Run cancellation and CLI tests and verify RED**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_order_control.py work/world_cup_market_maker/tests/test_cli.py -v`

Expected: FAIL because the adapters and CLI do not exist.

- [ ] **Step 3: Implement market-scoped adapter with no account-wide method**

```python
class AuthenticatedOrderControl:
    def __init__(self, client: Any) -> None:
        self._client = client

    def cancel_market_orders(self, condition_id: str) -> Any:
        if not condition_id:
            raise ValueError("missing_condition_id")
        return self._client.cancel_market_orders(OrderMarketCancelParams(market=condition_id))
```

The interface intentionally does not expose `cancel_all`. Runtime construction requires L2 credentials from process environment and an explicit `--cancel-enabled` flag. This phase never reads existing local environment files and never constructs or submits an order.

- [ ] **Step 4: Implement `scan`, `collect`, `status`, and `replay` commands**

```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="world-cup-mm")
    parser.add_argument("--database", type=Path, default=Path("data/world_cup_mm.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("scan")
    collect = commands.add_parser("collect")
    collect.add_argument("--all-eligible", action="store_true")
    collect.add_argument("--cancel-enabled", action="store_true")
    collect.add_argument("--max-messages", type=int)
    commands.add_parser("status")
    commands.add_parser("replay")
    return parser
```

`scan` persists a successful manifest and JSON summary. `collect` defaults to frontier assets and refuses `--cancel-enabled` unless every required credential is present. `status` reports data-only/cancel-capable, selected markets, book readiness, and risk states. `replay` rebuilds normalized state into isolated in-memory objects and compares it with stored latest state.

- [ ] **Step 5: Run CLI tests and commit**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_order_control.py work/world_cup_market_maker/tests/test_cli.py -v`

Expected: all cancellation and CLI tests PASS.

```bash
git add work/world_cup_market_maker/src/world_cup_mm/order_control.py work/world_cup_market_maker/src/world_cup_mm/cli.py work/world_cup_market_maker/tests/test_order_control.py work/world_cup_market_maker/tests/test_cli.py
git commit -m "feat: add safe World Cup data commands"
```

### Task 8: Documentation, full verification, and official live smoke

**Files:**
- Create: `work/world_cup_market_maker/README.md`
- Create: `work/world_cup_market_maker/tests/test_live_official.py`
- Modify: `docs/superpowers/plans/2026-07-18-polymarket-world-cup-prematch-market-making.md`

- [ ] **Step 1: Write an opt-in official smoke test**

```python
@pytest.mark.live
def test_official_gamma_returns_world_cup_direct_match_candidates(tmp_path):
    result = run_scan(Store(tmp_path / "live.sqlite3"), GammaClient(), now=datetime.now(timezone.utc))
    assert result.source == "https://gamma-api.polymarket.com/events"
    assert all(m.game_start_time.tzinfo is timezone.utc for m in result.eligible)
```

The live test validates current response shape and persistence only. It does not assert a fixed market count, liquidity value, or named team because those values are empirical and time-sensitive.

- [ ] **Step 2: Document exact commands and safety boundary**

```markdown
## Run

Use the verified project interpreter:

    work/polymarket-api-py312-venv/bin/python -m pip install -e work/world_cup_market_maker
    work/polymarket-api-py312-venv/bin/world-cup-mm scan
    work/polymarket-api-py312-venv/bin/world-cup-mm status

The default mode is data-only. It cannot place orders and cannot report quote capability. Prematch timing boundaries are user-specified and remain subject to later empirical validation.
```

- [ ] **Step 3: Run the complete deterministic suite**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests -m 'not live' -v`

Expected: all deterministic tests PASS with no warnings or collection errors.

- [ ] **Step 4: Run official Gamma and WebSocket smoke checks**

Run: `work/polymarket-api-py312-venv/bin/python -m pytest work/world_cup_market_maker/tests/test_live_official.py -m live -v`

Expected: current official Gamma response is accepted and at least one current direct-match candidate is persisted if such a market exists; if no future match exists, the test must report a valid empty eligible set plus explicit rejections rather than fabricate success.

Run: `work/polymarket-api-py312-venv/bin/world-cup-mm collect --max-messages 1`

Expected: the collector stores one official market frame, exits successfully, and `status` reports the resulting session. The single-frame bound is an estimate used only to keep the smoke check finite; it cannot authorize a trading decision.

- [ ] **Step 5: Verify repository state and commit**

```bash
git diff --check
git status --short
git add work/world_cup_market_maker/README.md work/world_cup_market_maker/tests/test_live_official.py docs/superpowers/plans/2026-07-18-polymarket-world-cup-prematch-market-making.md
git commit -m "docs: document World Cup market data workflow"
```

Expected: only intended new package and documentation files are committed; pre-existing untracked workspace files remain untouched.
