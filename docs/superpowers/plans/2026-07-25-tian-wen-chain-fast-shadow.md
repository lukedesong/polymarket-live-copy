# Tian-Wen Chain Fast Shadow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run an independent read-only Polygon observer that captures Tian-Wen source fills and the immediate full CLOB book, then measures them against the existing public Data API discovery path.

**Architecture:** A single Python daemon uses allow-listed JSON-RPC calls to follow new Polygon blocks and decode Polymarket V2 `OrderFilled` logs. It writes only to a new SQLite database, while a second worker polls public user trades and records the first Data API match and comparison book. The old paper database is opened only through SQLite `mode=ro` to seed token metadata.

**Tech Stack:** Python standard library, SQLite WAL, Polygon JSON-RPC, Polymarket public Data API/CLOB API, launchd, pytest.

---

## File map

- `work/wallet_copy_paper/tian_wen_chain_shadow.py`: decoder, allow-listed network client, isolated store, chain observer, Data API matcher, status renderer and CLI.
- `work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py`: deterministic unit tests and recorded V2 log fixture tests.
- `work/wallet_copy_paper/tests/test_tian_wen_chain_shadow_live.py`: opt-in public RPC/Data API integration verification.
- `work/wallet_copy_paper/com.luke.polymarket.tian-wen-chain-shadow.plist`: independent launchd service.
- `work/wallet_copy_paper/start_tian_wen_chain_shadow.sh`: install and start the independent service.
- `work/wallet_copy_paper/stop_tian_wen_chain_shadow.sh`: stop only the independent service.
- `work/wallet_copy_paper/status_tian_wen_chain_shadow.sh`: print launchd and SQLite/status evidence.
- `work/wallet_copy_paper/README_tian_wen_chain_shadow.md`: evidence boundaries, files, commands and metric definitions.
- `/Users/luke/Documents/Codex/2026-06-03/git-hub-skill/market-events/ARMORY.md`: register the read-only Polygon RPC dependency and fallback.

### Task 1: Freeze decoder, isolation and metric contracts in failing tests

**Files:**
- Create: `work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py`
- Create: `work/wallet_copy_paper/tian_wen_chain_shadow.py`

- [ ] **Step 1: Add tests for V2 log decoding**

Create a recorded `OrderFilled` fixture whose topics contain the source wallet and whose seven data words encode side, token, maker amount, taker amount, fee, builder and metadata. Assert:

```python
event = shadow.decode_order_filled(log, shadow.SOURCE_WALLET)
assert event["source_role"] == "maker"
assert event["side"] == "BUY"
assert event["token_id"] == "123"
assert event["quantity"] == Decimal("10")
assert event["notional"] == Decimal("3")
assert event["price"] == Decimal("0.3")
```

Add equivalent SELL, source-as-taker and unrelated-wallet cases. For source-as-taker, assert `side == "UNKNOWN"` so complementary and mint/merge transactions are not guessed.

- [ ] **Step 2: Add tests for request safety and old-database isolation**

Assert that only `eth_blockNumber`, `eth_getBlockByNumber`, `eth_getLogs` and `eth_getTransactionReceipt` pass the RPC validator; order-signing or transaction-sending methods must raise `ReadOnlyViolation`. Create an old SQLite fixture, open it with `open_old_paper_read_only()`, and assert a write raises `sqlite3.OperationalError`.

- [ ] **Step 3: Add tests for idempotence and observation-time metrics**

Insert the same chain event twice and assert one row remains. Insert a snapshot with distinct request start, request finish and API book timestamp; assert delay metrics use `chain_seen_at_ms` and `data_api_seen_at_ms`, never the book timestamp. Insert a catch-up event and assert it is excluded from live latency aggregates.

- [ ] **Step 4: Run the focused tests and verify the intended failure**

Run:

```bash
python -m pytest work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py -q
```

Expected: collection fails because `tian_wen_chain_shadow` does not yet expose the tested contracts.

- [ ] **Step 5: Implement the minimal decoder and store**

Implement:

```python
def decode_order_filled(log: dict[str, Any], source_wallet: str) -> dict[str, Any] | None:
    topics = [str(value).lower() for value in log.get("topics", [])]
    if len(topics) != 4 or topics[0] != ORDER_FILLED_TOPIC:
        return None
    maker = topic_address(topics[2])
    taker = topic_address(topics[3])
    if source_wallet.lower() not in {maker, taker}:
        return None
    words = split_words(log["data"])
    side_code = int(words[0], 16)
    source_role = "maker" if maker == source_wallet.lower() else "taker"
    side = ("BUY" if side_code == 0 else "SELL") if source_role == "maker" else "UNKNOWN"
    return decoded_amounts(side, source_role, words, log)
```

Create an isolated schema with `processed_blocks`, `chain_events`, `book_snapshots`, `data_api_rows`, `data_api_matches`, `token_metadata` and `runtime_state`. Use chain ID, transaction hash and log index as the chain-event unique key.

- [ ] **Step 6: Run the focused tests until green**

Run the same pytest command. Expected: all decoder, safety, idempotence and metric tests pass.

### Task 2: Build deterministic block observation and immediate book capture

**Files:**
- Modify: `work/wallet_copy_paper/tian_wen_chain_shadow.py`
- Modify: `work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py`

- [ ] **Step 1: Add failing observer tests**

Use a fake RPC client returning a startup head, then a later block containing one maker log and one counterparty leg. Use a fake CLOB client returning a complete book. Assert:

```python
observer.initialize_watermark()
assert store.chain_event_count() == 0
observer.run_chain_cycle()
assert store.chain_event_count() == 2
assert store.snapshot_count(channel="chain") == 1
assert store.runtime_value("last_processed_block") == str(next_block)
```

Assert a restart catches up persisted blocks with `catchup=true`, a repeated cycle stays idempotent, and a changed stored block hash rewinds orphaned rows to the common ancestor before continuing.

- [ ] **Step 2: Run observer tests and verify failure**

Run the observer test selection. Expected: failures show the observer and reorg methods are missing.

- [ ] **Step 3: Implement the allow-listed clients and observer**

Implement an HTTP JSON-RPC client with primary/fallback URLs and per-request timing. Query V2 normal and negative-risk exchange addresses from the official deployment list. For each new block:

1. fetch and persist the block header;
2. query maker-topic logs first;
3. persist each source-order event;
4. immediately request and save one full book per transaction/token;
5. query and save source-as-taker counterparty legs without duplicating the book;
6. advance the completed-block watermark only after all writes finish.

On first run, persist the current head as a watermark and do not create historical first-seen samples. At process restart, mark blocks at or below the boot head as catch-up.

- [ ] **Step 4: Implement common-ancestor reorg recovery**

Before processing a new head, compare the stored last block hash to the current RPC hash. If it differs, walk stored block numbers backward until the first matching hash, mark later events orphaned, delete their timing samples from aggregates, and resume from the common ancestor.

- [ ] **Step 5: Run observer tests until green**

Run:

```bash
python -m pytest work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py -q -k "observer or reorg or watermark or snapshot"
```

Expected: selected tests pass with no network.

### Task 3: Add Data API A/B matching, metadata scope and status evidence

**Files:**
- Modify: `work/wallet_copy_paper/tian_wen_chain_shadow.py`
- Modify: `work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py`

- [ ] **Step 1: Add failing matcher tests**

Feed duplicate Data API fragments for one transaction/token/side. Assert raw rows are retained once, quantity and source VWAP aggregate correctly, only the first Data API observation time is retained, and exactly one `data_api` book snapshot is captured.

- [ ] **Step 2: Add failing metadata and status tests**

Seed token metadata from an old paper fixture opened read-only. Assert known Trump speech-word tokens are `IN_SCOPE`, unrelated tokens are `OUT_OF_SCOPE`, and unknown tokens remain `PENDING` while still retaining their chain book. Render status and assert:

```python
assert status["paper_only"] is True
assert status["real_order_submitted"] is False
assert status["old_paper_database_mode"] == "read_only"
assert "chain_seen_at_ms" in status["recent_events"][0]
```

- [ ] **Step 3: Implement Data API matching**

Poll public trades for the source wallet with `takerOnly=false`. Hash and store raw rows, group them by transaction/token/side, match primary chain events, preserve the earliest observed time, and capture one complete comparison book immediately after first match.

- [ ] **Step 4: Implement metadata cache and status renderer**

Copy old `source_state` rows through a read-only SQLite URI into the new database. Let a matched Data API row enrich missing metadata. Render `status.json` and a compact `status.html` with safety flags, heartbeat, block progress, errors, live/catch-up counts, A/B counts, real observation delays and recent raw-book links represented as database record IDs.

- [ ] **Step 5: Run the complete deterministic suite**

Run:

```bash
python -m pytest work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py -q
```

Expected: all deterministic tests pass.

### Task 4: Add service controls, documentation and dependency registration

**Files:**
- Create: `work/wallet_copy_paper/com.luke.polymarket.tian-wen-chain-shadow.plist`
- Create: `work/wallet_copy_paper/start_tian_wen_chain_shadow.sh`
- Create: `work/wallet_copy_paper/stop_tian_wen_chain_shadow.sh`
- Create: `work/wallet_copy_paper/status_tian_wen_chain_shadow.sh`
- Create: `work/wallet_copy_paper/README_tian_wen_chain_shadow.md`
- Modify: `/Users/luke/Documents/Codex/2026-06-03/git-hub-skill/market-events/ARMORY.md`

- [ ] **Step 1: Add the independent launchd definition**

Use label `com.luke.polymarket.tian-wen-chain-shadow`, the project Python environment, a dedicated `tian_wen_chain_shadow_runtime` directory, `KeepAlive=true`, and separate stdout/stderr files. Do not reference private keys or order credentials.

- [ ] **Step 2: Add start, stop and status controls**

The start script copies only the new plist and bootstraps only the new label. The stop script boots out only that label. The status script prints launchd state, status JSON and a read-only SQLite integrity check.

- [ ] **Step 3: Document evidence boundaries**

Explain that `chain_seen` and `data_api_seen` are empirical first-observation times from this process, `book_timestamp` is supplied by CLOB and is not substituted for them, no value proves a fill at that price, and no real-order path exists.

- [ ] **Step 4: Register the RPC dependency**

Add the Polygon public read-only JSON-RPC primary and fallback, their purpose, allowed methods, failure behavior and replacement rule to the existing armory. Do not record credentials because these endpoints require none.

- [ ] **Step 5: Validate static service files**

Run:

```bash
plutil -lint work/wallet_copy_paper/com.luke.polymarket.tian-wen-chain-shadow.plist
zsh -n work/wallet_copy_paper/start_tian_wen_chain_shadow.sh
zsh -n work/wallet_copy_paper/stop_tian_wen_chain_shadow.sh
zsh -n work/wallet_copy_paper/status_tian_wen_chain_shadow.sh
```

Expected: all commands succeed.

### Task 5: Verify against public chain evidence and run the service

**Files:**
- Create: `work/wallet_copy_paper/tests/test_tian_wen_chain_shadow_live.py`
- Generate: `work/wallet_copy_paper/tian_wen_chain_shadow_runtime/shadow.sqlite3`
- Generate: `work/wallet_copy_paper/tian_wen_chain_shadow_runtime/status.json`
- Generate: `work/wallet_copy_paper/tian_wen_chain_shadow_runtime/status.html`

- [ ] **Step 1: Add an opt-in historical receipt test**

Read one transaction hash from the old paper ledger, fetch its receipt and block, decode the source-order event, and compare block timestamp, token, direction, price and aggregate quantity with the public Data API rows for that transaction. Mark this test `live` so the deterministic suite never depends on network availability.

- [ ] **Step 2: Run deterministic and live verification**

Run:

```bash
python -m pytest work/wallet_copy_paper/tests/test_tian_wen_chain_shadow.py -q
RUN_LIVE_POLYMARKET_TESTS=1 python -m pytest work/wallet_copy_paper/tests/test_tian_wen_chain_shadow_live.py -q
```

Expected: deterministic tests pass; public integration test passes against the recorded historical transaction.

- [ ] **Step 3: Capture old-ledger invariants and start the new service**

Read old account, paper-position and ledger summaries before launch. Start the independent service, wait for two heartbeat observations, and reread those summaries. Attribute any concurrent legitimate old-daemon row to the old process; verify the new process code only opened the old database read-only.

- [ ] **Step 4: Verify runtime evidence**

Check:

```bash
work/wallet_copy_paper/status_tian_wen_chain_shadow.sh
```

Expected: one launchd process, SQLite `integrity_check=ok`, heartbeat changes, processed block increases, and status reports `paper_only=true` plus `real_order_submitted=false`.

- [ ] **Step 5: Run regression tests and inspect errors**

Run the wallet-copy test suite and inspect the new stderr log. Expected: existing tests remain green and the new log has no repeated exception loop.
