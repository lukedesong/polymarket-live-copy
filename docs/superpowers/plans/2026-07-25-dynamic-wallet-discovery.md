# Dynamic Wallet Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the fixed legacy-wallet loop with public leaderboard discovery, persistent resumable candidate state, non-crypto sleeve analysis, and strategy/lifecycle reports.

**Architecture:** Keep the user-facing entry point in `candidate_scan_once.py` and add testable pure functions for leaderboard normalization, state merging, queue ordering, history pagination, sleeve filtering, and lifecycle classification. The CLI discovers and checkpoints the full dynamic pool, optionally defers analysis through an explicit operational cap, and writes both JSON and Markdown without touching any paper ledger.

**Tech Stack:** Python standard library, pytest, Polymarket public Data API.

---

### Task 1: Dynamic leaderboard discovery and candidate-pool state

**Files:**
- Modify: `work/wallet_copy_paper/candidate_scan_once.py`
- Create: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [x] **Step 1: Write failing discovery and pool tests**

```python
from candidate_scan_once import (
    discover_leaderboard_candidates,
    merge_candidate_pool,
    ordered_candidate_wallets,
)


def leaderboard_row(wallet, name, rank):
    return {
        "proxyWallet": wallet,
        "userName": name,
        "rank": str(rank),
        "pnl": 12.5,
        "vol": 50.0,
    }


def test_discovery_excludes_crypto_and_deduplicates_origins():
    calls = []

    def fetch(endpoint, params):
        calls.append((endpoint, dict(params)))
        if params["category"] == "WEATHER":
            return [leaderboard_row("0x" + "a" * 40, "weather-a", 1)]
        return [leaderboard_row("0x" + "A" * 40, "politics-a", 2)]

    found, coverage = discover_leaderboard_candidates(
        fetch,
        categories=("WEATHER", "CRYPTO", "POLITICS"),
        periods=("MONTH",),
        observed_at="2026-07-25T00:00:00+00:00",
    )

    assert {params["category"] for _, params in calls} == {"WEATHER", "POLITICS"}
    assert list(found) == ["0x" + "a" * 40]
    assert {origin["category"] for origin in found["0x" + "a" * 40]["origins"]} == {
        "WEATHER",
        "POLITICS",
    }
    assert coverage["complete"] is True


def test_pool_keeps_dynamic_wallet_and_legacy_is_only_provenance():
    dynamic = "0x" + "b" * 40
    legacy = "0x" + "c" * 40
    pool = merge_candidate_pool(
        {},
        {
            dynamic: {
                "wallet": dynamic,
                "name": "new-wallet",
                "origins": [{"source": "leaderboard", "rank": 1}],
            }
        },
        {"legacy-name": legacy},
        observed_at="2026-07-25T00:00:00+00:00",
    )

    assert set(pool) == {dynamic, legacy}
    assert pool[dynamic]["legacy_seed"] is False
    assert pool[legacy]["legacy_seed"] is True
    assert ordered_candidate_wallets(pool)[0] == dynamic
```

- [x] **Step 2: Run the focused tests and verify RED**

Run:

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  -m pytest -q tests/test_candidate_scan_once.py
```

Working directory: `work/wallet_copy_paper`.

Expected: collection fails because the discovery/state functions do not exist.

- [x] **Step 3: Implement public leaderboard discovery and state primitives**

Add the official non-crypto categories, approved periods, and external API page bounds:

```python
LEGACY_SEEDS = dict(WALLETS)
LEADERBOARD_CATEGORIES = (
    "OVERALL", "POLITICS", "SPORTS", "ESPORTS", "CULTURE",
    "MENTIONS", "WEATHER", "ECONOMICS", "TECH", "FINANCE",
)
LEADERBOARD_PERIODS = ("MONTH", "ALL")
LEADERBOARD_PAGE_SIZE = 50
LEADERBOARD_MAX_OFFSET = 1000
CRYPTO_DOMAIN = "加密资产与代币"
```

Implement:

```python
def normalize_wallet(value: Any) -> str | None:
    wallet = str(value or "").lower()
    return wallet if re.fullmatch(r"0x[a-f0-9]{40}", wallet) else None


def discover_leaderboard_candidates(
    fetch,
    *,
    categories=LEADERBOARD_CATEGORIES,
    periods=LEADERBOARD_PERIODS,
    observed_at: str,
):
    candidates = {}
    pair_coverage = []
    for category in categories:
        if category == "CRYPTO":
            continue
        for period in periods:
            pair = {"category": category, "period": period, "rows": 0, "complete": False}
            for offset in range(0, LEADERBOARD_MAX_OFFSET + 1, LEADERBOARD_PAGE_SIZE):
                page = fetch("v1/leaderboard", {
                    "category": category,
                    "timePeriod": period,
                    "orderBy": "PNL",
                    "limit": LEADERBOARD_PAGE_SIZE,
                    "offset": offset,
                })
                pair["rows"] += len(page)
                for row in page:
                    wallet = normalize_wallet(row.get("proxyWallet"))
                    if wallet is None:
                        continue
                    item = candidates.setdefault(wallet, {
                        "wallet": wallet,
                        "name": str(row.get("userName") or wallet),
                        "origins": [],
                    })
                    origin = {
                        "source": "leaderboard",
                        "category": category,
                        "period": period,
                        "rank": row.get("rank"),
                        "pnl": row.get("pnl"),
                        "vol": row.get("vol"),
                        "observed_at": observed_at,
                    }
                    if origin not in item["origins"]:
                        item["origins"].append(origin)
                if len(page) < LEADERBOARD_PAGE_SIZE:
                    pair["complete"] = True
                    break
            pair_coverage.append(pair)
    return candidates, {
        "pairs": pair_coverage,
        "complete": all(item["complete"] for item in pair_coverage),
    }
```

Implement pool merging so existing analysis/checkpoint fields survive discovery refresh, legacy entries receive `legacy_seed` provenance only, and deterministic queue ordering prefers never-analyzed dynamic candidates, then changed candidates, then oldest analyzed candidates:

```python
def merge_candidate_pool(existing, discovered, legacy_seeds, *, observed_at):
    merged = {wallet: dict(item) for wallet, item in existing.items()}
    legacy_by_wallet = {
        wallet.lower(): name for name, wallet in legacy_seeds.items()
    }
    for wallet, incoming in discovered.items():
        item = merged.setdefault(wallet, {"wallet": wallet, "origins": []})
        item["name"] = incoming["name"]
        item["first_seen"] = item.get("first_seen") or observed_at
        item["last_seen"] = observed_at
        item["legacy_seed"] = wallet in legacy_by_wallet
        item["leaderboard_discovered"] = True
        for origin in incoming["origins"]:
            if origin not in item["origins"]:
                item["origins"].append(origin)
    for wallet, name in legacy_by_wallet.items():
        item = merged.setdefault(wallet, {"wallet": wallet, "origins": []})
        item["name"] = item.get("name") or name
        item["first_seen"] = item.get("first_seen") or observed_at
        item["last_seen"] = item.get("last_seen") or observed_at
        item["legacy_seed"] = True
        item.setdefault("leaderboard_discovered", False)
        origin = {"source": "legacy_seed", "name": name}
        if origin not in item["origins"]:
            item["origins"].append(origin)
    return merged


def ordered_candidate_wallets(pool):
    def key(pair):
        wallet, item = pair
        unseen = not item.get("last_analysis_at")
        latest = int(item.get("latest_source_trade_timestamp") or 0)
        analyzed = int(item.get("last_analyzed_source_trade_timestamp") or 0)
        changed = not unseen and latest > analyzed
        phase = 0 if unseen else 1 if changed else 2
        legacy_only = item.get("legacy_seed", False) and not item.get(
            "leaderboard_discovered", False
        )
        return (
            phase,
            legacy_only if unseen else False,
            item.get("last_analysis_at") or "",
            wallet,
        )
    return [wallet for wallet, _ in sorted(pool.items(), key=key)]
```

- [x] **Step 4: Run focused tests and verify GREEN**

Run the focused command from Step 2.

Expected: all discovery/state tests pass.

- [x] **Step 5: Commit Task 1**

```bash
git add work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
git commit -m "feat: discover dynamic wallet candidates"
```

### Task 2: Full trade history, non-crypto sleeve, and lifecycle classification

**Files:**
- Modify: `work/wallet_copy_paper/candidate_scan_once.py`
- Modify: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [x] **Step 1: Add failing pagination, filtering, and lifecycle tests**

```python
def trade(wallet, condition, asset, side, timestamp, title, outcome="Yes"):
    return {
        "proxyWallet": wallet,
        "conditionId": condition,
        "asset": asset,
        "side": side,
        "timestamp": timestamp,
        "title": title,
        "eventSlug": title.lower().replace(" ", "-"),
        "outcome": outcome,
        "transactionHash": f"{condition}-{asset}-{side}-{timestamp}",
    }


def test_trade_history_uses_windows_and_taker_false():
    wallet = "0x" + "d" * 40
    calls = []

    def fetch(endpoint, params):
        calls.append(dict(params))
        end = params.get("end")
        offset = params["offset"]
        if end is None:
            return (
                [trade(wallet, "c", "a", "BUY", 5, "Weather temperature"),
                 trade(wallet, "c", "a", "BUY", 4, "Weather temperature")]
                if offset == 0 else
                [trade(wallet, "c", "a", "BUY", 3, "Weather temperature"),
                 trade(wallet, "c", "a", "SELL", 3, "Weather temperature")]
            )
        if end == 3 and offset == 0:
            return [
                trade(wallet, "c", "a", "BUY", 3, "Weather temperature"),
                trade(wallet, "c", "a", "SELL", 3, "Weather temperature"),
            ]
        if end == 3 and offset == 2:
            return [
                trade(wallet, "old", "b", "BUY", 2, "Politics election"),
                trade(wallet, "old", "b", "BUY", 1, "Politics election"),
            ]
        return [trade(wallet, "old", "b", "BUY", 1, "Politics election")]

    rows, complete, detail = fetch_trades(
        wallet, fetch=fetch, page_size=2, max_offset=2
    )

    assert complete is True
    assert {row["timestamp"] for row in rows} == {1, 2, 3, 4, 5}
    assert all(call["takerOnly"] == "false" for call in calls)
    assert any(call.get("end") == 3 for call in calls)
    assert detail["window_count"] > 1


def test_crypto_rows_are_removed_but_mixed_wallet_survives():
    rows = [
        trade("0x" + "e" * 40, "crypto", "btc", "BUY", 1, "Bitcoin above 100k?"),
        trade("0x" + "e" * 40, "weather", "temp", "BUY", 2, "NYC temperature"),
    ]
    kept, removed = partition_noncrypto_rows(rows)
    assert [row["conditionId"] for row in kept] == ["weather"]
    assert [row["conditionId"] for row in removed] == ["crypto"]


def test_lifecycle_separates_hold_exit_and_basket():
    rows = [
        trade("0x" + "f" * 40, "hold", "hold-yes", "BUY", 1, "Election", "Yes"),
        trade("0x" + "f" * 40, "exit", "exit-yes", "BUY", 2, "Election", "Yes"),
        trade("0x" + "f" * 40, "exit", "exit-yes", "SELL", 3, "Election", "Yes"),
        trade("0x" + "f" * 40, "basket", "basket-yes", "BUY", 4, "Election", "Yes"),
        trade("0x" + "f" * 40, "basket", "basket-no", "BUY", 5, "Election", "No"),
    ]
    result = analyze_trade_lifecycle(rows, closed_condition_ids={"hold"})
    assert result["conditions"]["hold"]["lifecycle"] == "HOLD_TO_RESOLUTION"
    assert result["conditions"]["exit"]["lifecycle"] == "ACTIVE_EXIT"
    assert result["conditions"]["basket"]["lifecycle"] == "BASKET_OR_HEDGE"
    assert result["strategy_state"] == "FORMULA_RESEARCH"
```

- [x] **Step 2: Run the new tests and verify RED**

Run the focused test command.

Expected: failures identify missing window-pagination, partition, and lifecycle functions.

- [x] **Step 3: Implement complete-history window pagination**

Replace the fixed two-offset trade reader with:

```python
def fetch_trades(
    wallet: str,
    *,
    fetch=api_get,
    page_size: int = 10_000,
    max_offset: int = 10_000,
):
    accepted = []
    window_end = None
    windows = []
    while True:
        pages = []
        for offset in range(0, max_offset + 1, page_size):
            params = {
                "user": wallet,
                "takerOnly": "false",
                "limit": page_size,
                "offset": offset,
                "start": 1,
            }
            if window_end is not None:
                params["end"] = window_end
            page = fetch("trades", params)
            pages.extend(page)
            if len(page) < page_size:
                accepted.extend(pages)
                windows.append({"end": window_end, "rows": len(pages), "complete": True})
                return accepted, True, {"windows": windows, "window_count": len(windows)}
        timestamps = [int(number(row.get("timestamp"))) for row in pages]
        if not timestamps:
            return accepted, False, {"windows": windows, "block_reason": "missing_timestamp"}
        cutoff = min(timestamps)
        if window_end is not None and cutoff >= window_end:
            return accepted, False, {
                "windows": windows,
                "block_reason": "same_timestamp_exceeds_offset_window",
            }
        accepted.extend(row for row in pages if int(number(row.get("timestamp"))) > cutoff)
        windows.append({"end": window_end, "rows": len(pages), "complete": False})
        window_end = cutoff
```

The production defaults are external Data API constraints. A non-progressing same-timestamp boundary returns incomplete coverage and blocks promotion.

- [x] **Step 4: Implement sleeve filtering and lifecycle analysis**

Add `partition_noncrypto_rows`, `analyze_trade_lifecycle`, and the strategy-state rules. Group by condition, retain side/outcome/asset structure, count exact same-timestamp opposite-side activity, and use only zero/non-zero logical gates:

```python
def partition_noncrypto_rows(rows):
    kept, removed = [], []
    for row in rows:
        domain = classify_domain(
            str(row.get("title") or ""), str(row.get("eventSlug") or "")
        )
        (removed if domain == CRYPTO_DOMAIN else kept).append(row)
    return kept, removed


def analyze_trade_lifecycle(trades, *, closed_condition_ids):
    grouped = defaultdict(list)
    for row in trades:
        grouped[str(row.get("conditionId") or row.get("eventSlug") or "unknown")].append(row)
    conditions = {}
    same_timestamp_cycles = 0
    formula_conditions = 0
    directional_conditions = 0
    for condition, rows in grouped.items():
        assets = {str(row.get("asset") or "") for row in rows}
        outcomes = {str(row.get("outcome") or "") for row in rows}
        sides = {str(row.get("side") or "").upper() for row in rows}
        side_by_asset_time = defaultdict(set)
        for row in rows:
            side_by_asset_time[
                (str(row.get("asset") or ""), int(number(row.get("timestamp"))))
            ].add(str(row.get("side") or "").upper())
        cycles = sum(values == {"BUY", "SELL"} for values in side_by_asset_time.values())
        same_timestamp_cycles += cycles
        if len({value for value in assets if value}) > 1 or len(
            {value for value in outcomes if value}
        ) > 1:
            lifecycle = "BASKET_OR_HEDGE"
            formula_conditions += 1
        elif sides == {"BUY", "SELL"}:
            lifecycle = "ACTIVE_EXIT"
            formula_conditions += 1
        elif sides == {"BUY"} and condition in closed_condition_ids:
            lifecycle = "HOLD_TO_RESOLUTION"
            directional_conditions += 1
        elif sides == {"BUY"}:
            lifecycle = "OPEN_OR_UNRESOLVED"
            directional_conditions += 1
        elif sides == {"SELL"}:
            lifecycle = "SELL_ONLY_INCOMPLETE_HISTORY"
            formula_conditions += 1
        else:
            lifecycle = "UNRESOLVED"
            formula_conditions += 1
        conditions[condition] = {
            "lifecycle": lifecycle,
            "sides": sorted(sides),
            "assets": sorted(assets),
            "outcomes": sorted(outcomes),
            "same_timestamp_opposite_side_cycles": cycles,
        }
    if not conditions:
        state = "NO_NONCRYPTO_SLEEVE"
    elif same_timestamp_cycles and not directional_conditions:
        state = "OBSERVABLE_MM_OR_SPEED"
    elif formula_conditions:
        state = "FORMULA_RESEARCH"
    else:
        state = "DIRECTIONAL_RESEARCH_CANDIDATE"
    return {
        "strategy_state": state,
        "conditions": conditions,
        "same_timestamp_opposite_side_cycles": same_timestamp_cycles,
        "directional_condition_count": directional_conditions,
        "formula_condition_count": formula_conditions,
    }
```

- no non-crypto rows → `NO_NONCRYPTO_SLEEVE`;
- only one-sided directional lifecycle evidence → `DIRECTIONAL_RESEARCH_CANDIDATE`;
- exact same-timestamp opposite sides with no directional condition → `OBSERVABLE_MM_OR_SPEED`;
- mixed lifecycle evidence → `FORMULA_RESEARCH`.

Update `scan_wallet` so trades, closed positions, and open positions are partitioned before summaries; report removed crypto-row counts, lifecycle evidence, profile URL, and `BLOCK_DATA` when history is incomplete.

- [x] **Step 5: Run focused and existing non-live tests**

Run:

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  -m pytest -q -m 'not live' tests
```

Working directory: `work/wallet_copy_paper`.

Expected: all tests pass with the existing paper tests unchanged.

- [x] **Step 6: Commit Task 2**

```bash
git add work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
git commit -m "feat: classify copy strategy lifecycle"
```

### Task 3: Resumable CLI, atomic checkpoints, and human report

**Files:**
- Modify: `work/wallet_copy_paper/candidate_scan_once.py`
- Modify: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [x] **Step 1: Add failing CLI/checkpoint integration test**

```python
def test_main_writes_dynamic_pool_snapshot_and_markdown(tmp_path):
    dynamic = "0x" + "1" * 40

    def fetch(endpoint, params):
        if endpoint == "v1/leaderboard":
            return [leaderboard_row(dynamic, "dynamic-weather", 1)]
        raise AssertionError(f"unexpected endpoint in discover-only run: {endpoint}")

    output = tmp_path / "snapshot.json"
    state = tmp_path / "state.json"
    report = tmp_path / "snapshot.md"
    exit_code = main([
        "--output", str(output),
        "--state", str(state),
        "--report", str(report),
        "--categories", "WEATHER",
        "--periods", "MONTH",
        "--discover-only",
    ], fetch=fetch)

    payload = json.loads(output.read_text())
    pool = json.loads(state.read_text())
    assert exit_code == 0
    assert dynamic in pool["candidates"]
    assert payload["scope"]["dynamic_universe"] is True
    assert payload["scope"]["candidate_pool_size"] > len(LEGACY_SEEDS)
    assert "dynamic-weather" in report.read_text()
    assert "polymarket.com/profile/" in report.read_text()
```

- [x] **Step 2: Run the integration test and verify RED**

Run the focused test command.

Expected: `main` rejects injected arguments/fetcher and does not create state or Markdown.

- [x] **Step 3: Implement state, queue, CLI, and reports**

Implement:

```python
def load_candidate_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "candidates": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload.get("candidates"), dict):
        raise ValueError("candidate state is missing candidates")
    return payload


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)
```

Refactor `main(argv=None, fetch=api_get)` with:

- required `--output`;
- optional `--state` and `--report`, derived from output when omitted;
- approved default categories/periods with explicit CLI overrides;
- optional `--max-wallets`, labelled as an operational deferral cap;
- `--discover-only` for source-coverage snapshots;
- discovery, state merge, atomic checkpoint after each analyzed wallet;
- queue selection with dynamic unseen wallets ahead of legacy-only seeds;
- activity probing only after the unseen queue is exhausted;
- source errors retained in state and output rather than silently dropping candidates;
- JSON coverage and numeric provenance replacing the fixed `wallet_count` universe language;
- Markdown rows containing strategy state, sleeve, lifecycle, evidence, and Polymarket profile URL.

The execution skeleton is:

```python
def main(argv=None, *, fetch=api_get):
    parser = build_parser()
    args = parser.parse_args(argv)
    generated_at = datetime.now(timezone.utc).isoformat()
    state_path = args.state or args.output.with_name("candidate_discovery_state.json")
    report_path = args.report or args.output.with_suffix(".md")
    state = load_candidate_state(state_path)
    discovered, discovery_coverage = discover_leaderboard_candidates(
        fetch,
        categories=tuple(args.categories),
        periods=tuple(args.periods),
        observed_at=generated_at,
    )
    pool = merge_candidate_pool(
        state["candidates"],
        discovered,
        LEGACY_SEEDS,
        observed_at=generated_at,
    )
    selected = [] if args.discover_only else ordered_candidate_wallets(pool)
    if args.max_wallets is not None:
        selected = selected[:args.max_wallets]
    wallet_results = []
    for wallet in selected:
        item = pool[wallet]
        try:
            result = scan_wallet(item["name"], wallet, fetch=fetch)
            wallet_results.append(result)
            item["last_analysis_at"] = generated_at
            item["last_analyzed_source_trade_timestamp"] = result[
                "latest_source_trade_timestamp"
            ]
            item["analysis_status"] = result["strategy"]["strategy_state"]
            item.pop("analysis_error", None)
        except Exception as error:
            item["analysis_error"] = f"{type(error).__name__}: {error}"
        write_json_atomic(state_path, {
            "schema_version": 1,
            "updated_at": generated_at,
            "candidates": pool,
        })
    payload = build_snapshot(
        generated_at=generated_at,
        pool=pool,
        discovery_coverage=discovery_coverage,
        wallet_results=wallet_results,
        selected=selected,
        discover_only=args.discover_only,
        operational_cap=args.max_wallets,
    )
    write_json_atomic(args.output, payload)
    report_path.write_text(render_markdown(payload, pool), encoding="utf-8")
    return 0
```

- [x] **Step 4: Run focused tests and verify GREEN**

Run the focused test command.

Expected: all candidate scanner tests pass.

- [x] **Step 5: Run the complete non-live wallet-copy suite**

Run:

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  -m pytest -q -m 'not live' tests
```

Working directory: `work/wallet_copy_paper`.

Expected: zero failures.

- [x] **Step 6: Commit Task 3**

```bash
git add work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
git commit -m "feat: checkpoint dynamic wallet scans"
```

### Task 4: Public read-only validation and final audit

**Files:**
- Modify: `docs/superpowers/plans/2026-07-25-dynamic-wallet-discovery.md`
- Generate (ignored/local evidence): `/tmp/polymarket-dynamic-wallet-validation/`

- [x] **Step 1: Run syntax and full non-live verification**

```bash
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  -m py_compile candidate_scan_once.py
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  -m pytest -q -m 'not live' tests
```

Working directory: `work/wallet_copy_paper`.

Expected: compilation succeeds and all non-live tests pass.

- [x] **Step 2: Run a bounded public discovery snapshot**

```bash
mkdir -p /tmp/polymarket-dynamic-wallet-validation
/Users/luke/Documents/polymarket/work/polymarket-api-py312-venv/bin/python \
  work/wallet_copy_paper/candidate_scan_once.py \
  --output /tmp/polymarket-dynamic-wallet-validation/weather-month.json \
  --state /tmp/polymarket-dynamic-wallet-validation/state.json \
  --report /tmp/polymarket-dynamic-wallet-validation/weather-month.md \
  --categories WEATHER \
  --periods MONTH \
  --discover-only
```

The WEATHER/MONTH scope is an explicit validation-run scope, not a research qualification threshold.

- [x] **Step 3: Audit the live snapshot**

Read the generated files and verify:

- the discovery coverage reports success or an explicit boundary/failure;
- at least one leaderboard-origin address is not a legacy seed;
- every address is normalized and deduplicated;
- the profile URL is present;
- `paper_only=true` and `real_order_submitted=false`;
- no source history or delayed-copy result is mislabeled `COPYABLE_EVIDENCE`.

- [x] **Step 4: Audit diffs and safety**

```bash
git diff --check
git status --short
git diff -- work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
rg -n "private.key|api.key|secret|POST|place.order|create.order|live.trading" \
  work/wallet_copy_paper/candidate_scan_once.py
```

Expected: no whitespace errors, no credential/order path, and only planned files changed.

- [x] **Step 5: Mark the plan complete and commit it**

Change every completed checkbox in this plan from `[ ]` to `[x]`, rerun `git diff --check`, then:

```bash
git add docs/superpowers/plans/2026-07-25-dynamic-wallet-discovery.md
git commit -m "docs: complete dynamic wallet discovery plan"
```
