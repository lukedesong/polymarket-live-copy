# Copyability-First Wallet Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make wallet strategy shape descriptive while allowing every relevant wallet into deep analysis unless data is absent, and move speed concerns onto a separate forward-copyability axis.

**Architecture:** Add one pure execution-speed summarizer shared by light and deep scans. Keep existing lifecycle labels for source behavior, add an independent `copyability_state`, and base `deep_scan_eligible` on relevant data availability instead of buy/sell structure or page saturation.

**Tech Stack:** Python standard library, pytest, Polymarket public Data API

---

### Task 1: Lock the new light-screen contract with failing tests

**Files:**
- Modify: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`
- Test: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [ ] **Step 1: Change the active-exit regression test**

Replace the old exclusion assertion with:

```python
def test_light_screen_keeps_active_exit_for_full_history():
    wallet = "0x" + "8" * 40

    def fetch(endpoint: str, params: dict[str, object]):
        assert endpoint == "trades"
        return [
            trade(wallet, "exit", "yes", "BUY", 10, "Election"),
            trade(wallet, "exit", "yes", "SELL", 11, "Election"),
        ]

    result = light_screen_wallet("active-exit", wallet, fetch=fetch)

    assert result["strategy"]["strategy_state"] == "FORMULA_RESEARCH"
    assert result["copyability_state"] == "SPEED_REVIEW"
    assert result["deep_scan_eligible"] is True
```

The timestamps are synthetic test fixtures. The qualification rule comes from
the user-approved copyability contract, not those fixture values.

- [ ] **Step 2: Change the saturated-page regression test**

Assert that saturation remains visible but no longer changes the source
strategy state or blocks the deep scan:

```python
assert result["strategy"]["high_frequency_recent_page_saturated"] is True
assert result["strategy"]["strategy_state"] == (
    "DIRECTIONAL_RESEARCH_CANDIDATE"
)
assert result["copyability_state"] == "SPEED_REVIEW"
assert result["deep_scan_eligible"] is True
```

- [ ] **Step 3: Add rolling-window and no-sleeve tests**

Import `SPEED_OBSERVATION_WINDOW_SECONDS` and add:

```python
def test_rolling_speed_window_is_evidence_not_rejection():
    wallet = "0x" + "3" * 40
    rows = [
        trade(wallet, "cycle", "yes", "BUY", 100, "Election"),
        trade(
            wallet,
            "cycle",
            "yes",
            "SELL",
            100 + SPEED_OBSERVATION_WINDOW_SECONDS - 1,
            "Election",
        ),
    ]

    result = light_screen_wallet(
        "rapid-exit",
        wallet,
        fetch=lambda endpoint, params: rows,
    )

    assert result["strategy"]["rapid_opposite_side_transition_count"] == 1
    assert result["copyability_state"] == "SPEED_REVIEW"
    assert result["deep_scan_eligible"] is True


def test_no_noncrypto_sleeve_is_not_deep_scan_eligible():
    wallet = "0x" + "4" * 40
    rows = [
        trade(wallet, "crypto", "btc", "BUY", 100, "Bitcoin above 100k?")
    ]

    result = light_screen_wallet(
        "crypto-only",
        wallet,
        fetch=lambda endpoint, params: rows,
    )

    assert result["copyability_state"] == "NO_NONCRYPTO_SLEEVE"
    assert result["deep_scan_eligible"] is False
```

The rolling observation duration is a **user-specified value** and is not a
rejection threshold.

- [ ] **Step 4: Run the focused tests and verify RED**

Run:

```bash
work/polymarket-api-py312-venv/bin/python -m pytest \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py \
  -k "active_exit or saturated or rolling_speed or no_noncrypto" -v
```

Expected: failures show the old active-exit/page-saturation exclusions and
missing copyability metrics.

- [ ] **Step 5: Commit only after GREEN in Task 2**

Do not commit a knowingly failing state.

### Task 2: Implement speed evidence and independent copyability

**Files:**
- Modify: `work/wallet_copy_paper/candidate_scan_once.py`
- Modify: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [ ] **Step 1: Add the observation-window constant**

Add beside the recent light-screen API limit:

```python
# User-specified observation window. It raises research evidence only and
# never acts as a qualification or rejection threshold.
SPEED_OBSERVATION_WINDOW_SECONDS = 60
```

- [ ] **Step 2: Add a pure speed summarizer**

Add a function that deduplicates logical actions by transaction hash, asset,
side, and timestamp; calculates the maximum unique transaction count in any
rolling observation window; and counts adjacent same-asset transitions whose
side changes within the window:

```python
def execution_speed_summary(
    trades: list[dict[str, Any]],
    *,
    window_seconds: int = SPEED_OBSERVATION_WINDOW_SECONDS,
) -> dict[str, Any]:
    actions = sorted(
        {
            (
                int(number(row.get("timestamp"))),
                str(row.get("transactionHash") or ""),
                str(row.get("asset") or ""),
                str(row.get("side") or "").upper(),
            )
            for row in trades
            if int(number(row.get("timestamp"))) > 0
            and row.get("transactionHash")
            and row.get("side")
        }
    )
    transaction_times: dict[str, int] = {}
    actions_by_asset: dict[
        str, list[tuple[int, str, str]]
    ] = defaultdict(list)
    for timestamp, transaction_hash, asset, side in actions:
        transaction_times[transaction_hash] = min(
            timestamp,
            transaction_times.get(transaction_hash, timestamp),
        )
        actions_by_asset[asset].append(
            (timestamp, transaction_hash, side)
        )

    timeline = sorted(
        (timestamp, transaction_hash)
        for transaction_hash, timestamp in transaction_times.items()
    )
    maximum = 0
    left = 0
    for right, (timestamp, _) in enumerate(timeline):
        while (
            left <= right
            and timestamp - timeline[left][0] >= window_seconds
        ):
            left += 1
        maximum = max(maximum, right - left + 1)

    transition_count = 0
    for asset_actions in actions_by_asset.values():
        for previous, current in zip(
            asset_actions,
            asset_actions[1:],
            strict=False,
        ):
            if (
                previous[2] != current[2]
                and current[0] - previous[0] < window_seconds
            ):
                transition_count += 1

    return {
        "speed_observation_window_seconds": window_seconds,
        "maximum_unique_transactions_in_rolling_window": maximum,
        "rapid_opposite_side_transition_count": transition_count,
    }
```

Return these stable fields:

```python
{
    "speed_observation_window_seconds": window_seconds,
    "maximum_unique_transactions_in_rolling_window": maximum,
    "rapid_opposite_side_transition_count": transition_count,
}
```

- [ ] **Step 3: Apply the summary in the light screen**

Keep existing exact-second and saturation evidence, remove the state rewrite,
and derive:

```python
speed_risk_observed = bool(
    burst_count
    or speed["rapid_opposite_side_transition_count"]
    or public_page_saturated_within_one_utc_day
)
lifecycle.update(speed)
lifecycle["execution_speed_risk_observed"] = speed_risk_observed

copyability_state = (
    "NO_NONCRYPTO_SLEEVE"
    if not noncrypto_trades
    else "SPEED_REVIEW"
    if speed_risk_observed
    else "NEEDS_FORWARD_PAPER"
)
```

Set:

```python
"copyability_state": copyability_state,
"deep_scan_eligible": bool(noncrypto_trades),
```

Do not reinterpret page saturation as source strategy state.

- [ ] **Step 4: Apply the same axis in the deep scan**

Calculate the speed summary from complete non-crypto history. Set
`copyability_state` to `BLOCK_DATA` when source coverage is incomplete,
otherwise `SPEED_REVIEW` when timing risk is observed, otherwise
`NEEDS_FORWARD_PAPER`.

Do not emit `FORWARD_COPYABLE` or `UNFOLLOWABLE_EXECUTION` from source history.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```bash
work/polymarket-api-py312-venv/bin/python -m pytest \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py \
  -k "active_exit or saturated or rolling_speed or no_noncrypto" -v
```

Expected: selected tests pass.

- [ ] **Step 6: Commit the behavior change**

```bash
git add \
  work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
git commit -m "fix: screen wallets by copyability evidence"
```

### Task 3: Preserve the new state through orchestration and reports

**Files:**
- Modify: `work/wallet_copy_paper/candidate_scan_once.py`
- Modify: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [ ] **Step 1: Write a failing orchestration test**

Replace the old test that expected active exits to stop at light depth. Mock
`trades`, `closed-positions`, and `positions`, then assert:

```python
assert payload["wallets"][0]["analysis_depth"] == "DEEP_HISTORY"
assert payload["wallets"][0]["copyability_state"] == "SPEED_REVIEW"
assert pool["candidates"][dynamic]["copyability_state"] == "SPEED_REVIEW"
```

- [ ] **Step 2: Run the orchestration test and verify RED**

Run:

```bash
work/polymarket-api-py312-venv/bin/python -m pytest \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py \
  -k "main_deep_scans_active_exit" -v
```

Expected: the persisted candidate lacks `copyability_state` before the code
change.

- [ ] **Step 3: Persist and render copyability**

When updating the candidate pool, store:

```python
item["copyability_state"] = result["copyability_state"]
```

Add a copyability column to the human-readable strategy table and state in the
conclusion boundary that lifecycle does not reject a wallet; only delayed
forward evidence may establish execution rejection.

- [ ] **Step 4: Run the orchestration test and verify GREEN**

Run the command from Step 2. Expected: pass.

- [ ] **Step 5: Commit report and persistence changes**

```bash
git add \
  work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
git commit -m "feat: report wallet copyability separately"
```

### Task 4: Full verification

**Files:**
- Verify: `work/wallet_copy_paper/candidate_scan_once.py`
- Verify: `work/wallet_copy_paper/tests/test_candidate_scan_once.py`

- [ ] **Step 1: Run the complete candidate scanner test file**

```bash
work/polymarket-api-py312-venv/bin/python -m pytest \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py -v
```

Expected: all tests pass with no failures.

- [ ] **Step 2: Compile the scanner**

```bash
work/polymarket-api-py312-venv/bin/python -m py_compile \
  work/wallet_copy_paper/candidate_scan_once.py
```

Expected: exit code zero and no output.

- [ ] **Step 3: Check the scoped diff**

```bash
git diff --check HEAD~2..HEAD -- \
  work/wallet_copy_paper/candidate_scan_once.py \
  work/wallet_copy_paper/tests/test_candidate_scan_once.py
```

Expected: exit code zero and no whitespace errors.

- [ ] **Step 4: Confirm unrelated user changes remain untouched**

```bash
git status --short
```

Expected: pre-existing unrelated changes remain; no runtime ledger, paper
account, private credential, or live-order file is modified by this plan.
