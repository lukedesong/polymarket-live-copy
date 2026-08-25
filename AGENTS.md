# AGENTS.md

This is a local rules/onboarding file for agents. It is intentionally **not** part
of the versioned release manifest (`MANIFEST.sha256`), matching the project's
existing convention that release surface stays limited to `app/`, `tools/`,
`ops/`, `systemd/`.

## Cursor Cloud specific instructions

### What this is
Polymarket 实时跟单系统 (real-time copy-trading execution engine). One product,
run as multiple "sleeve" daemons (cd90 / zockdo / wallet_9506, plus hot-standby
variants) that copy a source wallet's BUY/SELL actions. See `README.md` for the
behavior contract and safety principles.

### Runtime / dependencies
- Python **3.12** is preinstalled. Use `python3` — there is **no `python`** shim on PATH.
- Third-party deps are **not vendored** in the repo and there is intentionally no
  committed `requirements.txt` (production uses a prebuilt server venv). The Cloud
  Agent update script installs them via pip. The direct deps are
  `py-clob-client-v2` (module `py_clob_client_v2`, the official Polymarket **V2**
  CLOB SDK on PyPI), `websockets`, `eth-account`, `eth-utils`. `pytest` arrives
  transitively. If you add new imports, install the package and update the Cloud
  update script accordingly.

### Test / lint / build
- Tests: run `python3 -m pytest` from the repo root. `pytest.ini` sets
  `pythonpath = app tools`, so app modules import by bare name. Full suite is
  **319 tests** (matches `COMMITTED.json` / `CANDIDATE_TEST_RECEIPT.json`).
  Without `py_clob_client_v2`, `tests/test_cd90_live_copy.py` fails at import.
- Lint: none configured (no ruff/flake8/black/mypy). Do not invent one.
- Build: none. "Build/verify" here means the release-transaction tooling under
  `tools/` (hash manifest + version authority); not needed for local dev/testing.

### Running the app safely (IMPORTANT)
- `python3 app/cd90_live_copy.py --status --runtime-dir <dir>` is **fully offline**:
  it creates the per-sleeve `live.sqlite3` and writes `status.json` / `status.html`.
  Safe to run anytime. Same `--status` exists on the sleeve wrappers
  (`app/zockdo_live_copy.py`, `app/wallet9506_live_copy.py`).
- `--run`, `--run-hot-standby`, `--preflight`, `--reconcile-unresolved-only`,
  `--establish-forward-watermark` require live credentials (from server-only
  `EnvironmentFile`s) and reach the Polymarket/Polygon network against the **real
  authenticated trading account**. Do **not** run these, and do **not** set
  `POLYMARKET_LIVE_TRADING=1` / `I_UNDERSTAND_THIS_PLACES_A_REAL_ORDER=1`, unless
  the operator explicitly authorizes it for a specific action. This system is
  live-only (实盘); there is no paper/simulation mode.
- Pure core sizing logic lives in `app/cd90_live_sizing.py`
  (`derive_fixed_share_scale`, `plan_action`) and is side-effect-free — the best
  place to exercise copy-buy / inventory-capped copy-sell decisions without any
  network or orders.
