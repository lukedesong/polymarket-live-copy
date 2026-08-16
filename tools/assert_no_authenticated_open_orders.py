#!/usr/bin/env python3
"""Fail closed when the authenticated CLOB wallet still has open orders.

The live release transaction uses this read-only gate immediately before and
after it quiesces all executors.  It never cancels, edits, or submits an
exchange order.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1] / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from cd90_live_copy import build_authenticated_live_client  # noqa: E402


def main() -> int:
    env = dict(os.environ)
    env.setdefault(
        "POLYMARKET_SHARED_WALLET_LOCK_PATH",
        "/srv/polymarket-live/runtime/authenticated-wallet.lock",
    )
    env.setdefault(
        "POLYMARKET_SHARED_WALLET_COORDINATOR_PATH",
        "/srv/polymarket-live/runtime/shared_wallet/coordinator.sqlite3",
    )
    open_orders = build_authenticated_live_client(env).get_open_orders()
    if not isinstance(open_orders, list):
        print("AUTHENTICATED_OPEN_ORDER_GATE:state=UNEXPECTED_RESPONSE")
        return 2
    print(f"AUTHENTICATED_OPEN_ORDER_GATE:count={len(open_orders)}")
    return 0 if not open_orders else 2


if __name__ == "__main__":
    raise SystemExit(main())
