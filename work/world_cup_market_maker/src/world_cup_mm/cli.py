from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from py_clob_client_v2 import ClobClient
from py_clob_client_v2.clob_types import ApiCreds

from .collector import collect_market_and_sports
from .discovery import (
    GAMMA_EVENTS_URL,
    GammaClient,
    classify_events,
    fetch_all_events,
    ranked_markets,
)
from .order_control import AuthenticatedOrderControl, RecordingOrderControl
from .market_params import ClobMarketParamsClient
from .paper_runtime import PaperRuntimeSink
from .risk import RiskContext, RiskState, evaluate_risk
from .runtime import RiskRuntime, monitor_risk
from .storage import Store, replay_events


CANCEL_CREDENTIAL_NAMES = (
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
    "POLYMARKET_SIGNATURE_TYPE",
)
FUNDER_ADDRESS_NAMES = (
    "POLYMARKET_WALLET_ADDRESS",
    "DEPOSIT_WALLET_ADDRESS",
    "POLYMARKET_FUNDER_ADDRESS",
)
POLYGON_CHAIN_ID = 137  # official external chain constraint


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="world-cup-mm")
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data/world_cup_mm.sqlite3"),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("scan")
    collect = commands.add_parser("collect")
    collect.add_argument("--all-eligible", action="store_true")
    collect.add_argument("--cancel-enabled", action="store_true")
    collect.add_argument("--max-messages", type=int)
    commands.add_parser("status")
    commands.add_parser("replay")
    paper_run = commands.add_parser("paper-run")
    # User specified one hour at launch; seconds are formula-derived by the caller.
    paper_run.add_argument("--duration-seconds", type=int)
    paper_run.set_defaults(cancel_enabled=False)
    commands.add_parser("paper-status")
    commands.add_parser("paper-export")
    return parser


def run_scan(
    store: Store,
    client: Any,
    *,
    now: datetime | None = None,
    scan_id: str | None = None,
) -> dict[str, Any]:
    started_at = now or datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        raise ValueError("scan_time_missing_timezone")
    events = fetch_all_events(client.fetch_page)
    eligible, rejected = classify_events(events, now=started_at)
    ranked = ranked_markets(eligible)
    completed_at = datetime.now(timezone.utc) if now is None else now
    identity = scan_id or str(uuid.uuid4())
    store.record_scan(
        identity,
        events=events,
        eligible=ranked,
        rejected=rejected,
        started_at=started_at,
        completed_at=completed_at,
        source_url=GAMMA_EVENTS_URL,
    )
    return {
        "scan_id": identity,
        "source": GAMMA_EVENTS_URL,
        "event_count": len(events),
        "eligible_count": len(ranked),
        "frontier_count": sum(1 for market in ranked if market.frontier),
        "rejected_count": len(rejected),
        "markets": [
            {
                "event_id": market.event_id,
                "event_title": market.event_title,
                "market_id": market.market_id,
                "question": market.question,
                "condition_id": market.condition_id,
                "token_ids": list(market.token_ids),
                "game_start_time": market.game_start_time.isoformat(),
                "liquidity": str(market.liquidity),
                "volume_24h": str(market.volume_24h),
                "frontier": market.frontier,
            }
            for market in ranked
        ],
    }


def build_status(
    store: Store,
    *,
    cancel_capable: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    session = store.latest_session_summary()
    session_connected = bool(session and session["connected"])
    markets = store.selected_markets(all_eligible=False)
    market_status: list[dict[str, Any]] = []
    all_future_quote_eligible = bool(markets)
    for market in markets:
        ready = all(store.book_ready(asset_id) for asset_id in market.token_ids)
        decision = evaluate_risk(
            start=market.game_start_time,
            now=current,
            context=RiskContext(
                condition_id=market.condition_id,
                market_open=True,
                accepting_orders=True,
                market_ws_connected=session_connected,
                book_ready=ready,
                sports_live=store.latest_sports_live(market.event_slug),
                cancel_armed=cancel_capable,
            ),
        )
        future_quote_eligible = decision.state is RiskState.PREMATCH_OPEN and cancel_capable
        all_future_quote_eligible = all_future_quote_eligible and future_quote_eligible
        market_status.append(
            {
                "market_id": market.market_id,
                "condition_id": market.condition_id,
                "game_start_time": market.game_start_time.isoformat(),
                "book_ready": ready,
                "risk_state": decision.state.value,
                "risk_reason": decision.reason,
                "risk_actions": [action.value for action in decision.actions],
                "seconds_to_start": decision.seconds_to_start,
                "future_quote_eligible": future_quote_eligible,
            }
        )
    return {
        "mode": "cancel_capable" if cancel_capable else "data_only",
        "cancel_capable": cancel_capable,
        "quote_capable": False,
        "future_quote_eligible": all_future_quote_eligible,
        "latest_scan": store.latest_scan_summary(),
        "latest_session": session,
        "raw_event_count": store.raw_event_count(),
        "trade_count": store.trade_count(),
        "markets": market_status,
    }


def run_replay(store: Store) -> dict[str, Any]:
    session = store.latest_session_summary()
    if session is None:
        raise RuntimeError("no_collector_session")
    session_id = str(session["session_id"])
    books = replay_events(store.raw_events(session_id))
    return {
        "session_id": session_id,
        "books": {
            asset_id: {
                "best_bid": str(book.best_bid) if book.best_bid is not None else None,
                "best_ask": str(book.best_ask) if book.best_ask is not None else None,
            }
            for asset_id, book in books.items()
        },
    }


def build_paper_status(store: Store) -> dict[str, Any]:
    account = store.paper_account()
    return {
        "mode": "paper_only",
        "latest_session": store.latest_session_summary(),
        "official_trade_events": store.trade_count(),
        "paper_fill_count": store.paper_fill_count(),
        "open_paper_order_count": len(store.open_paper_orders()),
        "buy_cost": str(account.buy_cost),
        "sell_proceeds": str(account.sell_proceeds),
        "realized_profit": str(account.realized_profit),
        "unrealized_profit": str(account.unrealized_profit),
        "total_profit": str(account.total_profit),
        "positions": [
            {
                "market_id": position.market_id,
                "asset_id": position.asset_id,
                "outcome": position.outcome,
                "quantity": str(position.quantity),
                "average_cost": str(position.average_cost),
                "mark_price": str(position.mark_price),
                "realized_profit": str(position.realized_profit),
                "unrealized_profit": str(position.unrealized_profit),
            }
            for position in store.paper_positions()
        ],
    }


def build_paper_export(store: Store) -> dict[str, Any]:
    return {"overview": build_paper_status(store), "fills": store.paper_fill_rows()}


async def run_paper_collection(
    store: Store,
    markets: list[Any],
    *,
    duration_seconds: int | None,
) -> dict[str, Any]:
    if duration_seconds is not None and duration_seconds <= 0:
        raise ValueError("duration_seconds_must_be_positive")
    params_client = ClobMarketParamsClient()
    params_by_condition = {
        market.condition_id: params_client.fetch(market.condition_id)
        for market in markets
    }
    session_id = str(uuid.uuid4())
    sink = PaperRuntimeSink(
        store,
        session_id,
        markets,
        params_by_condition,
    )
    asset_ids = list(
        dict.fromkeys(token for market in markets for token in market.token_ids)
    )
    collector = collect_market_and_sports(asset_ids, sink)
    timed_out = False
    if duration_seconds is None:
        message_count = await collector
    else:
        try:
            message_count = await asyncio.wait_for(collector, timeout=duration_seconds)
        except TimeoutError:
            timed_out = True
            message_count = store.raw_event_count()
    return {
        "session_id": session_id,
        "mode": "paper_only",
        "duration_complete": timed_out,
        "message_count": message_count,
        "paper": build_paper_status(store),
    }


def make_order_control(*, cancel_enabled: bool, env: Mapping[str, str]):
    if not cancel_enabled:
        return RecordingOrderControl()
    missing = [name for name in CANCEL_CREDENTIAL_NAMES if not env.get(name)]
    funder = next((env[name] for name in FUNDER_ADDRESS_NAMES if env.get(name)), None)
    if funder is None:
        missing.append("POLYMARKET_WALLET_ADDRESS")
    if missing:
        raise RuntimeError(f"missing_cancel_credentials:{','.join(missing)}")
    creds = ApiCreds(
        api_key=env["POLYMARKET_API_KEY"],
        api_secret=env["POLYMARKET_API_SECRET"],
        api_passphrase=env["POLYMARKET_API_PASSPHRASE"],
    )
    client = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=POLYGON_CHAIN_ID,
        key=env["POLYMARKET_PRIVATE_KEY"],
        creds=creds,
        signature_type=int(env["POLYMARKET_SIGNATURE_TYPE"]),
        funder=funder,
        use_server_time=True,
    )
    return AuthenticatedOrderControl(client)


class StoreSink:
    def __init__(
        self,
        store: Store,
        session_id: str,
        *,
        selection_mode: str,
        started_at: datetime | None = None,
        risk_runtime: Any | None = None,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.selection_mode = selection_mode
        self.started_at = started_at
        self.risk_runtime = risk_runtime

    def _evaluate_risk(self) -> None:
        if self.risk_runtime is not None:
            self.risk_runtime.evaluate_all()

    async def connected(self) -> None:
        self.store.start_session(
            self.session_id,
            selection_mode=self.selection_mode,
            started_at=self.started_at,
        )
        self._evaluate_risk()

    async def market_event(self, payload: dict[str, Any]) -> None:
        self.store.record_market_event(self.session_id, payload)
        self._evaluate_risk()

    async def sports_event(self, payload: dict[str, Any]) -> None:
        self.store.record_sports_event(self.session_id, payload)
        self._evaluate_risk()

    async def disconnected(self) -> None:
        self.store.invalidate_session_books(self.session_id)
        self._evaluate_risk()


async def collect_with_risk(
    asset_ids: list[str],
    sink: StoreSink,
    runtime: RiskRuntime,
    *,
    max_messages: int | None,
) -> int:
    stop = asyncio.Event()
    collector_task = asyncio.create_task(
        collect_market_and_sports(asset_ids, sink, max_messages=max_messages)
    )
    risk_task = asyncio.create_task(monitor_risk(runtime, stop))
    try:
        done, _pending = await asyncio.wait(
            {collector_task, risk_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if risk_task in done:
            error = risk_task.exception()
            if error is not None:
                collector_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await collector_task
                raise error
        return await collector_task
    finally:
        stop.set()
        if not risk_task.done():
            await risk_task


def _print_json(payload: Mapping[str, Any]) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = Store(args.database)
    try:
        if args.command == "scan":
            _print_json(run_scan(store, GammaClient()))
            return 0
        if args.command == "status":
            _print_json(build_status(store, cancel_capable=False))
            return 0
        if args.command == "replay":
            _print_json(run_replay(store))
            return 0
        if args.command == "paper-status":
            _print_json(build_paper_status(store))
            return 0
        if args.command == "paper-export":
            _print_json(build_paper_export(store))
            return 0
        if args.command == "paper-run":
            markets = store.selected_markets(all_eligible=False)
            if not markets:
                run_scan(store, GammaClient())
                markets = store.selected_markets(all_eligible=False)
            if not markets:
                raise RuntimeError("no_world_cup_frontier_markets")
            _print_json(
                asyncio.run(
                    run_paper_collection(
                        store,
                        markets,
                        duration_seconds=args.duration_seconds,
                    )
                )
            )
            return 0
        if args.command == "collect":
            order_control = make_order_control(
                cancel_enabled=args.cancel_enabled,
                env=os.environ,
            )
            markets = store.selected_markets(all_eligible=args.all_eligible)
            if not markets:
                raise RuntimeError("no_selected_markets_run_scan_first")
            asset_ids = list(
                dict.fromkeys(
                    token_id for market in markets for token_id in market.token_ids
                )
            )
            session_id = str(uuid.uuid4())
            sink = StoreSink(
                store,
                session_id,
                selection_mode="all_eligible" if args.all_eligible else "frontier",
            )
            runtime = RiskRuntime(
                store,
                markets,
                order_control,
                cancel_capable=args.cancel_enabled,
            )
            sink.risk_runtime = runtime
            message_count = asyncio.run(
                collect_with_risk(
                    asset_ids,
                    sink,
                    runtime,
                    max_messages=args.max_messages,
                )
            )
            _print_json(
                {
                    "session_id": session_id,
                    "selection_mode": sink.selection_mode,
                    "message_count": message_count,
                }
            )
            return 0
        raise RuntimeError("unknown_command")
    except Exception as exc:
        _print_json({"status": "error", "error": f"{type(exc).__name__}:{exc}"})
        return 1
    finally:
        store.close()


if __name__ == "__main__":
    sys.exit(main())
