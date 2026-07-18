from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import dataclass
from typing import Any, Protocol, Sequence

import websockets


MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SPORTS_WS_URL = "wss://sports-api.polymarket.com/ws"
MARKET_HEARTBEAT_SECONDS = 10  # documented external channel constraint


class MarketSink(Protocol):
    async def connected(self) -> None: ...

    async def market_event(self, payload: dict[str, Any]) -> None: ...

    async def disconnected(self) -> None: ...


class SportsSink(Protocol):
    async def sports_event(self, payload: dict[str, Any]) -> None: ...


@dataclass(frozen=True, slots=True)
class SportsFrame:
    pong_required: bool
    payload: dict[str, Any] | None


def market_subscription(asset_ids: Sequence[str]) -> dict[str, Any]:
    assets = [str(asset_id) for asset_id in asset_ids if str(asset_id)]
    if not assets:
        raise ValueError("empty_market_subscription")
    return {
        "assets_ids": assets,
        "type": "market",
        "custom_feature_enabled": True,
    }


def decode_market_frame(frame: str | bytes) -> list[dict[str, Any]]:
    text = frame.decode() if isinstance(frame, bytes) else frame
    payload = json.loads(text)
    items = payload if isinstance(payload, list) else [payload]
    if not all(isinstance(item, dict) for item in items):
        raise ValueError("market_frame_item_not_object")
    return items


def parse_sports_frame(frame: str | bytes) -> SportsFrame:
    text = frame.decode() if isinstance(frame, bytes) else frame
    if text == "ping":
        return SportsFrame(pong_required=True, payload=None)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("sports_frame_not_object")
    return SportsFrame(pong_required=False, payload=payload)


async def handle_market_disconnect(store: Any, session_id: str) -> None:
    store.invalidate_session_books(session_id)


async def _market_heartbeat(websocket: Any) -> None:
    while True:
        await asyncio.sleep(MARKET_HEARTBEAT_SECONDS)
        await websocket.send("PING")


async def collect_market(
    asset_ids: Sequence[str],
    sink: MarketSink,
    *,
    max_messages: int | None = None,
    uri: str = MARKET_WS_URL,
) -> int:
    if max_messages is not None and max_messages <= 0:
        raise ValueError("max_messages_must_be_positive")
    count = 0
    async with websockets.connect(uri, ping_interval=None) as websocket:
        await websocket.send(json.dumps(market_subscription(asset_ids)))
        heartbeat = asyncio.create_task(_market_heartbeat(websocket))
        await sink.connected()
        try:
            async for frame in websocket:
                if frame in ("PONG", b"PONG"):
                    continue
                for payload in decode_market_frame(frame):
                    await sink.market_event(payload)
                    count += 1
                    if max_messages is not None and count >= max_messages:
                        return count
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            await sink.disconnected()
    return count


async def collect_sports(
    sink: SportsSink,
    *,
    uri: str = SPORTS_WS_URL,
) -> None:
    async with websockets.connect(uri, ping_interval=None) as websocket:
        async for raw_frame in websocket:
            frame = parse_sports_frame(raw_frame)
            if frame.pong_required:
                await websocket.send("pong")
            elif frame.payload is not None:
                await sink.sports_event(frame.payload)


async def collect_market_and_sports(
    asset_ids: Sequence[str],
    sink: MarketSink | SportsSink,
    *,
    max_messages: int | None = None,
) -> int:
    sports_task = asyncio.create_task(collect_sports(sink))  # type: ignore[arg-type]
    try:
        return await collect_market(
            asset_ids,
            sink,  # type: ignore[arg-type]
            max_messages=max_messages,
        )
    finally:
        sports_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sports_task
