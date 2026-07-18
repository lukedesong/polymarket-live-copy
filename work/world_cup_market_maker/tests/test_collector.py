import asyncio
import json

import pytest

from world_cup_mm.collector import (
    SportsFrame,
    decode_market_frame,
    handle_market_disconnect,
    market_subscription,
    parse_sports_frame,
)


def test_market_subscription_uses_documented_asset_envelope():
    assert market_subscription(["a", "b"]) == {
        "assets_ids": ["a", "b"],
        "type": "market",
        "custom_feature_enabled": True,
    }


def test_market_subscription_rejects_empty_assets():
    with pytest.raises(ValueError, match="empty_market_subscription"):
        market_subscription([])


def test_decode_market_frame_accepts_documented_object_or_initial_list():
    book = {"event_type": "book", "asset_id": "a"}

    assert decode_market_frame(json.dumps(book)) == [book]
    assert decode_market_frame(json.dumps([book])) == [book]


def test_decode_market_frame_rejects_non_object_items():
    with pytest.raises(ValueError, match="market_frame_item_not_object"):
        decode_market_frame('[{"event_type":"book"}, 3]')


def test_disconnect_invalidates_all_session_books():
    class FakeStore:
        def __init__(self):
            self.invalidated = []

        def invalidate_session_books(self, session_id):
            self.invalidated.append(session_id)

    store = FakeStore()

    asyncio.run(handle_market_disconnect(store, "session-a"))

    assert store.invalidated == ["session-a"]


def test_sports_ping_requires_immediate_pong_action():
    assert parse_sports_frame("ping") == SportsFrame(
        pong_required=True,
        payload=None,
    )


def test_sports_json_payload_is_preserved():
    payload = {"slug": "fifwc-fra-eng-2026-07-18", "live": True}

    assert parse_sports_frame(json.dumps(payload)) == SportsFrame(
        pong_required=False,
        payload=payload,
    )
