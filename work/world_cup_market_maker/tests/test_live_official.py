from datetime import datetime, timezone

import pytest

from world_cup_mm.cli import run_scan
from world_cup_mm.discovery import GAMMA_EVENTS_URL, GammaClient
from world_cup_mm.storage import Store


@pytest.mark.live
def test_official_gamma_scan_persists_current_response_shape(tmp_path):
    store = Store(tmp_path / "official.sqlite3")

    result = run_scan(
        store,
        GammaClient(),
        now=datetime.now(timezone.utc),
        scan_id="official-smoke",
    )

    assert result["source"] == GAMMA_EVENTS_URL
    assert result["event_count"] > 0
    assert store.latest_scan_summary()["scan_id"] == "official-smoke"
    assert all(
        market.game_start_time.tzinfo is timezone.utc
        for market in store.selected_markets(all_eligible=True)
    )
