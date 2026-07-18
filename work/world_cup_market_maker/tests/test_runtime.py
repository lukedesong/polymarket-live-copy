from datetime import datetime, timedelta, timezone

from world_cup_mm.order_control import RecordingOrderControl
from world_cup_mm.runtime import RiskRuntime, next_transition_time
from world_cup_mm.storage import StoredMarket


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)


class FakeStore:
    def __init__(self, *, connected=True, ready=True, sports_live=False):
        self.connected = connected
        self.ready = ready
        self.sports_live = sports_live
        self.decisions = []
        self.actions = []

    def latest_session_summary(self):
        return {"connected": self.connected, "session_id": "session-a"}

    def book_ready(self, _asset_id):
        return self.ready

    def latest_sports_live(self, _slug):
        return self.sports_live

    def record_risk_decision(self, decision, *, decided_at):
        self.decisions.append((decision, decided_at))

    def record_risk_action(self, condition_id, action, delivery_status, *, created_at, detail=None):
        self.actions.append((condition_id, action, delivery_status, detail))


def market(start):
    return StoredMarket(
        scan_id="scan-a",
        market_id="market-a",
        event_id="event-a",
        event_slug="fifwc-fra-eng-2026-07-18",
        question="Will France win?",
        market_slug="france-win",
        condition_id="condition-a",
        token_ids=("yes-a", "no-a"),
        game_start_time=start,
        liquidity_text="100",
        volume_24h_text="80",
        frontier=True,
    )


def test_cancel_boundary_executes_market_scoped_cancel_once():
    store = FakeStore()
    control = RecordingOrderControl()
    runtime = RiskRuntime(store, [market(NOW + timedelta(minutes=5))], control, cancel_capable=True)

    first = runtime.evaluate_all(now=NOW)
    second = runtime.evaluate_all(now=NOW)

    assert first[0].state.value == "CANCELLED_BLOCKED"
    assert second[0].state.value == "CANCELLED_BLOCKED"
    assert control.calls == ["condition-a"]
    assert store.actions == [
        ("condition-a", "CANCEL_MARKET_ORDERS", "delivered", None)
    ]


def test_data_disconnect_executes_cancel_when_armed():
    store = FakeStore(connected=False)
    control = RecordingOrderControl()
    runtime = RiskRuntime(store, [market(NOW + timedelta(hours=1))], control, cancel_capable=True)

    decisions = runtime.evaluate_all(now=NOW)

    assert decisions[0].state.value == "DATA_BLOCKED"
    assert control.calls == ["condition-a"]


def test_data_only_runtime_never_claims_or_executes_cancel():
    store = FakeStore(connected=False)
    control = RecordingOrderControl()
    runtime = RiskRuntime(store, [market(NOW + timedelta(hours=1))], control, cancel_capable=False)

    runtime.evaluate_all(now=NOW)

    assert control.calls == []
    assert store.actions == []


def test_next_transition_uses_exact_user_specified_boundary():
    start = NOW + timedelta(minutes=40)

    assert next_transition_time([market(start)], NOW) == NOW + timedelta(minutes=10)
