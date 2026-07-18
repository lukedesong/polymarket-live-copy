import pytest

from world_cup_mm.order_control import (
    AuthenticatedOrderControl,
    RecordingOrderControl,
)


class FakeClient:
    def __init__(self):
        self.market_payloads = []
        self.cancel_all_called = False

    def cancel_market_orders(self, payload):
        self.market_payloads.append(payload)
        return {"canceled": True}

    def cancel_all(self):
        self.cancel_all_called = True
        raise AssertionError("account-wide cancellation must never be called")


def test_recording_adapter_is_idempotent_by_condition():
    adapter = RecordingOrderControl()

    adapter.cancel_market_orders("condition-a")
    adapter.cancel_market_orders("condition-a")

    assert adapter.calls == ["condition-a"]


def test_authenticated_adapter_uses_market_scoped_cancel_only():
    client = FakeClient()

    result = AuthenticatedOrderControl(client).cancel_market_orders("condition-a")

    assert result == {"canceled": True}
    assert len(client.market_payloads) == 1
    assert client.market_payloads[0].market == "condition-a"
    assert client.market_payloads[0].asset_id is None
    assert client.cancel_all_called is False


def test_authenticated_adapter_rejects_missing_condition():
    with pytest.raises(ValueError, match="missing_condition_id"):
        AuthenticatedOrderControl(FakeClient()).cancel_market_orders("")
