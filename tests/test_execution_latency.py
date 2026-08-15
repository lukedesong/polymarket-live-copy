from decimal import Decimal
from threading import Barrier

import cd90_live_copy as live
from cd90_live_copy import CLOBExecutionAdapter


class ParallelBookTickClient:
    def __init__(self):
        self.tick_reads = 0

    def get_order_book(self, token_id: str):
        assert token_id == "token-1"
        return {
            "market": "condition-1",
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
            "asks": [{"price": "0.51", "size": "20"}],
            "bids": [{"price": "0.50", "size": "20"}],
        }

    def get_tick_size(self, token_id: str):
        self.tick_reads += 1
        raise AssertionError("market metadata already carries the official tick")

    def get_clob_market_info(self, condition_id: str):
        assert condition_id == "condition-1"
        return {
            "mos": "5",
            "mts": "0.01",
            "fd": {"r": "0.04", "e": 1},
        }


def test_snapshot_uses_market_metadata_tick_without_a_duplicate_endpoint_read():
    client = ParallelBookTickClient()
    adapter = CLOBExecutionAdapter(
        client,
        minimum_marketable_buy_notional_usd=Decimal("1"),
    )

    snapshot = adapter.snapshot(token_id="token-1", side="BUY")

    assert snapshot["best_price"] == "0.51"
    assert snapshot["tick_size"] == "0.01"
    assert client.tick_reads == 0


class ParallelMappingExecution:
    def __init__(self):
        self.barrier = Barrier(2, timeout=1)

    def snapshot(self, *, token_id: str, side: str):
        self.barrier.wait()
        return {"token_id": token_id, "side": side}

    def condition_mapping_for_token(self, token_id: str):
        self.barrier.wait()
        return {
            "condition_id": "condition-1",
            "primary_token_id": token_id,
            "secondary_token_id": "token-2",
        }


def test_missing_condition_mapping_is_read_while_market_snapshot_waits():
    snapshot, mapping, mapping_error = live._snapshot_with_optional_condition_mapping(
        execution=ParallelMappingExecution(),
        token_id="token-1",
        side="BUY",
        prefetch_mapping=True,
    )

    assert snapshot == {"token_id": "token-1", "side": "BUY"}
    assert mapping == {
        "condition_id": "condition-1",
        "primary_token_id": "token-1",
        "secondary_token_id": "token-2",
    }
    assert mapping_error is None


def test_prefetched_mapping_failure_stays_separate_from_book_failure():
    execution = ParallelMappingExecution()

    def fail_mapping(_token_id: str):
        execution.barrier.wait()
        raise ConnectionError("mapping unavailable")

    execution.condition_mapping_for_token = fail_mapping
    snapshot, mapping, mapping_error = live._snapshot_with_optional_condition_mapping(
        execution=execution,
        token_id="token-1",
        side="BUY",
        prefetch_mapping=True,
    )

    assert snapshot == {"token_id": "token-1", "side": "BUY"}
    assert mapping is None
    assert isinstance(mapping_error, ConnectionError)
