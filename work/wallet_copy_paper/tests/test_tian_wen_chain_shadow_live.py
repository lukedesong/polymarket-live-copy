import os
from decimal import Decimal

import pytest

import tian_wen_chain_shadow as shadow


D = Decimal

# Empirical public transaction recorded by the existing paper ledger. Its source
# fill is also present in the current public Data API window on 2026-07-25.
HISTORICAL_TX = (
    "0x65236c620f88ba236caa9192f27124b0bf8f64706202aa7421475aedba553405"
)
HISTORICAL_TOKEN = (
    "55576603361906226012210559986541125535490079827793578269579424308995169855881"
)


pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("RUN_LIVE_POLYMARKET_TESTS") != "1",
        reason="set RUN_LIVE_POLYMARKET_TESTS=1 for public network verification",
    ),
]


def test_historical_receipt_matches_public_data_api_source_fill():
    receipt_evidence = shadow.verify_transaction(HISTORICAL_TX)
    primary = [
        event
        for event in receipt_evidence["source_events"]
        if event["source_order"]
        and event["token_id"] == HISTORICAL_TOKEN
        and event["side"] == "BUY"
    ]
    assert len(primary) == 1
    event = primary[0]

    rows = shadow.PublicPolymarketClient().get_trades(shadow.SOURCE_WALLET)
    groups = [
        group
        for group in shadow.group_data_rows(rows)
        if group["transaction_hash"] == HISTORICAL_TX
        and group["token_id"] == HISTORICAL_TOKEN
        and group["side"] == "BUY"
    ]
    assert len(groups) == 1
    group = groups[0]

    assert receipt_evidence["block_timestamp"] == group["source_timestamp"]
    assert D(event["quantity"]) == group["source_quantity"] == D("1346.65")
    # Formula-derived comparison: the public Data API currently exposes ten
    # decimal price places, so compare its value to that exact quantization.
    assert D(event["price"]).quantize(D("0.0000000001")) == group["source_vwap"]


def test_indexed_log_queries_find_both_source_roles_on_recorded_block():
    rpc = shadow.RpcClient()
    # Empirical block number from HISTORICAL_TX's public receipt.
    block_number = 90_826_192

    maker_logs = rpc.source_logs(
        block_number,
        shadow.SOURCE_WALLET,
        "maker",
    )
    taker_logs = rpc.source_logs(
        block_number,
        shadow.SOURCE_WALLET,
        "taker",
    )

    assert any(
        row["transactionHash"].lower() == HISTORICAL_TX
        and int(row["logIndex"], 16) == 599
        for row in maker_logs
    )
    assert any(
        row["transactionHash"].lower() == HISTORICAL_TX
        for row in taker_logs
    )
