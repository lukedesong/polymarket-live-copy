from decimal import Decimal

from paper_dashboard_statement import build_account_statement


D = Decimal


def executed_row(
    row_id: int,
    *,
    asset: str,
    side: str = "",
    kind: str = "REBALANCE",
    status: str = "FILLED",
    quantity: str,
    notional: str,
    fee: str = "0",
    reason: str = "",
) -> dict:
    # Synthetic empirical fixtures only; these are not trading parameters.
    return {
        "id": row_id,
        "observed_at": 1784950000 + row_id,
        "asset": asset,
        "side": side,
        "kind": kind,
        "status": status,
        "quantity": quantity,
        "notional": notional,
        "fee": fee,
        "reason": reason,
    }


def test_replay_separates_open_cost_and_realized_sell_pnl():
    rows = [
        executed_row(
            1,
            asset="a",
            side="BUY",
            quantity="10",
            notional="3",
            fee="0.1",
        ),
        executed_row(
            2,
            asset="a",
            side="SELL",
            quantity="4",
            notional="2",
            fee="0.05",
        ),
    ]

    statement = build_account_statement(
        rows,
        metadata_by_asset={
            "a": {
                "title": "Market A",
                "outcome": "Yes",
            }
        },
    )

    closed = statement["closed_records"][0]
    assert closed["title"] == "Market A"
    assert closed["outcome"] == "Yes"
    assert closed["close_type"] == "卖出"
    assert closed["cost_basis"] == "1.24"
    assert closed["net_recovered"] == "1.95"
    assert closed["realized_pnl"] == "0.71"
    assert statement["positions_by_asset"]["a"]["quantity"] == "6"
    assert statement["positions_by_asset"]["a"]["average_cost"] == "0.31"
    assert statement["reconstructed_realized_pnl"] == "0.71"
    assert statement["replay_errors"] == []


def test_replay_calculates_winning_and_losing_settlements():
    rows = [
        executed_row(
            1,
            asset="win",
            side="BUY",
            quantity="5",
            notional="2",
        ),
        executed_row(
            2,
            asset="lose",
            side="BUY",
            quantity="4",
            notional="1",
        ),
        executed_row(
            3,
            asset="win",
            kind="SETTLEMENT",
            status="SETTLED",
            quantity="5",
            notional="5",
            reason="WINNER",
        ),
        executed_row(
            4,
            asset="lose",
            kind="SETTLEMENT",
            status="SETTLED",
            quantity="4",
            notional="0",
            reason="LOSER",
        ),
    ]

    statement = build_account_statement(rows, metadata_by_asset={})
    records = {row["asset"]: row for row in statement["closed_records"]}

    assert records["win"]["close_type"] == "结算盈利"
    assert records["win"]["cost_basis"] == "2.0"
    assert records["win"]["net_recovered"] == "5"
    assert records["win"]["realized_pnl"] == "3.0"
    assert records["lose"]["close_type"] == "结算亏损"
    assert records["lose"]["cost_basis"] == "1.00"
    assert records["lose"]["net_recovered"] == "0"
    assert records["lose"]["realized_pnl"] == "-1.00"
    assert statement["positions_by_asset"] == {}
    assert statement["reconstructed_realized_pnl"] == "2.00"


def test_skipped_rows_do_not_change_statement():
    rows = [
        executed_row(
            1,
            asset="a",
            side="BUY",
            status="SKIPPED",
            quantity="0",
            notional="0",
        )
    ]

    statement = build_account_statement(rows, metadata_by_asset={})

    assert statement["closed_records"] == []
    assert statement["positions_by_asset"] == {}
    assert statement["reconstructed_realized_pnl"] == "0"
    assert statement["replay_errors"] == []


def test_invalid_close_is_reported_without_fabricating_pnl():
    rows = [
        executed_row(
            1,
            asset="a",
            side="SELL",
            quantity="1",
            notional="0.5",
        )
    ]

    statement = build_account_statement(rows, metadata_by_asset={})

    assert statement["closed_records"] == []
    assert statement["reconstructed_realized_pnl"] == "0"
    assert statement["replay_errors"] == [
        "ledger:1:invalid_close_quantity"
    ]
