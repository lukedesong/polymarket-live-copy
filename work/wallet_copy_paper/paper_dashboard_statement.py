from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


D = Decimal
ZERO = D("0")
SHANGHAI = ZoneInfo("Asia/Shanghai")
EXECUTED_STATUSES = {"FILLED", "PARTIAL", "SETTLED"}


def format_observed_at_shanghai(value: Any) -> str:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return "未知"
    return datetime.fromtimestamp(timestamp, SHANGHAI).strftime(
        "%Y-%m-%d %H:%M:%S（上海时间）"
    )


def build_account_statement(
    ledger_rows: Iterable[Mapping[str, Any]],
    *,
    metadata_by_asset: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    states: dict[str, tuple[Decimal, Decimal]] = defaultdict(
        lambda: (ZERO, ZERO)
    )
    closed_records: list[dict[str, str]] = []
    errors: list[str] = []
    reconstructed = ZERO

    for row in ledger_rows:
        if str(row.get("status", "")) not in EXECUTED_STATUSES:
            continue
        ledger_id = str(row.get("id", ""))
        asset = str(row.get("asset", ""))
        try:
            quantity = D(str(row.get("quantity", "0")))
            notional = D(str(row.get("notional", "0")))
            fee = D(str(row.get("fee", "0")))
        except InvalidOperation:
            errors.append(f"ledger:{ledger_id}:invalid_decimal")
            continue
        held, average_cost = states[asset]
        side = str(row.get("side", ""))
        kind = str(row.get("kind", ""))

        if side == "BUY":
            new_quantity = held + quantity
            if quantity <= ZERO or new_quantity <= ZERO:
                errors.append(f"ledger:{ledger_id}:invalid_buy_quantity")
                continue
            new_average = (
                held * average_cost + notional + fee
            ) / new_quantity
            states[asset] = (new_quantity, new_average)
            continue

        if side != "SELL" and kind != "SETTLEMENT":
            continue
        if quantity <= ZERO or quantity > held:
            errors.append(f"ledger:{ledger_id}:invalid_close_quantity")
            continue

        cost_basis = average_cost * quantity
        net_recovered = notional - fee
        realized_pnl = net_recovered - cost_basis
        reconstructed += realized_pnl
        metadata = metadata_by_asset.get(asset, {})
        reason = str(row.get("reason", ""))
        close_type = (
            "结算盈利"
            if kind == "SETTLEMENT" and reason == "WINNER"
            else "结算亏损"
            if kind == "SETTLEMENT"
            else "卖出"
        )
        closed_records.append(
            {
                "ledger_id": ledger_id,
                "observed_at": str(row.get("observed_at", "")),
                "close_time_shanghai": format_observed_at_shanghai(
                    row.get("observed_at")
                ),
                "asset": asset,
                "title": str(metadata.get("title", asset)),
                "outcome": str(metadata.get("outcome", "")),
                "close_type": close_type,
                "quantity": str(quantity),
                "cost_basis": str(cost_basis),
                "net_recovered": str(net_recovered),
                "fee": str(fee),
                "realized_pnl": str(realized_pnl),
            }
        )
        remaining = held - quantity
        states[asset] = (
            remaining,
            average_cost if remaining > ZERO else ZERO,
        )

    return {
        "closed_records": list(reversed(closed_records)),
        "positions_by_asset": {
            asset: {
                "quantity": str(quantity),
                "average_cost": str(average_cost),
            }
            for asset, (quantity, average_cost) in states.items()
            if quantity > ZERO
        },
        "reconstructed_realized_pnl": str(reconstructed),
        "replay_errors": errors,
    }
