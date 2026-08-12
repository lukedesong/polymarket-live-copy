"""Formula-derived, per-share performance statistics for tennis research."""
from __future__ import annotations

import math
from typing import Mapping, Sequence

import numpy as np


def share_pnl(price: float, won: bool, fee_per_share: float = 0.0) -> tuple[float, float]:
    """Return settlement PnL and deployed cost for one binary-outcome share."""
    if not math.isfinite(price) or not 0.0 < price < 1.0:
        raise ValueError("price must be a finite binary-market price")
    if not math.isfinite(fee_per_share) or fee_per_share < 0.0:
        raise ValueError("fee_per_share must be finite and non-negative")
    cost = price + fee_per_share
    return (1.0 - cost if won else -cost), cost


def performance_metrics(
    pnls: Sequence[float] | None = None,
    deployed_costs: Sequence[float] | None = None,
    *,
    ledger_entries: Sequence[Mapping[str, object]] | None = None,
) -> dict[str, float | int | None]:
    """Reconcile settlement-ordered PnL without annualizing returns.

    Callers with a ledger must supply ``finish_ts`` and ``event_id`` so the
    equity path and maximum drawdown are independent of decision/input order.
    """
    if ledger_entries is not None:
        if pnls is not None or deployed_costs is not None:
            raise ValueError("supply either ledger_entries or pnls and deployed_costs")
        ordered = sorted(
            ledger_entries,
            key=lambda entry: (int(entry["finish_ts"]), str(entry["event_id"])),
        )
        pnls = [float(entry["pnl"]) for entry in ordered]
        deployed_costs = [float(entry["deployed_cost"]) for entry in ordered]
    if pnls is None or deployed_costs is None:
        raise ValueError("pnls and deployed_costs are required without ledger_entries")
    pnl = np.asarray(pnls, dtype=float)
    costs = np.asarray(deployed_costs, dtype=float)
    if pnl.ndim != 1 or costs.ndim != 1 or pnl.size != costs.size:
        raise ValueError("pnls and deployed_costs must be equal-length one-dimensional sequences")
    if not np.isfinite(pnl).all() or not np.isfinite(costs).all() or (costs < 0.0).any():
        raise ValueError("ledger values must be finite and deployed costs non-negative")

    equity = np.concatenate(([0.0], np.cumsum(pnl)))
    drawdowns = np.maximum.accumulate(equity) - equity
    volatility = float(np.std(pnl, ddof=1)) if pnl.size > 1 else None
    mean = float(np.mean(pnl)) if pnl.size else None
    total_cost = float(np.sum(costs))
    return {
        "observations": int(pnl.size),
        "net_pnl": float(np.sum(pnl)),
        "deployed_cost": total_cost,
        "roi": float(np.sum(pnl) / total_cost) if total_cost else None,
        "max_drawdown_usd": float(np.max(drawdowns)),
        "max_drawdown_settlement_cashflow_usd": float(np.max(drawdowns)),
        "max_drawdown_basis": "SETTLEMENT_CASHFLOW",
        "return_mean": mean,
        "return_volatility": volatility,
        "sharpe_per_match": mean / volatility if mean is not None and volatility else None,
    }
