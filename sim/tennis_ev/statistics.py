"""Formula-derived, per-share performance statistics for tennis research."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class BootstrapMatchBlocks:
    """Block-bootstrap draws keyed by independent event ID."""

    match_ids: tuple[tuple[str, ...], ...]
    sums: tuple[float, ...]


def _validate_p_values(p_values: Sequence[float]) -> np.ndarray:
    values = np.asarray(p_values, dtype=float)
    if values.ndim != 1 or not np.isfinite(values).all() or ((values < 0.0) | (values > 1.0)).any():
        raise ValueError("p-values must be finite values in [0, 1]")
    return values


def benjamini_hochberg(p_values: Sequence[float]) -> list[float]:
    """Return FDR q-values in original order with the monotonicity constraint."""
    values = _validate_p_values(p_values)
    if not values.size:
        return []
    order = np.argsort(values, kind="stable")
    ranked = values[order]
    adjusted = ranked * values.size / np.arange(1, values.size + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1]
    output = np.empty_like(adjusted)
    output[order] = np.minimum(adjusted, 1.0)
    return output.tolist()


def bonferroni(p_values: Sequence[float], *, tested_conditions: int) -> list[float]:
    """Return family-wise sensitivity values using the full test denominator."""
    values = _validate_p_values(p_values)
    if isinstance(tested_conditions, bool) or not isinstance(tested_conditions, int) or tested_conditions < len(values):
        raise ValueError("tested_conditions must be an integer no smaller than p-value count")
    return np.minimum(values * tested_conditions, 1.0).tolist()


def bootstrap_match_blocks(
    rows: Sequence[tuple[str, float]], *, draws: int, seed: int,
) -> BootstrapMatchBlocks:
    """Resample complete event blocks, never individual outcome rows."""
    if isinstance(draws, bool) or not isinstance(draws, int) or draws <= 0:
        raise ValueError("draws must be a positive integer")
    blocks: dict[str, list[float]] = {}
    for match_id, pnl in rows:
        if not isinstance(match_id, str) or not match_id:
            raise ValueError("match IDs must be nonempty strings")
        value = float(pnl)
        if not math.isfinite(value):
            raise ValueError("PnL must be finite")
        blocks.setdefault(match_id, []).append(value)
    ids = tuple(sorted(blocks))
    if not ids:
        return BootstrapMatchBlocks(tuple(() for _ in range(draws)), tuple(0.0 for _ in range(draws)))
    rng = np.random.default_rng(seed)
    indices = rng.integers(0, len(ids), size=(draws, len(ids)))
    block_sums = np.asarray([sum(blocks[match_id]) for match_id in ids], dtype=float)
    return BootstrapMatchBlocks(
        match_ids=tuple(
            tuple(match_id for index in draw for match_id in [ids[index]] * len(blocks[ids[index]]))
            for draw in indices
        ),
        sums=tuple(np.sum(block_sums[indices], axis=1).tolist()),
    )


def bootstrap_interval(
    rows: Sequence[tuple[str, float]], *, draws: int, seed: int, level: float = 0.95,
) -> tuple[float | None, float | None]:
    """Percentile interval for total block PnL; no outcome-row pseudo-samples."""
    if not 0.0 < level < 1.0:
        raise ValueError("level must be strictly between zero and one")
    if not rows:
        return None, None
    samples = np.asarray(bootstrap_match_blocks(rows, draws=draws, seed=seed).sums)
    lower, upper = np.quantile(samples, [(1.0 - level) / 2.0, 1.0 - (1.0 - level) / 2.0])
    return float(lower), float(upper)


def block_sign_permutation_p_value(
    rows: Sequence[tuple[str, float]], *, draws: int, seed: int,
) -> float | None:
    """One-sided matched-block sign permutation p-value for positive total PnL.

    This is a proxy-price null test, not evidence that historical samples were
    executable fills.
    """
    if not rows:
        return None
    observed = sum(value for _, value in rows)
    blocks: dict[str, float] = {}
    for match_id, value in rows:
        blocks[match_id] = blocks.get(match_id, 0.0) + float(value)
    values = np.asarray(list(blocks.values()), dtype=float)
    rng = np.random.default_rng(seed)
    signs = rng.choice(np.array([-1.0, 1.0]), size=(draws, values.size))
    null = np.sum(signs * values, axis=1)
    return float((1 + np.count_nonzero(null >= observed)) / (draws + 1))


def outcome_side_permutation_p_value(
    selections: Sequence[tuple[object, object]], *, draws: int, seed: int,
) -> float | None:
    """Test a directional choice against its other outcome within each match.

    Each pair is ``(selected_outcome, complementary_outcome)``.  The null
    randomizes that *choice*, retaining the asymmetric binary payout implied by
    each side's reference price.  A sign flip of realised PnL is invalid here:
    it changes the price-dependent payoff rather than selecting the other side
    of the same market.
    """
    if isinstance(draws, bool) or not isinstance(draws, int) or draws <= 0:
        raise ValueError("draws must be a positive integer")
    if not selections:
        return None
    selected_pnls: list[float] = []
    alternate_pnls: list[float] = []
    for selected, alternate in selections:
        selected_pnl, _ = share_pnl(float(selected.current_price), bool(selected.won))
        alternate_pnl, _ = share_pnl(float(alternate.current_price), bool(alternate.won))
        selected_pnls.append(selected_pnl)
        alternate_pnls.append(alternate_pnl)
    observed = float(sum(selected_pnls))
    rng = np.random.default_rng(seed)
    choices = rng.integers(0, 2, size=(draws, len(selected_pnls)))
    selected_array = np.asarray(selected_pnls, dtype=float)
    alternate_array = np.asarray(alternate_pnls, dtype=float)
    null = np.sum(np.where(choices == 0, selected_array, alternate_array), axis=1)
    return float((1 + np.count_nonzero(null >= observed)) / (draws + 1))


def contribution_diagnostics(pnls: Sequence[float]) -> dict[str, float | int | None]:
    """Expose dependence on one large event rather than hiding concentration."""
    values = np.asarray(pnls, dtype=float)
    if values.ndim != 1 or not np.isfinite(values).all():
        raise ValueError("pnls must be a finite one-dimensional sequence")
    if not values.size:
        return {"largest_contribution": None, "largest_index": None, "pnl_without_largest": 0.0}
    index = int(np.argmax(np.abs(values)))
    return {
        "largest_contribution": float(values[index]),
        "largest_index": index,
        "pnl_without_largest": float(np.sum(values) - values[index]),
    }


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
