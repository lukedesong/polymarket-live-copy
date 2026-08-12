"""Capital-aware hedge, sizing, and date-block simulation helpers.

All prices in this module are all-in per-share costs supplied by the caller.
Historical reference prices therefore remain proxy inputs unless the caller
also has contemporaneous book, fee, and depth evidence.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import heapq
import math
from typing import Mapping, Sequence

import numpy as np


# User-specified research scenarios, not optimized strategy parameters.
HEDGE_WEIGHTS: tuple[float, ...] = (0.90, 0.80, 0.70, 0.60)
FIXED_FRACTIONS: tuple[float, ...] = (0.01, 0.02, 0.05, 0.10)
DEFAULT_INITIAL_CASH_USD = 10_000.0
DEFAULT_MONTE_CARLO_PATHS = 10_000


def _cost(value: float, field: str) -> float:
    value = float(value)
    if not math.isfinite(value) or not 0.0 < value < 1.0:
        raise ValueError(f"{field} must be a finite value in (0, 1)")
    return value


def _weight(value: float, field: str = "high_weight") -> float:
    value = float(value)
    if not math.isfinite(value) or not 0.0 <= value <= 1.0:
        raise ValueError(f"{field} must be a finite value in [0, 1]")
    return value


def hedged_pnl(*, high_cost: float, low_cost: float, high_weight: float, high_won: bool) -> float:
    """Settlement PnL per one unit of deployed cash for a complementary hedge.

    ``high_weight`` cash buys ``high_weight / high_cost`` high shares.  The
    remainder buys low shares.  This keeps the stated allocation tied to cash,
    rather than incorrectly treating the allocations as share counts.
    """
    high_cost = _cost(high_cost, "high_cost")
    low_cost = _cost(low_cost, "low_cost")
    high_weight = _weight(high_weight)
    winning_shares = high_weight / high_cost if high_won else (1.0 - high_weight) / low_cost
    return winning_shares - 1.0


def complementary_cost_check(high_cost: float, low_cost: float) -> dict[str, float]:
    """Report the one-share-per-side no-arbitrage identity before hedge tests."""
    high_cost = _cost(high_cost, "high_cost")
    low_cost = _cost(low_cost, "low_cost")
    combined = high_cost + low_cost
    return {
        "combined_unit_cost": combined,
        "locked_unit_pnl": 1.0 - combined,
        "locked_unit_loss": combined - 1.0,
    }


def hedge_scenarios() -> tuple[dict[str, object], ...]:
    """Return the user-specified cash allocations with explicit provenance."""
    return tuple(
        {
            "high_weight": high_weight,
            "low_weight": 1.0 - high_weight,
            "label": f"{int(high_weight * 100)}/{int((1.0 - high_weight) * 100)}",
            "numeric_provenance": "USER_SPECIFIED_VALUE",
        }
        for high_weight in HEDGE_WEIGHTS
    )


@dataclass(frozen=True)
class HedgeObservation:
    """A same-match hedge with independent decision-price favorite evidence.

    All-in costs can differ from quoted decision prices because they include
    fees.  Favorite semantics therefore use the contemporaneous reference
    prices, while PnL continues to use the all-in costs.
    """

    event_id: str
    decision_ts: int
    high_cost: float
    low_cost: float
    high_won: bool
    high_reference_price: float
    low_reference_price: float

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id must be nonempty")
        _cost(self.high_cost, "high_cost")
        _cost(self.low_cost, "low_cost")
        high_reference_price = _cost(self.high_reference_price, "high_reference_price")
        low_reference_price = _cost(self.low_reference_price, "low_reference_price")
        if high_reference_price <= low_reference_price:
            raise ValueError("high_reference_price must be greater than low_reference_price")
        object.__setattr__(self, "decision_ts", int(self.decision_ts))


def _return_summary(rates: Sequence[float]) -> dict[str, float | None]:
    values = np.asarray(rates, dtype=float)
    if not values.size:
        return {"net_pnl": 0.0, "roi": None, "volatility": None,
                "max_drawdown": 0.0, "loss_probability": None}
    equity = np.concatenate(([0.0], np.cumsum(values)))
    drawdown = np.maximum.accumulate(equity) - equity
    return {
        "net_pnl": float(np.sum(values)),
        "roi": float(np.mean(values)),
        "volatility": float(np.std(values, ddof=1)) if len(values) > 1 else None,
        "max_drawdown": float(np.max(drawdown)),
        "loss_probability": float(np.mean(values < 0.0)),
    }


def compare_hedge(
    observations: Sequence[HedgeObservation], *, high_weight: float,
) -> dict[str, float | int | None | str]:
    """Compare high-only and hedged outcomes on each same-match cash unit."""
    high_weight = _weight(high_weight)
    ordered = sorted(observations, key=lambda item: (item.decision_ts, item.event_id))
    unhedged = [1.0 / item.high_cost - 1.0 if item.high_won else -1.0 for item in ordered]
    hedged = [hedged_pnl(high_cost=item.high_cost, low_cost=item.low_cost,
                         high_weight=high_weight, high_won=item.high_won) for item in ordered]
    direct = _return_summary(unhedged)
    hedge = _return_summary(hedged)
    return {
        "observations": len(ordered),
        "deployed_capital_per_observation": 1.0,
        "high_weight": high_weight,
        "low_weight": 1.0 - high_weight,
        "unhedged_net_pnl": direct["net_pnl"],
        "hedged_net_pnl": hedge["net_pnl"],
        "paired_net_pnl_difference": float(hedge["net_pnl"] - direct["net_pnl"]),
        "unhedged_roi": direct["roi"],
        "hedged_roi": hedge["roi"],
        "unhedged_volatility": direct["volatility"],
        "hedged_volatility": hedge["volatility"],
        "unhedged_max_drawdown": direct["max_drawdown"],
        "hedged_max_drawdown": hedge["max_drawdown"],
        "unhedged_loss_probability": direct["loss_probability"],
        "hedged_loss_probability": hedge["loss_probability"],
        "economic_result_basis": "ALL_IN_COST_INPUT_REQUIRED",
    }


@dataclass(frozen=True)
class Trade:
    """One already-selected binary trade expressed as return per deployed cash."""

    event_id: str
    entry_ts: int
    settle_ts: int
    pnl_rate: float

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id must be nonempty")
        if int(self.settle_ts) < int(self.entry_ts):
            raise ValueError("settle_ts must not precede entry_ts")
        pnl_rate = float(self.pnl_rate)
        if not math.isfinite(pnl_rate) or pnl_rate < -1.0:
            raise ValueError("pnl_rate must be finite and no less than -1")
        object.__setattr__(self, "entry_ts", int(self.entry_ts))
        object.__setattr__(self, "settle_ts", int(self.settle_ts))
        object.__setattr__(self, "pnl_rate", pnl_rate)


@dataclass(frozen=True)
class LedgerEntry:
    event_id: str
    entry_ts: int
    settle_ts: int
    pnl_rate: float
    stake: float
    accepted: bool
    skip_reason: str | None
    available_cash_before: float
    reserved_cash_after: float
    equity_before: float
    settlement_pnl: float


def _positive_finite(value: float, field: str) -> float:
    value = float(value)
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(f"{field} must be finite and positive")
    return value


def _fraction(value: float) -> float:
    value = float(value)
    if not math.isfinite(value) or not 0.0 < value <= 1.0:
        raise ValueError("fraction must be finite and in (0, 1]")
    return value


def run_fixed_fraction(
    trades: Sequence[Trade], *, initial_cash: float = DEFAULT_INITIAL_CASH_USD, fraction: float,
) -> tuple[LedgerEntry, ...]:
    """Replay entries with cash reserved until their actual settlement timestamps.

    Staking always uses free cash, so open matches cannot reuse their reserved
    principal.  A skipped entry is retained in the ledger instead of silently
    disappearing from the denominator.
    """
    initial_cash = _positive_finite(initial_cash, "initial_cash")
    fraction = _fraction(fraction)
    ordered = sorted(trades, key=lambda item: (item.entry_ts, item.event_id, item.settle_ts))
    free_cash = initial_cash
    reserved_cash = 0.0
    realized_pnl = 0.0
    pending: list[tuple[int, str, float, float]] = []
    entries: list[LedgerEntry] = []

    for item in ordered:
        while pending and pending[0][0] <= item.entry_ts:
            _, _, released_stake, settlement_pnl = heapq.heappop(pending)
            reserved_cash -= released_stake
            free_cash += released_stake + settlement_pnl
            realized_pnl += settlement_pnl
        available_before = free_cash
        equity_before = free_cash + reserved_cash
        stake = available_before * fraction
        if stake <= 0.0:
            entries.append(LedgerEntry(
                item.event_id, item.entry_ts, item.settle_ts, item.pnl_rate, 0.0, False,
                "INSUFFICIENT_AVAILABLE_CASH", available_before, reserved_cash,
                equity_before, 0.0,
            ))
            continue
        settlement_pnl = stake * item.pnl_rate
        free_cash -= stake
        reserved_cash += stake
        heapq.heappush(pending, (item.settle_ts, item.event_id, stake, settlement_pnl))
        entries.append(LedgerEntry(
            item.event_id, item.entry_ts, item.settle_ts, item.pnl_rate, stake, True, None,
            available_before, reserved_cash, equity_before, settlement_pnl,
        ))
    return tuple(entries)


def binary_kelly(win_probability: float, all_in_cost: float) -> float:
    """Formula-derived Kelly fraction for a paid binary share."""
    probability = float(win_probability)
    if not math.isfinite(probability) or not 0.0 <= probability <= 1.0:
        raise ValueError("win_probability must be in [0, 1]")
    cost = _cost(all_in_cost, "all_in_cost")
    net_odds = (1.0 - cost) / cost
    return max(0.0, min(1.0, (net_odds * probability - (1.0 - probability)) / net_odds))


def executable_kelly(train_win_rate: float, cost: float, edge_interval_low: float) -> float:
    """Return zero unless the training-only edge interval clears zero."""
    edge_interval_low = float(edge_interval_low)
    if not math.isfinite(edge_interval_low):
        raise ValueError("edge_interval_low must be finite")
    return 0.0 if edge_interval_low <= 0.0 else binary_kelly(train_win_rate, cost)


def kelly_scenarios(train_win_rate: float, cost: float, edge_interval_low: float) -> dict[str, float | str]:
    """Full Kelly plus explicitly heuristic half/quarter sensitivities."""
    full = executable_kelly(train_win_rate, cost, edge_interval_low)
    return {
        "full_kelly": full,
        "half_kelly": full / 2.0,
        "quarter_kelly": full / 4.0,
        "full_kelly_provenance": "FORMULA_DERIVED_VALUE",
        "reduced_kelly_provenance": "HEURISTIC_SENSITIVITY",
        "training_only": "true",
    }


@dataclass(frozen=True)
class MonteCarloResult:
    equity_paths: np.ndarray
    sampled_block_ids: tuple[tuple[str, ...], ...]
    sampled_unit: str
    initial_cash: float
    fraction: float


def _replay_equity(trades: Sequence[Trade], initial_cash: float, fraction: float) -> list[float]:
    ledger = run_fixed_fraction(trades, initial_cash=initial_cash, fraction=fraction)
    settlements = sorted(
        (entry.settle_ts, entry.event_id, entry.settlement_pnl)
        for entry in ledger if entry.accepted
    )
    equity = initial_cash
    history = [equity]
    for _, _, pnl in settlements:
        equity += pnl
        history.append(equity)
    return history


def _concatenate_blocks(blocks: Sequence[Sequence[Trade]]) -> tuple[Trade, ...]:
    """Sequence resampled dates while retaining chronological overlap inside each."""
    output: list[Trade] = []
    next_base = 0
    for block_index, block in enumerate(blocks):
        ordered = sorted(block, key=lambda item: (item.entry_ts, item.event_id, item.settle_ts))
        if not ordered:
            continue
        origin = min(item.entry_ts for item in ordered)
        shift = next_base - origin
        shifted = tuple(
            replace(item, event_id=f"{block_index}:{item.event_id}",
                    entry_ts=item.entry_ts + shift, settle_ts=item.settle_ts + shift)
            for item in ordered
        )
        output.extend(shifted)
        next_base = max(item.settle_ts for item in shifted) + 1
    return tuple(output)


def monte_carlo(
    dated_blocks: Mapping[str, Sequence[Trade]], *, paths: int = DEFAULT_MONTE_CARLO_PATHS,
    initial_cash: float = DEFAULT_INITIAL_CASH_USD, fraction: float, seed: int,
) -> MonteCarloResult:
    """Date-block bootstrap that retains same-day order and capital occupancy."""
    if isinstance(paths, bool) or not isinstance(paths, int) or paths <= 0:
        raise ValueError("paths must be a positive integer")
    initial_cash = _positive_finite(initial_cash, "initial_cash")
    fraction = _fraction(fraction)
    block_ids = tuple(sorted(dated_blocks))
    if not block_ids:
        return MonteCarloResult(np.full((paths, 1), initial_cash), tuple(() for _ in range(paths)),
                                "UTC_DATE_BLOCK", initial_cash, fraction)
    normalized = {block_id: tuple(dated_blocks[block_id]) for block_id in block_ids}
    for block_id, block in normalized.items():
        if not isinstance(block_id, str) or not block_id:
            raise ValueError("date block IDs must be nonempty strings")
        if not all(isinstance(item, Trade) for item in block):
            raise ValueError("date blocks must contain Trade records")
    rng = np.random.default_rng(seed)
    sampled_indices = rng.integers(0, len(block_ids), size=(paths, len(block_ids)))
    histories: list[list[float]] = []
    sampled_ids: list[tuple[str, ...]] = []
    for draw in sampled_indices:
        ids = tuple(block_ids[int(index)] for index in draw)
        sampled_ids.append(ids)
        histories.append(_replay_equity(_concatenate_blocks([normalized[key] for key in ids]), initial_cash, fraction))
    width = max(len(history) for history in histories)
    paths_array = np.empty((paths, width), dtype=float)
    for index, history in enumerate(histories):
        paths_array[index, :len(history)] = history
        paths_array[index, len(history):] = history[-1]
    return MonteCarloResult(paths_array, tuple(sampled_ids), "UTC_DATE_BLOCK", initial_cash, fraction)


def summarize_paths(equity_paths: np.ndarray) -> dict[str, float | int | str]:
    """Summarize Monte Carlo paths; zero is the only mathematical ruin line."""
    paths = np.asarray(equity_paths, dtype=float)
    if paths.ndim != 2 or paths.shape[0] == 0 or paths.shape[1] == 0 or not np.isfinite(paths).all():
        raise ValueError("equity_paths must be a nonempty finite two-dimensional array")
    peaks = np.maximum.accumulate(paths, axis=1)
    drawdown = np.divide(peaks - paths, peaks, out=np.zeros_like(paths), where=peaks > 0.0)
    ruined = np.any(paths <= 0.0, axis=1)
    finals = paths[:, -1]
    max_drawdowns = np.max(drawdown, axis=1)
    return {
        "paths": int(paths.shape[0]),
        "ruin_boundary_usd": 0.0,
        "ruin_definition": "EQUITY_AT_OR_BELOW_ZERO",
        "ruined_paths": int(np.count_nonzero(ruined)),
        "ruin_probability": float(np.mean(ruined)),
        "max_drawdown_p05": float(np.quantile(max_drawdowns, 0.05)),
        "max_drawdown_median": float(np.median(max_drawdowns)),
        "max_drawdown_p95": float(np.quantile(max_drawdowns, 0.95)),
        "max_drawdown_unit": "FRACTION_OF_PRIOR_PEAK",
        "max_drawdown_provenance": "FORMULA_DERIVED_VALUE",
        "median_final_equity_usd": float(np.median(finals)),
        "final_equity_p05_usd": float(np.quantile(finals, 0.05)),
        "final_equity_p95_usd": float(np.quantile(finals, 0.95)),
        "structural_ruin_limitation": "FRACTIONAL_SIZING_CAN_MAKE_ZERO_RUIN_UNREACHABLE_IN_FINITE_PATHS",
    }


def annualized_return(
    *, final_equity: float, initial_cash: float, covered_days: float | None,
    capital_coverage_complete: bool,
) -> dict[str, float | str | None]:
    """Annualize only when both elapsed time and capital coverage are evidenced."""
    initial_cash = _positive_finite(initial_cash, "initial_cash")
    final_equity = float(final_equity)
    if not math.isfinite(final_equity) or final_equity < 0.0:
        raise ValueError("final_equity must be finite and non-negative")
    if covered_days is None or not capital_coverage_complete:
        return {"status": "BLOCK_DATA", "annualized_return": None,
                "reason": "MISSING_TIME_OR_CAPITAL_COVERAGE"}
    covered_days = float(covered_days)
    if not math.isfinite(covered_days) or covered_days <= 0.0 or final_equity <= 0.0:
        return {"status": "BLOCK_DATA", "annualized_return": None,
                "reason": "INVALID_TIME_OR_NONPOSITIVE_FINAL_EQUITY"}
    return {
        "status": "AVAILABLE",
        "annualized_return": (final_equity / initial_cash) ** (365.25 / covered_days) - 1.0,
        "reason": "FORMULA_DERIVED_WITH_COMPLETE_TIME_AND_CAPITAL_COVERAGE",
    }
