"""Causal research-event construction and chronological holdout controls.

This module operates on immutable normalized records.  Historical price paths
are reference-price observations, never inferred executable orders or score
states.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from statistics import fmean
from typing import Iterable, Mapping, Sequence

import numpy as np

from .data import MatchRecord
from . import statistics


# Five minutes is a user-specified research checkpoint, not an optimized value.
SCHEDULED_START_PROXY_SECONDS = 5 * 60


@dataclass(frozen=True)
class ResearchEvent:
    event_id: str
    market_id: str
    token_id: str
    decision_ts: int
    finish_ts: int
    outcome_index: int
    won: bool
    opening_price: float | None
    reference_entry_price: float
    current_price: float
    absolute_change: float
    relative_change: float
    realized_volatility: float | None
    path_high: float
    path_low: float
    elapsed_seconds: int
    level: str | None
    best_bid: float | None
    best_ask: float | None
    visible_depth_usd: float | None
    feature_availability: tuple[str, ...]
    observed_prices: tuple[float, ...]
    decision_family: str = "EXPLICIT_DECISION"
    source_sha256: str = ""


@dataclass(frozen=True)
class FeeSchedule:
    """An immutable, source-backed per-share fee formula input."""

    rate: float
    exponent: float
    source: str

    def __post_init__(self) -> None:
        try:
            rate = float(self.rate)
            exponent = float(self.exponent)
        except (TypeError, ValueError) as error:
            raise ValueError("fee schedule rate and exponent must be numeric") from error
        if not math.isfinite(rate) or rate < 0.0:
            raise ValueError("fee schedule rate must be finite and non-negative")
        if not math.isfinite(exponent) or exponent <= 0.0:
            raise ValueError("fee schedule exponent must be finite and positive")
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("fee schedule source must be nonempty")
        object.__setattr__(self, "rate", rate)
        object.__setattr__(self, "exponent", exponent)
        object.__setattr__(self, "source", self.source.strip())


@dataclass(frozen=True)
class Split:
    train: tuple[MatchRecord, ...]
    test: tuple[MatchRecord, ...]
    purged: tuple[MatchRecord, ...]
    boundary_ts: int
    achieved_train_fraction: float


@dataclass(frozen=True)
class FrozenManifest:
    """Training-only discovery provenance; serialized form is reproducible."""

    split_cutoff_ts: int | None
    training_source_hashes_sha256: str
    training_source_hashes: tuple[str, ...]
    source_event_ids: tuple[str, ...]
    feature_definitions: tuple[str, ...]
    empirical_cut_points: Mapping[str, tuple[float, ...]]
    selected_rule_ids: tuple[str, ...]
    ranking_order: tuple[str, ...]
    cost_specification: Mapping[str, object]
    significance_alpha: float
    significance_provenance: str
    kelly_inputs: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-safe, lexically ordered representation."""
        return {
            "cost_specification": dict(sorted(self.cost_specification.items())),
            "empirical_cut_points": {
                key: list(self.empirical_cut_points[key])
                for key in sorted(self.empirical_cut_points)
            },
            "feature_definitions": list(self.feature_definitions),
            "kelly_inputs": dict(sorted(self.kelly_inputs.items())),
            "ranking_order": list(self.ranking_order),
            "selected_rule_ids": list(self.selected_rule_ids),
            "significance_alpha": self.significance_alpha,
            "significance_provenance": self.significance_provenance,
            "source_event_ids": list(self.source_event_ids),
            "split_cutoff_ts": self.split_cutoff_ts,
            "training_source_hashes": list(self.training_source_hashes),
            "training_source_hashes_sha256": self.training_source_hashes_sha256,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def _realized_volatility(prices: tuple[float, ...]) -> float | None:
    if len(prices) < 2:
        return None
    log_returns = [math.log(later / earlier) for earlier, later in zip(prices, prices[1:])]
    return math.sqrt(fmean(value * value for value in log_returns))


def build_event(
    match: MatchRecord,
    *,
    decision_ts: int,
    outcome_index: int,
    decision_family: str = "EXPLICIT_DECISION",
) -> ResearchEvent:
    """Build one outcome event using only observations available by its decision."""
    if outcome_index not in (0, 1):
        raise ValueError("outcome_index must identify one binary outcome")
    if not match.pregame_ts <= decision_ts < match.finish_ts:
        raise ValueError("decision timestamp must precede settlement and follow pregame reference")

    outcome = match.outcomes[outcome_index]
    observed = tuple(price for timestamp, price in outcome.path if timestamp <= decision_ts)
    current = observed[-1] if observed else outcome.pregame_price
    path_prices = observed or (outcome.pregame_price,)
    availability = ["REFERENCE_ENTRY_PRICE"]
    if observed:
        availability.append("HISTORICAL_PATH_TO_DECISION")
    else:
        availability.append("NO_PATH_OBSERVATION_BY_DECISION")
    if outcome.opening_price is None:
        availability.append("BLOCK_DATA_OPENING_PRICE")
    # MatchRecord has no observation time for these optional fields.  They may
    # describe a book observed after this decision, so they are unavailable
    # until a future timestamped snapshot lookup is wired into this builder.
    availability.append("BLOCK_DATA_EXECUTION_BOOK")
    availability.append("BLOCK_DATA_VISIBLE_DEPTH")
    # MatchRecord has no timestamp for match_state, including when it is
    # non-null.  It therefore cannot establish a state at this decision.
    availability.append("BLOCK_DATA_MATCH_STATE")

    return ResearchEvent(
        event_id=match.event_id,
        market_id=match.market_id,
        token_id=outcome.token_id,
        decision_ts=decision_ts,
        finish_ts=match.finish_ts,
        outcome_index=outcome_index,
        won=outcome.won,
        opening_price=outcome.opening_price,
        reference_entry_price=outcome.pregame_price,
        current_price=current,
        absolute_change=current - outcome.pregame_price,
        relative_change=(current - outcome.pregame_price) / outcome.pregame_price,
        realized_volatility=_realized_volatility(observed),
        path_high=max(path_prices),
        path_low=min(path_prices),
        elapsed_seconds=decision_ts - match.start_ts,
        level=match.level,
        best_bid=None,
        best_ask=None,
        visible_depth_usd=None,
        feature_availability=tuple(availability),
        observed_prices=observed,
        decision_family=decision_family,
        source_sha256=match.source_sha256,
    )


def build_decision_point_events(
    matches: Iterable[MatchRecord],
) -> tuple[tuple[ResearchEvent, ...], dict[str, int]]:
    """Create only decision families supported by the supplied observations.

    Normalized historical rows do not contain timestamped score or serving
    states.  Those families are therefore counted as unavailable instead of
    inferred from price moves.
    """
    events: list[ResearchEvent] = []
    match_count = 0
    coverage = {
        "PRE_MATCH_REFERENCE": 0,
        "SCHEDULED_START_PROXY": 0,
        "FIRST_SET_END": 0,
        "SECOND_SET_END": 0,
        "DECIDING_SET": 0,
        "SCORE_LEAD": 0,
        "SERVER_STATE": 0,
        "BLOCK_DATA_MATCH_STATE": 0,
    }
    for match in matches:
        match_count += 1
        for outcome_index in range(len(match.outcomes)):
            events.append(
                build_event(
                    match, decision_ts=match.pregame_ts, outcome_index=outcome_index,
                    decision_family="PRE_MATCH_REFERENCE",
                )
            )
            coverage["PRE_MATCH_REFERENCE"] += 1
            proxy_ts = match.start_ts + SCHEDULED_START_PROXY_SECONDS
            if proxy_ts < match.finish_ts:
                events.append(
                    build_event(
                        match, decision_ts=proxy_ts, outcome_index=outcome_index,
                        decision_family="SCHEDULED_START_PROXY",
                    )
                )
                coverage["SCHEDULED_START_PROXY"] += 1
        # A bare MatchRecord.match_state has no observation timestamp or
        # checkpoint semantics, so it cannot establish an historical state.
        coverage["BLOCK_DATA_MATCH_STATE"] += 1
    coverage["matches_seen"] = match_count
    return tuple(events), coverage


def chronological_split(
    matches: Sequence[MatchRecord], *, train_fraction: float) -> Split:
    """Return a purged holdout with labels strictly before test decisions.

    A match that spans the cutoff is excluded because its outcome becomes
    known after a later test match could already have been entered.
    """
    if not 0.0 < train_fraction < 1.0:
        raise ValueError("train_fraction must be strictly between zero and one")
    ordered = tuple(sorted(matches, key=lambda match: (match.finish_ts, match.event_id)))
    if len(ordered) < 2:
        raise ValueError("at least two matches are required for a holdout split")
    if len({match.finish_ts for match in ordered}) == 1:
        raise ValueError("cannot split an all-equal finish timestamp group")

    candidates: list[Split] = []
    # A cutoff is the earliest decision timestamp represented in test, not a
    # training settlement timestamp.  This retains every label known strictly
    # before that decision while discarding only intervals that overlap it.
    for cutoff in sorted({match.pregame_ts for match in ordered}):
        train = tuple(match for match in ordered if match.finish_ts < cutoff)
        test = tuple(match for match in ordered if match.pregame_ts >= cutoff)
        purged = tuple(match for match in ordered if match not in train and match not in test)
        if not train or not test:
            continue
        retained = len(train) + len(test)
        candidates.append(Split(
            train=train,
            test=test,
            purged=purged,
            boundary_ts=cutoff,
            achieved_train_fraction=len(train) / retained,
        ))
    if not candidates:
        raise ValueError("cannot form a causally valid chronological holdout")
    return min(
        candidates,
        key=lambda split: (
            abs(split.achieved_train_fraction - train_fraction),
            len(split.purged),
            split.boundary_ts,
        ),
    )


def freeze_training_manifest(
    training_events: Iterable[ResearchEvent], *, alpha: float,
    split_cutoff_ts: int | None = None,
) -> FrozenManifest:
    """Freeze only information present in training events.

    ``alpha`` is a conventional research setting, not evidence of an economic
    advantage.  Rules and Kelly inputs remain empty until training-only
    discovery supplies them in later pipeline tasks.
    """
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be strictly between zero and one")
    events = tuple(training_events)
    source_event_ids = tuple(sorted({event.event_id for event in events}))
    source_hashes = tuple(sorted({event.source_sha256 for event in events if event.source_sha256}))
    cutoff = split_cutoff_ts if split_cutoff_ts is not None else max(
        (event.finish_ts for event in events), default=None
    )
    return FrozenManifest(
        split_cutoff_ts=cutoff,
        training_source_hashes_sha256=hashlib.sha256(
            "\n".join(source_hashes).encode("utf-8")
        ).hexdigest(),
        training_source_hashes=source_hashes,
        source_event_ids=source_event_ids,
        feature_definitions=(
            "current_price", "absolute_change", "relative_change", "realized_volatility",
            "path_high", "path_low", "elapsed_seconds", "level",
        ),
        empirical_cut_points={
            "current_price": tuple(sorted({event.current_price for event in events})),
            "absolute_change": tuple(sorted({event.absolute_change for event in events})),
            "relative_change": tuple(sorted({event.relative_change for event in events})),
        },
        selected_rule_ids=(),
        ranking_order=(),
        cost_specification={
            "execution_price": "HISTORICAL_REFERENCE_PRICE_ONLY",
            "fee_schedule": None,
            "fee_status": "BLOCK_DATA_FEE",
            "slippage": None,
        },
        significance_alpha=alpha,
        significance_provenance="CONVENTIONAL_RESEARCH_SETTING",
        kelly_inputs={},
    )


# The intervals are user-specified reporting bands.  Their half-open form is
# a deterministic convention that prevents a boundary price being counted twice.
FAVORITE_PRICE_BANDS: tuple[tuple[float, float], ...] = (
    (0.55, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 1.00),
)


def _binary_decision_groups(
    events: Iterable[ResearchEvent],
) -> tuple[tuple[ResearchEvent, ResearchEvent], ...]:
    """Return complete binary choices, ordered without inspecting outcomes."""
    grouped: dict[tuple[str, int, str], list[ResearchEvent]] = {}
    for event in events:
        grouped.setdefault(
            (event.event_id, event.decision_ts, event.decision_family), []
        ).append(event)
    groups: list[tuple[ResearchEvent, ResearchEvent]] = []
    for group in grouped.values():
        ordered = tuple(sorted(group, key=lambda event: event.outcome_index))
        if len(ordered) != 2:
            raise ValueError("binary decision group must contain exactly two outcomes")
        left, right = ordered
        if (
            left.event_id != right.event_id
            or left.market_id != right.market_id
            or left.decision_family != right.decision_family
            or left.decision_ts != right.decision_ts
            or left.finish_ts != right.finish_ts
        ):
            raise ValueError("binary decision pair must share event, market, decision family, time, and finish")
        if {left.outcome_index, right.outcome_index} != {0, 1}:
            raise ValueError("binary decision pair must have outcome indices {0, 1}")
        if left.token_id == right.token_id or left.won == right.won:
            raise ValueError("binary decision pair requires distinct token IDs and complementary winners")
        groups.append((ordered[0], ordered[1]))
    return tuple(sorted(groups, key=lambda pair: (
        pair[0].decision_ts, pair[0].finish_ts, pair[0].event_id,
    )))


def _fee_per_share(fee_schedule: FeeSchedule | None, price: float) -> float:
    """Return the supplied schedule's fee for a share at the supplied price."""
    if fee_schedule is None:
        return 0.0
    if not math.isfinite(price) or not 0.0 < price < 1.0:
        raise ValueError("price must be a finite binary-market price")
    return fee_schedule.rate * (price * (1.0 - price)) ** fee_schedule.exponent


def _ledger_item(
    event: ResearchEvent, *, pnl: float, cost: float, fee_schedule: FeeSchedule | None,
) -> dict[str, object]:
    return {
        "event_id": event.event_id,
        "token_id": event.token_id,
        "decision_ts": event.decision_ts,
        "finish_ts": event.finish_ts,
        "outcome_index": event.outcome_index,
        "entry_price": event.current_price,
        "fee_per_share": cost - event.current_price,
        "fee_rate": fee_schedule.rate if fee_schedule else None,
        "fee_exponent": fee_schedule.exponent if fee_schedule else None,
        "fee_source": fee_schedule.source if fee_schedule else None,
        "won": event.won,
        "pnl": pnl,
        "deployed_cost": cost,
    }


def favorite_baseline(
    events: Iterable[ResearchEvent], *, fee_schedule: FeeSchedule | None = None,
    decision_family: str = "PRE_MATCH_REFERENCE",
) -> list[dict[str, object]]:
    """Buy one favorite per match from a single, explicit decision family."""
    selected: list[ResearchEvent] = []
    skipped_ties = 0
    for left, right in _binary_decision_groups(
        event for event in events if event.decision_family == decision_family
    ):
        if left.current_price == right.current_price:
            skipped_ties += 1
            continue
        selected.append(left if left.current_price > right.current_price else right)

    output: list[dict[str, object]] = []
    for lower, upper in FAVORITE_PRICE_BANDS:
        in_band = [event for event in selected if lower <= event.current_price < upper]
        pnls_and_costs = [
            statistics.share_pnl(
                event.current_price, event.won,
                _fee_per_share(fee_schedule, event.current_price),
            )
            for event in in_band
        ]
        ledger = [
            _ledger_item(event, pnl=pnl, cost=cost, fee_schedule=fee_schedule)
            for event, (pnl, cost) in zip(in_band, pnls_and_costs)
        ]
        metrics = statistics.performance_metrics(ledger_entries=ledger)
        output.append({
            "price_band": f"[{lower:.2f},{upper:.2f})",
            "eligible_matches": len(in_band),
            "skipped_tied_favorites": skipped_ties,
            "fee_schedule": (
                {"rate": fee_schedule.rate, "exponent": fee_schedule.exponent, "source": fee_schedule.source}
                if fee_schedule else None
            ),
            "fee_source": fee_schedule.source if fee_schedule else None,
            "economic_result_basis": (
                "GROSS_REFERENCE_PROXY" if fee_schedule is None
                else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"
            ),
            "execution_cost_block": "BLOCK_DATA_FEE" if fee_schedule is None else None,
            "metrics": metrics,
            "max_drawdown_basis": "SETTLEMENT_CASHFLOW",
            "trade_ledger": ledger,
        })
    return output


def random_baseline(
    events: Iterable[ResearchEvent], *, draws: int, seed: int,
    fee_schedule: FeeSchedule | None = None,
    decision_family: str = "PRE_MATCH_REFERENCE",
) -> dict[str, object]:
    """Generate a matched random-side null distribution over eligible matches."""
    if isinstance(draws, bool) or not isinstance(draws, int) or draws <= 0:
        raise ValueError("draws must be a positive integer")
    groups = tuple(sorted(
        _binary_decision_groups(
            event for event in events if event.decision_family == decision_family
        ),
        key=lambda pair: (pair[0].finish_ts, pair[0].event_id),
    ))
    random_generator = np.random.default_rng(seed)
    selection = random_generator.integers(0, 2, size=(draws, len(groups)))
    prices = np.asarray([[left.current_price, right.current_price] for left, right in groups])
    wins = np.asarray([[left.won, right.won] for left, right in groups])
    if groups:
        picked_prices = np.take_along_axis(prices[None, :, :], selection[:, :, None], axis=2)[:, :, 0]
        picked_wins = np.take_along_axis(wins[None, :, :], selection[:, :, None], axis=2)[:, :, 0]
        fees = (
            0.0 if fee_schedule is None else fee_schedule.rate
            * (picked_prices * (1.0 - picked_prices)) ** fee_schedule.exponent
        )
        pnl = np.where(picked_wins, 1.0 - (picked_prices + fees), -(picked_prices + fees))
        costs = picked_prices + fees
        net_pnl = np.sum(pnl, axis=1)
        deployed_cost = np.sum(costs, axis=1)
        roi = net_pnl / deployed_cost
        equity = np.concatenate((np.zeros((draws, 1)), np.cumsum(pnl, axis=1)), axis=1)
        drawdown = np.max(np.maximum.accumulate(equity, axis=1) - equity, axis=1)
        if len(groups) > 1:
            standard_deviation = np.std(pnl, axis=1, ddof=1)
            sharpe = np.divide(
                np.mean(pnl, axis=1), standard_deviation,
                out=np.full(draws, np.nan), where=standard_deviation != 0.0,
            )
        else:
            sharpe = np.full(draws, np.nan)
    else:
        net_pnl = np.zeros(draws)
        roi = np.full(draws, np.nan)
        drawdown = np.zeros(draws)
        sharpe = np.full(draws, np.nan)
    return {
        "draws": draws,
        "seed": seed,
        "eligible_matches": len(groups),
        "selections_per_draw": len(groups),
        "fee_schedule": (
            {"rate": fee_schedule.rate, "exponent": fee_schedule.exponent, "source": fee_schedule.source}
            if fee_schedule else None
        ),
        "fee_source": fee_schedule.source if fee_schedule else None,
        "economic_result_basis": (
            "GROSS_REFERENCE_PROXY" if fee_schedule is None
            else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"
        ),
        "execution_cost_block": "BLOCK_DATA_FEE" if fee_schedule is None else None,
        "eligible_match_ledger": [
            {
                "event_id": left.event_id,
                "decision_ts": left.decision_ts,
                "finish_ts": left.finish_ts,
                "decision_family": left.decision_family,
                "outcome_token_ids": (left.token_id, right.token_id),
            }
            for left, right in groups
        ],
        "net_pnl_distribution": net_pnl.tolist(),
        "roi_distribution": roi.tolist(),
        "max_drawdown_usd_distribution": drawdown.tolist(),
        "max_drawdown_settlement_cashflow_usd_distribution": drawdown.tolist(),
        "max_drawdown_basis": "SETTLEMENT_CASHFLOW",
        "sharpe_per_match_distribution": sharpe.tolist(),
    }
