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
from bisect import bisect_left, bisect_right
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
class Clause:
    feature: str
    operator: str
    value: object

    def matches(self, event: ResearchEvent) -> bool:
        value = getattr(event, self.feature, None)
        if value is None:
            return False
        if self.operator == "GE":
            return float(value) >= float(self.value)
        if self.operator == "LT":
            return float(value) < float(self.value)
        if self.operator == "EQ":
            return value == self.value
        raise ValueError(f"unsupported condition operator: {self.operator}")

    def to_dict(self) -> dict[str, object]:
        return {"feature": self.feature, "operator": self.operator, "value": self.value}


@dataclass(frozen=True)
class Condition:
    rule_id: str
    family: str
    clauses: tuple[Clause, ...]
    numeric_provenance: str

    def to_dict(self) -> dict[str, object]:
        return {
            "clauses": [clause.to_dict() for clause in self.clauses],
            "family": self.family,
            "numeric_provenance": self.numeric_provenance,
            "rule_id": self.rule_id,
        }


@dataclass(frozen=True)
class ConditionResult:
    rule_id: str
    selection_rank: int | None
    matched_events: int
    matched_matches: int
    wins: int
    losses: int
    net_pnl: float
    deployed_cost: float
    roi: float | None
    bootstrap_lower: float | None
    bootstrap_upper: float | None
    raw_p_value: float | None
    q_value: float | None
    bonferroni_p_value: float | None
    largest_contribution: float | None
    pnl_without_largest: float
    reject_reason: str | None
    mean_pnl: float | None = None
    economic_result_basis: str = "GROSS_REFERENCE_PROXY"
    execution_cost_block: str = "BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY"


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
    selected_rule_definitions: Mapping[str, Mapping[str, object]]
    ranking_order: tuple[str, ...]
    condition_ledger: tuple[Mapping[str, object], ...]
    cost_specification: Mapping[str, object]
    significance_alpha: float
    significance_provenance: str
    kelly_inputs: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-safe, lexically ordered representation."""
        payload = {
            "cost_specification": dict(sorted(self.cost_specification.items())),
            "empirical_cut_points": {
                key: list(self.empirical_cut_points[key])
                for key in sorted(self.empirical_cut_points)
            },
            "feature_definitions": list(self.feature_definitions),
            "kelly_inputs": dict(sorted(self.kelly_inputs.items())),
            "ranking_order": list(self.ranking_order),
            "condition_ledger": [dict(sorted(item.items())) for item in self.condition_ledger],
            "selected_rule_ids": list(self.selected_rule_ids),
            "selected_rule_definitions": {
                key: dict(sorted(self.selected_rule_definitions[key].items()))
                for key in sorted(self.selected_rule_definitions)
            },
            "significance_alpha": self.significance_alpha,
            "significance_provenance": self.significance_provenance,
            "source_event_ids": list(self.source_event_ids),
            "split_cutoff_ts": self.split_cutoff_ts,
            "training_source_hashes": list(self.training_source_hashes),
            "training_source_hashes_sha256": self.training_source_hashes_sha256,
        }
        return dict(sorted(payload.items()))

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

    # Calculate candidate sizes from sorted timestamps, then materialize only
    # the chosen partition.  Comparing a MatchRecord to every train/test tuple
    # makes the old purge construction quadratic and repeatedly traverses paths.
    finish_times = tuple(match.finish_ts for match in ordered)
    pregame_times = tuple(sorted(match.pregame_ts for match in ordered))
    candidates: list[tuple[int, int, int, int, float]] = []
    # A cutoff is the earliest decision timestamp represented in test, not a
    # training settlement timestamp.  This retains every label known strictly
    # before that decision while discarding only intervals that overlap it.
    for cutoff in sorted(set(pregame_times)):
        train_count = bisect_left(finish_times, cutoff)
        # The cutoff itself belongs to neither side.  This prevents a decision
        # at the same timestamp as the last known training label from entering
        # the holdout as if it were strictly later information.
        test_count = len(ordered) - bisect_right(pregame_times, cutoff)
        if not train_count or not test_count:
            continue
        retained = train_count + test_count
        candidates.append((cutoff, train_count, test_count, len(ordered) - retained,
                           train_count / retained))
    if not candidates:
        raise ValueError("cannot form a causally valid chronological holdout")
    cutoff, _, _, _, fraction = min(
        candidates,
        key=lambda candidate: (
            abs(candidate[4] - train_fraction), candidate[3], candidate[0],
        ),
    )
    train = tuple(match for match in ordered if match.finish_ts < cutoff)
    test = tuple(match for match in ordered if match.pregame_ts > cutoff)
    purged = tuple(match for match in ordered if not (
        match.finish_ts < cutoff or match.pregame_ts > cutoff
    ))
    return Split(train=train, test=test, purged=purged, boundary_ts=cutoff,
                 achieved_train_fraction=fraction)


_NUMERIC_FEATURE_FAMILIES: tuple[tuple[str, str], ...] = (
    ("market_level", "current_price"),
    ("path_behavior", "absolute_change"),
    ("path_behavior", "relative_change"),
    ("path_behavior", "realized_volatility"),
    ("path_behavior", "path_high"),
    ("path_behavior", "path_low"),
    ("timing", "elapsed_seconds"),
)


_UNAVAILABLE_FAMILY_RULES: tuple[tuple[str, str, str], ...] = (
    (
        "UNAVAILABLE_MATCH_STATE",
        "match_state",
        "INADEQUATE_FIELD_COVERAGE",
    ),
    (
        "UNAVAILABLE_EXECUTION_FEASIBILITY",
        "execution_feasibility",
        "INADEQUATE_FIELD_COVERAGE",
    ),
    (
        "UNAVAILABLE_INVALID_ARITHMETIC",
        "input_validation",
        "INVALID_ARITHMETIC",
    ),
)


def _unavailable_family_ledger(events: Sequence[ResearchEvent]) -> tuple[dict[str, object], ...]:
    """Record unavailable families and rejected arithmetic as non-tradable rows.

    Historical normalization rejects non-finite/out-of-domain prices before a
    ``ResearchEvent`` can exist.  This explicit validation row prevents that
    rejection boundary from being mistaken for a tested, empty condition.
    """
    has_match_state = any(
        "BLOCK_DATA_MATCH_STATE" not in event.feature_availability for event in events
    )
    has_execution = any(
        event.best_bid is not None and event.best_ask is not None
        and event.visible_depth_usd is not None
        and "BLOCK_DATA_EXECUTION_BOOK" not in event.feature_availability
        and "BLOCK_DATA_VISIBLE_DEPTH" not in event.feature_availability
        for event in events
    )
    rows: list[dict[str, object]] = []
    for rule_id, family, reason in _UNAVAILABLE_FAMILY_RULES:
        unavailable = (
            family == "input_validation"
            or (family == "match_state" and not has_match_state)
            or (family == "execution_feasibility" and not has_execution)
        )
        if unavailable:
            rows.append({
                "rule_id": rule_id,
                "family": family,
                "matched_events": 0,
                "matched_matches": 0,
                "wins": 0,
                "losses": 0,
                "net_pnl": 0.0,
                "bootstrap_lower": None,
                "bootstrap_upper": None,
                "raw_p_value": None,
                "q_value": None,
                "bonferroni_p_value": None,
                "mean_pnl": None,
                "largest_contribution": None,
                "pnl_without_largest": 0.0,
                "economic_result_basis": "NOT_EVALUATED",
                "execution_cost_block": "NOT_APPLICABLE",
                "reject_reason": reason,
                "selection_eligible": False,
            })
    return tuple(rows)


def _rule_value(value: object) -> str:
    return format(float(value), ".12g") if isinstance(value, (int, float)) else str(value)


def _training_cut_points(values: Sequence[float]) -> tuple[float, ...]:
    """Bound empirical cuts to training values, retaining at most four partitions."""
    distinct = tuple(sorted({float(value) for value in values if math.isfinite(float(value))}))
    if len(distinct) <= 4:
        return distinct
    quantiles = np.quantile(np.asarray(distinct), [0.0, 0.25, 0.5, 0.75, 1.0])
    # Quantiles are snapped back to observed training values: no synthesized cut.
    return tuple(sorted({min(distinct, key=lambda value: abs(value - quantile)) for quantile in quantiles}))


def generate_candidates(events: Iterable[ResearchEvent]) -> tuple[Condition, ...]:
    """Generate a bounded, training-only condition universe.

    Historical rows contain no timestamped score state or executable book, so
    those families are omitted rather than modeled from missing values.
    """
    rows = tuple(events)
    candidates: list[Condition] = []
    for family, feature in _NUMERIC_FEATURE_FAMILIES:
        cuts = _training_cut_points([
            getattr(event, feature) for event in rows if getattr(event, feature) is not None
        ])
        for cut in cuts:
            for operator in ("GE", "LT"):
                clause = Clause(feature, operator, cut)
                candidates.append(Condition(
                    rule_id=f"{feature}_{operator.lower()}_{_rule_value(cut)}",
                    family=family,
                    clauses=(clause,),
                    numeric_provenance="EMPIRICAL_TRAINING_VALUE",
                ))
    levels = tuple(sorted({event.level for event in rows if event.level is not None}))
    for level in levels:
        clause = Clause("level", "EQ", level)
        candidates.append(Condition(
            rule_id=f"level_eq_{level}", family="competition", clauses=(clause,),
            numeric_provenance="OBSERVED_TRAINING_CATEGORY",
        ))

    # Pairwise rules are deliberately limited to individually usable components
    # from different families; no higher-order search is permitted.
    one_dimensional = tuple(candidate for candidate in candidates if any(
        evaluate_condition(candidate, event) for event in rows
    ))
    pairwise: list[Condition] = []
    for index, left in enumerate(one_dimensional):
        for right in one_dimensional[index + 1:]:
            if left.family == right.family:
                continue
            clauses = left.clauses + right.clauses
            rule_id = f"{left.rule_id}__and__{right.rule_id}"
            pairwise.append(Condition(
                rule_id=rule_id,
                family="pairwise",
                clauses=clauses,
                numeric_provenance="EMPIRICAL_TRAINING_VALUE",
            ))
    return tuple(sorted(candidates + pairwise, key=lambda candidate: candidate.rule_id))


def evaluate_condition(condition: Condition, event: ResearchEvent) -> bool:
    return all(clause.matches(event) for clause in condition.clauses)


def _condition_from_dict(raw: Mapping[str, object]) -> Condition:
    clauses = tuple(Clause(
        feature=str(clause["feature"]), operator=str(clause["operator"]), value=clause["value"],
    ) for clause in raw["clauses"])  # type: ignore[index]
    return Condition(
        rule_id=str(raw["rule_id"]), family=str(raw["family"]), clauses=clauses,
        numeric_provenance=str(raw["numeric_provenance"]),
    )


def _condition_rows(condition: Condition, events: Sequence[ResearchEvent]) -> list[ResearchEvent]:
    return [event for event in events if evaluate_condition(condition, event)]


def _condition_selection_groups(
    condition: Condition, events: Sequence[ResearchEvent],
) -> tuple[tuple[ResearchEvent, ResearchEvent], ...]:
    """Choose no more than one fixed, non-optimized position per match.

    A condition can match both outcomes and multiple decision families for a
    physical match.  These are not independent trades.  Within a binary
    decision, choose the higher current price (then token ID); across a match,
    prefer the fixed pre-match reference, then the earliest decision, then the
    same price/token ordering.  This is a predeclared accounting convention,
    not a condition-specific or outcome-optimized parameter.  The other member
    of the retained binary pair is preserved for the matched-side randomization
    null.
    """
    grouped: dict[tuple[str, int, str], list[ResearchEvent]] = {}
    for event in events:
        grouped.setdefault((event.event_id, event.decision_ts, event.decision_family), []).append(event)
    complete_events = tuple(
        event for group in grouped.values() if len(group) == 2 for event in group
    )
    per_decision: list[tuple[ResearchEvent, ResearchEvent]] = []
    for left, right in _binary_decision_groups(complete_events):
        candidates = [event for event in (left, right) if evaluate_condition(condition, event)]
        if not candidates:
            continue
        selected = min(candidates, key=lambda event: (-event.current_price, event.token_id))
        alternate = right if selected is left else left
        per_decision.append((selected, alternate))
    by_match: dict[str, list[tuple[ResearchEvent, ResearchEvent]]] = {}
    for pair in per_decision:
        by_match.setdefault(pair[0].event_id, []).append(pair)
    selections = [
        min(
            pairs,
            key=lambda pair: (
                pair[0].decision_family != "PRE_MATCH_REFERENCE",
                pair[0].decision_ts,
                -pair[0].current_price,
                pair[0].token_id,
            ),
        )
        for pairs in by_match.values()
    ]
    return tuple(sorted(selections, key=lambda pair: (
        pair[0].decision_ts, pair[0].finish_ts, pair[0].event_id,
    )))


def _proxy_pnl_rows(
    events: Sequence[ResearchEvent], *, fee_schedule: FeeSchedule | None = None,
) -> tuple[list[tuple[str, float]], list[float], list[float]]:
    rows: list[tuple[str, float]] = []
    pnls: list[float] = []
    costs: list[float] = []
    for event in events:
        pnl, cost = statistics.share_pnl(
            event.current_price, event.won, _fee_per_share(fee_schedule, event.current_price),
        )
        rows.append((event.event_id, pnl))
        pnls.append(pnl)
        costs.append(cost)
    return rows, pnls, costs


def _evaluate_conditions(
    candidates: Sequence[Condition], events: Sequence[ResearchEvent], *, seed: int,
    draws: int | None = None, bootstrap_draws: int | None = None,
    permutation_draws: int | None = None,
    fee_schedule: FeeSchedule | None = None,
) -> tuple[ConditionResult, ...]:
    # ``draws`` is retained for existing internal callers; split controls let
    # the public pipeline state each simulation count explicitly.
    if draws is not None:
        bootstrap_draws = draws if bootstrap_draws is None else bootstrap_draws
        permutation_draws = draws if permutation_draws is None else permutation_draws
    if bootstrap_draws is None or permutation_draws is None:
        raise ValueError("bootstrap_draws and permutation_draws are required")
    preliminary: list[ConditionResult] = []
    raw_ps: list[float] = []
    raw_indices: list[int] = []
    for index, condition in enumerate(candidates):
        selections = _condition_selection_groups(condition, events)
        matched = [selected for selected, _ in selections]
        if not selections:
            preliminary.append(ConditionResult(
                condition.rule_id, None, 0, 0, 0, 0, 0.0, 0.0, None, None,
                None, None, None, None, None, 0.0, "ZERO_MATCHES",
                economic_result_basis=("GROSS_REFERENCE_PROXY" if fee_schedule is None
                                       else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"),
                execution_cost_block=("BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY" if fee_schedule is None
                                      else "BLOCK_DATA_SLIPPAGE_LIQUIDITY"),
            ))
            continue
        pnl_rows, pnls, costs = _proxy_pnl_rows(matched, fee_schedule=fee_schedule)
        lower, upper = statistics.bootstrap_interval(
            pnl_rows, draws=bootstrap_draws, seed=seed + index,
        )
        p_value = statistics.outcome_side_permutation_p_value(
            selections,
            pnl_pairs=tuple(
                (
                    statistics.share_pnl(
                        selected.current_price, selected.won,
                        _fee_per_share(fee_schedule, selected.current_price),
                    )[0],
                    statistics.share_pnl(
                        alternate.current_price, alternate.won,
                        _fee_per_share(fee_schedule, alternate.current_price),
                    )[0],
                )
                for selected, alternate in selections
            ),
            draws=permutation_draws,
            seed=seed + len(candidates) + index,
        )
        diagnostic = statistics.contribution_diagnostics([
            sum(value for row_id, value in pnl_rows if row_id == event_id)
            for event_id in sorted({row_id for row_id, _ in pnl_rows})
        ])
        result = ConditionResult(
            condition.rule_id, None, len(matched), len({event.event_id for event in matched}),
            sum(event.won for event in matched), sum(not event.won for event in matched),
            sum(pnls), sum(costs), (sum(pnls) / sum(costs) if sum(costs) else None),
            lower, upper, p_value, None, None,
            diagnostic["largest_contribution"], float(diagnostic["pnl_without_largest"]),
            None if sum(pnls) > 0.0 else "NON_POSITIVE_TRAINING_EV",
            mean_pnl=(sum(pnls) / len(pnls) if pnls else None),
            economic_result_basis=("GROSS_REFERENCE_PROXY" if fee_schedule is None
                                   else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"),
            execution_cost_block=("BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY" if fee_schedule is None
                                  else "BLOCK_DATA_SLIPPAGE_LIQUIDITY"),
        )
        preliminary.append(result)
        if p_value is not None:
            raw_indices.append(index)
            raw_ps.append(p_value)
    q_values = statistics.benjamini_hochberg(raw_ps)
    bonferroni_values = statistics.bonferroni(raw_ps, tested_conditions=len(candidates))
    output = list(preliminary)
    for raw_index, q_value, family_p in zip(raw_indices, q_values, bonferroni_values):
        result = output[raw_index]
        output[raw_index] = ConditionResult(
            **{**result.__dict__, "q_value": q_value, "bonferroni_p_value": family_p}
        )
    return tuple(output)


def freeze_training_manifest(
    training_events: Iterable[ResearchEvent], *, alpha: float,
    split_cutoff_ts: int | None = None,
    fee_schedule: FeeSchedule | None = None,
    bootstrap_draws: int = 10_000,
    permutation_draws: int = 10_000,
) -> FrozenManifest:
    """Discover on training data only and freeze rules before holdout access."""
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be strictly between zero and one")
    if isinstance(bootstrap_draws, bool) or not isinstance(bootstrap_draws, int) or bootstrap_draws <= 0:
        raise ValueError("bootstrap_draws must be a positive integer")
    if isinstance(permutation_draws, bool) or not isinstance(permutation_draws, int) or permutation_draws <= 0:
        raise ValueError("permutation_draws must be a positive integer")
    events = tuple(training_events)
    source_event_ids = tuple(sorted({event.event_id for event in events}))
    source_hashes = tuple(sorted({event.source_sha256 for event in events if event.source_sha256}))
    cutoff = split_cutoff_ts if split_cutoff_ts is not None else max(
        (event.finish_ts for event in events), default=None
    )
    candidates = generate_candidates(events)
    results = _evaluate_conditions(
        candidates, events, bootstrap_draws=bootstrap_draws,
        permutation_draws=permutation_draws, seed=20260812,
                                   fee_schedule=fee_schedule)
    candidate_by_id = {candidate.rule_id: candidate for candidate in candidates}
    selected_results = []
    for result in results:
        reason = result.reject_reason
        if result.raw_p_value is not None and (result.q_value is None or result.q_value > alpha):
            reason = "FDR_NOT_SIGNIFICANT"
        if reason is not None:
            result = ConditionResult(**{**result.__dict__, "reject_reason": reason})
        selected_results.append(result)
    results = tuple(selected_results)
    ranked = sorted(
        (result for result in results if result.reject_reason is None and result.bootstrap_lower is not None and result.bootstrap_lower > 0.0),
        key=lambda result: (-float(result.bootstrap_lower), float(result.q_value if result.q_value is not None else 1.0), result.rule_id),
    )[:50]
    ranking_order = tuple(result.rule_id for result in ranked)
    ledger = tuple({
        "rule_id": result.rule_id, "matched_events": result.matched_events,
        "family": candidate_by_id[result.rule_id].family,
        "matched_matches": result.matched_matches, "wins": result.wins, "losses": result.losses,
        "net_pnl": result.net_pnl, "bootstrap_lower": result.bootstrap_lower,
        "bootstrap_upper": result.bootstrap_upper, "raw_p_value": result.raw_p_value,
        "q_value": result.q_value, "bonferroni_p_value": result.bonferroni_p_value,
        "mean_pnl": result.mean_pnl,
        "largest_contribution": result.largest_contribution,
        "pnl_without_largest": result.pnl_without_largest,
        "economic_result_basis": result.economic_result_basis,
        "execution_cost_block": result.execution_cost_block,
        "reject_reason": result.reject_reason,
        "selection_eligible": result.rule_id in ranking_order,
    } for result in results) + _unavailable_family_ledger(events)
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
            feature: _training_cut_points([
                getattr(event, feature) for event in events if getattr(event, feature) is not None
            ])
            for _, feature in _NUMERIC_FEATURE_FAMILIES
        },
        selected_rule_ids=ranking_order,
        selected_rule_definitions={
            rule_id: candidate_by_id[rule_id].to_dict() for rule_id in ranking_order
        },
        ranking_order=ranking_order,
        condition_ledger=ledger,
        cost_specification={
            "execution_price": "HISTORICAL_REFERENCE_PRICE_ONLY",
            "fee_schedule": (None if fee_schedule is None else {
                "rate": fee_schedule.rate, "exponent": fee_schedule.exponent,
                "source": fee_schedule.source,
            }),
            "fee_status": "BLOCK_DATA_FEE" if fee_schedule is None else "SUPPLIED_FEE_SCHEDULE",
            "slippage": None,
            "economic_result_basis": ("GROSS_REFERENCE_PROXY" if fee_schedule is None
                                      else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"),
        },
        significance_alpha=alpha,
        significance_provenance="CONVENTIONAL_RESEARCH_SETTING",
        kelly_inputs={
            "source": "TRAINING_ONLY_GROSS_REFERENCE_PROXY",
            "bootstrap_draws": bootstrap_draws,
            "permutation_draws": permutation_draws,
            "draw_provenance": "USER_SPECIFIED_SIMULATION_COUNT",
        },
    )


def evaluate_holdout(
    manifest: FrozenManifest, holdout_events: Iterable[ResearchEvent], *,
    fee_schedule: FeeSchedule | None = None,
    bootstrap_draws: int = 10_000,
    permutation_draws: int = 10_000,
) -> tuple[ConditionResult, ...]:
    """Evaluate the frozen rank order exactly once; this function never selects."""
    events = tuple(holdout_events)
    if isinstance(bootstrap_draws, bool) or not isinstance(bootstrap_draws, int) or bootstrap_draws <= 0:
        raise ValueError("bootstrap_draws must be a positive integer")
    if isinstance(permutation_draws, bool) or not isinstance(permutation_draws, int) or permutation_draws <= 0:
        raise ValueError("permutation_draws must be a positive integer")
    overlap = sorted({event.event_id for event in events} & set(manifest.source_event_ids))
    if overlap:
        raise ValueError("holdout overlaps frozen training event IDs")
    if manifest.split_cutoff_ts is not None and any(
        event.decision_ts <= manifest.split_cutoff_ts for event in events
    ):
        raise ValueError("holdout decisions must be strictly after frozen split cutoff")
    frozen_fee = manifest.cost_specification.get("fee_schedule")
    supplied_fee = None if fee_schedule is None else {
        "rate": fee_schedule.rate, "exponent": fee_schedule.exponent, "source": fee_schedule.source,
    }
    if frozen_fee != supplied_fee:
        raise ValueError("holdout fee schedule must match the frozen training fee schedule")
    results: list[ConditionResult] = []
    for rank, rule_id in enumerate(manifest.selected_rule_ids, start=1):
        raw = manifest.selected_rule_definitions.get(rule_id)
        if raw is None:
            raise ValueError(f"frozen manifest is missing rule definition: {rule_id}")
        condition = _condition_from_dict(raw)
        selections = _condition_selection_groups(condition, events)
        matched = [selected for selected, _ in selections]
        pnl_rows, pnls, costs = _proxy_pnl_rows(matched, fee_schedule=fee_schedule)
        lower, upper = statistics.bootstrap_interval(
            pnl_rows, draws=bootstrap_draws, seed=20260812 + rank,
        )
        p_value = statistics.outcome_side_permutation_p_value(
            selections,
            pnl_pairs=tuple(
                (
                    statistics.share_pnl(
                        selected.current_price, selected.won,
                        _fee_per_share(fee_schedule, selected.current_price),
                    )[0],
                    statistics.share_pnl(
                        alternate.current_price, alternate.won,
                        _fee_per_share(fee_schedule, alternate.current_price),
                    )[0],
                )
                for selected, alternate in selections
            ),
            draws=permutation_draws,
            seed=20270812 + rank,
        )
        diagnostic = statistics.contribution_diagnostics([
            sum(value for row_id, value in pnl_rows if row_id == event_id)
            for event_id in sorted({row_id for row_id, _ in pnl_rows})
        ])
        results.append(ConditionResult(
            rule_id, rank, len(matched), len({event.event_id for event in matched}),
            sum(event.won for event in matched), sum(not event.won for event in matched),
            sum(pnls), sum(costs), (sum(pnls) / sum(costs) if sum(costs) else None),
            lower, upper, p_value, None, None, diagnostic["largest_contribution"],
            float(diagnostic["pnl_without_largest"]), "ZERO_MATCHES" if not matched else None,
            mean_pnl=(sum(pnls) / len(pnls) if pnls else None),
            economic_result_basis=("GROSS_REFERENCE_PROXY" if fee_schedule is None
                                   else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"),
            execution_cost_block=("BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY" if fee_schedule is None
                                  else "BLOCK_DATA_SLIPPAGE_LIQUIDITY"),
        ))
    raw_indices = [index for index, result in enumerate(results) if result.raw_p_value is not None]
    raw_ps = [results[index].raw_p_value for index in raw_indices]
    q_values = statistics.benjamini_hochberg(raw_ps) if raw_ps else []
    bonferroni_values = statistics.bonferroni(raw_ps, tested_conditions=len(results)) if raw_ps else []
    for index, q_value, family_p in zip(raw_indices, q_values, bonferroni_values):
        result = results[index]
        reason = result.reject_reason
        if q_value > manifest.significance_alpha:
            reason = "FDR_NOT_SIGNIFICANT"
        results[index] = ConditionResult(**{
            **result.__dict__, "q_value": q_value,
            "bonferroni_p_value": family_p, "reject_reason": reason,
        })
    return tuple(results)


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
