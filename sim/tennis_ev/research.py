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

from .data import MatchRecord


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
            "fee_rate": None,
            "slippage": None,
        },
        significance_alpha=alpha,
        significance_provenance="CONVENTIONAL_RESEARCH_SETTING",
        kelly_inputs={},
    )
