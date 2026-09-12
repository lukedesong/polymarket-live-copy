"""Shadow-4 nowcast probability engine for still-possible highest-temp NO.

RECORD / research only. This module never submits orders and never reads
POLYMARKET_LIVE_TRADING.

It replaces shadow-3's daily-max member pool with:
1. hourly ensemble members
2. a common per-model bias from the latest station obs vs model mean at that time
3. remaining-hour peaks floored by the already-observed daily max

Screening still-possible NO uses the most adverse model hit rate. Coverage
band (default 85%) and 50/50 market blend stay the shadow-3 gates. Dead bins
are labeled, never bought here, and never inferred from zero members.

Bias is applied in full to remaining hours. Do not invent a fade factor.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

from weather_high_temp_model import (
    FORECAST_COVERAGE_BAND,
    ONE,
    WeatherModelError,
    ZERO,
    blend_posteriors,
    coverage_band,
    devig_yes_prices,
    dutch_book_no,
    no_ev,
    selected_no_basket,
    taker_fee,
    whole_degree,
    yes_cluster_diagnosis,
)


TAIL_RISK_UNKNOWN = "TAIL_RISK_UNKNOWN"
DEAD_EXCLUDED_FROM_S4 = "DEAD_EXCLUDED_FROM_S4"
DEFAULT_MIN_EDGE = Decimal("0.04")
DEFAULT_MODEL_WEIGHT = Decimal("0.5")
DEFAULT_COVERAGE_ALPHA = Decimal("0.85")


def c_to_unit(celsius: Decimal, unit: str) -> Decimal:
    if unit.upper() == "F":
        return celsius * Decimal("9") / Decimal("5") + Decimal("32")
    return celsius


@dataclass(frozen=True)
class HourlyMember:
    hours: tuple[datetime, ...]
    temps_c: tuple[Decimal | None, ...]

    def __post_init__(self) -> None:
        if len(self.hours) != len(self.temps_c):
            raise WeatherModelError("HOURLY_MEMBER_LENGTH_MISMATCH")


def linear_temp_at(member: HourlyMember, when: datetime) -> Decimal | None:
    """Linear interpolation between adjacent hourly points. No extrapolation."""

    points = [
        (hour, temp)
        for hour, temp in zip(member.hours, member.temps_c)
        if temp is not None
    ]
    if not points:
        return None
    if when < points[0][0] or when > points[-1][0]:
        return None
    for index in range(len(points) - 1):
        left_t, left_v = points[index]
        right_t, right_v = points[index + 1]
        if when < left_t or when > right_t:
            continue
        span = (right_t - left_t).total_seconds()
        if span <= 0:
            return left_v
        weight = Decimal(str((when - left_t).total_seconds() / span))
        return left_v + (right_v - left_v) * weight
    return points[-1][1]


def model_mean_at(members: Sequence[HourlyMember], when: datetime) -> Decimal | None:
    values = [linear_temp_at(member, when) for member in members]
    present = [value for value in values if value is not None]
    if not present:
        return None
    return sum(present, ZERO) / Decimal(len(present))


def remaining_max_c(
    member: HourlyMember,
    *,
    when: datetime,
    event_date: date,
) -> Decimal | None:
    values = [
        temp
        for hour, temp in zip(member.hours, member.temps_c)
        if temp is not None and hour > when and hour.date() == event_date
    ]
    if not values:
        return None
    return max(values)


def member_final_c(
    member: HourlyMember,
    *,
    when: datetime,
    event_date: date,
    delta_c: Decimal,
    observed_max_c: Decimal,
) -> Decimal:
    remaining = remaining_max_c(member, when=when, event_date=event_date)
    if remaining is None:
        return observed_max_c
    return max(observed_max_c, remaining + delta_c)


def _histogram(buckets, wholes: Sequence[Decimal]) -> tuple[Decimal, ...]:
    if not wholes:
        raise WeatherModelError("MISSING_ENSEMBLE")
    counts = [ZERO] * len(buckets)
    for whole in wholes:
        hits = [index for index, bucket in enumerate(buckets) if bucket.contains(whole)]
        if len(hits) != 1:
            raise WeatherModelError("ENSEMBLE_MEMBER_NOT_IN_UNIQUE_BUCKET")
        counts[hits[0]] += ONE
    total = Decimal(len(wholes))
    return tuple(count / total for count in counts)


def _zero_dead_and_renormalize(buckets, probs, *, observed_whole: Decimal):
    adjusted: list[Decimal] = []
    live_mass = ZERO
    for bucket, prob in zip(buckets, probs):
        if bucket.is_dead(observed_whole):
            adjusted.append(ZERO)
        else:
            adjusted.append(prob)
            live_mass += prob
    if live_mass <= ZERO:
        raise WeatherModelError("NO_LIVE_PROBABILITY_MASS")
    return tuple(item / live_mass for item in adjusted)


def per_model_histogram(
    buckets,
    members: Sequence[HourlyMember],
    *,
    when: datetime,
    event_date: date,
    obs_temp_c: Decimal,
    observed_max_c: Decimal,
    observed_whole: Decimal,
    unit: str,
) -> tuple[tuple[Decimal, ...], Decimal]:
    mean = model_mean_at(members, when)
    if mean is None:
        raise WeatherModelError("MISSING_MODEL_MEAN_AT_OBS")
    delta = obs_temp_c - mean
    wholes = []
    for member in members:
        final_c = member_final_c(
            member,
            when=when,
            event_date=event_date,
            delta_c=delta,
            observed_max_c=observed_max_c,
        )
        wholes.append(whole_degree(c_to_unit(final_c, unit)))
    hist = _histogram(buckets, wholes)
    return _zero_dead_and_renormalize(
        buckets, hist, observed_whole=observed_whole
    ), delta


def average_histograms(histograms: Sequence[Sequence[Decimal]]) -> tuple[Decimal, ...]:
    if not histograms:
        raise WeatherModelError("MISSING_ENSEMBLE")
    width = len(histograms[0])
    n = Decimal(len(histograms))
    averaged = []
    for index in range(width):
        averaged.append(sum((hist[index] for hist in histograms), ZERO) / n)
    total = sum(averaged, ZERO)
    if total <= ZERO:
        raise WeatherModelError("NONPOSITIVE_MODEL_AVERAGE")
    return tuple(item / total for item in averaged)


def adverse_hit_rates(histograms: Sequence[Sequence[Decimal]]) -> tuple[Decimal, ...]:
    if not histograms:
        raise WeatherModelError("MISSING_ENSEMBLE")
    width = len(histograms[0])
    return tuple(max(hist[index] for hist in histograms) for index in range(width))


def qualify_nowcast_no_books(
    buckets,
    *,
    observed_max: Decimal,
    observed_max_c: Decimal,
    obs_temp_c: Decimal,
    obs_time: datetime,
    event_date: date,
    unit: str,
    model_members: Mapping[str, Sequence[HourlyMember]],
    min_edge: Decimal = DEFAULT_MIN_EDGE,
    model_weight: Decimal = DEFAULT_MODEL_WEIGHT,
    coverage_alpha: Decimal = DEFAULT_COVERAGE_ALPHA,
) -> dict[str, Any]:
    if not model_members:
        raise WeatherModelError("MISSING_HOURLY_ENSEMBLE")
    observed_whole = whole_degree(observed_max)
    per_model: dict[str, tuple[Decimal, ...]] = {}
    deltas: dict[str, str] = {}
    for name, members in model_members.items():
        if not members:
            continue
        hist, delta = per_model_histogram(
            buckets,
            members,
            when=obs_time,
            event_date=event_date,
            obs_temp_c=obs_temp_c,
            observed_max_c=observed_max_c,
            observed_whole=observed_whole,
            unit=unit,
        )
        per_model[name] = hist
        deltas[name] = str(delta)
    if not per_model:
        raise WeatherModelError("MISSING_HOURLY_ENSEMBLE")
    histograms = list(per_model.values())
    p_avg = average_histograms(histograms)
    p_adverse = adverse_hit_rates(histograms)
    market = devig_yes_prices(buckets, observed_whole=observed_whole)
    blended = blend_posteriors(p_avg, market, model_weight=model_weight)
    band = set(coverage_band(blended, alpha=coverage_alpha))
    rows: list[dict[str, Any]] = []
    qualified_indexes: list[int] = []
    for index, bucket in enumerate(buckets):
        dead = bucket.is_dead(observed_whole)
        live_unknown = (not dead) and p_adverse[index] <= ZERO
        in_band = (not dead) and (not live_unknown) and index in band
        row: dict[str, Any] = {
            "title": bucket.title,
            "status": "dead" if dead else "possible",
            "p_model": str(p_avg[index]),
            "p_adverse": str(p_adverse[index]),
            "p_market_devig": str(market[index]),
            "p_blend": str(blended[index]),
            "side": "NO",
            "in_coverage_band": in_band,
            "tail_unknown": live_unknown,
            "no_ask": None if bucket.no_ask is None else str(bucket.no_ask),
            "fee_rate": None if bucket.fee_rate is None else str(bucket.fee_rate),
            "ev": None,
            "qualify": False,
            "skip_reason": None,
            "race_vs_htt": False,
        }
        if dead:
            row["skip_reason"] = DEAD_EXCLUDED_FROM_S4
            rows.append(row)
            continue
        if live_unknown:
            row["skip_reason"] = TAIL_RISK_UNKNOWN
            rows.append(row)
            continue
        if bucket.no_ask is None:
            row["skip_reason"] = "MISSING_NO_ASK"
            rows.append(row)
            continue
        if bucket.fee_rate is None:
            row["skip_reason"] = "MISSING_FEE_RATE"
            rows.append(row)
            continue
        ev = no_ev(p_yes=p_adverse[index], no_ask=bucket.no_ask, fee_rate=bucket.fee_rate)
        row["ev"] = str(ev)
        row["fee"] = str(taker_fee(rate=bucket.fee_rate, price=bucket.no_ask))
        if in_band:
            row["skip_reason"] = FORECAST_COVERAGE_BAND
            rows.append(row)
            continue
        if ev >= min_edge:
            row["qualify"] = True
            qualified_indexes.append(index)
        else:
            row["skip_reason"] = "NO_EDGE_BELOW_MIN"
        rows.append(row)
    band_titles = [buckets[index].title for index in sorted(band)]
    return {
        "observed_whole": str(observed_whole),
        "obs_temp_c": str(obs_temp_c),
        "min_edge": str(min_edge),
        "model_weight": str(model_weight),
        "coverage_alpha": str(coverage_alpha),
        "coverage_band": band_titles,
        "nowcast_deltas_c": deltas,
        "models_used": sorted(per_model),
        "selected_no_basket": selected_no_basket(
            buckets,
            blended,
            qualified_indexes=qualified_indexes,
            observed_whole=observed_whole,
        ),
        "poly_live_trading_armed": False,
        "buckets": rows,
        "dutch_book_no": dutch_book_no(buckets),
        "yes_optional": yes_cluster_diagnosis(buckets, blended),
    }
