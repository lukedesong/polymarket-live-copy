"""Highest-temperature NO qualifier: posterior vs executable NO, no live orders.

YES is an extra diagnostic only. This module never claims a YES edge and never
treats a YES cluster as a hedge of a NO book.

Dead buckets are still raced: htt is faster on average, but leftover NO asks
are qualified when fee-after EV is positive. Empty books are a lost print,
not a refusal to race.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Mapping, Sequence
import json
import re


ZERO = Decimal("0")
ONE = Decimal("1")
DEFAULT_MIN_EDGE = Decimal("0.04")
DEFAULT_MODEL_WEIGHT = Decimal("0.5")
DEFAULT_SPREAD_INFLATION = Decimal("0.15")
HTT_WALLET = "0x6011655c4afb76f36dd1b08a137a1ba73466b31e"
YES_OPTIONAL_REASON = "YES_OPTIONAL_NO_CLAIMED_EDGE"
DEAD_NO_ASK_GONE = "DEAD_NO_ASK_GONE"
FORECAST_EXCLUDE_NEGATIVE = "FORECAST_EXCLUDE_NEGATIVE_EV"

_TITLE_BELOW = re.compile(
    r"(?P<hi>\d+(?:\.\d+)?)\s*°\s*(?P<unit>[FC])\s*or\s+below",
    re.IGNORECASE,
)
_TITLE_HIGHER = re.compile(
    r"(?P<lo>\d+(?:\.\d+)?)\s*°\s*(?P<unit>[FC])\s*or\s+higher",
    re.IGNORECASE,
)
_TITLE_RANGE = re.compile(
    r"(?P<lo>\d+(?:\.\d+)?)\s*[-–]\s*(?P<hi>\d+(?:\.\d+)?)\s*°\s*(?P<unit>[FC])",
    re.IGNORECASE,
)
_TITLE_SINGLE = re.compile(
    r"(?P<x>\d+(?:\.\d+)?)\s*°\s*(?P<unit>[FC])\s*$",
    re.IGNORECASE,
)
_STATION_SITE = re.compile(r"[?&]site=([A-Za-z0-9]+)", re.IGNORECASE)
_STATION_ICAO = re.compile(r"/([A-Z]{4})(?:[/?]|$)", re.IGNORECASE)


class WeatherModelError(ValueError):
    """A market or observation input cannot be turned into a posterior."""


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise WeatherModelError(f"INVALID_{field.upper()}") from exc
    if not result.is_finite():
        raise WeatherModelError(f"INVALID_{field.upper()}")
    return result


def _text(value: Any) -> str:
    return str(value or "").strip()


def whole_degree(value: Decimal) -> Decimal:
    """Resolution uses whole degrees by truncation, not rounding."""

    quantized = value.to_integral_value(rounding=ROUND_DOWN)
    return quantized


def parse_station_icao(resolution_source: str) -> str:
    source = _text(resolution_source)
    site = _STATION_SITE.search(source)
    if site:
        return site.group(1).upper()
    icao = _STATION_ICAO.search(source)
    if icao:
        return icao.group(1).upper()
    raise WeatherModelError("MISSING_STATION_ICAO")


@dataclass(frozen=True)
class TempBucket:
    title: str
    unit: str
    lo: Decimal | None
    hi: Decimal | None
    yes_token_id: str
    no_token_id: str
    fee_rate: Decimal | None
    yes_ask: Decimal | None
    no_ask: Decimal | None

    def contains(self, whole: Decimal) -> bool:
        if self.lo is not None and whole < self.lo:
            return False
        if self.hi is not None and whole > self.hi:
            return False
        return True

    def is_dead(self, observed_whole: Decimal) -> bool:
        return self.hi is not None and self.hi < observed_whole


def parse_bucket_bounds(title: str) -> tuple[Decimal | None, Decimal | None, str]:
    text = _text(title)
    matched = _TITLE_BELOW.search(text)
    if matched:
        return (
            None,
            _decimal(matched.group("hi"), field="bucket_hi"),
            matched.group("unit").upper(),
        )
    matched = _TITLE_HIGHER.search(text)
    if matched:
        return (
            _decimal(matched.group("lo"), field="bucket_lo"),
            None,
            matched.group("unit").upper(),
        )
    matched = _TITLE_RANGE.search(text)
    if matched:
        lo = _decimal(matched.group("lo"), field="bucket_lo")
        hi = _decimal(matched.group("hi"), field="bucket_hi")
        if hi < lo:
            raise WeatherModelError("INVERTED_BUCKET_RANGE")
        return lo, hi, matched.group("unit").upper()
    matched = _TITLE_SINGLE.search(text)
    if matched:
        point = _decimal(matched.group("x"), field="bucket_point")
        return point, point, matched.group("unit").upper()
    raise WeatherModelError(f"UNPARSEABLE_BUCKET_TITLE:{text}")


def taker_fee(*, rate: Decimal, price: Decimal) -> Decimal:
    if rate < ZERO or price < ZERO or price > ONE:
        raise WeatherModelError("INVALID_FEE_INPUT")
    return rate * price * (ONE - price)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    raise WeatherModelError("INVALID_JSON_LIST")


def _outcome_tokens(market: Mapping[str, Any]) -> tuple[str, str]:
    outcomes = [str(item) for item in _json_list(market.get("outcomes"))]
    tokens = [str(item) for item in _json_list(market.get("clobTokenIds"))]
    if len(outcomes) != 2 or len(tokens) != 2:
        raise WeatherModelError("INVALID_OUTCOME_TOKENS")
    lowered = [item.casefold() for item in outcomes]
    if lowered == ["yes", "no"]:
        return tokens[0], tokens[1]
    if lowered == ["no", "yes"]:
        return tokens[1], tokens[0]
    raise WeatherModelError("OUTCOMES_NOT_YES_NO")


def _fee_rate(market: Mapping[str, Any]) -> Decimal | None:
    schedule = market.get("feeSchedule")
    if not isinstance(schedule, dict):
        return None
    raw = schedule.get("rate")
    if raw is None:
        return None
    rate = _decimal(raw, field="fee_rate")
    if rate < ZERO:
        raise WeatherModelError("NEGATIVE_FEE_RATE")
    return rate


def buckets_from_gamma_event(event: Mapping[str, Any]) -> tuple[TempBucket, ...]:
    markets = event.get("markets")
    if not isinstance(markets, list) or not markets:
        raise WeatherModelError("MISSING_EVENT_MARKETS")
    buckets: list[TempBucket] = []
    units: set[str] = set()
    for market in markets:
        if not isinstance(market, dict):
            raise WeatherModelError("INVALID_EVENT_MARKET")
        title = _text(market.get("groupItemTitle") or market.get("question"))
        lo, hi, unit = parse_bucket_bounds(title)
        units.add(unit)
        yes_token, no_token = _outcome_tokens(market)
        buckets.append(
            TempBucket(
                title=title,
                unit=unit,
                lo=lo,
                hi=hi,
                yes_token_id=yes_token,
                no_token_id=no_token,
                fee_rate=_fee_rate(market),
                yes_ask=None,
                no_ask=None,
            )
        )
    if len(units) != 1:
        raise WeatherModelError("MIXED_BUCKET_UNITS")
    return tuple(buckets)


def attach_asks(
    buckets: Sequence[TempBucket],
    *,
    yes_asks: Mapping[str, Decimal | None],
    no_asks: Mapping[str, Decimal | None],
) -> tuple[TempBucket, ...]:
    attached: list[TempBucket] = []
    for bucket in buckets:
        attached.append(
            TempBucket(
                title=bucket.title,
                unit=bucket.unit,
                lo=bucket.lo,
                hi=bucket.hi,
                yes_token_id=bucket.yes_token_id,
                no_token_id=bucket.no_token_id,
                fee_rate=bucket.fee_rate,
                yes_ask=yes_asks.get(bucket.yes_token_id),
                no_ask=no_asks.get(bucket.no_token_id),
            )
        )
    return tuple(attached)


def truncated_ensemble(
    members: Sequence[Decimal],
    *,
    observed_max: Decimal,
) -> tuple[Decimal, ...]:
    observed = whole_degree(observed_max)
    if not members:
        raise WeatherModelError("MISSING_ENSEMBLE")
    truncated: list[Decimal] = []
    for member in members:
        high = whole_degree(max(observed_max, member))
        truncated.append(max(observed, high))
    return tuple(truncated)


def _histogram(
    buckets: Sequence[TempBucket],
    members: Sequence[Decimal],
) -> tuple[Decimal, ...]:
    counts = [ZERO] * len(buckets)
    total = Decimal(len(members))
    for member in members:
        hits = [index for index, bucket in enumerate(buckets) if bucket.contains(member)]
        if len(hits) != 1:
            raise WeatherModelError("ENSEMBLE_MEMBER_NOT_IN_UNIQUE_BUCKET")
        counts[hits[0]] += ONE
    return tuple(count / total for count in counts)


def _zero_dead_and_renormalize(
    buckets: Sequence[TempBucket],
    probs: Sequence[Decimal],
    *,
    observed_whole: Decimal,
) -> tuple[Decimal, ...]:
    adjusted: list[Decimal] = []
    live_mass = ZERO
    for bucket, prob in zip(buckets, probs, strict=True):
        if bucket.is_dead(observed_whole):
            adjusted.append(ZERO)
        else:
            adjusted.append(prob)
            live_mass += prob
    if live_mass <= ZERO:
        raise WeatherModelError("NO_LIVE_PROBABILITY_MASS")
    return tuple(item / live_mass for item in adjusted)


def inflate_spread(
    buckets: Sequence[TempBucket],
    probs: Sequence[Decimal],
    *,
    observed_whole: Decimal,
    inflation: Decimal,
) -> tuple[Decimal, ...]:
    if inflation < ZERO or inflation > ONE:
        raise WeatherModelError("INVALID_SPREAD_INFLATION")
    live_indexes = [
        index
        for index, bucket in enumerate(buckets)
        if not bucket.is_dead(observed_whole)
    ]
    if not live_indexes:
        raise WeatherModelError("NO_LIVE_BUCKETS")
    uniform = ONE / Decimal(len(live_indexes))
    mixed: list[Decimal] = []
    for index, bucket in enumerate(buckets):
        if bucket.is_dead(observed_whole):
            mixed.append(ZERO)
        else:
            mixed.append((ONE - inflation) * probs[index] + inflation * uniform)
    return _zero_dead_and_renormalize(buckets, mixed, observed_whole=observed_whole)


def devig_yes_prices(
    buckets: Sequence[TempBucket],
    *,
    observed_whole: Decimal,
) -> tuple[Decimal, ...]:
    raw: list[Decimal] = []
    for bucket in buckets:
        if bucket.yes_ask is None:
            raise WeatherModelError("MISSING_YES_ASK")
        raw.append(bucket.yes_ask)
    total = sum(raw, ZERO)
    if total <= ZERO:
        raise WeatherModelError("NONPOSITIVE_YES_SUM")
    normalized = tuple(item / total for item in raw)
    return _zero_dead_and_renormalize(
        buckets,
        normalized,
        observed_whole=observed_whole,
    )


def blend_posteriors(
    model: Sequence[Decimal],
    market: Sequence[Decimal],
    *,
    model_weight: Decimal,
) -> tuple[Decimal, ...]:
    if model_weight < ZERO or model_weight > ONE:
        raise WeatherModelError("INVALID_MODEL_WEIGHT")
    if len(model) != len(market):
        raise WeatherModelError("POSTERIOR_LENGTH_MISMATCH")
    blended = [
        model_weight * left + (ONE - model_weight) * right
        for left, right in zip(model, market, strict=True)
    ]
    total = sum(blended, ZERO)
    if total <= ZERO:
        raise WeatherModelError("NONPOSITIVE_BLEND")
    return tuple(item / total for item in blended)


def model_posterior(
    buckets: Sequence[TempBucket],
    *,
    observed_max: Decimal,
    members: Sequence[Decimal],
    spread_inflation: Decimal = DEFAULT_SPREAD_INFLATION,
) -> tuple[Decimal, ...]:
    observed_whole = whole_degree(observed_max)
    truncated = truncated_ensemble(members, observed_max=observed_max)
    histogram = _histogram(buckets, truncated)
    cleared = _zero_dead_and_renormalize(
        buckets,
        histogram,
        observed_whole=observed_whole,
    )
    return inflate_spread(
        buckets,
        cleared,
        observed_whole=observed_whole,
        inflation=spread_inflation,
    )


def no_ev(*, p_yes: Decimal, no_ask: Decimal, fee_rate: Decimal) -> Decimal:
    return (ONE - p_yes) - no_ask - taker_fee(rate=fee_rate, price=no_ask)


def yes_ev(*, p_yes: Decimal, yes_ask: Decimal, fee_rate: Decimal) -> Decimal:
    return p_yes - yes_ask - taker_fee(rate=fee_rate, price=yes_ask)


def forecast_exclude_no_ev(
    buckets: Sequence[TempBucket],
    probs: Sequence[Decimal],
) -> tuple[Decimal, str]:
    if len(buckets) < 2:
        raise WeatherModelError("NOT_ENOUGH_BUCKETS")
    excluded = max(range(len(probs)), key=lambda index: probs[index])
    selected = [
        (bucket, probs[index])
        for index, bucket in enumerate(buckets)
        if index != excluded
    ]
    cost = ZERO
    for bucket, _prob in selected:
        if bucket.no_ask is None or bucket.fee_rate is None:
            return ZERO, FORECAST_EXCLUDE_NEGATIVE
        cost += bucket.no_ask + taker_fee(rate=bucket.fee_rate, price=bucket.no_ask)
    share_count = Decimal(len(selected))
    p_excluded = probs[excluded]
    expected_payout = share_count - (ONE - p_excluded)
    ev = expected_payout - cost
    label = FORECAST_EXCLUDE_NEGATIVE if ev < ZERO else "FORECAST_EXCLUDE_NONNEGATIVE_EV"
    return ev, label


def dutch_book_no(
    buckets: Sequence[TempBucket],
) -> dict[str, Any]:
    cost = ZERO
    for bucket in buckets:
        if bucket.no_ask is None or bucket.fee_rate is None:
            return {
                "possible": False,
                "reason": "MISSING_NO_BOOK_OR_FEE",
            }
        cost += bucket.no_ask + taker_fee(rate=bucket.fee_rate, price=bucket.no_ask)
    payout = Decimal(len(buckets) - 1)
    edge = payout - cost
    return {
        "possible": edge > ZERO,
        "cost": str(cost),
        "payout": str(payout),
        "edge": str(edge),
    }


def yes_cluster_diagnosis(
    buckets: Sequence[TempBucket],
    probs: Sequence[Decimal],
    *,
    width: int = 1,
) -> dict[str, Any]:
    peak = max(range(len(probs)), key=lambda index: probs[index])
    start = max(0, peak - width)
    end = min(len(buckets) - 1, peak + width)
    cost = ZERO
    p_hit = ZERO
    titles: list[str] = []
    for index in range(start, end + 1):
        bucket = buckets[index]
        titles.append(bucket.title)
        p_hit += probs[index]
        if bucket.yes_ask is None or bucket.fee_rate is None:
            return {
                "titles": titles,
                "qualify": False,
                "skip_reason": YES_OPTIONAL_REASON,
                "ev": None,
                "claimed_positive_edge": False,
            }
        cost += bucket.yes_ask + taker_fee(rate=bucket.fee_rate, price=bucket.yes_ask)
    ev = p_hit - cost
    return {
        "titles": titles,
        "p_hit": str(p_hit),
        "cost": str(cost),
        "ev": str(ev),
        "qualify": False,
        "claimed_positive_edge": False,
        "skip_reason": YES_OPTIONAL_REASON,
    }


def qualify_no_books(
    buckets: Sequence[TempBucket],
    *,
    observed_max: Decimal,
    members: Sequence[Decimal],
    min_edge: Decimal = DEFAULT_MIN_EDGE,
    model_weight: Decimal = DEFAULT_MODEL_WEIGHT,
    spread_inflation: Decimal = DEFAULT_SPREAD_INFLATION,
) -> dict[str, Any]:
    observed_whole = whole_degree(observed_max)
    model = model_posterior(
        buckets,
        observed_max=observed_max,
        members=members,
        spread_inflation=spread_inflation,
    )
    market = devig_yes_prices(buckets, observed_whole=observed_whole)
    blended = blend_posteriors(model, market, model_weight=model_weight)
    rows: list[dict[str, Any]] = []
    for index, bucket in enumerate(buckets):
        dead = bucket.is_dead(observed_whole)
        p_model = model[index]
        p_market = market[index]
        p_blend = blended[index]
        row: dict[str, Any] = {
            "title": bucket.title,
            "status": "dead" if dead else "possible",
            "p_model": str(p_model),
            "p_market_devig": str(p_market),
            "p_blend": str(p_blend),
            "side": "NO",
            "no_ask": None if bucket.no_ask is None else str(bucket.no_ask),
            "fee_rate": None if bucket.fee_rate is None else str(bucket.fee_rate),
            "ev": None,
            "qualify": False,
            "skip_reason": None,
        }
        row["race_vs_htt"] = dead
        if bucket.no_ask is None:
            row["skip_reason"] = DEAD_NO_ASK_GONE if dead else "MISSING_NO_ASK"
            rows.append(row)
            continue
        if bucket.fee_rate is None:
            row["skip_reason"] = "MISSING_FEE_RATE"
            rows.append(row)
            continue
        p_yes = ZERO if dead else p_blend
        ev = no_ev(p_yes=p_yes, no_ask=bucket.no_ask, fee_rate=bucket.fee_rate)
        row["ev"] = str(ev)
        row["fee"] = str(taker_fee(rate=bucket.fee_rate, price=bucket.no_ask))
        if dead:
            qualifies = ev > ZERO
        else:
            qualifies = ev >= min_edge
        if qualifies:
            row["qualify"] = True
        else:
            row["skip_reason"] = "NO_EDGE_BELOW_MIN"
        rows.append(row)
    exclude_ev, exclude_label = forecast_exclude_no_ev(buckets, blended)
    return {
        "observed_whole": str(observed_whole),
        "min_edge": str(min_edge),
        "model_weight": str(model_weight),
        "spread_inflation": str(spread_inflation),
        "htt_wallet": HTT_WALLET,
        "poly_live_trading_armed": False,
        "buckets": rows,
        "forecast_exclude_diagnosis": {
            "ev": str(exclude_ev),
            "label": exclude_label,
        },
        "dutch_book_no": dutch_book_no(buckets),
        "yes_optional": yes_cluster_diagnosis(buckets, blended),
    }
