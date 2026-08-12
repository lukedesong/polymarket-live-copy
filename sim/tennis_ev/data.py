"""Causal, lossless adapters for tennis-market research data.

Historical public price-history samples are reference prices only.  This module
never fills absent historical books, trades, or match-state observations.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence


class DataValidationError(ValueError):
    """A source row cannot be used without changing its meaning."""

    def __init__(self, reason: str, field: str) -> None:
        super().__init__(f"{reason}: {field}")
        self.reason = reason
        self.field = field


@dataclass(frozen=True)
class OutcomeRecord:
    token_id: str
    name: str
    pregame_price: float
    opening_price: float | None
    won: bool
    path: tuple[tuple[int, float], ...]
    best_bid: float | None = None
    best_ask: float | None = None
    visible_depth_usd: float | None = None


@dataclass(frozen=True)
class MatchRecord:
    event_id: str
    market_id: str
    level: str | None
    title: str
    start_ts: int
    finish_ts: int
    pregame_ts: int
    outcomes: tuple[OutcomeRecord, OutcomeRecord]
    price_fidelity: str
    match_state: Mapping[str, object] | None
    source_sha256: str


@dataclass(frozen=True)
class TradeRecord:
    event_id: str
    token_id: str
    timestamp: int
    price: float
    size: float | None
    side: str | None
    transaction_id: str | None
    maker_taker_role: str = "UNKNOWN"


@dataclass(frozen=True)
class ExclusionRecord:
    event_id: str
    reason: str
    detail: str


@dataclass(frozen=True)
class ForwardSnapshotRecord:
    event_id: str
    token_id: str
    observed_at: int
    price: float
    best_bid: float | None
    best_ask: float | None
    visible_depth_usd: float | None
    match_state: Mapping[str, object] | None
    price_fidelity: str = "CONTEMPORANEOUS_BOOK"


@dataclass(frozen=True)
class ForwardPayload:
    """Observed forward data, with public trades distinct from book snapshots."""

    snapshots: tuple[ForwardSnapshotRecord, ...]
    trades: tuple[TradeRecord, ...]


def validate_binary_price(value: object, field: str) -> float:
    try:
        price = float(value)
    except (TypeError, ValueError) as exc:
        raise DataValidationError("PRICE_OUT_OF_DOMAIN", field) from exc
    if not math.isfinite(price) or not 0.0 < price < 1.0:
        raise DataValidationError("PRICE_OUT_OF_DOMAIN", field)
    return price


def _parse_array(value: object, field: str) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise DataValidationError("INVALID_JSON_FIELD", field) from exc
        if isinstance(parsed, list):
            return parsed
    raise DataValidationError("INVALID_ARRAY_FIELD", field)


def _required_text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if value is None or str(value).strip() == "":
        raise DataValidationError("MISSING_REQUIRED_FIELD", field)
    return str(value)


def _required_int(row: Mapping[str, object], field: str) -> int:
    try:
        value = int(row[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise DataValidationError("MISSING_REQUIRED_FIELD", field) from exc
    if value <= 0:
        raise DataValidationError("TIMESTAMP_OUT_OF_DOMAIN", field)
    return value


def _path(value: object, field: str, start_ts: int) -> tuple[tuple[int, float], ...]:
    rows = _parse_array(value, field)
    points: list[tuple[int, float]] = []
    for point in rows:
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            raise DataValidationError("INVALID_PATH_POINT", field)
        try:
            elapsed = int(point[0])
        except (TypeError, ValueError) as exc:
            raise DataValidationError("INVALID_PATH_POINT", field) from exc
        if elapsed < 0:
            raise DataValidationError("INVALID_PATH_POINT", field)
        points.append((start_ts + elapsed, validate_binary_price(point[1], field)))
    deduplicated: list[tuple[int, float]] = []
    for timestamp, price in sorted(points):
        if deduplicated and timestamp == deduplicated[-1][0]:
            if price != deduplicated[-1][1]:
                raise DataValidationError("CONFLICTING_PATH_TIMESTAMP", field)
            continue
        deduplicated.append((timestamp, price))
    return tuple(deduplicated)


def _historical_outcome(
    row: Mapping[str, object], side: str, start_ts: int, name: str
) -> OutcomeRecord:
    return OutcomeRecord(
        token_id=_required_text(row, f"{side}_token"),
        name=name,
        pregame_price=validate_binary_price(row.get(f"{side}_pregame_price"), f"{side}_pregame_price"),
        opening_price=None,
        won=_boolean(row.get(f"{side}_won"), f"{side}_won"),
        path=_path(row.get(f"{side}_path"), f"{side}_path", start_ts),
    )


def _historical_names(row: Mapping[str, object]) -> tuple[str, str]:
    """Return names in the artifact's high/low order, not display order."""
    outcomes = _parse_array(row.get("outcomes"), "outcomes")
    if len(outcomes) != 2 or any(not isinstance(item, str) or not item for item in outcomes):
        raise DataValidationError("INVALID_OUTCOMES", "outcomes")
    prices_value = row.get("pregame_prices")
    if prices_value is None:
        high_name = row.get("high_outcome")
        low_name = row.get("low_outcome")
        if not isinstance(high_name, str) or not isinstance(low_name, str):
            raise DataValidationError("OUTCOME_PRICE_ALIGNMENT_MISSING", "high_outcome/low_outcome")
        if high_name == low_name or {high_name, low_name} != {str(outcomes[0]), str(outcomes[1])}:
            raise DataValidationError("OUTCOME_PRICE_ALIGNMENT_MISSING", "high_outcome/low_outcome")
        return high_name, low_name
    prices = _parse_array(prices_value, "pregame_prices")
    if len(prices) != 2:
        raise DataValidationError("INVALID_OUTCOMES", "pregame_prices")
    high_price = validate_binary_price(row.get("high_pregame_price"), "high_pregame_price")
    low_price = validate_binary_price(row.get("low_pregame_price"), "low_pregame_price")
    aligned = [validate_binary_price(price, "pregame_prices") for price in prices]
    try:
        high_index = next(i for i, price in enumerate(aligned) if math.isclose(price, high_price, abs_tol=1e-12))
        low_index = next(i for i, price in enumerate(aligned) if math.isclose(price, low_price, abs_tol=1e-12))
    except StopIteration as exc:
        raise DataValidationError("OUTCOME_PRICE_ALIGNMENT_MISSING", "pregame_prices") from exc
    if high_index == low_index:
        raise DataValidationError("OUTCOME_PRICE_ALIGNMENT_AMBIGUOUS", "pregame_prices")
    return str(outcomes[high_index]), str(outcomes[low_index])


def _boolean(value: object, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise DataValidationError("INVALID_WINNER", field)


def normalize_historical_row(
    row: Mapping[str, object], *, source_sha256: str
) -> tuple[MatchRecord | None, list[ExclusionRecord]]:
    """Normalize one old path row, quarantining rather than silently repairing it."""
    if not isinstance(row, Mapping):
        return None, [ExclusionRecord(event_id="", reason="INVALID_ROW_MAPPING", detail="row")]
    event_id = str(row.get("event_id") or "")
    try:
        start_ts = _required_int(row, "start_ts")
        finish_ts = _required_int(row, "actual_finish_ts")
        pregame_ts = _required_int(row, "pregame_timestamp")
        if not pregame_ts < start_ts < finish_ts:
            raise DataValidationError("INVALID_MATCH_CHRONOLOGY", "pregame_timestamp/start_ts/actual_finish_ts")
        high_name, low_name = _historical_names(row)
        high = _historical_outcome(row, "high", start_ts, high_name)
        low = _historical_outcome(row, "low", start_ts, low_name)
        if high.won == low.won:
            raise DataValidationError("AMBIGUOUS_WINNER", "high_won/low_won")
        if any(timestamp >= finish_ts for outcome in (high, low) for timestamp, _ in outcome.path):
            raise DataValidationError("PATH_POINT_AFTER_FINISH", "high_path/low_path")
        return (
            MatchRecord(
                event_id=_required_text(row, "event_id"),
                market_id=_required_text(row, "market_id"),
                level=str(row["series"]) if row.get("series") not in (None, "") else None,
                title=str(row.get("title") or ""),
                start_ts=start_ts,
                finish_ts=finish_ts,
                pregame_ts=pregame_ts,
                outcomes=(high, low),
                price_fidelity="HISTORICAL_REFERENCE_PRICE",
                match_state=None,
                source_sha256=source_sha256,
            ),
            [],
        )
    except DataValidationError as exc:
        return None, [ExclusionRecord(event_id=event_id, reason=exc.reason, detail=exc.field)]


def _optional_price(value: object, field: str) -> float | None:
    if value is None:
        return None
    return validate_binary_price(value, field)


def _optional_nonnegative(value: object, field: str) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataValidationError("VALUE_OUT_OF_DOMAIN", field) from exc
    if not math.isfinite(number) or number < 0:
        raise DataValidationError("VALUE_OUT_OF_DOMAIN", field)
    return number


def normalize_forward_snapshot(snapshot: Mapping[str, object]) -> ForwardPayload:
    """Normalize supplied live observations; no network request is made here."""
    event_id = _required_text(snapshot, "event_id")
    observed_at = _required_int(snapshot, "observed_at")
    state = snapshot.get("match_state")
    if state is not None and not isinstance(state, Mapping):
        raise DataValidationError("INVALID_MATCH_STATE", "match_state")
    tokens = _parse_array(snapshot.get("tokens"), "tokens")
    normalized: list[ForwardSnapshotRecord] = []
    for index, token in enumerate(tokens):
        if not isinstance(token, Mapping):
            raise DataValidationError("INVALID_TOKEN", f"tokens[{index}]")
        normalized.append(
            ForwardSnapshotRecord(
                event_id=event_id,
                token_id=_required_text(token, "token_id"),
                observed_at=observed_at,
                price=validate_binary_price(token.get("price"), "price"),
                best_bid=_optional_price(token.get("best_bid"), "best_bid"),
                best_ask=_optional_price(token.get("best_ask"), "best_ask"),
                visible_depth_usd=_optional_nonnegative(token.get("visible_depth_usd"), "visible_depth_usd"),
                match_state=state,
            )
        )
    public_trades = _parse_array(snapshot.get("public_trades", []), "public_trades")
    trades: list[TradeRecord] = []
    for index, trade in enumerate(public_trades):
        if not isinstance(trade, Mapping):
            raise DataValidationError("INVALID_TRADE", f"public_trades[{index}]")
        side = trade.get("side")
        if side is not None:
            side = str(side)
        transaction_id = trade.get("transaction_id")
        if transaction_id is not None:
            transaction_id = str(transaction_id)
        trades.append(
            TradeRecord(
                event_id=event_id,
                token_id=_required_text(trade, "token_id"),
                timestamp=_required_int(trade, "timestamp"),
                price=validate_binary_price(trade.get("price"), "price"),
                size=_optional_nonnegative(trade.get("size"), "size"),
                side=side,
                transaction_id=transaction_id,
            )
        )
    return ForwardPayload(tuple(normalized), tuple(trades))


def build_coverage_manifest(
    raw_rows: int,
    matches: Sequence[MatchRecord],
    exclusions: Sequence[ExclusionRecord],
    *,
    source_path: Path | None = None,
    source_sha256: str | None = None,
) -> dict[str, object]:
    if raw_rows != len(matches) + len(exclusions):
        raise ValueError("coverage denominator does not reconcile")
    reasons = Counter(item.reason for item in exclusions)
    timestamp_coverage = {
        "earliest_pregame_ts": min((match.pregame_ts for match in matches), default=None),
        "latest_pregame_ts": max((match.pregame_ts for match in matches), default=None),
        "earliest_start_ts": min((match.start_ts for match in matches), default=None),
        "latest_start_ts": max((match.start_ts for match in matches), default=None),
        "earliest_finish_ts": min((match.finish_ts for match in matches), default=None),
        "latest_finish_ts": max((match.finish_ts for match in matches), default=None),
    }
    manifest: dict[str, object] = {
        "raw_rows": raw_rows,
        "usable_matches": len(matches),
        "excluded_matches": len(exclusions),
        "exclusions_by_reason": dict(sorted(reasons.items())),
        "unique_markets": len({match.market_id for match in matches}),
        "settlement_coverage": {
            "settled_matches": len(matches),
            "usable_matches": len(matches),
            "fraction": 1.0 if matches else None,
        },
        "pagination_status": "UNKNOWN_ARTIFACT",
        "truncation_status": "UNKNOWN_ARTIFACT",
        "timestamp_coverage": timestamp_coverage,
        "earliest_finish_ts": timestamp_coverage["earliest_finish_ts"],
        "latest_finish_ts": timestamp_coverage["latest_finish_ts"],
        "execution_book_matches": sum(
            all(outcome.best_bid is not None and outcome.best_ask is not None for outcome in match.outcomes)
            for match in matches
        ),
        "match_state_matches": sum(match.match_state is not None for match in matches),
    }
    if source_path is not None:
        manifest["source_path"] = str(source_path)
    if source_sha256 is not None:
        manifest["source_sha256"] = source_sha256
    return manifest


def _read_rows(path: Path) -> list[Mapping[str, object] | ExclusionRecord]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as source:
        if path.name.endswith(".jsonl") or path.name.endswith(".jsonl.gz"):
            rows: list[Mapping[str, object] | ExclusionRecord] = []
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    rows.append(
                        ExclusionRecord(
                            event_id="",
                            reason="INVALID_JSON_ROW",
                            detail=f"line {line_number}",
                        )
                    )
            return rows
        if path.name.endswith(".json") or path.name.endswith(".json.gz"):
            parsed = json.load(source)
            if not isinstance(parsed, list):
                raise ValueError("historical JSON source must be an array")
            return parsed
        return list(csv.DictReader(source))


def load_historical_matches(
    path: str | Path,
) -> tuple[
    tuple[MatchRecord, ...], tuple[ForwardSnapshotRecord, ...], tuple[TradeRecord, ...],
    tuple[Mapping[str, object], ...], tuple[ExclusionRecord, ...], dict[str, object],
]:
    """Load immutable historical rows and preserve absent evidence as empty collections."""
    source_path = Path(path)
    source_sha256 = hashlib.sha256(source_path.read_bytes()).hexdigest()
    rows = _read_rows(source_path)
    matches: list[MatchRecord] = []
    exclusions: list[ExclusionRecord] = []
    for row in rows:
        if isinstance(row, ExclusionRecord):
            exclusions.append(row)
            continue
        match, quarantined = normalize_historical_row(row, source_sha256=source_sha256)
        if match is not None:
            matches.append(match)
        exclusions.extend(quarantined)
    manifest = build_coverage_manifest(
        len(rows), matches, exclusions, source_path=source_path, source_sha256=source_sha256
    )
    return tuple(matches), (), (), (), tuple(exclusions), manifest
