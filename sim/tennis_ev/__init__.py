"""Reusable data and research components for Polymarket tennis EV studies."""

from .data import (
    ExclusionRecord,
    ForwardPayload,
    ForwardSnapshotRecord,
    MatchRecord,
    OutcomeRecord,
    TradeRecord,
    build_coverage_manifest,
    load_historical_matches,
    normalize_forward_snapshot,
    normalize_historical_row,
)

__all__ = [
    "ExclusionRecord",
    "ForwardPayload",
    "ForwardSnapshotRecord",
    "MatchRecord",
    "OutcomeRecord",
    "TradeRecord",
    "build_coverage_manifest",
    "load_historical_matches",
    "normalize_forward_snapshot",
    "normalize_historical_row",
]
