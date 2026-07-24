#!/usr/bin/env python3
"""Read-only public-wallet research snapshot.

This script never imports authenticated trading code. It reads the public
Polymarket Data API and writes a bounded research snapshot for manual review.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATA_API = "https://data-api.polymarket.com"
USER_AGENT = "wallet-expert-sleeve-research/1.0"

LEGACY_SEEDS = {
    "ratehikes": "0xa309f903dbbd559e87d8d368834b8e41355c4cf2",
    "LlamaLoco0000": "0x93fb8127a1b21a112b11af936361225df0563e4a",
    "tetrose": "0x74471a007ddcc488f6d57b5e86dfb35a8d48a16d",
    "Themsnw": "0x69c5c6f6a4dda665991c203bc8d5df3e006b1e01",
    "Adrink3D": "0x54e15a0a77e3147ac78831804e5dde13cb589d5a",
    "russell110320": "0x118689b24aead1d6e9507b8068d056b2ec4f051b",
    "ZorroDeLaVega": "0xaae9b2c5ad90e82b5068c7f8a4b491997633d661",
    "sabsabinxz": "0xd3ecb2aee0d65622da559ff356b00e8c2e626603",
}

LEADERBOARD_CATEGORIES = (
    "OVERALL",
    "POLITICS",
    "SPORTS",
    "ESPORTS",
    "CULTURE",
    "MENTIONS",
    "WEATHER",
    "ECONOMICS",
    "TECH",
    "FINANCE",
)
LEADERBOARD_PERIODS = ("MONTH", "ALL")
LEADERBOARD_ALLOWED_PERIODS = ("DAY", "WEEK", "MONTH", "ALL")
# External constraints documented by Polymarket's public leaderboard endpoint.
LEADERBOARD_PAGE_SIZE = 50
LEADERBOARD_MAX_OFFSET = 1_000
CRYPTO_DOMAIN = "加密资产与代币"


DOMAIN_RULES: list[tuple[str, re.Pattern[str]]] = [
    (
        "Fed利率",
        re.compile(
            r"\bfed\b|federal reserve|fomc|interest rates?|rate cuts?|"
            r"rate hikes?|target federal funds|lower bound|upper bound",
            re.I,
        ),
    ),
    (
        "气候温度",
        re.compile(
            r"temperature|hottest|warmest|global warming|degrees? c|ºc|°c|"
            r"celsius",
            re.I,
        ),
    ),
    (
        "伊朗与霍尔木兹",
        re.compile(
            r"iran|iranian|khamenei|hormuz|tehran|enriched uranium|"
            r"mou negotiations",
            re.I,
        ),
    ),
    (
        "英国政局",
        re.compile(
            r"starmer|labour|burnham|bridget phillipson|uk prime minister|"
            r"british prime minister|downing street",
            re.I,
        ),
    ),
    (
        "特朗普言论与行程",
        re.compile(
            r"trump.*(?:say|speak|insult|mention|speech|attend|meet|"
            r"photograph)|what will trump|who will trump",
            re.I,
        ),
    ),
    (
        "体育比赛与球员",
        re.compile(
            r"(?:^|[-_/])(?:mlb|nba|nfl|nhl|atp|wta|ufc|fifwc|epl|ucl)"
            r"(?:[-_/]|$)|\bvs\.\s|world cup|ballon d.?or|spread:|"
            r"\d\+ goals|team to advance|play for the",
            re.I,
        ),
    ),
    (
        "加密资产与代币",
        re.compile(
            r"bitcoin|\bbtc\b|ethereum|\beth\b|\bxrp\b|solana|crypto|"
            r"market cap|fdv|token launch|airdrop",
            re.I,
        ),
    ),
    (
        "科技与产品发布",
        re.compile(
            r"gemini|spacex|starship|arc prize|openai|\bgpt\b|claude|"
            r"iphone|ai model|released? by",
            re.I,
        ),
    ),
    (
        "公司财报",
        re.compile(r"earnings|quarterly|non-?gaap|\beps\b|revenue", re.I),
    ),
    (
        "选举与一般政治",
        re.compile(
            r"election|president|prime minister|government|senate|"
            r"supreme court|justice|alito|nominee|cabinet|political opponent",
            re.I,
        ),
    ),
    (
        "能源与宏观市场",
        re.compile(
            r"crude oil|\boil\b|\bgold\b|nasdaq|s&p|dow jones|recession|"
            r"inflation",
            re.I,
        ),
    ),
    (
        "文化与社媒",
        re.compile(
            r"mrbeast|youtube|pok[eé]mon|oscar|grammy|movie|box office|"
            r"video.*views",
            re.I,
        ),
    ),
]


def api_get(endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{DATA_API}/{endpoint}?{query}",
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.load(response)
            if not isinstance(payload, list):
                raise RuntimeError(f"{endpoint} returned a non-list payload")
            return payload
        except Exception as error:  # pragma: no cover - network retry
            last_error = error
            time.sleep(1 + attempt)
    assert last_error is not None
    raise last_error


def normalize_wallet(value: Any) -> str | None:
    wallet = str(value or "").lower()
    return wallet if re.fullmatch(r"0x[a-f0-9]{40}", wallet) else None


def discover_leaderboard_candidates(
    fetch: Any,
    *,
    categories: tuple[str, ...] = LEADERBOARD_CATEGORIES,
    periods: tuple[str, ...] = LEADERBOARD_PERIODS,
    observed_at: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    pair_coverage: list[dict[str, Any]] = []
    for category in categories:
        if category == "CRYPTO":
            continue
        for period in periods:
            pair: dict[str, Any] = {
                "category": category,
                "period": period,
                "rows": 0,
                "complete": False,
                "error": None,
            }
            for offset in range(
                0, LEADERBOARD_MAX_OFFSET + 1, LEADERBOARD_PAGE_SIZE
            ):
                try:
                    page = fetch(
                        "v1/leaderboard",
                        {
                            "category": category,
                            "timePeriod": period,
                            "orderBy": "PNL",
                            "limit": LEADERBOARD_PAGE_SIZE,
                            "offset": offset,
                        },
                    )
                except Exception as error:
                    pair["error"] = (
                        f"{type(error).__name__}: {error}"
                    )
                    break
                pair["rows"] += len(page)
                for row in page:
                    wallet = normalize_wallet(row.get("proxyWallet"))
                    if wallet is None:
                        continue
                    item = candidates.setdefault(
                        wallet,
                        {
                            "wallet": wallet,
                            "name": str(row.get("userName") or wallet),
                            "origins": [],
                        },
                    )
                    if row.get("userName"):
                        item["name"] = str(row["userName"])
                    origin = {
                        "source": "leaderboard",
                        "category": category,
                        "period": period,
                        "rank": row.get("rank"),
                        "pnl": row.get("pnl"),
                        "vol": row.get("vol"),
                        "observed_at": observed_at,
                    }
                    if origin not in item["origins"]:
                        item["origins"].append(origin)
                if len(page) < LEADERBOARD_PAGE_SIZE:
                    pair["complete"] = True
                    break
            pair_coverage.append(pair)
    return candidates, {
        "pairs": pair_coverage,
        "complete": bool(pair_coverage)
        and all(item["complete"] for item in pair_coverage),
    }


def merge_candidate_pool(
    existing: dict[str, dict[str, Any]],
    discovered: dict[str, dict[str, Any]],
    legacy_seeds: dict[str, str],
    *,
    observed_at: str,
) -> dict[str, dict[str, Any]]:
    merged = {wallet: dict(item) for wallet, item in existing.items()}
    legacy_by_wallet = {
        wallet.lower(): name for name, wallet in legacy_seeds.items()
    }
    for wallet, incoming in discovered.items():
        item = merged.setdefault(wallet, {"wallet": wallet, "origins": []})
        item.setdefault("origins", [])
        item["name"] = incoming["name"]
        item["first_seen"] = item.get("first_seen") or observed_at
        item["last_seen"] = observed_at
        item["legacy_seed"] = wallet in legacy_by_wallet
        item["leaderboard_discovered"] = True
        for origin in incoming["origins"]:
            if origin not in item["origins"]:
                item["origins"].append(origin)
    for wallet, name in legacy_by_wallet.items():
        item = merged.setdefault(wallet, {"wallet": wallet, "origins": []})
        item.setdefault("origins", [])
        item["name"] = item.get("name") or name
        item["first_seen"] = item.get("first_seen") or observed_at
        item["last_seen"] = item.get("last_seen") or observed_at
        item["legacy_seed"] = True
        item.setdefault("leaderboard_discovered", False)
        origin = {"source": "legacy_seed", "name": name}
        if origin not in item["origins"]:
            item["origins"].append(origin)
    return merged


def ordered_candidate_wallets(
    pool: dict[str, dict[str, Any]],
) -> list[str]:
    def key(pair: tuple[str, dict[str, Any]]) -> tuple[Any, ...]:
        wallet, item = pair
        unseen = not item.get("last_analysis_at")
        latest = int(item.get("latest_source_trade_timestamp") or 0)
        analyzed = int(item.get("last_analyzed_source_trade_timestamp") or 0)
        changed = not unseen and latest > analyzed
        phase = 0 if unseen else 1 if changed else 2
        legacy_only = item.get("legacy_seed", False) and not item.get(
            "leaderboard_discovered", False
        )
        return (
            phase,
            legacy_only if unseen else False,
            item.get("last_analysis_at") or "",
            wallet,
        )

    return [wallet for wallet, _ in sorted(pool.items(), key=key)]


def fetch_trades(
    wallet: str,
    *,
    fetch: Any = api_get,
    page_size: int = 10_000,
    max_offset: int = 10_000,
) -> tuple[list[dict[str, Any]], bool, dict[str, Any]]:
    """Read full user history with documented timestamp-window pagination.

    The production page size and offset are external Data API constraints.
    Smaller values are injectable only for deterministic pagination tests.
    """

    accepted: list[dict[str, Any]] = []
    windows: list[dict[str, Any]] = []
    window_end: int | None = None
    while True:
        pages: list[dict[str, Any]] = []
        for offset in range(0, max_offset + 1, page_size):
            params: dict[str, Any] = {
                "user": wallet,
                "takerOnly": "false",
                "limit": page_size,
                "offset": offset,
                "start": 1,
            }
            if window_end is not None:
                params["end"] = window_end
            page = fetch("trades", params)
            pages.extend(page)
            if len(page) < page_size:
                accepted.extend(pages)
                windows.append(
                    {
                        "end": window_end,
                        "rows": len(pages),
                        "complete": True,
                    }
                )
                return accepted, True, {
                    "windows": windows,
                    "window_count": len(windows),
                    "block_reason": None,
                }

        timestamps = [
            int(number(row.get("timestamp")))
            for row in pages
            if int(number(row.get("timestamp"))) > 0
        ]
        if not timestamps:
            return accepted, False, {
                "windows": windows,
                "window_count": len(windows),
                "block_reason": "missing_timestamp",
            }
        cutoff = min(timestamps)
        if window_end is not None and cutoff >= window_end:
            return accepted, False, {
                "windows": windows,
                "window_count": len(windows),
                "block_reason": "same_timestamp_exceeds_offset_window",
            }
        accepted.extend(
            row
            for row in pages
            if int(number(row.get("timestamp"))) > cutoff
        )
        windows.append(
            {
                "end": window_end,
                "rows": len(pages),
                "complete": False,
                "next_end": cutoff,
            }
        )
        window_end = cutoff


def fetch_closed_positions(
    wallet: str, *, fetch: Any = api_get
) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    complete = False
    for offset in range(0, 100_001, 50):
        page = fetch(
            "closed-positions",
            {
                "user": wallet,
                "limit": 50,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
            },
        )
        rows.extend(page)
        if len(page) < 50:
            complete = True
            break
    return rows, complete


def fetch_open_positions(
    wallet: str, *, fetch: Any = api_get
) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    complete = False
    for offset in range(0, 10_001, 500):
        page = fetch(
            "positions",
            {
                "user": wallet,
                "limit": 500,
                "offset": offset,
                "sizeThreshold": 0,
                "sortBy": "INITIAL",
                "sortDirection": "DESC",
            },
        )
        rows.extend(page)
        if len(page) < 500:
            complete = True
            break
    return rows, complete


def number(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    return result if math.isfinite(result) else 0.0


def classify_domain(title: str, event_slug: str) -> str:
    text = f"{title} {event_slug}"
    for label, pattern in DOMAIN_RULES:
        if pattern.search(text):
            return label
    return "其他/未分类"


def partition_noncrypto_rows(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    for row in rows:
        domain = classify_domain(
            str(row.get("title") or ""),
            str(row.get("eventSlug") or ""),
        )
        target = removed if domain == CRYPTO_DOMAIN else kept
        target.append(row)
    return kept, removed


def analyze_trade_lifecycle(
    trades: list[dict[str, Any]],
    *,
    closed_condition_ids: set[str],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trades:
        condition = str(
            row.get("conditionId")
            or row.get("eventSlug")
            or row.get("asset")
            or "unknown"
        )
        grouped[condition].append(row)

    conditions: dict[str, dict[str, Any]] = {}
    same_timestamp_cycles = 0
    formula_conditions = 0
    directional_conditions = 0
    for condition, rows in grouped.items():
        assets = {str(row.get("asset") or "") for row in rows}
        outcomes = {str(row.get("outcome") or "") for row in rows}
        sides = {
            str(row.get("side") or "").upper()
            for row in rows
            if row.get("side")
        }
        side_by_asset_time: dict[tuple[str, int], set[str]] = defaultdict(set)
        for row in rows:
            timestamp = int(number(row.get("timestamp")))
            side = str(row.get("side") or "").upper()
            if timestamp and side:
                side_by_asset_time[
                    (str(row.get("asset") or ""), timestamp)
                ].add(side)
        cycles = sum(
            values == {"BUY", "SELL"}
            for values in side_by_asset_time.values()
        )
        same_timestamp_cycles += cycles

        nonempty_assets = {value for value in assets if value}
        nonempty_outcomes = {value for value in outcomes if value}
        if len(nonempty_assets) > 1 or len(nonempty_outcomes) > 1:
            lifecycle = "BASKET_OR_HEDGE"
            formula_conditions += 1
        elif sides == {"BUY", "SELL"}:
            lifecycle = "ACTIVE_EXIT"
            formula_conditions += 1
        elif sides == {"BUY"} and condition in closed_condition_ids:
            lifecycle = "HOLD_TO_RESOLUTION"
            directional_conditions += 1
        elif sides == {"BUY"}:
            lifecycle = "OPEN_OR_UNRESOLVED"
            directional_conditions += 1
        elif sides == {"SELL"}:
            lifecycle = "SELL_ONLY_INCOMPLETE_HISTORY"
            formula_conditions += 1
        else:
            lifecycle = "UNRESOLVED"
            formula_conditions += 1

        conditions[condition] = {
            "lifecycle": lifecycle,
            "sides": sorted(sides),
            "assets": sorted(nonempty_assets),
            "outcomes": sorted(nonempty_outcomes),
            "raw_fill_rows": len(rows),
            "same_timestamp_opposite_side_cycles": cycles,
        }

    if not conditions:
        strategy_state = "NO_NONCRYPTO_SLEEVE"
    elif same_timestamp_cycles and not directional_conditions:
        strategy_state = "OBSERVABLE_MM_OR_SPEED"
    elif formula_conditions:
        strategy_state = "FORMULA_RESEARCH"
    else:
        strategy_state = "DIRECTIONAL_RESEARCH_CANDIDATE"
    lifecycle_counts = Counter(
        item["lifecycle"] for item in conditions.values()
    )
    return {
        "strategy_state": strategy_state,
        "conditions": conditions,
        "lifecycle_counts": dict(sorted(lifecycle_counts.items())),
        "same_timestamp_opposite_side_cycles": same_timestamp_cycles,
        "directional_condition_count": directional_conditions,
        "formula_condition_count": formula_conditions,
    }


def iso_date(timestamp: int | float | None) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(int(timestamp), timezone.utc).date().isoformat()


def utc_month(timestamp: int | float | None) -> str:
    if not timestamp:
        return "unknown"
    return datetime.fromtimestamp(int(timestamp), timezone.utc).strftime("%Y-%m")


def safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def round_number(value: float | None) -> float | None:
    return None if value is None else round(value, 8)


def aggregate_events(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        event_key = str(
            row.get("eventSlug")
            or row.get("conditionId")
            or row.get("asset")
            or "unknown"
        )
        item = grouped.setdefault(
            event_key,
            {
                "event_key": event_key,
                "titles": set(),
                "condition_ids": set(),
                "assets": set(),
                "timestamp": 0,
                "pnl": 0.0,
                "cost": 0.0,
                "position_rows": 0,
                "domain_votes": Counter(),
            },
        )
        title = str(row.get("title") or "")
        slug = str(row.get("eventSlug") or "")
        item["titles"].add(title)
        item["condition_ids"].add(str(row.get("conditionId") or ""))
        item["assets"].add(str(row.get("asset") or ""))
        item["timestamp"] = max(item["timestamp"], int(number(row.get("timestamp"))))
        item["pnl"] += number(row.get("realizedPnl"))
        item["cost"] += abs(number(row.get("avgPrice")) * number(row.get("totalBought")))
        item["position_rows"] += 1
        item["domain_votes"][classify_domain(title, slug)] += 1

    events: list[dict[str, Any]] = []
    for item in grouped.values():
        domain = item["domain_votes"].most_common(1)[0][0]
        events.append(
            {
                "event_key": item["event_key"],
                "titles": sorted(item["titles"]),
                "condition_count": len({x for x in item["condition_ids"] if x}),
                "asset_count": len({x for x in item["assets"] if x}),
                "timestamp": item["timestamp"],
                "date": iso_date(item["timestamp"]),
                "month": utc_month(item["timestamp"]),
                "pnl": item["pnl"],
                "cost": item["cost"],
                "position_rows": item["position_rows"],
                "domain": domain,
            }
        )
    return sorted(events, key=lambda item: (item["timestamp"], item["event_key"]))


def period_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    pnl = sum(item["pnl"] for item in events)
    cost = sum(item["cost"] for item in events)
    positive = [item["pnl"] for item in events if item["pnl"] > 0]
    monthly = defaultdict(float)
    for item in events:
        monthly[item["month"]] += item["pnl"]
    return {
        "events": len(events),
        "position_rows": sum(item["position_rows"] for item in events),
        "start": iso_date(min((item["timestamp"] for item in events), default=0)),
        "end": iso_date(max((item["timestamp"] for item in events), default=0)),
        "reported_realized_pnl": round_number(pnl),
        "estimated_cost": round_number(cost),
        "reported_pnl_over_estimated_cost": round_number(safe_ratio(pnl, cost)),
        "winning_events": sum(item["pnl"] > 0 for item in events),
        "losing_events": sum(item["pnl"] < 0 for item in events),
        "positive_pnl": round_number(sum(positive)),
        "largest_positive_event_share": round_number(
            safe_ratio(max(positive, default=0.0), sum(positive))
        ),
        "top_five_positive_event_share": round_number(
            safe_ratio(sum(sorted(positive, reverse=True)[:5]), sum(positive))
        ),
        "positive_months": sum(value > 0 for value in monthly.values()),
        "negative_months": sum(value < 0 for value in monthly.values()),
        "monthly_reported_pnl": {
            month: round_number(value) for month, value in sorted(monthly.items())
        },
    }


def domain_summaries(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_domain: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in events:
        by_domain[item["domain"]].append(item)

    total_events = len(events)
    total_cost = sum(item["cost"] for item in events)
    summaries: list[dict[str, Any]] = []
    for domain, domain_events in by_domain.items():
        ordered = sorted(domain_events, key=lambda item: (item["timestamp"], item["event_key"]))
        split = len(ordered) // 2
        discovery = ordered[:split]
        later = ordered[split:]
        summary = period_summary(ordered)
        summary.update(
            {
                "domain": domain,
                "decision_share_by_event": round_number(
                    safe_ratio(len(ordered), total_events)
                ),
                "risk_share_by_estimated_cost": round_number(
                    safe_ratio(sum(item["cost"] for item in ordered), total_cost)
                ),
                "exploratory_event_median_split": {
                    "method": (
                        "Formula-derived chronological median by event count; "
                        "exploratory only, not a validated regime boundary."
                    ),
                    "discovery": period_summary(discovery),
                    "later": period_summary(later),
                },
                "largest_events_by_absolute_pnl": [
                    {
                        "event": item["event_key"],
                        "date": item["date"],
                        "pnl": round_number(item["pnl"]),
                        "cost": round_number(item["cost"]),
                        "titles": item["titles"][:3],
                    }
                    for item in sorted(
                        ordered, key=lambda item: abs(item["pnl"]), reverse=True
                    )[:5]
                ],
            }
        )
        summaries.append(summary)
    return sorted(
        summaries,
        key=lambda item: (
            item["events"],
            abs(item["reported_realized_pnl"] or 0),
        ),
        reverse=True,
    )


def trade_summary(
    trades: list[dict[str, Any]], truncated: bool
) -> dict[str, Any]:
    timestamps = [int(number(row.get("timestamp"))) for row in trades if row.get("timestamp")]
    transaction_hashes = {
        str(row.get("transactionHash"))
        for row in trades
        if row.get("transactionHash")
    }
    conditions = {
        str(row.get("conditionId")) for row in trades if row.get("conditionId")
    }
    assets = {str(row.get("asset")) for row in trades if row.get("asset")}
    exact_price_counts = Counter(str(row.get("price")) for row in trades)
    tx_per_minute: dict[str, set[str]] = defaultdict(set)
    for row in trades:
        timestamp = int(number(row.get("timestamp")))
        transaction_hash = str(row.get("transactionHash") or "")
        if timestamp and transaction_hash:
            minute = datetime.fromtimestamp(timestamp, timezone.utc).strftime(
                "%Y-%m-%dT%H:%M"
            )
            tx_per_minute[minute].add(transaction_hash)

    condition_month_domain: dict[str, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for row in trades:
        month = utc_month(row.get("timestamp"))
        domain = classify_domain(
            str(row.get("title") or ""), str(row.get("eventSlug") or "")
        )
        condition_id = str(row.get("conditionId") or "")
        if condition_id:
            condition_month_domain[month][domain].add(condition_id)
    monthly_mix = []
    for month, domain_map in sorted(condition_month_domain.items()):
        total = len(set().union(*domain_map.values())) if domain_map else 0
        ranked = sorted(
            (
                {
                    "domain": domain,
                    "unique_conditions": len(values),
                    "condition_share": round_number(
                        safe_ratio(len(values), total)
                    ),
                }
                for domain, values in domain_map.items()
            ),
            key=lambda item: item["unique_conditions"],
            reverse=True,
        )
        monthly_mix.append({"month": month, "domains": ranked})

    return {
        "raw_fill_rows": len(trades),
        "coverage_truncated_at_public_offset_cap": truncated,
        "start": iso_date(min(timestamps)) if timestamps else None,
        "end": iso_date(max(timestamps)) if timestamps else None,
        "unique_transaction_hashes": len(transaction_hashes),
        "unique_conditions": len(conditions),
        "unique_assets": len(assets),
        "raw_fills_per_unique_transaction": round_number(
            safe_ratio(len(trades), len(transaction_hashes))
        ),
        "most_common_exact_price": (
            exact_price_counts.most_common(1)[0][0] if exact_price_counts else None
        ),
        "most_common_exact_price_fill_share": round_number(
            safe_ratio(
                exact_price_counts.most_common(1)[0][1] if exact_price_counts else 0,
                len(trades),
            )
        ),
        "maximum_unique_transactions_in_utc_clock_minute": max(
            (len(values) for values in tx_per_minute.values()), default=0
        ),
        "monthly_unique_condition_domain_mix": monthly_mix,
    }


def open_position_summary(
    rows: list[dict[str, Any]], complete: bool
) -> dict[str, Any]:
    by_domain: dict[str, dict[str, float]] = defaultdict(
        lambda: {"positions": 0.0, "initial_value": 0.0, "current_value": 0.0, "cash_pnl": 0.0}
    )
    for row in rows:
        domain = classify_domain(
            str(row.get("title") or ""), str(row.get("eventSlug") or "")
        )
        item = by_domain[domain]
        item["positions"] += 1
        item["initial_value"] += number(row.get("initialValue"))
        item["current_value"] += number(row.get("currentValue"))
        item["cash_pnl"] += number(row.get("cashPnl"))
    return {
        "pagination_complete": complete,
        "positions": len(rows),
        "initial_value": round_number(sum(number(row.get("initialValue")) for row in rows)),
        "current_value": round_number(sum(number(row.get("currentValue")) for row in rows)),
        "cash_pnl": round_number(sum(number(row.get("cashPnl")) for row in rows)),
        "domains": [
            {
                "domain": domain,
                **{
                    key: (int(value) if key == "positions" else round_number(value))
                    for key, value in values.items()
                },
            }
            for domain, values in sorted(
                by_domain.items(),
                key=lambda pair: pair[1]["initial_value"],
                reverse=True,
            )
        ],
    }


def scan_wallet(
    name: str, wallet: str, *, fetch: Any = api_get
) -> dict[str, Any]:
    trades, trades_complete, trade_coverage = fetch_trades(
        wallet, fetch=fetch
    )
    closed_rows, closed_complete = fetch_closed_positions(
        wallet, fetch=fetch
    )
    open_rows, open_complete = fetch_open_positions(wallet, fetch=fetch)
    noncrypto_trades, crypto_trades = partition_noncrypto_rows(trades)
    noncrypto_closed, crypto_closed = partition_noncrypto_rows(closed_rows)
    noncrypto_open, crypto_open = partition_noncrypto_rows(open_rows)
    events = aggregate_events(noncrypto_closed)
    closed_condition_ids = {
        str(row.get("conditionId"))
        for row in noncrypto_closed
        if row.get("conditionId")
    }
    lifecycle = analyze_trade_lifecycle(
        noncrypto_trades,
        closed_condition_ids=closed_condition_ids,
    )
    observed_strategy_state = lifecycle["strategy_state"]
    coverage_complete = (
        trades_complete and closed_complete and open_complete
    )
    block_reasons = []
    if not trades_complete:
        block_reasons.append(
            trade_coverage.get("block_reason") or "trades_incomplete"
        )
    if not closed_complete:
        block_reasons.append("closed_positions_incomplete")
    if not open_complete:
        block_reasons.append("open_positions_incomplete")
    if not coverage_complete:
        lifecycle["observed_strategy_state"] = observed_strategy_state
        lifecycle["strategy_state"] = "BLOCK_DATA"
    lifecycle["block_reasons"] = block_reasons
    latest_source_trade_timestamp = max(
        (int(number(row.get("timestamp"))) for row in trades),
        default=0,
    )
    latest_noncrypto_trade_timestamp = max(
        (int(number(row.get("timestamp"))) for row in noncrypto_trades),
        default=0,
    )
    return {
        "name": name,
        "wallet": wallet,
        "profile_url": f"https://polymarket.com/profile/{wallet}",
        "latest_source_trade_timestamp": latest_source_trade_timestamp,
        "latest_noncrypto_trade_timestamp": latest_noncrypto_trade_timestamp,
        "sources": {
            "trades": (
                f"{DATA_API}/trades?user={wallet}&takerOnly=false"
            ),
            "closed_positions": (
                f"{DATA_API}/closed-positions?user={wallet}"
            ),
            "open_positions": f"{DATA_API}/positions?user={wallet}",
        },
        "crypto_rows_removed": {
            "trades": len(crypto_trades),
            "closed_positions": len(crypto_closed),
            "open_positions": len(crypto_open),
        },
        "strategy": lifecycle,
        "trade_history_coverage": trade_coverage,
        "trades": trade_summary(noncrypto_trades, not trades_complete),
        "closed_positions": {
            "pagination_complete": closed_complete,
            "position_rows": len(noncrypto_closed),
            "event_level": period_summary(events),
            "domains": domain_summaries(events),
        },
        "open_positions": open_position_summary(
            noncrypto_open, open_complete
        ),
    }


def load_candidate_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "candidates": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("candidate state must be a JSON object")
    if not isinstance(payload.get("candidates"), dict):
        raise ValueError("candidate state is missing candidates")
    return payload


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(path)


def profile_url(wallet: str) -> str:
    return f"https://polymarket.com/profile/{wallet}"


def refresh_analyzed_candidate_activity(
    pool: dict[str, dict[str, Any]],
    *,
    fetch: Any,
    observed_at: str,
) -> None:
    """Refresh one-row public activity probes after the unseen queue is empty."""

    if any(not item.get("last_analysis_at") for item in pool.values()):
        return
    for wallet, item in pool.items():
        try:
            rows = fetch(
                "trades",
                {
                    "user": wallet,
                    "takerOnly": "false",
                    "limit": 1,
                    "offset": 0,
                    "start": 1,
                },
            )
            latest = max(
                (
                    int(number(row.get("timestamp")))
                    for row in rows
                    if row.get("timestamp")
                ),
                default=0,
            )
            item["latest_source_trade_timestamp"] = latest
            item["last_activity_probe_at"] = observed_at
            item.pop("activity_probe_error", None)
        except Exception as error:
            item["activity_probe_error"] = (
                f"{type(error).__name__}: {error}"
            )


def candidate_pool_rows(
    pool: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for wallet in sorted(pool):
        item = pool[wallet]
        rows.append(
            {
                "wallet": wallet,
                "name": item.get("name") or wallet,
                "profile_url": profile_url(wallet),
                "legacy_seed": bool(item.get("legacy_seed")),
                "leaderboard_discovered": bool(
                    item.get("leaderboard_discovered")
                ),
                "origins": item.get("origins", []),
                "first_seen": item.get("first_seen"),
                "last_seen": item.get("last_seen"),
                "last_analysis_at": item.get("last_analysis_at"),
                "analysis_status": item.get("analysis_status")
                or "PENDING",
                "analysis_error": item.get("analysis_error"),
                "activity_probe_error": item.get(
                    "activity_probe_error"
                ),
                "latest_source_trade_timestamp": item.get(
                    "latest_source_trade_timestamp"
                ),
                "last_analyzed_source_trade_timestamp": item.get(
                    "last_analyzed_source_trade_timestamp"
                ),
                "expert_sleeve": item.get("expert_sleeve"),
            }
        )
    return rows


def build_snapshot(
    *,
    generated_at: str,
    pool: dict[str, dict[str, Any]],
    discovery_coverage: dict[str, Any],
    wallet_results: list[dict[str, Any]],
    selected: list[str],
    deferred: list[str],
    failures: list[dict[str, str]],
    categories: tuple[str, ...],
    periods: tuple[str, ...],
    discover_only: bool,
    operational_cap: int | None,
) -> dict[str, Any]:
    leaderboard_candidates = sum(
        bool(item.get("leaderboard_discovered"))
        for item in pool.values()
    )
    legacy_candidates = sum(
        bool(item.get("legacy_seed")) for item in pool.values()
    )
    return {
        "generated_at": generated_at,
        "paper_only": True,
        "real_order_submitted": False,
        "scope": {
            "dynamic_universe": True,
            "requested_categories": list(categories),
            "requested_periods": list(periods),
            "leaderboard_order": "PNL",
            "candidate_pool_size": len(pool),
            "leaderboard_discovered_candidates": leaderboard_candidates,
            "legacy_seed_candidates": legacy_candidates,
            "selected_for_analysis_this_run": len(selected),
            "successfully_analyzed_this_run": len(wallet_results),
            "failed_this_run": len(failures),
            "deferred_candidates": len(deferred),
            "discover_only": discover_only,
            "operational_wallet_cap": (
                None
                if operational_cap is None
                else {
                    "value": operational_cap,
                    "provenance": (
                        "Invocation-specified operational estimate; "
                        "it only defers work and never qualifies a wallet."
                    ),
                }
            ),
        },
        "discovery_coverage": discovery_coverage,
        "numeric_provenance": {
            "external_constraints": (
                "Leaderboard page size/offset and trade page/window "
                "pagination follow the official public Data API."
            ),
            "user_approved_rules": (
                "MONTH plus ALL defaults, crypto-row removal, mixed-wallet "
                "non-crypto sleeve preservation, and legacy-seed demotion."
            ),
            "empirical": (
                "Leaderboard rows, public fills, positions, timestamps, "
                "reported realizedPnl, lifecycle counts, and coverage."
            ),
            "formula_derived": (
                "Address deduplication, event/domain aggregation, cost "
                "estimates, concentrations, and lifecycle grouping."
            ),
            "estimate": (
                "Title/eventSlug domain labels and any invocation wallet cap; "
                "neither can promote or reject a wallet by itself."
            ),
        },
        "limitations": [
            "BUY/SELL does not identify maker/taker role.",
            "Source-wallet PnL does not establish delayed follower profitability.",
            "COPYABLE_EVIDENCE requires separate forward paper execution evidence.",
            "Unknown or incomplete history remains reviewable and cannot be promoted.",
        ],
        "candidate_pool": candidate_pool_rows(pool),
        "selected_wallets": selected,
        "deferred_wallets": deferred,
        "failures": failures,
        "wallets": wallet_results,
    }


def markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace(
        "|", "\\|"
    ).replace("\n", " ")


def origin_label(origins: list[dict[str, Any]]) -> str:
    labels = []
    for origin in origins:
        if origin.get("source") == "leaderboard":
            rank = origin.get("rank")
            rank_label = f" rank={rank}" if rank is not None else ""
            labels.append(
                f"{origin.get('category')}/{origin.get('period')}"
                f"{rank_label}"
            )
        elif origin.get("source") == "legacy_seed":
            labels.append(f"legacy:{origin.get('name')}")
    return "; ".join(labels)


def render_markdown(
    snapshot: dict[str, Any],
    pool: dict[str, dict[str, Any]],
) -> str:
    scope = snapshot["scope"]
    coverage = snapshot["discovery_coverage"]
    lines = [
        "# Polymarket 动态钱包发现",
        "",
        f"- 生成时间：{snapshot['generated_at']}",
        "- 数据边界：公开只读；没有真实订单，也没有修改纸面账本。",
        f"- 候选池：{scope['candidate_pool_size']}",
        (
            "- 排行榜覆盖："
            + ("完整" if coverage.get("complete") else "不完整，详见覆盖记录")
        ),
        f"- 本轮成功分析：{scope['successfully_analyzed_this_run']}",
        f"- 本轮延期：{scope['deferred_candidates']}",
        "",
        "## 候选池",
        "",
        "| 名称 | 钱包网页 | 来源 | 策略状态 | 专家部分 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for wallet in sorted(pool):
        item = pool[wallet]
        name = markdown_cell(item.get("name") or wallet)
        url = profile_url(wallet)
        origins = markdown_cell(origin_label(item.get("origins", [])))
        status = markdown_cell(item.get("analysis_status") or "PENDING")
        sleeve = markdown_cell(item.get("expert_sleeve") or "")
        lines.append(
            f"| {name} | [{wallet}]({url}) | {origins} | "
            f"{status} | {sleeve} |"
        )

    if snapshot["wallets"]:
        lines.extend(
            [
                "",
                "## 本轮策略分析",
                "",
                "| 名称 | 钱包网页 | 非加密策略状态 | 生命周期 | 剔除加密记录 |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for result in snapshot["wallets"]:
            strategy = result["strategy"]
            lifecycle = ", ".join(
                f"{key}={value}"
                for key, value in strategy.get(
                    "lifecycle_counts", {}
                ).items()
            )
            removed = sum(result["crypto_rows_removed"].values())
            lines.append(
                f"| {markdown_cell(result['name'])} | "
                f"[{result['wallet']}]({result['profile_url']}) | "
                f"{strategy['strategy_state']} | "
                f"{markdown_cell(lifecycle)} | {removed} |"
            )

    failed_pairs = [
        pair
        for pair in coverage.get("pairs", [])
        if not pair.get("complete")
    ]
    if failed_pairs:
        lines.extend(["", "## 覆盖缺口", ""])
        for pair in failed_pairs:
            detail = pair.get("error") or "reached documented offset boundary"
            lines.append(
                f"- {pair.get('category')}/{pair.get('period')}: "
                f"{markdown_cell(detail)}"
            )
    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "- 源钱包历史只能产生研究候选，不能单独证明延迟跟单为正期望。",
            "- 中途买卖、篮子/对冲或同秒双向行为保留为公式研究或执行风险。",
            "- 只有单独的前瞻纸面成交证据才能升级复制结论。",
            "",
        ]
    )
    return "\n".join(lines)


def nonnegative_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover and analyze public Polymarket wallets."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=LEADERBOARD_CATEGORIES,
        default=list(LEADERBOARD_CATEGORIES),
    )
    parser.add_argument(
        "--periods",
        nargs="+",
        choices=LEADERBOARD_ALLOWED_PERIODS,
        default=list(LEADERBOARD_PERIODS),
    )
    parser.add_argument("--max-wallets", type=nonnegative_integer)
    parser.add_argument("--discover-only", action="store_true")
    return parser


def main(
    argv: list[str] | None = None,
    *,
    fetch: Any = api_get,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    generated_at = datetime.now(timezone.utc).isoformat()
    state_path = args.state or args.output.with_name(
        "candidate_discovery_state.json"
    )
    report_path = args.report or args.output.with_suffix(".md")
    state = load_candidate_state(state_path)
    discovered, discovery_coverage = discover_leaderboard_candidates(
        fetch,
        categories=tuple(args.categories),
        periods=tuple(args.periods),
        observed_at=generated_at,
    )
    pool = merge_candidate_pool(
        state["candidates"],
        discovered,
        LEGACY_SEEDS,
        observed_at=generated_at,
    )
    state_payload = {
        "schema_version": 1,
        "updated_at": generated_at,
        "candidates": pool,
    }
    write_json_atomic(state_path, state_payload)

    selected: list[str] = []
    deferred: list[str] = []
    wallet_results: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    if not args.discover_only:
        refresh_analyzed_candidate_activity(
            pool,
            fetch=fetch,
            observed_at=generated_at,
        )
        queue = ordered_candidate_wallets(pool)
        if args.max_wallets is None:
            selected = queue
        else:
            selected = queue[: args.max_wallets]
        deferred = queue[len(selected) :]

        for wallet in selected:
            item = pool[wallet]
            try:
                result = scan_wallet(
                    str(item.get("name") or wallet),
                    wallet,
                    fetch=fetch,
                )
                wallet_results.append(result)
                item["last_analysis_at"] = generated_at
                item["latest_source_trade_timestamp"] = result[
                    "latest_source_trade_timestamp"
                ]
                item["last_analyzed_source_trade_timestamp"] = result[
                    "latest_source_trade_timestamp"
                ]
                item["latest_noncrypto_trade_timestamp"] = result[
                    "latest_noncrypto_trade_timestamp"
                ]
                item["analysis_status"] = result["strategy"][
                    "strategy_state"
                ]
                domains = result["closed_positions"].get("domains", [])
                item["expert_sleeve"] = (
                    domains[0]["domain"] if domains else None
                )
                item.pop("analysis_error", None)
            except Exception as error:
                message = f"{type(error).__name__}: {error}"
                item["analysis_error"] = message
                failures.append({"wallet": wallet, "error": message})
            state_payload = {
                "schema_version": 1,
                "updated_at": generated_at,
                "candidates": pool,
            }
            write_json_atomic(state_path, state_payload)

    snapshot = build_snapshot(
        generated_at=generated_at,
        pool=pool,
        discovery_coverage=discovery_coverage,
        wallet_results=wallet_results,
        selected=selected,
        deferred=deferred,
        failures=failures,
        categories=tuple(args.categories),
        periods=tuple(args.periods),
        discover_only=args.discover_only,
        operational_cap=args.max_wallets,
    )
    write_json_atomic(args.output, snapshot)
    write_text_atomic(report_path, render_markdown(snapshot, pool))
    print(args.output)
    print(report_path)
    print(state_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
