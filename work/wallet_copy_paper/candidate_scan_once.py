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

WALLETS = {
    "ratehikes": "0xa309f903dbbd559e87d8d368834b8e41355c4cf2",
    "LlamaLoco0000": "0x93fb8127a1b21a112b11af936361225df0563e4a",
    "tetrose": "0x74471a007ddcc488f6d57b5e86dfb35a8d48a16d",
    "Themsnw": "0x69c5c6f6a4dda665991c203bc8d5df3e006b1e01",
    "Adrink3D": "0x54e15a0a77e3147ac78831804e5dde13cb589d5a",
    "russell110320": "0x118689b24aead1d6e9507b8068d056b2ec4f051b",
    "ZorroDeLaVega": "0xaae9b2c5ad90e82b5068c7f8a4b491997633d661",
    "sabsabinxz": "0xd3ecb2aee0d65622da559ff356b00e8c2e626603",
}


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


def fetch_trades(wallet: str) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    page_sizes: list[int] = []
    for offset in (0, 10_000):
        page = api_get(
            "trades",
            {
                "user": wallet,
                "takerOnly": "false",
                "limit": 10_000,
                "offset": offset,
            },
        )
        rows.extend(page)
        page_sizes.append(len(page))
        if len(page) < 10_000:
            break
    truncated = page_sizes == [10_000, 10_000]
    return rows, truncated


def fetch_closed_positions(wallet: str) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    complete = False
    for offset in range(0, 100_001, 50):
        page = api_get(
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


def fetch_open_positions(wallet: str) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    complete = False
    for offset in range(0, 10_001, 500):
        page = api_get(
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


def scan_wallet(name: str, wallet: str) -> dict[str, Any]:
    trades, trades_truncated = fetch_trades(wallet)
    closed_rows, closed_complete = fetch_closed_positions(wallet)
    open_rows, open_complete = fetch_open_positions(wallet)
    events = aggregate_events(closed_rows)
    return {
        "name": name,
        "wallet": wallet,
        "sources": {
            "trades": (
                f"{DATA_API}/trades?user={wallet}&takerOnly=false"
            ),
            "closed_positions": (
                f"{DATA_API}/closed-positions?user={wallet}"
            ),
            "open_positions": f"{DATA_API}/positions?user={wallet}",
        },
        "trades": trade_summary(trades, trades_truncated),
        "closed_positions": {
            "pagination_complete": closed_complete,
            "position_rows": len(closed_rows),
            "event_level": period_summary(events),
            "domains": domain_summaries(events),
        },
        "open_positions": open_position_summary(open_rows, open_complete),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generated_at = datetime.now(timezone.utc).isoformat()
    result = {
        "generated_at": generated_at,
        "paper_only": True,
        "real_order_submitted": False,
        "scope": {
            "wallet_count": len(WALLETS),
            "wallets": WALLETS,
            "candidate_origin": (
                "Five wallets found in the prior public-wallet exploration plus "
                "the three currently observed by the paper-only tracker."
            ),
            "not_an_exhaustive_universe_scan": True,
        },
        "numeric_provenance": {
            "empirical": (
                "Raw API rows, timestamps, transaction hashes, conditions, "
                "positions and Data API reported realizedPnl."
            ),
            "formula_derived": (
                "Shares, estimated cost=avgPrice*totalBought, PnL/cost, "
                "event aggregation, concentrations and chronological median split."
            ),
            "estimate": (
                "Domain labels are transparent title/eventSlug keyword heuristics; "
                "they are not authoritative Gamma tag classifications."
            ),
            "external_constraints": (
                "Data API trades limit and offset are each capped at 10,000; "
                "closed-position page size is capped at 50."
            ),
        },
        "limitations": [
            "BUY/SELL does not identify maker/taker role.",
            "Data API reported realizedPnl fee inclusion was not independently reconstructed.",
            "The event-median split is exploratory and is not a detected formula-change boundary.",
            "Historical source-wallet results do not establish delayed follower profitability.",
            "A full Gamma tag join and historical order-book reconstruction are not included.",
        ],
        "wallets": [],
    }
    for name, wallet in WALLETS.items():
        result["wallets"].append(scan_wallet(name, wallet))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
