"""Build a reproducible, local-only Polymarket tennis EV research bundle.

Historical path prices are reference-price proxies.  This command deliberately
does not fetch current books, infer missing match state, or submit orders.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from sim.tennis_ev import bankroll, data, report, research


DEFAULT_PATHS = Path("outputs/polymarket_tennis_exit_paths_primary.jsonl.gz")
DEFAULT_OUTPUT = Path("outputs/tennis_ev")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=Path, default=DEFAULT_PATHS,
                        help="historical JSON/JSONL/CSV path artifact (no network access)")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--train-fraction", type=float, default=0.70,
                        help="user-specified chronological training fraction")
    parser.add_argument("--alpha", type=float, default=0.05,
                        help="conventional research significance setting")
    parser.add_argument("--initial-cash", type=float, default=10_000.0,
                        help="user-specified simulation bankroll in USD")
    parser.add_argument("--simulation-paths", type=int, default=10_000,
                        help="user-specified Monte Carlo path count")
    parser.add_argument("--seed", type=int, default=20260812)
    parser.add_argument("--fee-rate", type=float, default=None,
                        help="optional externally evidenced fee formula rate")
    parser.add_argument("--fee-exponent", type=float, default=1.0)
    parser.add_argument("--fee-source", default=None,
                        help="required provenance when --fee-rate is supplied")
    return parser


def _fee_schedule(args: argparse.Namespace) -> research.FeeSchedule | None:
    if args.fee_rate is None:
        return None
    if not args.fee_source:
        raise ValueError("--fee-source is required with --fee-rate")
    return research.FeeSchedule(args.fee_rate, args.fee_exponent, args.fee_source)


def _events_for_matches(matches: Iterable[data.MatchRecord]) -> tuple[research.ResearchEvent, ...]:
    return research.build_decision_point_events(matches)[0]


def _first_frozen_ledger(
    manifest: research.FrozenManifest, events: Sequence[research.ResearchEvent], *,
    fee_schedule: research.FeeSchedule | None,
) -> list[dict[str, object]]:
    """Use only the first frozen rank as the single test-time strategy ledger."""
    if not manifest.ranking_order:
        return []
    raw = manifest.selected_rule_definitions[manifest.ranking_order[0]]
    condition = research._condition_from_dict(raw)  # frozen, serialized public manifest
    selected = [event for event, _ in research._condition_selection_groups(condition, tuple(events))]
    return [{
        "event_id": event.event_id, "market_id": event.market_id, "token_id": event.token_id,
        "decision_ts": event.decision_ts, "finish_ts": event.finish_ts,
        "rule_id": manifest.ranking_order[0], "entry_price": event.current_price,
        "fee_per_share": research._fee_per_share(fee_schedule, event.current_price),
        "deployed_cost": event.current_price + research._fee_per_share(fee_schedule, event.current_price),
        "net_pnl": ((1.0 - event.current_price - research._fee_per_share(fee_schedule, event.current_price))
                    if event.won else -(event.current_price + research._fee_per_share(fee_schedule, event.current_price))),
        "won": event.won, "selected": "true", "decision_family": event.decision_family,
        "fee_rate": fee_schedule.rate if fee_schedule else None,
        "fee_exponent": fee_schedule.exponent if fee_schedule else None,
        "fee_source": fee_schedule.source if fee_schedule else None,
        "economic_result_basis": ("GROSS_REFERENCE_PROXY" if fee_schedule is None
                                  else "REFERENCE_PROXY_NET_OF_SUPPLIED_FEE_SCHEDULE"),
        "execution_cost_block": ("BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY" if fee_schedule is None
                                 else "BLOCK_DATA_SLIPPAGE_LIQUIDITY"),
        "object": "frozen_holdout_selected_event", "unit": "one_share",
        "numeric_provenance": "FORMULA_DERIVED_VALUE",
    } for event in selected]


def _hedges(events: Sequence[research.ResearchEvent]) -> list[dict[str, object]]:
    pairs = research._binary_decision_groups(
        event for event in events if event.decision_family == "PRE_MATCH_REFERENCE"
    )
    observations = [bankroll.HedgeObservation(
        event_id=left.event_id, decision_ts=left.decision_ts,
        high_cost=left.current_price, low_cost=right.current_price, high_won=left.won,
        high_reference_price=left.current_price, low_reference_price=right.current_price,
    ) for left, right in pairs if left.current_price > right.current_price]
    return [
        {**bankroll.compare_hedge(observations, high_weight=float(item["high_weight"])),
         "label": item["label"], "numeric_provenance": item["numeric_provenance"],
         "execution_cost_block": "BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY"}
        for item in bankroll.hedge_scenarios()
    ]


def _bankroll_analyses(
    ledger: Sequence[dict[str, object]], manifest: research.FrozenManifest, args: argparse.Namespace,
) -> tuple[dict[str, object], dict[str, object]]:
    trades = tuple(bankroll.Trade(
        event_id=str(row["event_id"]), entry_ts=int(row["decision_ts"]),
        settle_ts=int(row["finish_ts"]),
        pnl_rate=float(row["net_pnl"]) / float(row["deployed_cost"]),
    ) for row in ledger)
    fixed: dict[str, object] = {}
    for fraction in bankroll.FIXED_FRACTIONS:
        entries = bankroll.run_fixed_fraction(trades, initial_cash=args.initial_cash, fraction=fraction)
        final = args.initial_cash + sum(entry.settlement_pnl for entry in entries if entry.accepted)
        fixed[f"fixed_{int(fraction * 100)}pct"] = {
            "fraction": fraction, "numeric_provenance": "USER_SPECIFIED_VALUE",
            "ledger": [asdict(entry) for entry in entries], "final_equity_usd": final,
            "annualized": bankroll.annualized_return(
                final_equity=final, initial_cash=args.initial_cash, covered_days=None,
                capital_coverage_complete=False,
            ),
        }
    if trades:
        wins = sum(float(row["won"]) for row in ledger) / len(ledger)
        average_cost = sum(float(row["deployed_cost"]) for row in ledger) / len(ledger)
    else:
        wins, average_cost = 0.0, 0.5
    training_lower = 0.0
    if manifest.ranking_order:
        by_id = {str(row["rule_id"]): row for row in manifest.condition_ledger}
        training_lower = float(by_id[manifest.ranking_order[0]].get("bootstrap_lower") or 0.0)
    kelly = bankroll.kelly_scenarios(wins, average_cost, training_lower)
    fixed["kelly"] = {**kelly, "execution_status": "BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY"}
    blocks: dict[str, list[bankroll.Trade]] = {}
    for trade in trades:
        key = datetime.fromtimestamp(trade.entry_ts, timezone.utc).date().isoformat()
        blocks.setdefault(key, []).append(trade)
    simulation = bankroll.monte_carlo(
        blocks, paths=args.simulation_paths, initial_cash=args.initial_cash,
        fraction=bankroll.FIXED_FRACTIONS[0], seed=args.seed,
    )
    summary = bankroll.summarize_paths(simulation.equity_paths)
    summary.update({
        "initial_cash_usd": args.initial_cash, "initial_cash_provenance": "USER_SPECIFIED_VALUE",
        "fraction": bankroll.FIXED_FRACTIONS[0], "fraction_provenance": "USER_SPECIFIED_VALUE",
        "sampled_unit": simulation.sampled_unit,
        "execution_status": "BLOCK_DATA_FEE_SLIPPAGE_LIQUIDITY",
    })
    return fixed, summary


def run(args: argparse.Namespace) -> report.ReportArtifacts:
    """Run the chronological research pipeline without external side effects."""
    schedule = _fee_schedule(args)
    matches, snapshots, trades, states, exclusions, coverage = data.load_historical_matches(args.paths)
    split = research.chronological_split(matches, train_fraction=args.train_fraction)
    train_events = _events_for_matches(split.train)
    test_events = _events_for_matches(split.test)
    manifest = research.freeze_training_manifest(
        train_events, alpha=args.alpha, split_cutoff_ts=split.boundary_ts, fee_schedule=schedule,
    )
    holdout = research.evaluate_holdout(manifest, test_events, fee_schedule=schedule)
    frozen_manifest = manifest.to_dict()
    frozen_manifest.update({
        "split": {"boundary_ts": split.boundary_ts, "train_matches": len(split.train),
                  "test_matches": len(split.test), "purged_matches": len(split.purged),
                  "achieved_train_fraction": split.achieved_train_fraction,
                  "requested_train_fraction": args.train_fraction,
                  "numeric_provenance": "USER_SPECIFIED_VALUE"},
        "verified_costs_captured": False,
        "fee_input": (None if schedule is None else {"rate": schedule.rate, "exponent": schedule.exponent,
                                                       "source": schedule.source}),
        "display_rule_id": manifest.ranking_order[0] if manifest.ranking_order else None,
    })
    coverage = {**coverage, "research_decision_coverage": {
        "train_events": len(train_events), "test_events": len(test_events),
        "train_matches": len(split.train), "test_matches": len(split.test),
        "purged_matches": len(split.purged),
    }}
    favorite_results = research.favorite_baseline(test_events, fee_schedule=schedule)
    random = research.random_baseline(test_events, draws=args.simulation_paths, seed=args.seed, fee_schedule=schedule)
    random["roi_samples"] = random["roi_distribution"]
    ledger = _first_frozen_ledger(manifest, test_events, fee_schedule=schedule)
    bankroll_results, monte_carlo_summary = _bankroll_analyses(ledger, manifest, args)
    return report.build_report(report.ResearchRun(
        coverage=coverage, test_events=test_events, holdout_results=holdout,
        frozen_manifest=frozen_manifest, exclusions=exclusions, matches=matches,
        snapshots=snapshots, trades=trades, states=states, favorite_results=favorite_results,
        favorite_calibration=research.favorite_baseline(train_events, fee_schedule=schedule),
        random_baseline=random, training_conditions=manifest.condition_ledger,
        hedge_results=_hedges(test_events), bankroll_results=bankroll_results,
        monte_carlo_summary=monte_carlo_summary, holdout_ledger=ledger,
    ), args.output_dir)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
