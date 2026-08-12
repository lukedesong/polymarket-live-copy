"""Terminal verdicts and reproducible evidence artifacts for tennis EV research.

The report consumes an already frozen run.  It does not discover conditions,
choose a strategy, fetch data, or submit an order.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import matplotlib
matplotlib.use("Agg")
from matplotlib import pyplot as plt
import numpy as np

from . import statistics


VERDICTS = frozenset({"VERIFIED_POSITIVE_EV", "NO_SIGNIFICANT_EDGE", "BLOCK_EXECUTION_DATA", "BLOCK_DATA"})


@dataclass(frozen=True)
class ResearchRun:
    """Logical input boundary shared by the future CLI and artifact writer."""
    coverage: Mapping[str, object]
    test_events: Sequence[object] = ()
    holdout_results: Sequence[object] = ()
    frozen_manifest: Mapping[str, object] = field(default_factory=dict)
    exclusions: Sequence[object] = ()
    matches: Sequence[object] = ()
    snapshots: Sequence[object] = ()
    trades: Sequence[object] = ()
    states: Sequence[object] = ()
    favorite_results: Sequence[Mapping[str, object]] = ()
    favorite_calibration: Sequence[Mapping[str, object]] = ()
    random_baseline: Mapping[str, object] = field(default_factory=dict)
    training_conditions: Sequence[Mapping[str, object]] = ()
    hedge_results: Sequence[Mapping[str, object]] = ()
    bankroll_results: Mapping[str, object] = field(default_factory=dict)
    monte_carlo_summary: Mapping[str, object] = field(default_factory=dict)
    # None means a legacy caller supplied no frozen ledger.  An empty sequence
    # is an explicit frozen result: the selected rule matched no holdout rows.
    holdout_ledger: Sequence[Mapping[str, object]] | None = None


@dataclass(frozen=True)
class ReportArtifacts:
    output_dir: Path
    event_ledger: Path
    result_json: Path
    report_markdown: Path
    artifact_manifest: Path


def decide_verdict(*, usable_test_matches: int, significant_positive_rules: int,
                   executable_book_matches: int, net_after_verified_costs: bool,
                   corrected_significance_passed: bool = False,
                   net_ev_interval_lower_positive: bool = False,
                   concentration_check_passed: bool = False,
                   execution_coverage_complete: bool = False) -> str:
    """Return only an allowed terminal state, conservatively.

    A verified claim needs every named criterion.  A proxy signal without full
    executable coverage is deliberately an execution-data block, not a trade.
    """
    if usable_test_matches <= 0:
        return "BLOCK_DATA"
    if significant_positive_rules <= 0:
        return "NO_SIGNIFICANT_EDGE"
    if not (executable_book_matches >= usable_test_matches and execution_coverage_complete):
        return "BLOCK_EXECUTION_DATA"
    if (net_after_verified_costs and corrected_significance_passed
            and net_ev_interval_lower_positive and concentration_check_passed):
        return "VERIFIED_POSITIVE_EV"
    return "BLOCK_EXECUTION_DATA"


def verdict_text(verdict: str) -> str:
    texts = {
        "VERIFIED_POSITIVE_EV": "冻结测试集在已捕获成本后满足预先定义的正EV验证条件。",
        "NO_SIGNIFICANT_EDGE": "没有发现统计显著优势。",
        "BLOCK_EXECUTION_DATA": "存在或可能存在统计信号，但历史手续费、滑点、盘口或流动性证据不足，不能证明可执行正EV。",
        "BLOCK_DATA": "关键数据或可用测试样本不足，无法作出统计结论。",
    }
    if verdict not in texts:
        raise ValueError(f"unknown verdict: {verdict}")
    return texts[verdict]


def _plain(value: object) -> object:
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _json(path: Path, payload: object) -> None:
    _atomic_bytes(path, (json.dumps(_plain(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode())


def _rows(rows: Sequence[object]) -> list[dict[str, object]]:
    return [dict(_plain(item)) for item in rows]  # type: ignore[arg-type]


def _csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    normalized = []
    for row in rows:
        item = {str(key): _plain(value) for key, value in row.items()}
        # The fixed fields make every emitted row self-describing even when a
        # source adapter only supplied raw observations.
        item.setdefault("object", path.stem)
        item.setdefault("unit", "record")
        item.setdefault("sample_count", 1)
        item.setdefault("time_interval", item.get("finish_ts", item.get("observed_at", "UNKNOWN")))
        item.setdefault("data_source", "ResearchRun")
        item.setdefault("numeric_provenance", "OBSERVED_VALUE")
        normalized.append(item)
    fields = sorted({key for row in normalized for key in row})
    if not fields:
        fields = ["object", "unit", "sample_count", "time_interval", "data_source", "numeric_provenance"]
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in normalized:
                writer.writerow({key: _cell(row.get(key)) for key in fields})
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _cell(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _event_ledger(run: ResearchRun) -> list[dict[str, object]]:
    if run.holdout_ledger is not None:
        return [dict(_plain(row)) for row in run.holdout_ledger]
    # Fallback is a transparent favorite baseline ledger, not an inferred
    # frozen-rule fill.  The CLI supplies holdout_ledger when a rule is frozen.
    groups: dict[tuple[object, object], list[object]] = {}
    for event in run.test_events:
        groups.setdefault((getattr(event, "event_id"), getattr(event, "decision_ts")), []).append(event)
    ledger: list[dict[str, object]] = []
    for (_, _), group in sorted(groups.items(), key=lambda item: (item[0][1], str(item[0][0]))):
        selected = max(group, key=lambda event: (getattr(event, "current_price"), str(getattr(event, "token_id"))))
        price = float(getattr(selected, "current_price"))
        won = bool(getattr(selected, "won"))
        ledger.append({
            "event_id": getattr(selected, "event_id"), "token_id": getattr(selected, "token_id"),
            "decision_ts": getattr(selected, "decision_ts"), "finish_ts": getattr(selected, "finish_ts"),
            "entry_price": price, "deployed_cost": price, "net_pnl": 1.0 - price if won else -price,
            "won": won, "selected": "true", "object": "holdout_selected_event", "unit": "one_share",
            "data_source": "ResearchRun.test_events", "numeric_provenance": "FORMULA_DERIVED_VALUE",
        })
    return ledger


def _condition_rows(results: Sequence[object]) -> list[dict[str, object]]:
    rows = _rows(results)
    for row in rows:
        row.update({"object": "frozen_holdout_condition", "unit": "independent_match",
                    "sample_count": row.get("matched_matches", 0), "data_source": "frozen_holdout",
                    "numeric_provenance": "EMPIRICAL_VALUE"})
    return rows


def _significance_alpha(manifest: Mapping[str, object]) -> float | None:
    """Read the frozen training threshold; never silently substitute one."""
    try:
        alpha = float(manifest["significance_alpha"])
    except (KeyError, TypeError, ValueError):
        return None
    return alpha if 0.0 < alpha <= 1.0 else None


def _significant(results: Sequence[object], *, alpha: float | None) -> list[Mapping[str, object]]:
    if alpha is None:
        return []
    output = []
    for item in _rows(results):
        if item.get("q_value") is not None and float(item["q_value"]) <= alpha and float(item.get("net_pnl", 0)) > 0:
            output.append(item)
    return output


def _display_rule_id(manifest: Mapping[str, object]) -> str | None:
    """Return the predeclared display rule, never a post-hoc holdout winner."""
    value = manifest.get("display_rule_id")
    if isinstance(value, str) and value:
        return value
    # Read old single-rule manifests deterministically; ambiguous manifests
    # cannot support a verdict without an explicit displayed rule.
    selected = manifest.get("selected_rule_ids")
    if isinstance(selected, Sequence) and not isinstance(selected, (str, bytes)) and len(selected) == 1:
        return str(selected[0])
    return None


def _display_result(
    results: Sequence[object], *, display_rule_id: str | None,
) -> Mapping[str, object] | None:
    if display_rule_id is None:
        # Legacy bundles that predate display_rule_id have no ambiguous rank
        # only when they contain exactly one frozen result.
        rows = _rows(results)
        return rows[0] if len(rows) == 1 else None
    for row in _rows(results):
        if str(row.get("rule_id")) == display_rule_id:
            return row
    return None


def _row_execution_evidence_verified(row: Mapping[str, object]) -> bool:
    """A global coverage count cannot prove an individual selected fill."""
    explicit_book = all(row.get(key) is not None for key in ("best_bid", "best_ask")) and any(
        row.get(key) is not None for key in ("visible_depth_usd", "depth", "visible_depth")
    )
    executable = bool(row.get("validated_executable_row", False)) or explicit_book
    cost_provenance = bool(row.get("cost_source")) and bool(row.get("cost_fidelity"))
    return executable and cost_provenance


def _write_charts(output_dir: Path, ledger: Sequence[Mapping[str, object]], run: ResearchRun) -> None:
    ordered = sorted(ledger, key=lambda row: (int(row.get("finish_ts", 0)), str(row.get("event_id", ""))))
    pnl = np.asarray([float(row.get("net_pnl", 0.0)) for row in ordered], dtype=float)
    equity = np.concatenate(([0.0], np.cumsum(pnl)))
    drawdown = np.maximum.accumulate(equity) - equity
    def save(name: str) -> None:
        temporary = output_dir / f".{name}.tmp"
        plt.tight_layout(); plt.savefig(temporary, format="png", dpi=120); plt.close()
        os.replace(temporary, output_dir / name)
    plt.figure(figsize=(7, 4)); plt.plot(equity); plt.title("Holdout equity (historical reference-price proxy)"); plt.xlabel("Settlement order"); plt.ylabel("PnL per share"); save("equity_curve.png")
    plt.figure(figsize=(7, 4)); plt.plot(drawdown); plt.title("Holdout drawdown (settlement cashflow)"); plt.xlabel("Settlement order"); plt.ylabel("Drawdown"); save("drawdown_curve.png")
    samples = run.random_baseline.get("roi_samples") or run.monte_carlo_summary.get("roi_samples") or [float(row.get("net_pnl", 0.0)) for row in ledger]
    plt.figure(figsize=(7, 4)); plt.hist(np.asarray(samples, dtype=float), bins="auto"); plt.title("ROI distribution (proxy where costs unavailable)"); plt.xlabel("ROI"); save("roi_distribution.png")
    labels, values = ["frozen"], [float(np.sum(pnl))]
    if run.favorite_results:
        labels.append("favorite"); values.append(float(sum(float(row.get("metrics", {}).get("net_pnl", 0.0)) for row in run.favorite_results)))
    if run.hedge_results:
        labels.append("hedge"); values.append(float(np.mean([float(row.get("hedged_net_pnl", 0.0)) for row in run.hedge_results])))
    plt.figure(figsize=(7, 4)); plt.bar(labels, values); plt.title("Strategy comparison (not necessarily executable)"); plt.ylabel("Net PnL"); save("strategy_comparison.png")


def _markdown(verdict: str, run: ResearchRun, result: Mapping[str, object]) -> str:
    return "\n".join((
        "# Polymarket Tennis Positive-EV Research", "",
        f"## Terminal verdict: `{verdict}`", "", verdict_text(verdict), "",
        "## Answers", "",
        f"1. Long-run positive EV: {verdict_text(verdict)}",
        "2. Attribution: player fundamentals are `BLOCK_DATA`; any observed signal is a historical market-price/behavior proxy, not evidence of a player model or liquidity edge.",
        "3. Fees, slippage, and liquidity: historical Bid/Ask and depth coverage are incomplete unless the coverage manifest proves otherwise; proxy results do not establish an executable advantage.",
        f"4. Holdout selected-event ledger: net PnL {result['holdout']['net_pnl']}, selected events {result['holdout']['selected_events']}, test matches {result['holdout']['usable_test_matches']}.",
        "", "## Provenance", "",
        "All rows state their object, unit, sample count or denominator, time field, data source, and numeric provenance in the machine-readable artifacts.", "",
    ))


def _build_bundle(run: ResearchRun, output_dir: Path) -> None:
    """Write a complete bundle into an empty staging directory."""
    ledger = _event_ledger(run)
    selected = [row for row in ledger if str(row.get("selected", "false")).lower() == "true"]
    total = sum(float(row.get("net_pnl", row.get("pnl", 0.0))) for row in selected)
    # A rule may select zero events even though the frozen test population is
    # usable.  The denominator for BLOCK_DATA is the whole test coverage, not
    # the selected rank-one ledger.
    usable_test_matches = len({getattr(event, "event_id", None) for event in run.test_events})
    usable_test_matches -= int(None in {getattr(event, "event_id", None) for event in run.test_events})
    coverage_test_matches = run.coverage.get("research_decision_coverage", {})
    if isinstance(coverage_test_matches, Mapping):
        usable_test_matches = max(usable_test_matches, int(coverage_test_matches.get("test_matches", 0) or 0))
    alpha = _significance_alpha(run.frozen_manifest)
    display_rule_id = _display_rule_id(run.frozen_manifest)
    displayed_result = _display_result(run.holdout_results, display_rule_id=display_rule_id)
    significant = (
        [displayed_result] if displayed_result is not None and alpha is not None
        and displayed_result.get("q_value") is not None
        and float(displayed_result["q_value"]) <= alpha
        and float(displayed_result.get("net_pnl", 0.0)) > 0.0
        else []
    )
    coverage_books = int(run.coverage.get("execution_book_matches", 0) or 0)
    verified_rows = [row for row in selected if _row_execution_evidence_verified(row)]
    full_execution = bool(selected) and len(verified_rows) == len(selected)
    verified_costs = bool(run.frozen_manifest.get("verified_costs_captured", False)) and full_execution
    lower_positive = all(float(row.get("bootstrap_lower") or 0.0) > 0.0 for row in significant) and bool(significant)
    concentration = all(float(row.get("pnl_without_largest") or 0.0) > 0.0 for row in significant) and bool(significant)
    if alpha is None:
        verdict = "BLOCK_DATA"
    else:
        verdict = decide_verdict(usable_test_matches=usable_test_matches, significant_positive_rules=len(significant),
                                  executable_book_matches=len(verified_rows), net_after_verified_costs=verified_costs,
                                  corrected_significance_passed=bool(significant), net_ev_interval_lower_positive=lower_positive,
                                  concentration_check_passed=concentration, execution_coverage_complete=full_execution)
    result = {"verdict": verdict, "holdout": {"object": "frozen_holdout_selected_event_ledger", "unit": "one_share", "selected_events": len(selected), "usable_test_matches": usable_test_matches, "net_pnl": total, "data_source": "research_event_ledger.csv", "numeric_provenance": "FORMULA_DERIVED_VALUE"}, "criteria": {"significance_alpha": alpha, "display_rule_id": display_rule_id, "significant_positive_rules": len(significant), "global_execution_book_matches": coverage_books, "row_verified_execution_cost_matches": len(verified_rows), "verified_costs_captured": verified_costs, "execution_coverage_complete": full_execution}}
    _json(output_dir / "coverage_manifest.json", run.coverage)
    _csv(output_dir / "normalized_matches.csv", _rows(run.matches)); _csv(output_dir / "market_snapshots.csv", _rows(run.snapshots)); _csv(output_dir / "trades.csv", _rows(run.trades)); _csv(output_dir / "match_state.csv", _rows(run.states)); _csv(output_dir / "exclusions.csv", _rows(run.exclusions))
    _csv(output_dir / "research_event_ledger.csv", ledger); _json(output_dir / "frozen_strategy_manifest.json", run.frozen_manifest)
    _csv(output_dir / "favorite_baseline.csv", list(run.favorite_results)); _csv(output_dir / "favorite_calibration.csv", list(run.favorite_calibration)); _json(output_dir / "random_baseline.json", run.random_baseline)
    conditions = list(run.training_conditions); _csv(output_dir / "all_training_conditions.csv", conditions); _csv(output_dir / "top_50_training_conditions.csv", conditions[:50]); _csv(output_dir / "frozen_holdout_results.csv", _condition_rows(run.holdout_results)); _csv(output_dir / "hedge_results.csv", list(run.hedge_results)); _json(output_dir / "bankroll_results.json", run.bankroll_results); _json(output_dir / "monte_carlo_summary.json", run.monte_carlo_summary); _json(output_dir / "result.json", result)
    _atomic_bytes(output_dir / "report.md", _markdown(verdict, run, result).encode())
    _write_charts(output_dir, ledger, run)
    manifest = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(output_dir.iterdir()) if path.is_file() and path.name != "artifact_manifest.json"}
    _json(output_dir / "artifact_manifest.json", {"sha256": manifest})


def _publish_bundle(stage: Path, destination: Path) -> None:
    """Replace a complete bundle with rename rollback if publication fails."""
    backup = destination.parent / f".{destination.name}.backup-{next(tempfile._get_candidate_names())}"
    moved_old = False
    try:
        if destination.exists():
            os.replace(destination, backup)
            moved_old = True
        os.replace(stage, destination)
    except BaseException:
        if moved_old and not destination.exists() and backup.exists():
            os.replace(backup, destination)
        raise
    else:
        if moved_old:
            shutil.rmtree(backup)


def build_report(run: ResearchRun, output_dir: Path) -> ReportArtifacts:
    """Publish a whole report directory, never a mix of two report generations."""
    output_dir = Path(output_dir)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.stage-", dir=output_dir.parent))
    try:
        _build_bundle(run, stage)
        _publish_bundle(stage, output_dir)
    except BaseException:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return ReportArtifacts(output_dir, output_dir / "research_event_ledger.csv", output_dir / "result.json", output_dir / "report.md", output_dir / "artifact_manifest.json")
