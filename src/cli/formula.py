"""CLI interface for KBO Sabermetrics Formula Registry and Reproducibility Audits."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from src.formulas.constants import LeagueConstantsEngine
from src.formulas.engine import FormulaEngine
from src.formulas.models import MetricCategory
from src.formulas.registry import FormulaRegistry
from src.formulas.reporter import FormulaReporter

if TYPE_CHECKING:
    from collections.abc import Sequence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the formula CLI."""
    parser = argparse.ArgumentParser(
        prog="python -m src.cli.formula",
        description="Inspect KBO Sabermetric formulas, linear weights, and reproducibility audits.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # 1. List
    p_list = subparsers.add_parser("list", help="List all registered sabermetric and classical metrics.")
    p_list.add_argument(
        "--category",
        "-c",
        choices=["BATTING", "PITCHING", "FIELDING", "BASERUNNING", "COMPOSITE"],
        default=None,
        help="Filter metrics by category.",
    )
    p_list.add_argument("--json", action="store_true", help="Output catalog as JSON.")

    # 2. Explain
    p_exp = subparsers.add_parser("explain", help="Display mathematical definition, LaTeX, and constants for a metric.")
    p_exp.add_argument("metric", type=str, help="Metric identifier (e.g. wOBA, OPS_PLUS, FIP, ERA, BABIP_BAT).")
    p_exp.add_argument("--season", "-s", type=int, default=2024, help="Season year for calibrated constants.")
    p_exp.add_argument("--json", action="store_true", help="Output specification as JSON.")

    # 3. Eval
    p_eval = subparsers.add_parser("eval", help="Evaluate formula on real player season data and check parity.")
    p_eval.add_argument("metric", type=str, help="Metric identifier to evaluate.")
    p_eval.add_argument("--player", "-p", type=str, required=True, help="Player name (e.g. '김도영') or player ID.")
    p_eval.add_argument("--season", "-s", type=int, default=2024, help="Season year (default: 2024).")
    p_eval.add_argument("--json", action="store_true", help="Output evaluation trace as JSON.")

    # 4. Audit
    p_aud = subparsers.add_parser("audit", help="Run reproducibility certification audit against database records.")
    p_aud.add_argument("--season", "-s", type=int, default=None, help="Target season year to audit.")
    p_aud.add_argument(
        "--category",
        "-c",
        choices=["BATTING", "PITCHING", "FIELDING", "BASERUNNING", "COMPOSITE"],
        default=None,
        help="Target metric category to audit.",
    )
    p_aud.add_argument("--sample", type=int, default=None, help="Limit number of player records evaluated.")
    p_aud.add_argument(
        "--save-artifact",
        type=str,
        default=None,
        help="Path to save audit report JSON artifact (e.g. data/certification/formula_audit_report.json).",
    )
    p_aud.add_argument("--json", action="store_true", help="Output audit report as JSON.")

    return parser.parse_args(argv)


def _handle_list(args: argparse.Namespace) -> int:
    """Handle list sub-action."""
    cat = MetricCategory[args.category] if args.category else None
    metrics = FormulaRegistry.list_all(category=cat)

    if args.json:
        payload = [m.to_dict() for m in metrics]
        sys.stdout.write(FormulaReporter.render_json(payload) + "\n")
    else:
        sys.stdout.write(FormulaReporter.render_catalog(metrics) + "\n")
    return 0


def _handle_explain(args: argparse.Namespace) -> int:
    """Handle explain sub-action."""
    metric_def = FormulaRegistry.get(args.metric)
    constants = LeagueConstantsEngine.get_baseline_constants(args.season)

    if args.json:
        payload = metric_def.to_dict()
        payload["calibrated_constants"] = constants
        sys.stdout.write(FormulaReporter.render_json(payload) + "\n")
    else:
        sys.stdout.write(FormulaReporter.render_explanation(metric_def, constants) + "\n")
    return 0


def _handle_eval(engine: FormulaEngine, args: argparse.Namespace) -> int:
    """Handle eval sub-action."""
    metric_def = FormulaRegistry.get(args.metric)
    result = engine.evaluate_player_metric(args.player, season=args.season, metric_id=args.metric)

    if args.json:
        sys.stdout.write(FormulaReporter.render_json(result) + "\n")
    else:
        sys.stdout.write(FormulaReporter.render_player_eval(result, metric_def) + "\n")
    return 0 if result.is_reproducible else 1


def _handle_audit(engine: FormulaEngine, args: argparse.Namespace) -> int:
    """Handle audit sub-action."""
    cat = MetricCategory[args.category] if args.category else None
    report = engine.audit_reproducibility(season=args.season, category=cat, sample=args.sample)

    json_str = FormulaReporter.render_json(report)

    if args.save_artifact:
        p = Path(args.save_artifact)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json_str, encoding="utf-8")
        sys.stdout.write(f"📁 Formula Reproducibility Audit report saved to {p}\n")

    if args.json:
        sys.stdout.write(json_str + "\n")
    else:
        sys.stdout.write(FormulaReporter.render_audit_report(report) + "\n")
    return 0 if report.is_compliant else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Execute formula CLI subcommand."""
    args = parse_args(argv)
    engine = FormulaEngine()

    try:
        if args.subcommand == "list":
            return _handle_list(args)
        if args.subcommand == "explain":
            return _handle_explain(args)
        if args.subcommand == "eval":
            return _handle_eval(engine, args)
        if args.subcommand == "audit":
            return _handle_audit(engine, args)
    except (
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        ZeroDivisionError,
        ArithmeticError,
        OSError,
        RuntimeError,
    ) as exc:
        logger.exception("Error in formula CLI")
        sys.stderr.write(f"❌ Error in formula CLI: {exc}\n")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
