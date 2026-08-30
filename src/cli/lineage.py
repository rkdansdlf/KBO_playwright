"""CLI interface for KBO Data Lineage & Provenance Tracking."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.utils.logger import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.lineage.engine import LineageEngine

logger = get_logger(__name__)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments for the lineage CLI."""
    parser = argparse.ArgumentParser(
        prog="python -m src.cli.lineage",
        description="Inspect KBO data provenance, transformation DAGs, and lineage integrity.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # 1. Game Lineage
    p_game = subparsers.add_parser("game", help="Trace provenance and source DAG for a specific game.")
    p_game.add_argument("game_id", type=str, help="Game ID identifier (e.g. 20210523LTOB0 or 20240401LGNC0).")
    p_game.add_argument(
        "--format",
        choices=["tree", "mermaid", "json"],
        default="tree",
        help="Output visualization format (default: tree).",
    )
    p_game.add_argument("--json", action="store_true", help="Shorthand for --format json.")

    # 2. Player Metric Lineage
    p_player = subparsers.add_parser("player", help="Trace derivation and contributing games for a player stat.")
    p_player.add_argument("player", type=str, help="Player name (e.g. '김도영') or player ID integer.")
    p_player.add_argument("--season", "-s", type=int, default=2024, help="Season year (default: 2024).")
    p_player.add_argument(
        "--metric",
        "-m",
        type=str,
        default="hits",
        help="Target metric to trace (e.g. hits, home_runs, avg, era, rbi).",
    )
    p_player.add_argument(
        "--format",
        choices=["tree", "mermaid", "json"],
        default="tree",
        help="Output visualization format (default: tree).",
    )
    p_player.add_argument("--json", action="store_true", help="Shorthand for --format json.")

    # 3. Lineage Audit
    p_audit = subparsers.add_parser("audit", help="Run system-wide or season-wide lineage completeness audit.")
    p_audit.add_argument("--season", "-s", type=int, default=None, help="Target season year to audit.")
    p_audit.add_argument(
        "--full",
        action="store_true",
        default=False,
        help="Execute exhaustive whole-population census audit across all 8 tables.",
    )
    p_audit.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Execute sampled audit for N entities (default: 500 when not in --full mode).",
    )
    p_audit.add_argument(
        "--save-artifact",
        type=str,
        default=None,
        help="Path to save audit report JSON artifact (e.g. data/certification/lineage_audit_report.json).",
    )
    p_audit.add_argument(
        "--format",
        choices=["tree", "json"],
        default="tree",
        help="Output visualization format (default: tree).",
    )
    p_audit.add_argument("--json", action="store_true", help="Shorthand for --format json.")

    return parser.parse_args(argv)


def _handle_game(engine: LineageEngine, args: argparse.Namespace) -> int:
    """Handle game lineage sub-action."""
    from src.lineage.reporter import LineageReporter

    report = engine.trace_game(args.game_id)
    fmt = "json" if getattr(args, "json", False) else args.format

    if fmt == "json":
        sys.stdout.write(LineageReporter.render_json(report) + "\n")
    elif fmt == "mermaid":
        sys.stdout.write(LineageReporter.render_mermaid(report.graph) + "\n")
    else:
        sys.stdout.write(LineageReporter.render_game_tree(report) + "\n")
    return 0


def _handle_player(engine: LineageEngine, args: argparse.Namespace) -> int:
    """Handle player lineage sub-action."""
    from src.lineage.reporter import LineageReporter

    report = engine.trace_player_metric(args.player, season=args.season, metric=args.metric)
    fmt = "json" if getattr(args, "json", False) else args.format

    if fmt == "json":
        sys.stdout.write(LineageReporter.render_json(report) + "\n")
    elif fmt == "mermaid":
        sys.stdout.write(LineageReporter.render_mermaid(report.graph) + "\n")
    else:
        sys.stdout.write(LineageReporter.render_player_tree(report) + "\n")
    return 0


def _handle_audit(engine: LineageEngine, args: argparse.Namespace) -> int:
    """Handle lineage audit sub-action."""
    from src.lineage.reporter import LineageReporter

    sample_val = args.sample if (args.sample is not None or not args.full) else None
    report = engine.audit_lineage(season=args.season, sample=sample_val, full=args.full)
    fmt = "json" if getattr(args, "json", False) else args.format

    json_content = LineageReporter.render_json(report)

    if args.save_artifact:
        art_path = Path(args.save_artifact)
        art_path.parent.mkdir(parents=True, exist_ok=True)
        art_path.write_text(json_content, encoding="utf-8")
        sys.stdout.write(f"📁 Lineage Audit report saved to {art_path}\n")

    if fmt == "json":
        sys.stdout.write(json_content + "\n")
    else:
        sys.stdout.write(LineageReporter.render_audit_tree(report) + "\n")
    return 0 if report.is_compliant else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Execute lineage CLI subcommand."""
    from src.lineage.engine import LineageEngine

    args = parse_args(argv)
    engine = LineageEngine()

    try:
        if args.subcommand == "game":
            return _handle_game(engine, args)
        if args.subcommand == "player":
            return _handle_player(engine, args)
        if args.subcommand == "audit":
            return _handle_audit(engine, args)
    except Exception as exc:
        logger.exception("Error during lineage tracing")
        sys.stderr.write(f"❌ Error during lineage tracing: {exc}\n")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
