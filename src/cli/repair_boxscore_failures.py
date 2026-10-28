"""CLI for automated repair and backfill of missing or inconsistent KBO games."""

from __future__ import annotations

# ruff: noqa: T201
import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING

from src.services.boxscore_repair_service import run_boxscore_repair

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser for boxscore repair."""
    parser = argparse.ArgumentParser(
        prog="repair_boxscore_failures",
        description="Auto-discover and repair missing or inconsistent KBO games.",
    )
    parser.add_argument(
        "--source",
        choices=["all", "integrity", "dlq", "log"],
        default="all",
        help="Source to discover repair candidates from (default: all).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target game date (YYYYMMDD or YYYY-MM-DD).",
    )
    parser.add_argument(
        "--game-id",
        type=str,
        default=None,
        help="Target specific KBO game_id (e.g. 20260505SSLG0).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist repaired game details and resolve DLQ entries (default: dry-run).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute boxscore repair CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    report = run_boxscore_repair(
        source=args.source,
        target_date=args.date,
        specific_game_id=args.game_id,
        apply=args.apply,
    )

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        return 0 if (report.failed == 0 and report.discrepancies == 0) else 1

    mode_str = "APPLY" if args.apply else "DRY-RUN"
    print(f"\n{'=' * 60}")
    print(f" Boxscore Repair Report [{mode_str}]")
    print(f"{'=' * 60}")
    print(f" Total candidates discovered : {report.total_candidates}")
    print(f" Repaired successfully       : {report.repaired}")
    print(f" Failed crawls/saves         : {report.failed}")
    print(f" Discrepancies remaining     : {report.discrepancies}")
    print(f"{'-' * 60}")

    for outcome in report.outcomes:
        status_symbol = "✅" if outcome.status == "repaired" else "❌"
        print(f" {status_symbol} {outcome.game_id} ({outcome.game_date}): {outcome.status}")
        for err in outcome.errors:
            print(f"    - {err}")

    print(f"{'=' * 60}\n")
    return 0 if (report.failed == 0 and report.discrepancies == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
