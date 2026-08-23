"""CLI entrypoint for running unified data quality and freshness audits."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST
from src.db.engine import get_db_session
from src.services.quality_hub import QualityHub
from src.utils.date_helpers import parse_date_str

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Run unified KBO data quality, regression, standings, and freshness audits.",
    )
    parser.add_argument("--year", type=int, default=None, help="Target season year (defaults to current year)")
    parser.add_argument("--date", type=str, default=None, help="Target date for standings/freshness (YYYY-MM-DD)")
    parser.add_argument("--freshness-days", type=int, default=7, help="Lookback days for data freshness evaluation")
    parser.add_argument("--gate-only", action="store_true", help="Run only statistical quality gate")
    parser.add_argument("--regression-only", action="store_true", help="Run only DB invariant regression pack")
    parser.add_argument("--standings-only", action="store_true", help="Run only standings integrity check")
    parser.add_argument("--freshness-only", action="store_true", help="Run only data freshness check")
    parser.add_argument("--json", action="store_true", help="Output report in JSON format")
    parser.add_argument("--output", type=Path, default=None, help="Save report output to specified file path")
    parser.add_argument("--strict", action="store_true", help="Fail with exit code 1 on WARN status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the unified quality check CLI.

    Returns:
        0 if PASS, 1 if WARN (or strict mode breach), 2 if FAIL or fatal error.

    """
    parser = build_parser()
    args = parser.parse_args(argv)

    now = datetime.now(KST)
    season = args.year or now.year
    target_date = parse_date_str(args.date) if args.date else now.date()

    has_filter = args.gate_only or args.regression_only or args.standings_only or args.freshness_only

    include_gate = args.gate_only or not has_filter
    include_reg = args.regression_only or not has_filter
    include_standings = args.standings_only or not has_filter
    include_freshness = args.freshness_only or not has_filter

    try:
        with get_db_session() as session:
            hub = QualityHub(session)
            report = hub.run_full_audit(
                season=season,
                target_date=target_date,
                freshness_days=args.freshness_days,
                include_quality_gate=include_gate,
                include_regression_pack=include_reg,
                include_standings=include_standings,
                include_freshness=include_freshness,
            )

            if args.json:
                output_text = json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
            else:
                output_text = hub.format_markdown(report)

            sys.stdout.write(output_text + "\n")

            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(output_text, encoding="utf-8")
                logger.info("Report saved to %s", args.output)

            if report.overall_status == "FAIL":
                return 2
            if report.overall_status == "WARN" and args.strict:
                return 1
            return 0
    except Exception as exc:
        logger.exception("Unified quality check failed")
        sys.stderr.write(f"Error executing unified quality check: {exc}\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
