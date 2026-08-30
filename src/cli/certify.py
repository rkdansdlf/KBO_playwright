"""CLI command for running KBO Production Platform and Historical Data Certification."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.certification.context import CertificationContext

logger = logging.getLogger(__name__)

EXIT_CERTIFIED = 0
EXIT_GATE_FAILURE = 1
EXIT_CONFIG_ERROR = 2
EXIT_INTERNAL_ERROR = 3


def build_parser() -> argparse.ArgumentParser:
    """Build command-line parser for kbo certify."""
    parser = argparse.ArgumentParser(
        description="Run end-to-end KBO Platform Production & Historical Data Certification Gate.",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Execute full production certification contract.",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Execute local development certification mode.",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Execute 45-season (1982~2026) historical data certification audit.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Target a specific season for historical certification audit (e.g. 2024).",
    )
    parser.add_argument(
        "--start-season",
        type=int,
        default=1982,
        help="Start season for historical audit range (default: 1982).",
    )
    parser.add_argument(
        "--end-season",
        type=int,
        default=2026,
        help="End season for historical audit range (default: 2026).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Display full un-condensed season matrix in ASCII scorecard.",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default=None,
        help="File path to save JSON certification report.",
    )
    parser.add_argument(
        "--gate",
        type=str,
        default=None,
        help="Filter and execute a specific gate (e.g. schema_migration, vector_rag, historical_data_45_seasons).",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop execution immediately on first blocking gate failure.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON report to stdout.",
    )
    return parser


def _run_historical_cli(args: argparse.Namespace, context: CertificationContext, json_out_path: Path) -> int:
    """Execute historical certification branch."""
    from src.certification.historical.reporter import HistoricalReporter
    from src.certification.historical.runner import HistoricalCertificationRunner

    try:
        hist_runner = HistoricalCertificationRunner()
        start_yr = args.season or args.start_season
        end_yr = args.season or args.end_season

        hist_report = hist_runner.run_historical_audit(
            context=context,
            start_season=start_yr,
            end_season=end_yr,
        )

        # Save JSON artifact
        saved_path = HistoricalReporter.save_json_report(hist_report, json_out_path)
        logger.info("Saved historical certification report to %s", saved_path)

        # Output to stdout
        if args.json:
            print(json.dumps(hist_report.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
        else:
            matrix_card = HistoricalReporter.render_ascii_matrix(hist_report, verbose=args.verbose)
            print(matrix_card)  # noqa: T201

    except Exception as exc:  # noqa: BLE001
        print(f"❌ Internal error during historical certification audit: {exc}", file=sys.stderr)  # noqa: T201
        return EXIT_INTERNAL_ERROR
    else:
        return EXIT_GATE_FAILURE if hist_report.failed_seasons > 0 else EXIT_CERTIFIED


def main(argv: list[str] | None = None) -> int:
    """Execute certification runner from command line."""
    parser = build_parser()
    args = parser.parse_args(argv)

    target_mode = "production" if args.production or not args.local else "local"
    default_json_out = "data/certification/historical.json" if args.historical else "data/certification/report.json"
    json_out_path = Path(args.json_out or default_json_out)

    from src.certification.context import CertificationContext

    try:
        context = CertificationContext(
            target=target_mode,
            fail_fast=args.fail_fast,
            filter_gate=args.gate,
            artifact_dir=json_out_path.parent,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"❌ Configuration error initializing certification context: {exc}", file=sys.stderr)  # noqa: T201
        return EXIT_CONFIG_ERROR

    if args.historical:
        return _run_historical_cli(args, context, json_out_path)

    # Standard Platform Production / Gate Certification
    from src.certification.reporter import CertificationReporter
    from src.certification.runner import CertificationRunner

    try:
        runner = CertificationRunner()
        report = runner.run_certification(context)

        # Save JSON artifact
        saved_path = CertificationReporter.save_json_report(report, json_out_path)
        logger.info("Saved certification report to %s", saved_path)

        # Output to stdout
        if args.json:
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
        else:
            card = CertificationReporter.render_ascii_card(report)
            print(card)  # noqa: T201

    except Exception as exc:  # noqa: BLE001
        print(f"❌ Internal error during certification run: {exc}", file=sys.stderr)  # noqa: T201
        return EXIT_INTERNAL_ERROR
    else:
        if report.status == "CERTIFIED":
            return EXIT_CERTIFIED
        if report.status == "CERTIFIED_WITH_WARNINGS" and not context.strict:
            return EXIT_CERTIFIED
        return EXIT_GATE_FAILURE


if __name__ == "__main__":
    sys.exit(main())
