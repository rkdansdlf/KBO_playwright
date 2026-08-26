"""CLI command for generating executive quality and system reports."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.reporting.dto import ReportFormat
from src.reporting.engine import ReportingEngine


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Generate comprehensive system and quality reports.")
    parser.add_argument(
        "--category",
        type=str,
        default="all",
        choices=["all", "quality", "gap", "freshness", "executive"],
        help="Report category to generate (default: all/executive).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Target season year for quality reports.",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="markdown",
        choices=["markdown", "json", "html"],
        help="Output rendering format (default: markdown).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional output file path to write the report to.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    engine = ReportingEngine()

    if args.category == "quality":
        report = engine.generate_quality_report(year=args.year)
    elif args.category == "gap":
        report = engine.generate_gap_report()
    elif args.category == "freshness":
        report = engine.generate_freshness_report()
    else:  # all / executive
        report = engine.generate_executive_dashboard()

    output_text = engine.render_report(report, format_type=ReportFormat(args.format))

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output_text, encoding="utf-8")
        print(f"[REPORT] Saved report ({report.report_id}) to {out_path}")  # noqa: T201
    else:
        print(output_text)  # noqa: T201

    return 1 if report.overall_status == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
