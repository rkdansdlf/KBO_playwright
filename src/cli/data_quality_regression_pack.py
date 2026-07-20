"""CLI for the crawler data quality regression pack."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import DATE_STR_LEN
from src.db.engine import create_engine_for_url as create_engine
from src.db.engine import get_oci_url
from src.validators.data_quality_regression_pack import (
    render_regression_report,
    report_to_json,
    run_regression_pack,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Run data quality regression invariants")

    parser.add_argument(
        "--database-url",
        default=None,
        help="Database URL to inspect. Defaults to DATABASE_URL, then OCI_DB_URL when available.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON report")
    parser.add_argument("--date", help="Scope game-level checks to YYYYMMDD")
    parser.add_argument("--year", type=int, help="Scope season-level checks to a season year")
    parser.add_argument(
        "--require-schema",
        action="store_true",
        help="Fail when required tables or columns are missing",
    )
    parser.add_argument("--output", type=Path, help="Write the JSON report to this file")
    args = parser.parse_args(argv)

    target_date = args.date.replace("-", "") if args.date else None
    if target_date and (len(target_date) != DATE_STR_LEN or not target_date.isdigit()):
        parser.error("--date must use YYYYMMDD or YYYY-MM-DD")
    season = args.year or (int(target_date[:4]) if target_date else None)

    database_url = args.database_url or os.getenv("DATABASE_URL") or get_oci_url()
    if not database_url:
        parser.error("database URL is required via --database-url, DATABASE_URL, or OCI_DB_URL")

    engine = create_engine(database_url)
    with engine.connect() as conn:
        report = run_regression_pack(
            conn,
            target_date=target_date,
            season=season,
            require_schema=args.require_schema,
        )

    if args.output:
        json_report = report_to_json(report)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json_report + "\n", encoding="utf-8")
    if args.json:
        sys.stdout.write(report_to_json(report) + "\n")
    else:
        sys.stdout.write(render_regression_report(report) + "\n")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
