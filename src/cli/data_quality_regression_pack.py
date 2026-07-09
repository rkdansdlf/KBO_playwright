"""CLI for the crawler data quality regression pack."""

from __future__ import annotations

import argparse
import os
import sys
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from src.db.engine import get_oci_url
from src.validators.data_quality_regression_pack import (
    render_regression_report,
    report_to_json,
    run_regression_pack,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the main entry point for this CLI command.

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
    args = parser.parse_args(argv)

    database_url = args.database_url or os.getenv("DATABASE_URL") or get_oci_url()
    if not database_url:
        parser.error("database URL is required via --database-url, DATABASE_URL, or OCI_DB_URL")

    engine = create_engine(database_url)
    with engine.connect() as conn:
        report = run_regression_pack(conn)

    if args.json:
        sys.stdout.write(report_to_json(report) + "\n")
    else:
        sys.stdout.write(render_regression_report(report) + "\n")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
