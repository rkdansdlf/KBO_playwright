"""CLI command for detecting database schema drift and generating remediation DDL."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from src.db.drift_detector import SchemaDriftDetector
from src.db.engine import Engine

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Execute schema drift detector CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Detect Database Schema Drift against ORM Models & Generate DDL")
    parser.add_argument("--dialect", "-d", type=str, default=None, help="Target dialect (oracle, sqlite, postgresql)")
    parser.add_argument("--db-url", type=str, default=None, help="Database connection URL (defaults to active Engine)")
    parser.add_argument("--tables", "-t", type=str, default=None, help="Comma-separated list of table names to check")
    parser.add_argument("--apply", action="store_true", help="Apply generated remediation DDL to database")
    parser.add_argument("--output-file", "-o", type=str, default=None, help="Save generated DDL statements to file")
    parser.add_argument("--strict", action="store_true", help="Exit with code 1 if any drift is detected")
    parser.add_argument("--json", action="store_true", help="Output report in JSON format")

    args = parser.parse_args(argv)

    # Resolve engine
    bind_engine = create_engine(args.db_url) if args.db_url else Engine

    table_filter = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else None

    detector = SchemaDriftDetector(bind_engine, dialect=args.dialect)
    report = detector.detect_drift(table_filter=table_filter)

    # Handle output file if specified
    if args.output_file and report.generated_ddl:
        out_path = Path(args.output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            f.write("\n".join(report.generated_ddl) + "\n")
        logger.info("Saved %d DDL statements to %s", len(report.generated_ddl), out_path)

    # Handle --apply flag
    if args.apply and report.generated_ddl:
        applied = detector.apply_remediation(report)
        if not args.json:
            print(f"✅ Successfully applied {applied} DDL remediation statements.")  # noqa: T201

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    else:
        print(report.to_markdown())  # noqa: T201

    if args.strict and report.drift_count > 0:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
