"""CLI command to run and inspect database schema migrations."""

from __future__ import annotations

import argparse
import json
import sys

from src.db.dto import MigrationDialect
from src.db.engine import Engine
from src.db.migration_engine import MigrationEngine


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Run and inspect database schema migrations.")
    parser.add_argument(
        "--dialect",
        type=str,
        default="oracle",
        choices=["oracle", "sqlite", "postgresql", "pgvector"],
        help="Database dialect for migration files (default: oracle).",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Target database URL (default: application Engine).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate migration without executing statements.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show migration status report without applying new migrations.",
    )
    parser.add_argument(
        "--include-safety-gated",
        action="store_true",
        help="Include migrations gated for safety.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI execution entrypoint."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.db_url:
        from sqlalchemy import create_engine

        engine = create_engine(args.db_url)
    else:
        engine = Engine

    dialect = MigrationDialect(args.dialect)
    mig_engine = MigrationEngine()

    if args.status:
        report = mig_engine.get_status(
            engine,
            dialect,
            include_safety_gated=args.include_safety_gated,
        )
    else:
        report = mig_engine.apply_migrations(
            engine,
            dialect,
            dry_run=args.dry_run,
            include_safety_gated=args.include_safety_gated,
        )

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        mode_str = "Status" if args.status else ("Dry Run" if args.dry_run else "Execution")
        print(f"=== Database Migration {mode_str} [{report.dialect.upper()}] ===")  # noqa: T201
        print(  # noqa: T201
            f"Available: {report.total_available} | Applied: {report.applied_count} | Pending: {report.pending_count}"
        )
        if report.pending_versions:
            print(f"Pending Versions: {report.pending_versions}")  # noqa: T201
        if report.results:
            for r in report.results:
                prefix = f"[{r.status}]"
                err = f" (Error: {r.error_message})" if r.error_message else ""
                print(f"  {prefix} v{r.version:03d} {r.filename} ({r.duration_seconds}s){err}")  # noqa: T201

    has_failures = any(r.status == "FAILED" for r in report.results)
    return 1 if has_failures else 0


if __name__ == "__main__":
    sys.exit(main())
