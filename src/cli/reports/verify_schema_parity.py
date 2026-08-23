"""CLI command to audit and verify schema parity between SQLAlchemy ORM models and database."""

from __future__ import annotations

import argparse
import json
import sys

from src.models.inspector import ModelInspector


def build_arg_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(description="Audit and verify database schema parity.")
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Database URL to audit against ORM models (default: application Engine).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail (exit code 1) on any WARNING level issues.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output parity report as JSON.",
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
        from src.db.engine import Engine

        engine = Engine

    inspector = ModelInspector()
    report = inspector.audit_engine(engine)

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))  # noqa: T201
    else:
        print(f"=== Schema Parity Audit ({report.total_tables} tables, {report.total_columns} columns) ===")  # noqa: T201
        print(  # noqa: T201
            f"Matched: {report.matched_tables} | Drifted: {report.drifted_tables} | Total Issues: {len(report.issues)}"
        )
        if report.issues:
            print("\nIssues:")  # noqa: T201
            for issue in report.issues:
                prefix = f"[{issue.severity}]"
                col_part = f".{issue.column_name}" if issue.column_name else ""
                print(f"  {prefix} {issue.table_name}{col_part}: {issue.issue_type} - {issue.message}")  # noqa: T201

    has_errors = any(i.severity == "ERROR" for i in report.issues)
    has_warnings = any(i.severity == "WARN" for i in report.issues)

    if has_errors or (args.strict and has_warnings):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
