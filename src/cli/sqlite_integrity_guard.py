"""CLI guard for file-backed SQLite integrity."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

from src.db.sqlite_integrity import (
    DEFAULT_QUARANTINE_ROOT,
    check_sqlite_database,
    default_corrupt_action,
    sqlite_guard_exit_code,
)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the SQLite integrity guard argument parser."""
    parser = argparse.ArgumentParser(description="Check and optionally quarantine a file-backed SQLite database.")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db"),
        help="Database URL to inspect. Non-SQLite URLs are skipped.",
    )
    parser.add_argument(
        "--action",
        choices=("none", "quarantine"),
        default=default_corrupt_action(),
        help="Action to take when corruption is detected.",
    )
    parser.add_argument(
        "--quarantine-root",
        default=str(DEFAULT_QUARANTINE_ROOT),
        help="Directory where corrupt SQLite file families are preserved.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat missing or empty SQLite files as failures.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a machine-readable JSON report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the SQLite integrity guard CLI."""
    args = build_arg_parser().parse_args(argv)
    report = check_sqlite_database(
        args.database_url,
        strict=args.strict,
        action=args.action,
        quarantine_root=Path(args.quarantine_root),
    )

    if args.json:
        sys.stdout.write(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True) + "\n")
    else:
        sys.stdout.write(f"{report.status}: {report.reason}\n")
        if report.database_path:
            sys.stdout.write(f"database_path={report.database_path}\n")
        if report.quarantine_dir:
            sys.stdout.write(f"quarantine_dir={report.quarantine_dir}\n")

    return sqlite_guard_exit_code(report, strict=args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
