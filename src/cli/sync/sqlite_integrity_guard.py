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
from src.notifications.alert_dto import AlertEvent, AlertSeverity, AlertSource
from src.notifications.bridge import apply_incidents

#: Incident keys are scoped per database file so an unrelated healthy run can
#: never clear another database's open quarantine.
SQLITE_QUARANTINE_PREFIX = "integrity:sqlite:quarantine:"


def quarantine_incident_key(database_path: object) -> str:
    """Return the incident key for one guarded database file."""
    return f"{SQLITE_QUARANTINE_PREFIX}{database_path or 'unknown'}"


def apply_quarantine_incident(report: object, *, notify: bool) -> None:
    """Open (or recover) the quarantine incident for one guard run.

    A quarantined or failed-quarantine file opens its own incident. A healthy
    run only recovers — and only when ``--notify`` was requested, matching the
    guard's previous behaviour of staying silent otherwise. Recovery targets the
    exact key rather than the whole namespace: this tool inspects a single
    database per invocation, so reconciling the namespace would let a healthy
    run for one file silently clear a different file's open quarantine.
    """
    status = str(getattr(report, "status", "") or "")
    database_path = getattr(report, "database_path", None) or "unknown"
    key = quarantine_incident_key(database_path)

    if status not in ("quarantined", "quarantine_failed"):
        if notify:
            apply_incidents([], resolve_keys=[key])
        return

    reason = getattr(report, "reason", None) or getattr(report, "error", None) or "Unknown error"
    quarantine_dir = getattr(report, "quarantine_dir", None) or "N/A"
    moved_files = [Path(str(f)).name for f in (getattr(report, "moved_files", None) or ())]
    apply_incidents(
        [
            AlertEvent(
                source=AlertSource.DATABASE,
                component=f"sqlite:{database_path}",
                severity=AlertSeverity.CRITICAL if status == "quarantine_failed" else AlertSeverity.ERROR,
                title=f"SQLite DB quarantine: {status}",
                message=(
                    f"database_path={database_path}\n"
                    f"quarantine_dir={quarantine_dir}\n"
                    f"moved_files={', '.join(moved_files) or 'None'}\n"
                    f"reason={reason}"
                ),
                incident_key=key,
                metadata={
                    "database_path": str(database_path),
                    "status": status,
                    "reason": str(reason),
                    "moved_files": moved_files,
                },
            ),
        ],
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
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Send Telegram/Slack notification when quarantine occurs or fails.",
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

    apply_quarantine_incident(report, notify=args.notify)

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
