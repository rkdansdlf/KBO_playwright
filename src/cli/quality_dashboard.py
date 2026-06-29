"""Build a compact dashboard from generated quality report JSON files."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.constants import KST
from src.utils.date_helpers import parse_date_str, parse_datetime_str

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DEFAULT_REPORT_DIR = Path("logs/quality_reports")


def _load_report(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        logger.warning("Skipping unreadable quality report: %s", path)
        return None
    if not isinstance(payload, dict):
        logger.warning("Skipping non-object quality report: %s", path)
        return None
    return payload


def _report_date(path: Path, report: dict[str, Any]) -> str:
    metrics = report.get("metrics") or {}
    date_value = metrics.get("date")
    if isinstance(date_value, str) and date_value:
        return date_value
    return path.stem


def _as_bool(value: object, *, default: bool = True) -> bool:
    return value if isinstance(value, bool) else default


def _section_ok(report: dict[str, Any], *path: str) -> bool:
    current: object = report
    for key in path:
        if not isinstance(current, dict):
            return True
        current = current.get(key)
    if isinstance(current, dict):
        return _as_bool(current.get("ok"))
    return _as_bool(current)


def _quality_failures(report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if not _section_ok(report, "quality_gate", "ok"):
        failures.append("quality_gate")
    if not _section_ok(report, "metrics", "relay_integrity", "ok"):
        failures.append("relay_integrity")
    if not _section_ok(report, "metrics", "standings_integrity", "ok"):
        failures.append("standings_integrity")
    if not _section_ok(report, "metrics", "parity", "ok"):
        failures.append("parity")
    if not _section_ok(report, "metrics", "pa_formula_integrity", "ok"):
        failures.append("pa_formula")
    return failures


def _record_from_report(path: Path, report: dict[str, Any]) -> dict[str, Any]:
    metrics = report.get("metrics") or {}
    gate = report.get("quality_gate") or {}
    failures = _quality_failures(report)
    return {
        "file": str(path),
        "date": _report_date(path, report),
        "generated_at": report.get("generated_at"),
        "total_games": metrics.get("total_games", 0),
        "completed_count": metrics.get("completed_count", 0),
        "status_counts": metrics.get("status_counts", {}),
        "quality_gate_ok": _as_bool(gate.get("ok")),
        "relay_ok": _section_ok(report, "metrics", "relay_integrity", "ok"),
        "standings_ok": _section_ok(report, "metrics", "standings_integrity", "ok"),
        "parity_ok": _section_ok(report, "metrics", "parity", "ok"),
        "pa_formula_ok": _section_ok(report, "metrics", "pa_formula_integrity", "ok"),
        "failures": failures,
    }


def _within_days(record: dict[str, Any], days: int | None) -> bool:
    if not _is_yyyymmdd(str(record["date"])):
        return False
    if days is None:
        return True
    try:
        report_date = parse_date_str(str(record["date"]))
    except ValueError:
        return True
    cutoff = datetime.now(KST).date() - timedelta(days=days)
    return report_date >= cutoff


def _is_yyyymmdd(value: str) -> bool:
    if len(value) != 8 or not value.isdigit():
        return False
    try:
        parse_datetime_str(value)
    except ValueError:
        return False
    return True


def load_quality_records(
    report_dir: Path,
    *,
    days: int | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Load quality records.

    Args:
        report_dir: Report Dir.
        days: Days.
        limit: Limit.
        report_dir: Report directory path.

    Returns:
        List of results.

    """
    records: list[dict[str, Any]] = []

    for path in sorted(report_dir.glob("*.json")):
        report = _load_report(path)
        if report is None:
            continue
        record = _record_from_report(path, report)
        if _within_days(record, days):
            records.append(record)
    records.sort(key=lambda item: str(item["date"]))
    if limit is not None and limit > 0:
        return records[-limit:]
    return records


def build_quality_dashboard(report_dir: Path, *, days: int | None = None, limit: int | None = None) -> dict[str, Any]:
    """
    Build quality dashboard.

    Args:
        report_dir: Report Dir.
        days: Days.
        limit: Limit.
        report_dir: Report directory path.

    Returns:
        Dictionary result.

    """
    records = load_quality_records(report_dir, days=days, limit=limit)

    failure_counts: dict[str, int] = {}
    for record in records:
        for failure in record["failures"]:
            failure_counts[failure] = failure_counts.get(failure, 0) + 1

    latest = records[-1] if records else None
    return {
        "report_dir": str(report_dir),
        "report_count": len(records),
        "window": {"days": days, "limit": limit},
        "latest": latest,
        "totals": {
            "games": sum(int(record.get("total_games") or 0) for record in records),
            "completed_games": sum(int(record.get("completed_count") or 0) for record in records),
            "failing_reports": sum(1 for record in records if record["failures"]),
        },
        "failure_counts": dict(sorted(failure_counts.items())),
        "records": records,
    }


def _log_dashboard_summary(dashboard: dict[str, Any]) -> None:
    latest = dashboard.get("latest") or {}
    totals = dashboard.get("totals") or {}
    logger.info("Quality dashboard: %s report(s)", dashboard["report_count"])
    if latest:
        logger.info(
            "Latest: %s total_games=%s completed=%s failures=%s",
            latest.get("date"),
            latest.get("total_games"),
            latest.get("completed_count"),
            latest.get("failures"),
        )
    logger.info(
        "Totals: games=%s completed=%s failing_reports=%s",
        totals.get("games", 0),
        totals.get("completed_games", 0),
        totals.get("failing_reports", 0),
    )
    if dashboard.get("failure_counts"):
        logger.info("Failure counts: %s", dashboard["failure_counts"])


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="Summarize generated quality report JSON files")
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=DEFAULT_REPORT_DIR,
        help="Directory containing quality report JSON files",
    )
    parser.add_argument("--days", type=int, help="Only include reports whose metric date is within N days")
    parser.add_argument("--limit", type=int, help="Only include the most recent N reports after filtering")
    parser.add_argument("--json", action="store_true", help="Print dashboard as JSON")
    args = parser.parse_args(argv)

    dashboard = build_quality_dashboard(args.report_dir, days=args.days, limit=args.limit)
    if args.json:
        logger.info(json.dumps(dashboard, ensure_ascii=False, indent=2, default=str))
    else:
        _log_dashboard_summary(dashboard)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
