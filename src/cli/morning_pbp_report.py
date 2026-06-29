"""
Morning PBP (Play-by-Play) Report CLI.

Send a daily Telegram notification summarizing PBP collection failures,
recovery results, and validation status from the most recent daily summary
and current database state.

Usage:
    python3 -m src.cli.morning_pbp_report
    python3 -m src.cli.morning_pbp_report --date 20260528
    python3 -m src.cli.morning_pbp_report --dry-run

"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

DAILY_SUMMARY_DIR = Path(__file__).resolve().parents[2] / "logs" / "daily_update_summary"


def _find_latest_summary(target_date: str | None = None) -> tuple[str, dict[str, Any]] | None:
    """
    Find the daily summary JSON for the given date (default: yesterday in KST).

    Args:
        target_date: Target date for the operation.

    """
    if target_date is None:
        seoul_tz = ZoneInfo("Asia/Seoul")
        yesterday = datetime.now(seoul_tz) - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")

    path = DAILY_SUMMARY_DIR / f"{target_date}.json"
    if not path.exists():
        return None

    try:
        with path.open(encoding="utf-8") as f:
            return target_date, json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to parse summary %s: %s", path, e)
        return None


def _query_pbp_validation_summary() -> dict[str, int]:
    """Return counts of games by canonical relay validation status from the DB."""
    counts: dict[str, int] = {
        "verified": 0,
        "recovered": 0,
        "unverified": 0,
        "source_incomplete": 0,
        "source_unavailable": 0,
        "pending_live": 0,
        "provisionally_valid": 0,
        "other": 0,
    }
    try:
        from sqlalchemy import text

        from src.db.engine import SessionLocal

        with SessionLocal() as session:
            rows = session.execute(
                text("""
                    SELECT validation_status AS status, COUNT(*)
                    FROM game_validation_metrics
                    GROUP BY validation_status
                """),
            ).fetchall()
            if not rows:
                rows = session.execute(
                    text("""
                        SELECT json_extract(m.source_payload, '$.pbp_validation_status') AS status, COUNT(*)
                        FROM game_metadata m
                        WHERE json_extract(m.source_payload, '$.pbp_validation_status') IS NOT NULL
                        GROUP BY json_extract(m.source_payload, '$.pbp_validation_status')
                    """),
                ).fetchall()
            for status, cnt in rows:
                key = status if status in counts else "other"
                counts[key] = counts.get(key, 0) + cnt
    except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
        logger.exception("[WARN] Failed to query PBP validation summary")
    return counts


def _append_relay_section(lines: list[str], relay_data: dict[str, Any], relay_failures: list[str]) -> None:
    lines.append(f"📡 <b>Relay Targets</b>: {relay_data.get('target_count', 0)} games")
    if relay_failures:
        lines.append(f"❌ <b>Failed Relay</b>: {len(relay_failures)} games")
        lines.extend(f"   - {game_id}" for game_id in relay_failures[:10])
        if len(relay_failures) > 10:
            lines.append(f"   ... and {len(relay_failures) - 10} more")
    else:
        lines.append("✅ <b>Relay</b>: All targets recovered")


def _append_detail_failures(lines: list[str], detail_failures: list[str]) -> None:
    if detail_failures:
        lines.append(f"⚠️ <b>Failed Detail</b>: {len(detail_failures)} games")
        lines.extend(f"   - {game_id}" for game_id in detail_failures[:5])


def _append_oci_skips(lines: list[str], oci_data: dict[str, Any]) -> None:
    skip_counts = oci_data.get("skip_counts", {})
    if skip_counts:
        lines.append(f"⏭️ <b>OCI Skips</b>: {sum(skip_counts.values())} games")
        lines.extend(f"   - {reason}: {count}" for reason, count in skip_counts.items())


def _append_validation_section(lines: list[str], validation_counts: dict[str, int]) -> None:
    lines.append("")
    verified = validation_counts.get("verified", 0)
    recovered = validation_counts.get("recovered", 0)
    unverified = validation_counts.get("unverified", 0)
    incomplete = validation_counts.get("source_incomplete", 0)
    unavailable = validation_counts.get("source_unavailable", 0)
    pending = validation_counts.get("pending_live", 0) + validation_counts.get("provisionally_valid", 0)
    other = validation_counts.get("other", 0)
    total = verified + recovered + unverified + incomplete + unavailable + pending + other
    if total > 0:
        icon = "✅" if unverified == 0 and incomplete == 0 else "⚠️"
        lines.append(
            f"{icon} <b>Validation</b>: {verified} verified / {recovered} recovered / "
            f"{unverified} unverified / {incomplete} incomplete / {unavailable} unavailable / "
            f"{pending} pending / {other} other",
        )
    else:
        lines.append("ℹ️ <b>Validation</b>: No data (pipeline may not have run yet)")


def _append_affected_games(lines: list[str], affected: list[str]) -> None:
    if affected:
        lines.append(f"\n📋 <b>Affected Games</b>: {len(affected)} total")
        lines.extend(f"   - {game_id}" for game_id in affected[:10])
        if len(affected) > 10:
            lines.append(f"   ... and {len(affected) - 10} more")


def _build_telegram_message(
    target_date: str,
    summary: dict[str, Any],
    validation_counts: dict[str, int],
    *,
    dry_run: bool,
) -> str:
    """
    Format a Telegram HTML message from the daily summary and validation data.

    Args:
        target_date: Target date for the operation.
        summary: Summary.
        validation_counts: Validation Counts.
        dry_run: If True, performs a dry run without persisting changes.

    """
    stability = summary.get("stability", {})

    retry = stability.get("retry_candidates", {})
    relay_failures: list[str] = retry.get("relay") or []
    detail_failures: list[str] = retry.get("detail") or []
    oci_data = stability.get("oci", {})
    relay_data = stability.get("relay", {})

    lines: list[str] = [f"📊 <b>PBP Morning Report ({target_date})</b>\n"]
    _append_relay_section(lines, relay_data, relay_failures)
    _append_detail_failures(lines, detail_failures)
    _append_oci_skips(lines, oci_data)
    _append_validation_section(lines, validation_counts)
    _append_affected_games(lines, stability.get("affected_game_ids", []))

    if dry_run:
        lines.append("\n🔹 <i>Dry-run mode — notification not sent</i>")

    return "\n".join(lines)


def _read_pbp_report_csv(target_date: str) -> list[dict[str, str]]:
    """
    Read the per-game PBP attempt CSV if available.

    Args:
        target_date: Target date for the operation.

    """
    csv_path = DAILY_SUMMARY_DIR / f"pbp_report_daily_{target_date}.csv"

    if not csv_path.exists():
        return []
    try:
        with csv_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except (csv.Error, OSError):
        logger.exception("Failed to read PBP CSV: %s", csv_path)
        return []
    return rows


def run_morning_report(
    target_date: str | None = None,
    *,
    dry_run: bool = False,
) -> bool:
    """
    Execute the morning PBP report.

    Return True if a notification was sent (or would be sent in dry-run).

    Args:
        target_date: Target date for the operation.
        dry_run: If True, performs a dry run without persisting changes.

    """
    found = _find_latest_summary(target_date)

    if found is None:
        date_str = target_date or "(yesterday)"
        msg = (
            f"📊 <b>PBP Morning Report ({date_str})</b>\n\n"
            f"⚠️ No daily summary found. The postgame pipeline may not have run."
        )
        if dry_run:
            logger.info(msg)
            return True
        from src.utils.alerting import SlackWebhookClient

        return SlackWebhookClient.send_alert(msg)

    target_date, summary = found

    validation_counts = _query_pbp_validation_summary()

    message = _build_telegram_message(target_date, summary, validation_counts, dry_run=dry_run)

    if dry_run:
        logger.info(message)
        return True

    from src.utils.alerting import SlackWebhookClient

    return SlackWebhookClient.send_alert(message)


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Morning PBP Report — Telegram notification")

    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date YYYYMMDD (default: yesterday KST)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the message to stdout instead of sending",
    )
    args = parser.parse_args(argv)

    success = run_morning_report(args.date, dry_run=args.dry_run)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
