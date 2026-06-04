"""
Morning PBP (Play-by-Play) Report CLI.

Sends a daily Telegram notification summarizing PBP collection failures,
recovery results, and validation status from the most recent daily summary
and current database state.

Usage:
    python3 -m src.cli.morning_pbp_report
    python3 -m src.cli.morning_pbp_report --date 20260528
    python3 -m src.cli.morning_pbp_report --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DAILY_SUMMARY_DIR = Path(__file__).resolve().parents[2] / "logs" / "daily_update_summary"


def _find_latest_summary(target_date: str | None = None) -> tuple[str, dict[str, Any]] | None:
    """Find the daily summary JSON for the given date (default: yesterday in KST)."""
    if target_date is None:
        seoul_tz = ZoneInfo("Asia/Seoul")
        yesterday = datetime.now(seoul_tz) - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")

    path = DAILY_SUMMARY_DIR / f"{target_date}.json"
    if not path.exists():
        return None

    try:
        with open(path, encoding="utf-8") as f:
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
                """)
            ).fetchall()
            if not rows:
                rows = session.execute(
                    text("""
                        SELECT json_extract(m.source_payload, '$.pbp_validation_status') AS status, COUNT(*)
                        FROM game_metadata m
                        WHERE json_extract(m.source_payload, '$.pbp_validation_status') IS NOT NULL
                        GROUP BY json_extract(m.source_payload, '$.pbp_validation_status')
                    """)
                ).fetchall()
            for status, cnt in rows:
                key = status if status in counts else "other"
                counts[key] = counts.get(key, 0) + cnt
    except Exception:
        logger.exception("[WARN] Failed to query PBP validation summary")
    return counts


def _build_telegram_message(
    target_date: str,
    summary: dict[str, Any],
    validation_counts: dict[str, int],
    dry_run: bool,
) -> str:
    """Format a Telegram HTML message from the daily summary and validation data."""
    stability = summary.get("stability", {})
    retry = stability.get("retry_candidates", {})
    relay_failures: list[str] = retry.get("relay") or []
    detail_failures: list[str] = retry.get("detail") or []
    oci_data = stability.get("oci", {})
    relay_data = stability.get("relay", {})

    lines: list[str] = []
    lines.append(f"📊 <b>PBP Morning Report ({target_date})</b>\n")

    # Relay recovery targets
    relay_target_count = relay_data.get("target_count", 0)
    lines.append(f"📡 <b>Relay Targets</b>: {relay_target_count} games")

    if relay_failures:
        lines.append(f"❌ <b>Failed Relay</b>: {len(relay_failures)} games")
        for gid in relay_failures[:10]:
            lines.append(f"   - {gid}")
        if len(relay_failures) > 10:
            lines.append(f"   ... and {len(relay_failures) - 10} more")
    else:
        lines.append("✅ <b>Relay</b>: All targets recovered")

    # Detail failures
    if detail_failures:
        lines.append(f"⚠️ <b>Failed Detail</b>: {len(detail_failures)} games")
        for gid in detail_failures[:5]:
            lines.append(f"   - {gid}")

    # OCI skip info
    skip_counts = oci_data.get("skip_counts", {})
    if skip_counts:
        total_skipped = sum(skip_counts.values())
        lines.append(f"⏭️ <b>OCI Skips</b>: {total_skipped} games")
        for reason, cnt in skip_counts.items():
            lines.append(f"   - {reason}: {cnt}")

    # PBP Validation status
    lines.append("")
    verified = validation_counts.get("verified", 0)
    recovered = validation_counts.get("recovered", 0)
    unver = validation_counts.get("unverified", 0)
    incomplete = validation_counts.get("source_incomplete", 0)
    unavailable = validation_counts.get("source_unavailable", 0)
    pending = validation_counts.get("pending_live", 0) + validation_counts.get("provisionally_valid", 0)
    oth = validation_counts.get("other", 0)
    total_val = verified + recovered + unver + incomplete + unavailable + pending + oth
    if total_val > 0:
        icon = "✅" if unver == 0 and incomplete == 0 else "⚠️"
        lines.append(
            f"{icon} <b>Validation</b>: {verified} verified / {recovered} recovered / "
            f"{unver} unverified / {incomplete} incomplete / {unavailable} unavailable / "
            f"{pending} pending / {oth} other"
        )
    else:
        lines.append("ℹ️ <b>Validation</b>: No data (pipeline may not have run yet)")

    # Affected games summary
    affected = stability.get("affected_game_ids", [])
    if affected:
        lines.append(f"\n📋 <b>Affected Games</b>: {len(affected)} total")
        for gid in affected[:10]:
            lines.append(f"   - {gid}")
        if len(affected) > 10:
            lines.append(f"   ... and {len(affected) - 10} more")

    if dry_run:
        lines.append("\n🔹 <i>Dry-run mode — notification not sent</i>")

    return "\n".join(lines)


def _read_pbp_report_csv(target_date: str) -> list[dict[str, str]]:
    """Read the per-game PBP attempt CSV if available."""
    csv_path = DAILY_SUMMARY_DIR / f"pbp_report_daily_{target_date}.csv"
    if not csv_path.exists():
        return []
    import csv

    rows: list[dict[str, str]] = []
    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception:
        logger.exception("Failed to read PBP CSV: %s", csv_path)
    return rows


def run_morning_report(
    target_date: str | None = None,
    *,
    dry_run: bool = False,
) -> bool:
    """Execute the morning PBP report.

    Returns True if a notification was sent (or would be sent in dry-run).
    """
    found = _find_latest_summary(target_date)

    if found is None:
        date_str = target_date or "(yesterday)"
        msg = (
            f"📊 <b>PBP Morning Report ({date_str})</b>\n\n"
            f"⚠️ No daily summary found. The postgame pipeline may not have run."
        )
        if dry_run:
            print(msg)
            return True
        from src.utils.alerting import SlackWebhookClient

        return SlackWebhookClient.send_alert(msg)

    target_date, summary = found

    validation_counts = _query_pbp_validation_summary()

    message = _build_telegram_message(target_date, summary, validation_counts, dry_run)

    if dry_run:
        print(message)
        return True

    from src.utils.alerting import SlackWebhookClient

    return SlackWebhookClient.send_alert(message)


def main(argv: Sequence[str] | None = None) -> int:
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
