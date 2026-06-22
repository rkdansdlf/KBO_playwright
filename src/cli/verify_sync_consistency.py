"""
verify_sync_consistency.py

CLI tool to verify data consistency between the local SQLite database
and the remote OCI PostgreSQL database.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.engine import create_engine_for_url, get_oci_url, get_source_db_url
from src.utils.alerting import SlackWebhookClient

load_dotenv()

logger = logging.getLogger(__name__)

SYNC_VERIFY_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)

TABLES_TO_VERIFY = [
    ("rag_chunks", ["source_table", "source_row_id"]),
    ("stadium_foods", ["stadium_name", "restaurant_name", "menu_item"]),
    ("ticket_schedules", ["game_date", "home_team", "platform"]),
    ("team_daily_roster", ["roster_date", "team_code", "player_id"]),
    ("players", ["kbo_person_id"]),
    ("game", ["game_id"]),
    ("player_season_batting", ["player_id", "season", "league", "level"]),
    ("player_season_pitching", ["player_id", "season", "league", "level"]),
    ("kbo_seasons", ["season_id"]),
    ("player_basic", ["player_id"]),
]


def check_table_counts(sqlite_conn: Connection, oci_conn: Connection) -> list[dict[str, Any]]:
    """Compares row counts for verified tables between SQLite and OCI."""
    results = []
    for table_name, pk_cols in TABLES_TO_VERIFY:
        # Check if table exists in SQLite
        sqlite_exists = inspect(sqlite_conn).has_table(table_name)
        oci_exists = inspect(oci_conn).has_table(table_name)

        if not sqlite_exists:
            logger.warning("⚠️  Table %s does not exist in local SQLite.", table_name)
            continue
        if not oci_exists:
            logger.warning("⚠️  Table %s does not exist in remote OCI.", table_name)
            results.append(
                {
                    "table_name": table_name,
                    "sqlite_count": get_row_count(sqlite_conn, table_name),
                    "oci_count": -1,
                    "delta": -1,
                    "status": "MISSING_ON_OCI",
                },
            )
            continue

        sqlite_count = get_row_count(sqlite_conn, table_name)
        oci_count = get_row_count(oci_conn, table_name)
        delta = sqlite_count - oci_count
        if delta == 0:
            status = "OK"
        elif delta < 0:
            status = "OK (OCI+)"
        else:
            status = "MISMATCH"

        results.append(
            {
                "table_name": table_name,
                "sqlite_count": sqlite_count,
                "oci_count": oci_count,
                "delta": delta,
                "status": status,
                "pk_cols": pk_cols,
            },
        )

    return results


def get_row_count(conn: Connection, table_name: str) -> int:
    try:
        res = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return res.scalar() or 0
    except SYNC_VERIFY_DB_EXCEPTIONS:
        logger.exception("Error getting count for %s", table_name)
        return 0


def check_deep_ids(
    sqlite_conn: Connection, oci_conn: Connection, table_name: str, pk_cols: list[str]
) -> tuple[int, list[object]]:
    """Performs deep ID-level matching to identify SQLite rows missing in OCI."""
    try:
        cols_str = ", ".join(pk_cols)

        # Fetch all primary keys from SQLite
        res_sqlite = sqlite_conn.execute(text(f"SELECT {cols_str} FROM {table_name}"))
        sqlite_rows = res_sqlite.fetchall()

        # Fetch all primary keys from OCI
        res_oci = oci_conn.execute(text(f"SELECT {cols_str} FROM {table_name}"))
        oci_rows = res_oci.fetchall()

        def stringify_row(row: Sequence[object]) -> tuple[str, ...]:
            return tuple(val.isoformat() if hasattr(val, "isoformat") else str(val) for val in row)

        sqlite_ids = {stringify_row(row) for row in sqlite_rows}
        oci_ids = {stringify_row(row) for row in oci_rows}

        missing_in_oci = sqlite_ids - oci_ids
        match_rate = (1.0 - (len(missing_in_oci) / len(sqlite_ids))) * 100 if sqlite_ids else 100.0

        # Un-tuple single keys for cleaner printing in sample
        sample_keys = []
        for row in list(missing_in_oci)[:10]:
            if len(row) == 1:
                sample_keys.append(row[0])
            else:
                sample_keys.append(row)

        return int(match_rate), sample_keys
    except SYNC_VERIFY_DB_EXCEPTIONS:
        logger.exception("Error performing deep check for %s", table_name)
        return 0, []


def _log_count_results(count_results: list[dict[str, Any]]) -> None:
    logger.info("\n┌──────────────────────────────┬──────────────┬──────────────┬──────────────┬────────────┐")
    logger.info("│ Table Name                   │ SQLite Count │ OCI Count    │ Delta        │ Status     │")
    logger.info("├──────────────────────────────┼──────────────┼──────────────┼──────────────┼────────────┤")
    for res in count_results:
        logger.info(
            "│ %s │ %s │ %s │ %s │ %s │",
            res["table_name"].ljust(28),
            str(res["sqlite_count"]).rjust(12),
            str(res["oci_count"] if res["oci_count"] != -1 else "N/A").rjust(12),
            str(res["delta"]).rjust(12),
            res["status"].ljust(10),
        )
    logger.info("└──────────────────────────────┴──────────────┴──────────────┴──────────────┴────────────┘")


def _collect_count_mismatches(count_results: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    mismatches = []
    alert_lines = []
    for res in count_results:
        if res["status"] in ("MISMATCH", "MISSING_ON_OCI"):
            mismatches.append(res)
            alert_lines.append(
                f"• <b>{res['table_name']}</b>: SQLite={res['sqlite_count']} vs OCI={res['oci_count']} (Delta={res['delta']})",
            )
    return mismatches, alert_lines


def _collect_deep_mismatches(
    sqlite_conn: Connection, oci_conn: Connection, count_results: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]]:
    mismatches = []
    alert_lines = []
    logger.info("\n🧬 Running Deep ID Verification...")
    for res in count_results:
        if res["status"] == "MISSING_ON_OCI":
            mismatches.append(res)
            alert_lines.append(f"• <b>{res['table_name']}</b>: Missing on OCI database.")
            continue
        table_name = res["table_name"]
        match_rate, missing_sample = check_deep_ids(sqlite_conn, oci_conn, table_name, res["pk_cols"])
        logger.info("  - %s: Match Rate = %s%%", table_name, match_rate)
        if match_rate < 100.0:
            sample_str = ", ".join(str(key) for key in missing_sample)
            logger.warning("    ⚠️  Missing sample IDs in OCI: %s", sample_str)
            mismatches.append(res)
            alert_lines.append(
                f"• <b>{table_name}</b>: Key ID match rate is {match_rate}% (Sample missing keys: {sample_str})"
            )
    return mismatches, alert_lines


def _send_consistency_mismatch_alert(alert_lines: list[str], *, trigger_alert: bool) -> None:
    if not trigger_alert:
        return
    alert_msg = "<b>⚠️ KBO DB Consistency Mismatch Alert</b>\n\n" + "\n".join(alert_lines)
    logger.info("📬 Sending alert webhook...")
    SlackWebhookClient.send_alert(alert_msg)


def run_consistency_audit(*, deep: bool = False, trigger_alert: bool = True) -> bool:
    source_url = get_source_db_url()
    target_url = get_oci_url()

    if not target_url:
        logger.error("❌ OCI target database URL is not configured.")
        return False

    logger.info("\n🔍 Connecting to SQLite and OCI databases...")
    sqlite_engine = create_engine_for_url(source_url, disable_sqlite_wal=True)
    oci_engine = create_engine_for_url(target_url, disable_sqlite_wal=True)

    try:
        with sqlite_engine.connect() as sqlite_conn, oci_engine.connect() as oci_conn:
            logger.info("📊 Comparing row counts...")
            count_results = check_table_counts(sqlite_conn, oci_conn)
            _log_count_results(count_results)
            mismatches, alert_lines = (
                _collect_deep_mismatches(sqlite_conn, oci_conn, count_results)
                if deep
                else _collect_count_mismatches(count_results)
            )

    except SYNC_VERIFY_DB_EXCEPTIONS as e:
        logger.exception("❌ Error during consistency check")
        if trigger_alert:
            SlackWebhookClient.send_error_alert(f"Database Consistency Checker failed with error:\n{e}")
        return False

    if mismatches:
        logger.info("\n🚨 Discovered %s database mismatch alerts!", len(mismatches))
        _send_consistency_mismatch_alert(alert_lines, trigger_alert=trigger_alert)
        return False
    logger.info("\n✅ All databases are fully synchronized and consistent!")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="KBO SQLite to OCI PostgreSQL consistency auditor")
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Perform deep ID-level matching to catch record discrepancies.",
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="Disable sending slack/telegram notifications on mismatch.",
    )
    args = parser.parse_args()

    success = run_consistency_audit(deep=args.deep, trigger_alert=not args.no_alert)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
