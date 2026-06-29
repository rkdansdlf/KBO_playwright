"""CLI 명령: health check."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

TABLE_CHECKS = [
    ("game", "game_date"),
    ("game_batting_stats", "game_id"),
    ("game_pitching_stats", "game_id"),
    ("team_events", "last_seen_at"),
    ("roster_transactions", "transaction_date"),
    ("ticket_prices", "season"),
    ("ticket_open_rules", "team_id"),
    ("stadium_seat_sections", "updated_at"),
    ("parking_lots", "updated_at"),
    ("parking_fee_rules", "parking_lot_id"),
    ("stadium_food_vendors", "last_verified_at"),
    ("stadium_food_menu_items", "vendor_id"),
    ("team_standings_daily", "standings_date"),
    ("stadium_transit_times", "measured_at"),
    ("stadium_congestion", "measured_at"),
    ("stadium_operation_notices", "published_at"),
    ("team_rivalries", "intensity"),
    ("cheer_songs", "introduction_year"),
]


def _check_datasource_health(session: Session) -> list[dict[str, Any]]:
    ds_repo = DataSourceRepository(session)
    rows = []
    for ds in ds_repo.get_all_active():
        stale = ""
        if ds.last_success_at:
            hours_since = (datetime.now(UTC).replace(tzinfo=None) - ds.last_success_at).total_seconds() / 3600
            stale = f"STALE ({hours_since:.0f}h)" if hours_since > 48 else f"ok ({hours_since:.0f}h ago)"
        else:
            stale = "NEVER"
        rows.append(
            {
                "key": ds.source_key,
                "domain": ds.target_domain,
                "freq": ds.crawl_frequency or "-",
                "stale": stale,
                "hash": (ds.last_content_hash or "-")[:12],
            },
        )
    return rows


def _check_table_health(session: Session) -> list[dict[str, Any]]:
    rows = []
    for table, date_col in TABLE_CHECKS:
        try:
            count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            latest = session.execute(text(f"SELECT MAX({date_col}) FROM {table}")).scalar()
            rows.append(
                {
                    "table": table,
                    "rows": count,
                    "latest": str(latest or "-")[:20],
                },
            )
        except SQLAlchemyError as e:
            logger.warning("Health check table %s query failed: %s", table, e)
            rows.append({"table": table, "rows": "ERR", "latest": str(e)[:40]})
    return rows


def run_health_check() -> None:
    """Run health."""
    with SessionLocal() as session:
        ds_rows = _check_datasource_health(session)
        table_rows = _check_table_health(session)

    logger.info("=" * 60)
    logger.info(" KBO Pipeline Health Check")
    logger.info("=" * 60)
    logger.info(" Timestamp: %s", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("")

    stale_count = sum(1 for r in ds_rows if r["stale"].startswith("STALE"))
    never_count = sum(1 for r in ds_rows if r["stale"] == "NEVER")
    empty_count = sum(1 for r in table_rows if r["rows"] == 0 or r["rows"] == "ERR")

    logger.info(" DataSources: %s active (%s stale, %s never crawled)", len(ds_rows), stale_count, never_count)
    logger.info(" Tables: %s checked (%s issues)", len(table_rows), empty_count)
    logger.info("")

    logger.info("--- DataSources ---")
    logger.info("  %-30s %-12s %-10s %-20s %s", "Key", "Domain", "Freq", "Status", "Hash")
    logger.info("  %s %s %s %s %s", "-" * 30, "-" * 12, "-" * 10, "-" * 20, "-" * 12)
    for r in ds_rows:
        logger.info("  %-30s %-12s %-10s %-20s %s", r["key"], r["domain"], r["freq"], r["stale"], r["hash"])

    logger.info("")
    logger.info("--- Tables ---")
    logger.info("  %-30s %-10s %s", "Table", "Rows", "Latest")
    logger.info("  %s %s %s", "-" * 30, "-" * 10, "-" * 30)
    for r in table_rows:
        logger.info("  %-30s %-10s %s", r["table"], str(r["rows"]), r["latest"])

    logger.info("")
    if stale_count or never_count or empty_count:
        logger.info(" ⚠ %s stale, %s never crawled, %s table issues", stale_count, never_count, empty_count)
    else:
        logger.info(" ✓ All systems healthy")

    logger.info("=" * 60)


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build arg parser.

    Returns:
        The result of the operation.

    """
    return argparse.ArgumentParser(description="KBO pipeline health check")


def main(argv: Sequence[str] | None = None) -> None:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    parser.parse_args(argv)
    run_health_check()


if __name__ == "__main__":
    main()
