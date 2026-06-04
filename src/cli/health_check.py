from __future__ import annotations

import argparse
import logging
from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import text

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository
from src.utils.safe_print import safe_print as print

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
    ("team_standing", "snapshot_date"),
]


def _check_datasource_health(session) -> list[dict[str, Any]]:
    ds_repo = DataSourceRepository(session)
    rows = []
    for ds in ds_repo.get_all_active():
        stale = ""
        if ds.last_success_at:
            hours_since = (datetime.utcnow() - ds.last_success_at).total_seconds() / 3600
            if hours_since > 48:
                stale = f"STALE ({hours_since:.0f}h)"
            else:
                stale = f"ok ({hours_since:.0f}h ago)"
        else:
            stale = "NEVER"
        rows.append(
            {
                "key": ds.source_key,
                "domain": ds.target_domain,
                "freq": ds.crawl_frequency or "-",
                "stale": stale,
                "hash": (ds.last_content_hash or "-")[:12],
            }
        )
    return rows


def _check_table_health(session) -> list[dict[str, Any]]:
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
                }
            )
        except Exception as e:
            rows.append({"table": table, "rows": "ERR", "latest": str(e)[:40]})
    return rows


def run_health_check() -> None:
    with SessionLocal() as session:
        ds_rows = _check_datasource_health(session)
        table_rows = _check_table_health(session)

    print("=" * 60)
    print(" KBO Pipeline Health Check")
    print("=" * 60)
    print(f" Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    stale_count = sum(1 for r in ds_rows if r["stale"].startswith("STALE"))
    never_count = sum(1 for r in ds_rows if r["stale"] == "NEVER")
    empty_count = sum(1 for r in table_rows if r["rows"] == 0 or r["rows"] == "ERR")

    print(f" DataSources: {len(ds_rows)} active ({stale_count} stale, {never_count} never crawled)")
    print(f" Tables: {len(table_rows)} checked ({empty_count} issues)")
    print()

    print("--- DataSources ---")
    print(f"  {'Key':<30} {'Domain':<12} {'Freq':<10} {'Status':<20} {'Hash'}")
    print(f"  {'-' * 30} {'-' * 12} {'-' * 10} {'-' * 20} {'-' * 12}")
    for r in ds_rows:
        print(f"  {r['key']:<30} {r['domain']:<12} {r['freq']:<10} {r['stale']:<20} {r['hash']}")

    print()
    print("--- Tables ---")
    print(f"  {'Table':<30} {'Rows':<10} {'Latest'}")
    print(f"  {'-' * 30} {'-' * 10} {'-' * 30}")
    for r in table_rows:
        print(f"  {r['table']:<30} {str(r['rows']):<10} {r['latest']}")

    print()
    if stale_count or never_count or empty_count:
        print(f" ⚠ {stale_count} stale, {never_count} never crawled, {empty_count} table issues")
    else:
        print(" ✓ All systems healthy")

    print("=" * 60)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO pipeline health check")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    parser.parse_args(argv)
    run_health_check()


if __name__ == "__main__":
    main()
