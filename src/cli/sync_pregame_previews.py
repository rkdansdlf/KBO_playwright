"""
Sync collected pregame preview rows for scheduled games to OCI/Postgres.

This is intentionally scoped to games that already have local preview summaries
or registered starting pitchers, so backfilled pregame data can be published
without pushing an entire season.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from sqlalchemy import text

from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.safe_print import safe_print as print

KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class PregameSyncTarget:
    game_date: str
    game_id: str
    away_pitcher: str
    home_pitcher: str
    has_preview: bool


def _yyyymmdd(value: str) -> str:
    normalized = value.replace("-", "")
    if len(normalized) != 8 or not normalized.isdigit():
        raise argparse.ArgumentTypeError(f"Invalid date: {value}. Use YYYYMMDD.")
    datetime.strptime(normalized, "%Y%m%d")
    return normalized


def _default_start_date() -> str:
    return datetime.now(KST).strftime("%Y%m%d")


def _default_end_date(days_ahead: int) -> str:
    return (datetime.now(KST).date() + timedelta(days=days_ahead)).strftime("%Y%m%d")


def find_pregame_sync_targets(start_date: str, end_date: str) -> list[PregameSyncTarget]:
    query = text(
        """
        SELECT
            REPLACE(CAST(g.game_date AS TEXT), '-', '') AS game_date,
            g.game_id,
            COALESCE(g.away_pitcher, '') AS away_pitcher,
            COALESCE(g.home_pitcher, '') AS home_pitcher,
            CASE WHEN p.game_id IS NULL THEN 0 ELSE 1 END AS has_preview
        FROM game g
        LEFT JOIN (
            SELECT DISTINCT game_id
            FROM game_summary
            WHERE summary_type = '프리뷰'
        ) p ON p.game_id = g.game_id
        WHERE REPLACE(CAST(g.game_date AS TEXT), '-', '') BETWEEN :start_date AND :end_date
          AND (
            p.game_id IS NOT NULL
            OR (g.away_pitcher IS NOT NULL AND g.away_pitcher != '')
            OR (g.home_pitcher IS NOT NULL AND g.home_pitcher != '')
          )
        ORDER BY g.game_date, g.game_id
        """
    )
    with SessionLocal() as session:
        rows = session.execute(query, {"start_date": start_date, "end_date": end_date}).all()

    return [
        PregameSyncTarget(
            game_date=str(row.game_date),
            game_id=str(row.game_id),
            away_pitcher=str(row.away_pitcher or ""),
            home_pitcher=str(row.home_pitcher or ""),
            has_preview=bool(row.has_preview),
        )
        for row in rows
    ]


def run_sync(args: argparse.Namespace) -> int:
    load_dotenv()
    target_url = args.target_url or os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not target_url:
        raise SystemExit("OCI_DB_URL or TARGET_DATABASE_URL is required")

    start_date = args.start_date or _default_start_date()
    end_date = args.end_date or _default_end_date(args.days_ahead)
    targets = find_pregame_sync_targets(start_date, end_date)

    if not targets:
        print(f"No local pregame preview targets found between {start_date} and {end_date}.")
        return 0

    complete_starters = sum(1 for target in targets if target.away_pitcher and target.home_pitcher)
    preview_rows = sum(1 for target in targets if target.has_preview)
    print(
        f"Pregame sync targets ({start_date}..{end_date}): "
        f"games={len(targets)}, starters_complete={complete_starters}, previews={preview_rows}"
    )
    for target in targets:
        print(
            f"  {target.game_date} {target.game_id}: "
            f"starter='{target.away_pitcher}' vs '{target.home_pitcher}', "
            f"preview={int(target.has_preview)}"
        )

    if args.dry_run:
        return 0

    synced = 0
    with SessionLocal() as session:
        syncer = OCISync(target_url, session)
        try:
            for target in targets:
                result = syncer.sync_specific_game(target.game_id)
                synced += 1
                print(f"Synced {target.game_id}: {result}")
        finally:
            syncer.close()

    print(f"Pregame preview sync finished. synced_games={synced}")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync collected pregame previews to OCI/Postgres")
    parser.add_argument("--start-date", type=_yyyymmdd, help="Start date YYYYMMDD. Defaults to today in KST.")
    parser.add_argument("--end-date", type=_yyyymmdd, help="End date YYYYMMDD. Defaults to today + --days-ahead.")
    parser.add_argument("--days-ahead", type=int, default=1, help="Default end-date offset when --end-date is omitted.")
    parser.add_argument("--target-url", help="Target DB URL. Defaults to OCI_DB_URL or TARGET_DATABASE_URL.")
    parser.add_argument("--dry-run", action="store_true", help="Only print target games.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    return run_sync(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
