"""
Backfill missing pregame preview data for scheduled games.

This CLI finds scheduled game dates whose preview summaries or starting
pitcher fields are incomplete, then runs daily_preview_batch for those dates.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import text

from src.cli.daily_preview_batch import run_preview_batch
from src.db.engine import SessionLocal
from src.utils.safe_print import safe_print as print

KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class PregameBackfillDate:
    target_date: str
    scheduled_total: int
    starters_complete: int
    preview_rows: int


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


def find_missing_pregame_dates(
    *,
    start_date: str,
    end_date: str,
    include_complete: bool = False,
    limit_dates: int | None = None,
) -> list[PregameBackfillDate]:
    query = """
        SELECT
            REPLACE(CAST(g.game_date AS TEXT), '-', '') AS target_date,
            COUNT(*) AS scheduled_total,
            SUM(
                CASE
                    WHEN g.away_pitcher IS NOT NULL AND g.away_pitcher != ''
                     AND g.home_pitcher IS NOT NULL AND g.home_pitcher != ''
                    THEN 1 ELSE 0
                END
            ) AS starters_complete,
            SUM(CASE WHEN p.game_id IS NOT NULL THEN 1 ELSE 0 END) AS preview_rows
        FROM game g
        LEFT JOIN (
            SELECT DISTINCT game_id
            FROM game_summary
            WHERE summary_type = '프리뷰'
        ) p ON p.game_id = g.game_id
        WHERE UPPER(g.game_status) = 'SCHEDULED'
          AND REPLACE(CAST(g.game_date AS TEXT), '-', '') BETWEEN :start_date AND :end_date
        GROUP BY g.game_date
    """
    if not include_complete:
        query += """
        HAVING starters_complete < scheduled_total
            OR preview_rows < scheduled_total
        """
    query += " ORDER BY g.game_date"
    if limit_dates is not None:
        query += " LIMIT :limit_dates"

    params: dict[str, object] = {
        "start_date": start_date,
        "end_date": end_date,
    }
    if limit_dates is not None:
        params["limit_dates"] = limit_dates

    with SessionLocal() as session:
        rows = session.execute(text(query), params).all()

    return [
        PregameBackfillDate(
            target_date=str(row.target_date),
            scheduled_total=int(row.scheduled_total or 0),
            starters_complete=int(row.starters_complete or 0),
            preview_rows=int(row.preview_rows or 0),
        )
        for row in rows
    ]


def get_pregame_date_status(target_date: str) -> PregameBackfillDate | None:
    statuses = find_missing_pregame_dates(
        start_date=target_date,
        end_date=target_date,
        include_complete=True,
        limit_dates=1,
    )
    return statuses[0] if statuses else None


async def run_backfill(args: argparse.Namespace) -> int:
    start_date = args.start_date or _default_start_date()
    end_date = args.end_date or _default_end_date(args.days_ahead)
    targets = find_missing_pregame_dates(
        start_date=start_date,
        end_date=end_date,
        include_complete=args.include_complete,
        limit_dates=args.limit_dates,
    )

    if not targets:
        print(f"No missing scheduled pregame dates found between {start_date} and {end_date}.")
        return 0

    print(f"Pregame backfill targets ({start_date}..{end_date}): {len(targets)} date(s)")
    for target in targets:
        print(
            f"  {target.target_date}: "
            f"starters={target.starters_complete}/{target.scheduled_total}, "
            f"preview={target.preview_rows}/{target.scheduled_total}"
        )

    if args.dry_run:
        return 0

    failed: list[str] = []
    incomplete: list[str] = []
    saved_total = 0
    for target in targets:
        print(f"\nRunning pregame backfill for {target.target_date}...")
        saved_ids = await run_preview_batch(target.target_date, sync_to_oci=not args.no_sync)
        saved_count = len(saved_ids)
        saved_total += saved_count
        print(f"Backfill result for {target.target_date}: saved={saved_count}")
        if args.fail_on_empty and target.scheduled_total and saved_count == 0:
            failed.append(target.target_date)
        if args.fail_on_incomplete:
            refreshed = get_pregame_date_status(target.target_date)
            if refreshed and refreshed.starters_complete < refreshed.scheduled_total:
                incomplete.append(
                    f"{target.target_date}: "
                    f"starters={refreshed.starters_complete}/{refreshed.scheduled_total}, "
                    f"preview={refreshed.preview_rows}/{refreshed.scheduled_total}"
                )

    print(
        "\nPregame backfill finished. "
        f"saved_total={saved_total}, failed_empty={len(failed)}, incomplete={len(incomplete)}"
    )
    if failed:
        print("Dates with scheduled games but no preview rows saved:")
        for target_date in failed:
            print(f"  {target_date}")
    if incomplete:
        print("Dates still missing complete starting pitchers:")
        for target in incomplete:
            print(f"  {target}")
    if failed or incomplete:
        return 1
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill missing scheduled pregame previews")
    parser.add_argument("--start-date", type=_yyyymmdd, help="Start date YYYYMMDD. Defaults to today in KST.")
    parser.add_argument("--end-date", type=_yyyymmdd, help="End date YYYYMMDD. Defaults to today + --days-ahead.")
    parser.add_argument("--days-ahead", type=int, default=1, help="Default end-date offset when --end-date is omitted.")
    parser.add_argument("--limit-dates", type=int, help="Limit number of target dates to process.")
    parser.add_argument("--include-complete", action="store_true", help="Include dates already complete.")
    parser.add_argument("--dry-run", action="store_true", help="Only print target dates.")
    parser.add_argument("--no-sync", action="store_true", help="Skip OCI sync after local writes.")
    parser.add_argument(
        "--fail-on-empty",
        action="store_true",
        help="Exit non-zero when a scheduled date saves zero preview rows.",
    )
    parser.add_argument(
        "--fail-on-incomplete",
        action="store_true",
        help="Exit non-zero when starters are still incomplete after backfill.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    return asyncio.run(run_backfill(args))


if __name__ == "__main__":
    sys.exit(main())
