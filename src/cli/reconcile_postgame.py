"""CLI for postgame status/score reconciliation."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta
from typing import Sequence

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.services.player_id_resolver import PlayerIdResolver
from src.services.postgame_reconciliation_service import (
    format_reconciliation_report,
    reconcile_postgame_range,
    write_reconciliation_csv,
)
from src.utils.safe_print import safe_print as print


async def run_reconciliation(args: argparse.Namespace) -> int:
    start_date, end_date = _resolve_date_range(args)
    year = int(end_date[:4])

    session = SessionLocal()
    try:
        resolver = PlayerIdResolver(session)
        for season_year in range(int(start_date[:4]), year + 1):
            resolver.preload_season_index(season_year)

        crawler = GameDetailCrawler(request_delay=args.delay, resolver=resolver)
        result = await reconcile_postgame_range(
            start_date,
            end_date,
            detail_crawler=crawler,
            concurrency=args.concurrency,
            extra_game_ids=args.game_id,
            log=print,
        )
    finally:
        session.close()

    print(
        "[POSTGAME-RECONCILE] "
        f"range={result.start_date}-{result.end_date} "
        f"candidates={result.candidates} changed={len(result.changes)}"
    )
    print(format_reconciliation_report(result.changes))

    if args.output_csv:
        output_path = write_reconciliation_csv(result.changes, args.output_csv)
        print(f"[POSTGAME-RECONCILE] report_csv={output_path}")

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Revisit recently started KBO games and report rows whose final "
            "status or score changed during reconciliation."
        )
    )
    parser.add_argument("--date", help="Single target date in YYYYMMDD format")
    parser.add_argument("--start-date", help="Start date in YYYYMMDD format")
    parser.add_argument("--end-date", help="End date in YYYYMMDD format")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="When only --end-date/--date is supplied, include this many prior days.",
    )
    parser.add_argument(
        "--game-id",
        action="append",
        default=[],
        help="Force-include a specific game_id in addition to date-range candidates.",
    )
    parser.add_argument("--concurrency", type=int, default=1, help="Max concurrent detail crawls")
    parser.add_argument("--delay", type=float, default=1.0, help="Request delay for detail crawler")
    parser.add_argument("--output-csv", help="Optional CSV path for changed-game report")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(run_reconciliation(args))
    except ValueError as exc:
        parser.error(str(exc))
        return 2


def _resolve_date_range(args: argparse.Namespace) -> tuple[str, str]:
    if args.date and (args.start_date or args.end_date):
        raise ValueError("--date cannot be combined with --start-date/--end-date")

    if args.date:
        _validate_date(args.date)
        if args.lookback_days is None:
            return args.date, args.date
        end_day = datetime.strptime(args.date, "%Y%m%d").date()
        start_day = end_day - timedelta(days=max(0, args.lookback_days))
        return start_day.strftime("%Y%m%d"), args.date

    if args.start_date and args.end_date:
        _validate_date(args.start_date)
        _validate_date(args.end_date)
        return _ordered_range(args.start_date, args.end_date)

    if args.end_date and args.lookback_days is not None:
        _validate_date(args.end_date)
        end_day = datetime.strptime(args.end_date, "%Y%m%d").date()
        start_day = end_day - timedelta(days=max(0, args.lookback_days))
        return start_day.strftime("%Y%m%d"), args.end_date

    raise ValueError("Use --date, --start-date/--end-date, or --end-date with --lookback-days")


def _ordered_range(start_date: str, end_date: str) -> tuple[str, str]:
    start_day = datetime.strptime(start_date, "%Y%m%d").date()
    end_day = datetime.strptime(end_date, "%Y%m%d").date()
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    return start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d")


def _validate_date(value: str) -> None:
    if len(value) != 8 or not value.isdigit():
        raise ValueError(f"Invalid date format: {value}. Use YYYYMMDD.")
    datetime.strptime(value, "%Y%m%d")


if __name__ == "__main__":
    raise SystemExit(main())
