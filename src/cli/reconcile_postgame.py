"""CLI for postgame status/score reconciliation."""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.db.engine import SessionLocal
from src.services.player_id_resolver import PlayerIdResolver
from src.services.postgame_reconciliation_service import (
    format_reconciliation_report,
    reconcile_postgame_range,
    write_reconciliation_csv,
)
from src.utils.date_helpers import parse_date_str, parse_datetime_str

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


async def run_reconciliation(args: argparse.Namespace) -> int:
    """Runs reconciliation.

    Args:
        args: Args.

    Returns:
        Integer result.

    """
    start_date, end_date = _resolve_date_range(args)
    year = int(end_date[:4])

    session = SessionLocal()
    try:
        resolver = PlayerIdResolver(
            session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        for season_year in range(int(start_date[:4]), year + 1):
            resolver.preload_season_index(season_year)

        crawler = GameDetailCrawler(request_delay=args.delay, resolver=resolver)
        result = await reconcile_postgame_range(
            start_date,
            end_date,
            detail_crawler=crawler,
            concurrency=args.concurrency,
            extra_game_ids=args.game_id,
            log=logger.info,
        )
    finally:
        session.close()

    logger.info(
        "[POSTGAME-RECONCILE] range=%s-%s candidates=%s changed=%s",
        result.start_date,
        result.end_date,
        result.candidates,
        len(result.changes),
    )
    logger.info(format_reconciliation_report(result.changes))

    if args.output_csv:
        output_path = write_reconciliation_csv(result.changes, args.output_csv)
        logger.info("[POSTGAME-RECONCILE] report_csv=%s", output_path)

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    """Builds arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(
        description=(
            "Revisit recently started KBO games and report rows whose final "
            "status or score changed during reconciliation."
        ),
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
    """Main entry point for this CLI command."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(run_reconciliation(args))
    except ValueError as exc:
        parser.error(str(exc))
        return 2


def _resolve_date_range(args: argparse.Namespace) -> tuple[str, str]:
    if args.date and (args.start_date or args.end_date):
        msg = "--date cannot be combined with --start-date/--end-date"
        raise ValueError(msg)

    if args.date:
        _validate_date(args.date)
        if args.lookback_days is None:
            return args.date, args.date
        end_day = parse_date_str(args.date)
        start_day = end_day - timedelta(days=max(0, args.lookback_days))
        return start_day.strftime("%Y%m%d"), args.date

    if args.start_date and args.end_date:
        _validate_date(args.start_date)
        _validate_date(args.end_date)
        return _ordered_range(args.start_date, args.end_date)

    if args.end_date and args.lookback_days is not None:
        _validate_date(args.end_date)
        end_day = parse_date_str(args.end_date)
        start_day = end_day - timedelta(days=max(0, args.lookback_days))
        return start_day.strftime("%Y%m%d"), args.end_date

    msg = "Use --date, --start-date/--end-date, or --end-date with --lookback-days"
    raise ValueError(msg)


def _ordered_range(start_date: str, end_date: str) -> tuple[str, str]:
    start_day = parse_date_str(start_date)
    end_day = parse_date_str(end_date)
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    return start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d")


def _validate_date(value: str) -> None:
    if len(value) != 8 or not value.isdigit():
        msg = f"Invalid date format: {value}. Use YYYYMMDD."
        raise ValueError(msg)
    parse_datetime_str(value)


if __name__ == "__main__":
    raise SystemExit(main())
