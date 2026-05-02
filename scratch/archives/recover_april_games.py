from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import get_games_by_date, save_game_detail


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d")


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end date must be greater than or equal to start date")

    days = (end - start).days
    return [(start + timedelta(days=offset)).strftime("%Y%m%d") for offset in range(days + 1)]


def resolve_target_dates(args: argparse.Namespace) -> list[str]:
    if args.dates:
        return [date.strip() for date in args.dates.split(",") if date.strip()]
    if args.start_date and args.end_date:
        return _date_range(args.start_date, args.end_date)
    raise ValueError("provide --dates or both --start-date and --end-date")


async def recover_dates(target_dates: list[str], request_delay: float) -> None:
    crawler = GameDetailCrawler(request_delay=request_delay)

    for date_str in target_dates:
        print(f"--- Processing Date: {date_str} ---")
        games = get_games_by_date(date_str)
        if not games:
            print(f"No games found for {date_str} in local DB.")
            continue

        for game in games:
            game_id = game.game_id
            game_date_formatted = game.game_date.strftime("%Y-%m-%d")
            print(f"Crawling {game_id} ({game_date_formatted})...")
            detail = await crawler.crawl_game(game_id, game_date_formatted, lightweight=False)
            if detail:
                if save_game_detail(detail):
                    print(f"✅ Saved Game Detail for {game_id}")
                else:
                    print(f"❌ Failed to save Game Detail for {game_id}")
            else:
                print(f"⚠️ Could not fetch detail for {game_id}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recover saved game details from local schedule rows.")
    parser.add_argument("--dates", help="Comma-separated dates in YYYYMMDD format")
    parser.add_argument("--start-date", help="Start date in YYYYMMDD format")
    parser.add_argument("--end-date", help="End date in YYYYMMDD format")
    parser.add_argument("--delay", type=float, default=0.5, help="Request delay in seconds")
    return parser


async def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    target_dates = resolve_target_dates(args)
    await recover_dates(target_dates, args.delay)


if __name__ == "__main__":
    asyncio.run(main())
