"""CLI entrypoint for crawling GameCenter box scores."""
from __future__ import annotations

import argparse
import asyncio
from typing import Iterable, List

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import GameRepository


async def crawl_game_details(args: argparse.Namespace) -> None:
    repo = GameRepository()
    schedules = repo.fetch_schedules(status=args.status, limit=args.limit)

    if not schedules:
        print("ℹ️  No schedules found for crawl")
        return

    print(f"📋 Games to crawl: {len(schedules)}")

    inputs = []
    for sched in schedules:
        repo.update_crawl_status(sched.game_id, 'in_progress')
        game_date = sched.game_date.strftime('%Y%m%d') if sched.game_date else sched.game_id[:8]
        inputs.append({'game_id': sched.game_id, 'game_date': game_date})

    crawler = GameDetailCrawler(request_delay=args.delay)
    results = await crawler.crawl_games(inputs)

    fetched_ids = {payload['game_id'] for payload in results}

    for payload in results:
        repo.save_game_detail(payload)

    missing = [g for g in schedules if g.game_id not in fetched_ids]
    for sched in missing:
        repo.update_crawl_status(sched.game_id, 'failed', 'Crawler returned no data')


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl KBO GameCenter details")
    parser.add_argument("--status", type=str, default="pending", help="Schedule crawl_status to target")
    parser.add_argument("--limit", type=int, default=10, help="Number of games to crawl")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between HTTP navigations")
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_game_details(args))


if __name__ == "__main__":  # pragma: no cover
    main()

