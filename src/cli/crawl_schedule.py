"""CLI for season schedule ingestion."""
from __future__ import annotations

import argparse
import asyncio
from typing import Iterable, List

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.repositories.game_repository import GameRepository


async def crawl_schedule(args: argparse.Namespace) -> None:
    months = parse_months(args.months)
    crawler = ScheduleCrawler(request_delay=args.delay)
    games = await crawler.crawl_season(args.year, months)
    print(f"📅 Total games discovered: {len(games)}")

    repo = GameRepository()
    repo.save_schedules(games)


def parse_months(months_arg: str | None) -> List[int]:
    if not months_arg:
        return list(range(3, 11))
    parts = [p.strip() for p in months_arg.split(',') if p.strip()]
    months: List[int] = []
    for part in parts:
        if '-' in part:
            start, end = part.split('-', 1)
            try:
                start_m = int(start)
                end_m = int(end)
            except ValueError:
                continue
            months.extend(range(start_m, end_m + 1))
        else:
            try:
                months.append(int(part))
            except ValueError:
                continue
    return sorted(set(months))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KBO season schedule crawler")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g. 2025)")
    parser.add_argument("--months", type=str, default=None, help="Comma separated months or ranges (e.g. 3-10)")
    parser.add_argument("--delay", type=float, default=1.2, help="Delay between page navigations")
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_schedule(args))


if __name__ == "__main__":  # pragma: no cover
    main()

