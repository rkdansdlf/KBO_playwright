"""
CLI entrypoint for retired/inactive player crawling.
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Iterable, Set

from src.crawlers.retire import RetiredPlayerListingCrawler, RetiredPlayerDetailCrawler
from src.parsers.player_profile_parser import parse_profile, PlayerProfileParsed
from src.parsers.retired_player_parser import (
    parse_retired_hitter_tables,
    parse_retired_pitcher_table,
)
from src.repositories.player_repository import PlayerRepository


async def determine_inactive_ids(
    start_year: int,
    end_year: int,
    active_year: int,
    request_delay: float,
) -> Set[str]:
    listing_crawler = RetiredPlayerListingCrawler(request_delay=request_delay)
    return await listing_crawler.determine_inactive_player_ids(
        start_year=start_year,
        end_year=end_year,
        active_year=active_year,
    )


async def process_player(
    player_id: str,
    detail_crawler: RetiredPlayerDetailCrawler,
    repository: PlayerRepository,
) -> None:
    detail_payload = await detail_crawler.fetch_player(player_id)
    hitter_payload = detail_payload.get("hitter")
    pitcher_payload = detail_payload.get("pitcher")

    profile_text = None
    if hitter_payload:
        profile_text = hitter_payload.get("profile_text")
    if not profile_text and pitcher_payload:
        profile_text = pitcher_payload.get("profile_text")

    if profile_text:
        parsed_profile = parse_profile(profile_text, is_active=False)
    else:
        parsed_profile = PlayerProfileParsed(is_active=False)

    player = await asyncio.to_thread(
        repository.upsert_player_profile,
        player_id,
        parsed_profile,
    )

    if not player:
        return

    if hitter_payload:
        batting_records = parse_retired_hitter_tables(hitter_payload.get("tables", []))
        for record in batting_records:
            await asyncio.to_thread(repository.upsert_season_batting, player.id, record)

    if pitcher_payload:
        tables = pitcher_payload.get("tables", [])
        if tables:
            pitching_records = parse_retired_pitcher_table(tables[0])
            for record in pitching_records:
                await asyncio.to_thread(repository.upsert_season_pitching, player.id, record)


async def crawl_retired_players(args: argparse.Namespace) -> None:
    inactive_ids = await determine_inactive_ids(
        start_year=args.start_year,
        end_year=args.end_year,
        active_year=args.active_year or args.end_year,
        request_delay=args.delay,
    )

    inactive_list = sorted(inactive_ids)
    if args.limit:
        inactive_list = inactive_list[: args.limit]

    print(f"📋 Retired candidates: {len(inactive_list)}")
    if not inactive_list:
        return

    detail_crawler = RetiredPlayerDetailCrawler(request_delay=args.delay)
    repository = PlayerRepository()
    semaphore = asyncio.Semaphore(args.concurrency)

    async def runner(pid: str):
        async with semaphore:
            try:
                await process_player(pid, detail_crawler, repository)
                print(f"✅ Processed retired player {pid}")
            except Exception as exc:
                print(f"❌ Failed to process player {pid}: {exc}")

    await asyncio.gather(*(runner(pid) for pid in inactive_list))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retired player crawling pipeline")
    parser.add_argument("--start-year", type=int, default=1982)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--active-year", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--delay", type=float, default=1.5, help="Request delay between Playwright navigations")
    parser.add_argument("--limit", type=int, default=None, help="Only process first N players (for debugging)")
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_retired_players(args))


if __name__ == "__main__":
    main()
