"""CLI entrypoint for Futures League stat collection."""
from __future__ import annotations

import argparse
import asyncio
from typing import Iterable, Set

from datetime import datetime

from src.crawlers.player_list_crawler import PlayerListCrawler
from src.crawlers.futures import FuturesProfileCrawler
from src.parsers.player_profile_parser import parse_profile, PlayerProfileParsed
from src.parsers.futures_stats_parser import parse_futures_tables
from src.repositories.player_repository import PlayerRepository


async def gather_active_player_ids(season_year: int, delay: float) -> Set[str]:
    crawler = PlayerListCrawler(request_delay=delay)
    result = await crawler.crawl_all_players(season_year=season_year)
    ids: Set[str] = set()
    for bucket in ("hitters", "pitchers"):
        for player in result.get(bucket, []):
            pid = player.get("player_id")
            if pid:
                ids.add(pid)
    return ids


async def process_player(
    player_id: str,
    futures_crawler: FuturesProfileCrawler,
    repository: PlayerRepository,
) -> None:
    payload = await futures_crawler.fetch_player_futures(player_id)
    tables = payload.get("tables", [])
    if not tables:
        print(f"ℹ️  No Futures tables for {player_id}")
        return

    profile_text = payload.get("profile_text")
    if profile_text:
        parsed_profile = parse_profile(profile_text, is_active=True)
    else:
        parsed_profile = PlayerProfileParsed(is_active=True)

    player = await asyncio.to_thread(
        repository.upsert_player_profile,
        player_id,
        parsed_profile,
    )

    if not player:
        return

    stats = parse_futures_tables(tables)
    for record in stats.get("batting", []):
        await asyncio.to_thread(repository.upsert_season_batting, player.id, record)

    for record in stats.get("pitching", []):
        await asyncio.to_thread(repository.upsert_season_pitching, player.id, record)

    print(f"✅ Futures updated for player {player_id}")


async def crawl_futures(args: argparse.Namespace) -> None:
    player_ids = await gather_active_player_ids(args.season, args.delay)
    if args.limit:
        player_ids = set(sorted(player_ids)[: args.limit])

    print(f"📋 Active players to process: {len(player_ids)}")
    if not player_ids:
        return

    futures_crawler = FuturesProfileCrawler(request_delay=args.delay)
    repository = PlayerRepository()
    semaphore = asyncio.Semaphore(args.concurrency)

    async def runner(pid: str):
        async with semaphore:
            try:
                await process_player(pid, futures_crawler, repository)
            except Exception as exc:
                print(f"❌ Futures crawl error for {pid}: {exc}")

    await asyncio.gather(*(runner(pid) for pid in sorted(player_ids)))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Futures League synchronization")
    parser.add_argument(
        "--season",
        type=int,
        default=datetime.now().year,
        help="Season year to crawl (defaults to current year)",
    )
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--limit", type=int, default=None)
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_futures(args))


if __name__ == "__main__":
    main()
