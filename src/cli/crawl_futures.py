"""
CLI for Futures League year-by-year batting stats collection.
Uses the new futures_batting crawler (HitterTotal pages).
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Sequence, Set
from datetime import datetime

from src.crawlers.player_list_crawler import PlayerListCrawler
from src.crawlers.futures_batting import fetch_and_parse_futures_batting
from src.repositories.player_repository import PlayerRepository
from src.repositories.save_futures_batting import save_futures_batting
from src.parsers.player_profile_parser import PlayerProfileParsed
from src.utils.safe_print import safe_print as print


async def gather_active_player_ids(season_year: int, delay: float) -> Set[str]:
    """Get list of all active players for given season."""
    print(f"Gathering active player list for {season_year}...")
    crawler = PlayerListCrawler(request_delay=delay)
    result = await crawler.crawl_all_players(season_year=season_year)

    ids: Set[str] = set()
    for bucket in ("hitters", "pitchers"):
        for player in result.get(bucket, []):
            pid = player.get("player_id")
            if pid:
                ids.add(pid)

    print(f"Found {len(ids)} active players")
    return ids


async def process_player(
    player_id: str,
    repository: PlayerRepository,
    delay: float,
) -> tuple[str, int]:
    """
    Process one player: crawl Futures stats and save to DB.

    Returns:
        (player_id, records_saved)
    """
    # Construct URL for year-by-year Futures stats
    profile_url = f"https://www.koreabaseball.com/Futures/Player/HitterTotal.aspx?playerId={player_id}"

    # Crawl and parse
    rows = await fetch_and_parse_futures_batting(player_id, profile_url)

    if not rows:
        return (player_id, 0)

    # Get or create player in DB
    player = await asyncio.to_thread(
        repository.upsert_player_profile,
        player_id,
        PlayerProfileParsed(is_active=True)
    )

    if not player:
        print(f"[WARN] Could not create player record for {player_id}")
        return (player_id, 0)

    # Save stats
    saved = await asyncio.to_thread(
        save_futures_batting,
        player.id,
        rows
    )

    return (player_id, saved)


async def crawl_futures(args: argparse.Namespace) -> None:
    """Main crawl logic."""
    print(f"\n=== Futures League Batting Stats Crawler ===")
    print(f"Season: {args.season}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Delay: {args.delay}s\n")

    # Step 1: Get player IDs
    player_ids = await gather_active_player_ids(args.season, args.delay)

    if args.limit:
        player_ids = set(sorted(player_ids)[:args.limit])
        print(f"Limited to {len(player_ids)} players\n")

    if not player_ids:
        print("No players to process")
        return

    # Step 2: Process players
    print(f"Processing {len(player_ids)} players...\n")

    repository = PlayerRepository()
    semaphore = asyncio.Semaphore(args.concurrency)

    results = []
    errors = []

    async def runner(pid: str):
        async with semaphore:
            try:
                result = await process_player(pid, repository, args.delay)
                results.append(result)

                player_id, saved = result
                if saved > 0:
                    print(f"[OK] {player_id}: {saved} seasons")
                else:
                    print(f"[SKIP] {player_id}: no Futures data")

            except Exception as exc:
                errors.append((pid, str(exc)))
                print(f"[ERROR] {pid}: {exc}")

    await asyncio.gather(*(runner(pid) for pid in sorted(player_ids)))

    # Step 3: Summary
    print(f"\n=== Summary ===")
    total_saved = sum(saved for _, saved in results)
    success_count = sum(1 for _, saved in results if saved > 0)

    print(f"Total players processed: {len(results)}")
    print(f"Players with Futures data: {success_count}")
    print(f"Total seasons saved: {total_saved}")
    print(f"Errors: {len(errors)}")

    if errors:
        print("\nErrors:")
        for pid, err in errors[:10]:  # Show first 10
            print(f"  {pid}: {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crawl year-by-year Futures batting stats for active players"
    )
    parser.add_argument(
        "--season",
        type=int,
        default=datetime.now().year,
        help="Season year (default: current year)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of concurrent requests (default: 3)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Delay between requests in seconds (default: 2.0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of players to process (for testing)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    asyncio.run(crawl_futures(args))


if __name__ == "__main__":
    main()
