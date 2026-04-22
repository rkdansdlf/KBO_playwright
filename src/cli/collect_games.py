
"""
Unified Game Data Collector (Details + optional direct relay fallback)
"""
import asyncio
import argparse
from typing import Optional

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.naver_relay_crawler import NaverRelayCrawler
from src.db.engine import SessionLocal
from src.services.game_collection_service import (
    crawl_and_save_game_details,
    load_game_targets_from_db,
)
from src.utils.safe_print import safe_print as print


async def collect_games(year: int, month: Optional[int] = None, force: bool = False, concurrency: Optional[int] = None):
    """
    Collects game details and relay data for a given year/month.
    Iterates through games in the database for that period.
    """
    targets = load_game_targets_from_db(year, month)
    print(f"🎯 Target: {len(targets)} games for {year}" + (f"-{month}" if month else ""))

    session = SessionLocal()
    try:
        from src.services.player_id_resolver import PlayerIdResolver
        resolver = PlayerIdResolver(session)
        resolver.preload_season_index(year)

        detail_crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        relay_crawler = NaverRelayCrawler()

        result = await crawl_and_save_game_details(
            targets,
            detail_crawler=detail_crawler,
            relay_crawler=relay_crawler,
            force=force,
            concurrency=concurrency,
            pause_every=10,
            pause_seconds=2.0,
            log=print,
        )
        print(
            "[FINISH] "
            f"detail_saved={result.detail_saved} detail_failed={result.detail_failed} "
            f"detail_skipped={result.detail_skipped_existing} "
            f"relay_games={result.relay_saved_games} relay_rows={result.relay_rows_saved} "
            f"relay_skipped={result.relay_skipped_existing}"
        )
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Collect game details and direct Naver relay fallback rows. "
            "For completed-game relay recovery, prefer scripts/fetch_kbo_pbp.py."
        )
    )
    parser.add_argument("--year", type=int, required=True, help="Target Year (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Target Month (Optional)")
    parser.add_argument("--concurrency", type=int, default=None, help="Max concurrent game detail crawls")
    parser.add_argument("--force", action="store_true", help="Recrawl and overwrite existing detail/relay rows")
    args = parser.parse_args()
    
    asyncio.run(collect_games(args.year, args.month, force=args.force, concurrency=args.concurrency))

if __name__ == "__main__":
    main()
