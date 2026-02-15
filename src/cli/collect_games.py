
"""
Unified Game Data Collector (Details + Relay + Summary)
"""
import asyncio
import argparse
from typing import List, Optional
from datetime import date

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.db.engine import SessionLocal
from src.models.game import Game
from src.utils.safe_print import safe_print as print


def _build_game_id_range(year: int, month: Optional[int]) -> tuple[str, str]:
    if month:
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1)
        else:
            end = date(year, month + 1, 1)
    else:
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


async def collect_games(year: int, month: Optional[int] = None, force: bool = False, concurrency: Optional[int] = None):
    """
    Collects game details and relay data for a given year/month.
    Iterates through games in the database for that period.
    """
    session = SessionLocal()
    try:
        start_id, end_id = _build_game_id_range(year, month)
        query = session.query(Game).filter(Game.game_id >= start_id, Game.game_id < end_id)

        games = query.all()
        print(f"🎯 Target: {len(games)} games for {year}" + (f"-{month}" if month else ""))
        
        # Initialize Resolver
        from src.services.player_id_resolver import PlayerIdResolver
        resolver = PlayerIdResolver(session)
        resolver.preload_season_index(year)
        
        detail_crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        relay_crawler = RelayCrawler(request_delay=1.0)
        
        inputs = [
            {
                "game_id": game.game_id,
                "game_date": game.game_date.strftime("%Y%m%d"),
            }
            for game in games
        ]

        detail_payloads = await detail_crawler.crawl_games(inputs, concurrency=concurrency)
        success_count = 0

        for idx, detail_data in enumerate(detail_payloads, 1):
            game_id = detail_data.get("game_id")
            game_date = detail_data.get("game_date")
            print(f"[{idx}/{len(detail_payloads)}] Saving {game_id} ({game_date})...")
            saved = save_game_detail(detail_data)
            if saved:
                print("   ✅ Details saved")
            else:
                print("   ⚠️ Details save failed")

            # 2. Relay (Play-by-play) - Temporarily Disabled due to Selector timeouts
            print(f"   ⚠️ Relay crawler disabled (selector issue)")

            success_count += 1
            if idx % 10 == 0:
                print("⏸️  Pausing briefly...")
                await asyncio.sleep(2)
                
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Collect Game Details & Relay")
    parser.add_argument("--year", type=int, required=True, help="Target Year (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Target Month (Optional)")
    parser.add_argument("--concurrency", type=int, default=None, help="Max concurrent game detail crawls")
    args = parser.parse_args()
    
    asyncio.run(collect_games(args.year, args.month, concurrency=args.concurrency))

if __name__ == "__main__":
    main()
