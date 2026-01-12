
"""
Unified Game Data Collector (Details + Relay + Summary)
"""
import asyncio
import argparse
from typing import List, Optional
from datetime import datetime, timedelta

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.db.engine import SessionLocal
from src.models.game import Game
from src.utils.safe_print import safe_print as print

async def collect_games(year: int, month: Optional[int] = None, force: bool = False):
    """
    Collects game details and relay data for a given year/month.
    Iterates through games in the database for that period.
    """
    session = SessionLocal()
    try:
        query = session.query(Game).filter(Game.season_id >= year * 10, Game.season_id <= year * 10 + 5)
        
        if month:
            # Filter by month (naive string check on game_id or proper date check)
            # Game ID format: YYYYMMDD...
            # A bit loose but effective for filtering ID
            start_date_str = f"{year}{month:02d}01"
            # Calculate end date of month roughly
            if month == 12:
                end_date_str = f"{year+1}0101"
            else:
                end_date_str = f"{year}{month+1:02d}01"
            
            # Using game_id prefix match
            query = query.filter(Game.game_id >= start_date_str, Game.game_id < end_date_str)
            
        games = query.all()
        print(f"🎯 Target: {len(games)} games for {year}" + (f"-{month}" if month else ""))
        
        # Initialize Resolver
        from src.services.player_id_resolver import PlayerIdResolver
        resolver = PlayerIdResolver(session)
        
        detail_crawler = GameDetailCrawler(request_delay=1.0, resolver=resolver)
        relay_crawler = RelayCrawler(request_delay=1.0)
        
        success_count = 0
        
        for idx, game in enumerate(games, 1):
            print(f"[{idx}/{len(games)}] Processing {game.game_id} ({game.game_date})...")
            
            # 1. Game Detail (Box Score)
            try:
                # We pass game_date string YYYYMMDD
                date_str = game.game_date.strftime("%Y%m%d")
                detail_data = await detail_crawler.crawl_game(game.game_id, date_str)
                if detail_data:
                    saved = save_game_detail(detail_data)
                    if saved:
                        print(f"   ✅ Details saved")
                    else:
                        print(f"   ⚠️ Details save failed")
                else:
                    print(f"   ❌ No detail data found")
            except Exception as e:
                print(f"   ❌ Error crawling details: {e}")

            # 2. Relay (Play-by-play) - Temporarily Disabled due to Selector timeouts
            # try:
            #     date_str = game.game_date.strftime("%Y%m%d")
            #     relay_data = await relay_crawler.crawl_game_relay(game.game_id, date_str)
            #     ...
            print(f"   ⚠️ Relay crawler disabled (selector issue)")

            success_count += 1
            if idx % 10 == 0:
                print(f"⏸️  Pausing briefly...")
                await asyncio.sleep(2)
                
    finally:
        session.close()

def main():
    parser = argparse.ArgumentParser(description="Collect Game Details & Relay")
    parser.add_argument("--year", type=int, required=True, help="Target Year (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Target Month (Optional)")
    args = parser.parse_args()
    
    asyncio.run(collect_games(args.year, args.month))

if __name__ == "__main__":
    main()
