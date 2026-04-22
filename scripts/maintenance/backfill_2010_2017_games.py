import asyncio
import argparse
import sys
import os
from datetime import datetime
from typing import List

# Add project root to path
sys.path.append(os.getcwd())

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import Game
from src.db.engine import SessionLocal
from src.services.game_collection_service import crawl_and_save_game_details
from src.services.schedule_collection_service import save_schedule_games
from src.utils.series_validation import get_available_series_by_year
from src.utils.safe_print import safe_print as print

async def backfill_year(year: int, series_list: List[str] = None):
    """
    Backfill game data for a single year.
    """
    print(f"\n" + "="*60)
    print(f"🚀 Starting Backfill for Year: {year}")
    print("="*60)
    
    if not series_list:
        series_list = get_available_series_by_year(year)
        # Exclude exhibition for now to save time/resources, focus on regular and postseason
        series_list = [s for s in series_list if s != 'exhibition']
    
    schedule_crawler = ScheduleCrawler(request_delay=1.0)
    detail_crawler = GameDetailCrawler(request_delay=1.5)
    
    all_game_ids = []
    
    # Step 1: Collect Schedules
    print(f"\n📅 Phase 1: Collecting Schedules for {year}...")
    
    # Map internal series names to KBO Series IDs (based on 2010 debug)
    # 0,9,6: Regular Season
    # 3,4,5,7: Postseason (Korean Series, Playoff, Semi-playoff, Wildcard)
    # 1: Exhibition
    
    kbo_series_ids = []
    if any(s == 'regular' for s in series_list):
        kbo_series_ids.append("0,9,6")
    
    if any(s in ['korean_series', 'playoff', 'semi_playoff', 'wildcard'] for s in series_list):
        kbo_series_ids.append("3,4,5,7")
        
    # Remove duplicates
    kbo_series_ids = list(set(kbo_series_ids))
    
    for sid in kbo_series_ids:
        try:
            print(f"  🔍 Crawling Series ID: {sid}...")
            games = await schedule_crawler.crawl_season(year, months=list(range(3, 12)), series_id=sid)
            print(f"  ✅ Found {len(games)} games for Series {sid}")
            
            result = save_schedule_games(games, log=print)
            if result.failed:
                print(f"  ⚠️ Schedule save failed for {result.failed} rows")
            for g in result.saved_games:
                all_game_ids.append((g['game_id'], g['game_date']))
        except Exception as e:
            print(f"  ❌ Error collecting schedule for {year} (Series {sid}): {e}")

    # Remove duplicates from all_game_ids
    unique_games = []
    seen_ids = set()
    for gid, gdate in all_game_ids:
        if gid not in seen_ids:
            unique_games.append({'game_id': gid, 'game_date': gdate})
            seen_ids.add(gid)
            
    print(f"📊 Total unique games to scrape: {len(unique_games)}")
    
    # Step 2: Scrape Details
    print(f"\n🎮 Phase 2: Scraping Game Details for {year}...")
    
    # Filter out already completed games if needed, but for backfill we usually want all
    # For efficiency, we use crawl_games which handles concurrency
    success_count = 0
    
    # Process in smaller chunks to avoid memory/connection issues
    chunk_size = 20
    for i in range(0, len(unique_games), chunk_size):
        chunk = unique_games[i:i + chunk_size]
        print(f"  📦 Processing chunk {i//chunk_size + 1}/{(len(unique_games)-1)//chunk_size + 1} ({len(chunk)} games)...")
        
        pending_chunk = []
        for game in chunk:
            gid = game['game_id']
            
            # Check if already has scores (skip if already done)
            with SessionLocal() as session:
                existing = session.query(Game).filter(Game.game_id == gid).one_or_none()
                if existing and existing.home_score is not None:
                    # print(f"    ⏭️  Skipping {gid} (already has score)")
                    success_count += 1
                    continue
            pending_chunk.append(game)

        if pending_chunk:
            result = await crawl_and_save_game_details(
                pending_chunk,
                detail_crawler=detail_crawler,
                force=True,
                concurrency=1,
                log=print,
            )
            success_count += result.detail_saved
            for item in result.items.values():
                if not item.detail_saved:
                    print(f"    ❌ Failed to crawl/save {item.game_id} (reason={item.failure_reason or 'unknown'})")
    
    print(f"\n🏁 Finished {year}: {success_count}/{len(unique_games)} games completed.")

async def main():
    parser = argparse.ArgumentParser(description="Backfill KBO game data for 2010-2017")
    parser.add_argument("--year", type=int, help="Specific year to backfill")
    parser.add_argument("--start", type=int, default=2010, help="Start year (default: 2010)")
    parser.add_argument("--end", type=int, default=2017, help="End year (default: 2017)")
    
    args = parser.parse_args()
    
    if args.year:
        years = [args.year]
    else:
        years = list(range(args.start, args.end + 1))
        
    print(f"🌟 Starting Historical Backfill for range: {years}")
    
    for year in years:
        await backfill_year(year)
        # Optional sleep between years
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
