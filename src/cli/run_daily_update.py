"""
KBO Daily Data Update Orchestrator
Processes game schedules, box scores, and cumulative player stats for a specific date.
"""
import asyncio
import argparse
import sys
import os
from datetime import datetime, timedelta

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail
from src.crawlers.player_batting_all_series_crawler import crawl_series_batting_stats
from src.crawlers.player_pitching_all_series_crawler import crawl_pitcher_series
from src.cli.sync_oci import main as sync_main
from src.utils.safe_print import safe_print as print

async def run_update(target_date: str, sync: bool = False, headless: bool = True, limit: int = None):
    """
    Main orchestration logic for daily updates.
    """
    print(f"\\n{'='*60}")
    print(f"🚀 KBO Daily Update Started for Date: {target_date}")
    print(f"{'='*60}")
    
    year = int(target_date[:4])
    month = int(target_date[4:6])
    
    # 1. Schedule Crawler to find games
    print("\\n📅 Step 1: Checking game schedule...")
    s_crawler = ScheduleCrawler()
    games = await s_crawler.crawl_schedule(year, month)
    
    # Filter for target_date
    daily_games = [g for g in games if str(g['game_date']).replace('-', '') == target_date]
    if limit and len(daily_games) > limit:
        daily_games = daily_games[:limit]
        print(f"   [LIMIT] Restricted to first {limit} games")
        
    print(f"   ✅ Found {len(daily_games)} games for {target_date}")
    
    if not daily_games:
        print(f"   ℹ️ No games scheduled for {target_date}. Continuing with stats update...")

    # 2. Game Detail Crawler
    if daily_games:
        print("\\n🎮 Step 2: Crawling game details (BoxScore)...")
        g_crawler = GameDetailCrawler()
        success_count = 0
        for g in daily_games:
            game_id = g['game_id']
            print(f"   📡 Processing Game: {game_id}")
            try:
                detail = await g_crawler.crawl_game(game_id, target_date)
                if detail:
                    save_success = save_game_detail(detail)
                    if save_success:
                        print(f"   ✅ Successfully saved {game_id}")
                        success_count += 1
                    else:
                        print(f"   ❌ Failed to save {game_id} to local DB")
                else:
                    print(f"   ⚠️ Could not fetch details for {game_id}")
            except Exception as e:
                print(f"   ❌ Error processing {game_id}: {e}")
        print(f"   ✅ Processed {success_count}/{len(daily_games)} game details")

    # 3. Cumulative Stats Update (Standard Seasonal Stats)
    print("\\n📈 Step 3: Updating cumulative player stats (Current Season)...")
    try:
        print("   🏏 Updating Batting Stats...")
        # Note: sync_playwright is used inside these crawlers
        # We must run them in a separate thread to avoid asyncio loop conflicts
        await asyncio.to_thread(
            crawl_series_batting_stats, 
            year=year, 
            series_key='regular', 
            save_to_db=True, 
            headless=headless, 
            limit=limit
        )
        
        print("\\n   ⚾ Updating Pitching Stats...")
        await asyncio.to_thread(
            crawl_pitcher_series, 
            year=year, 
            series_key='regular', 
            save_to_db=True, 
            headless=headless, 
            limit=limit
        )
        
        print(f"\\n   ✅ Local cumulative stats for {year} regular season updated")
    except Exception as e:
        print(f"   ❌ Error during stats update: {e}")

    print("\\n✨ Local data update sequence finished.")
    
    # 4. Sync to Supabase
    if sync:
        print("\\n☁️ Step 4: Synchronizing to Supabase...")
        try:
            # Sync new game details and related child tables
            print("   🔗 Syncing Game Details...")
            sync_main(["--game-details"])
            
            # Sync cumulative player stats (PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching)
            print("   🔗 Syncing Player Season Stats...")
            sync_main([]) # Default sync for these tables (UPSERT)
            
            print("   ✅ Supabase synchronization completed")
        except Exception as e:
            print(f"   ❌ Error during Supabase sync: {e}")

    print(f"\\n{'='*60}")
    print(f"🏁 Daily Update Finished for {target_date}")
    print(f"{'='*60}\\n")

def main():
    parser = argparse.ArgumentParser(description="KBO Daily Data Update Orchestrator")
    parser.add_argument(
        "--date", 
        type=str, 
        help="Target date in YYYYMMDD format. Defaults to yesterday."
    )
    parser.add_argument(
        "--sync", 
        action="store_true", 
        help="Whether to sync data to Supabase after local update."
    )
    parser.add_argument(
        "--headless", 
        action="store_true", 
        default=True,
        help="Run crawlers with browser headless"
    )
    parser.add_argument(
        "--no-headless", 
        action="store_false", 
        dest="headless",
        help="Run crawlers with browser UI visible"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        help="Limit number of games and players (for testing/debugging)"
    )
    
    args = parser.parse_args()
    
    target_date = args.date
    if not target_date:
        # Default to yesterday
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    elif len(target_date) != 8 or not target_date.isdigit():
        print(f"❌ Invalid date format: {target_date}. Please use YYYYMMDD.")
        sys.exit(1)
        
    asyncio.run(run_update(target_date, sync=args.sync, headless=args.headless, limit=args.limit))

if __name__ == "__main__":
    main()
