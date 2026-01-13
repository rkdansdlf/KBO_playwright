"""
Daily Automation Batch Script
Handles both "Daily Closing" (End-of-day data collection) and "Live Watcher" (Real-time relay capture).

Usage:
    python -m src.cli.daily_batch --mode closing
    python -m src.cli.daily_batch --mode live
"""
import argparse
import asyncio
import datetime
from datetime import timedelta
import logging
import os

from src.crawlers.schedule_crawler import ScheduleCrawler
from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.daily_roster_crawler import DailyRosterCrawler
from src.crawlers.relay_crawler import RelayCrawler
from src.cli.sync_supabase import main as sync_supabase_main
from src.repositories.game_repository import get_games_by_date, save_game_detail, save_relay_data
from src.repositories.team_repository import TeamRepository
from src.db.engine import SessionLocal

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("daily_batch.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DailyBatch")

async def run_closing(target_date: str = None):
    """
    Daily Closing Routine (Runs at 02:00 KST)
    1. Collect Roster for Target Date
    2. Collect Game Results for End games
    3. Sync to Supabase
    """
    if not target_date:
        # Default to Yesterday
        yesterday = datetime.datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")
    
    logger.info(f"=== Starting Daily Closing for {target_date} ===")

    # 1. Roster Collection
    logger.info(f"Step 1: Collecting Rosters for {target_date}")
    try:
        with SessionLocal() as db_session:
            team_repo = TeamRepository(db_session)
            crawler = DailyRosterCrawler(team_repo)
            await crawler.crawl_date_range(target_date, target_date)
        logger.info("Roster collection complete.")
    except Exception as e:
        logger.error(f"Roster collection failed: {e}")

    # 2. Game Detail Collection
    logger.info(f"Step 2: Collecting Game Details for {target_date}")
    try:
        year = int(target_date[:4])
        sch_crawler = ScheduleCrawler()
        # Ensure schedule is up to date for the month
        await sch_crawler.crawl_schedule(year, int(target_date[4:6]))
        
        # Now collect details for finished games
        games = get_games_by_date(target_date)
        
        detail_crawler = GameDetailCrawler()
        for game in games:
            # We enforce crawling for End games only in closing.
            if game.game_id:
                 data = await detail_crawler.crawl_game(game.game_id, target_date)
                 if data:
                     save_game_detail(data)
                     logger.info(f"Saved details for {game.game_id}")
        
        logger.info("Game Detail collection complete.")
    except Exception as e:
        logger.error(f"Game Detail collection failed: {e}")

    # 3. Supabase Sync
    logger.info("Step 3: Syncing to Supabase")
    try:
        # Run sync as subprocess to avoid global state issues
        process = await asyncio.create_subprocess_exec(
            "python", "-m", "src.cli.sync_supabase",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            logger.info(f"Sync Success: \n{stdout.decode()}")
        else:
            logger.error(f"Sync Failed: \n{stderr.decode()}")
    except Exception as e:
        logger.error(f"Supabase Sync failed: {e}")

    logger.info("=== Daily Closing Finished ===")


async def run_live_watcher():
    """
    Live Watcher Routine (Runs every 10-15 mins)
    """
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    today_year = int(today_str[:4])
    today_month = int(today_str[4:6])
    
    logger.info(f"=== Starting Live Watcher for {today_str} ===")
    
    # 1. Get Schedule for Today
    sch_crawler = ScheduleCrawler()
    
    try:
        games = await sch_crawler.crawl_schedule(today_year, today_month)
        today_games = [g for g in games if g['game_date'] == today_str]
        
        if not today_games:
            logger.info("No games scheduled for today.")
            return

        logger.info(f"Found {len(today_games)} games scheduled today.")
        
        relay_crawler = RelayCrawler()
        
        for game in today_games:
            game_id = game['game_id']
            # Try to crawl live data
            # RelayCrawler returns None if not live
            result = await relay_crawler.crawl_live_game(game_id)
            
            if result and result.get('events'):
                count = save_relay_data(game_id, result['events'])
                logger.info(f"Saved {count} relay events for {game_id}")
            else:
                pass 

    except Exception as e:
        logger.error(f"Live Watcher failed: {e}")

    logger.info("=== Live Watcher Finished ===")

def main():
    parser = argparse.ArgumentParser(description="KBO Daily Automation Batch")
    parser.add_argument("--mode", choices=["closing", "live"], required=True, help="Execution mode")
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD) for closing mode. Defaults to yesterday.")
    
    args = parser.parse_args()
    
    if args.mode == "closing":
        asyncio.run(run_closing(args.date))
    elif args.mode == "live":
        asyncio.run(run_live_watcher())

if __name__ == "__main__":
    main()
