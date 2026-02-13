
import asyncio
import os
import sys
import logging
from datetime import datetime
from typing import List, Dict

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.crawlers.schedule_crawler import ScheduleCrawler
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal
from src.repositories.game_repository import save_game_detail
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data/modern_recovery.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def get_games_to_recover(session, year: int, pool: AsyncPlaywrightPool) -> List[Dict[str, str]]:
    """Fetch game IDs for a specific season. First check DB, if empty, crawl schedule."""
    stmt = text("""
        SELECT game_id, game_date 
        FROM game 
        WHERE season_id = :year 
        ORDER BY game_date ASC
    """)
    result = session.execute(stmt, {"year": year}).fetchall()
    
    if result:
        logger.info(f"Found {len(result)} games in DB for {year}")
        return [{"game_id": row[0], "game_date": row[1].replace("-", "")} for row in result]
    
    # DB Empty? Crawl Schedule
    logger.info(f"DB empty for {year}. Crawling schedule...")
    schedule_crawler = ScheduleCrawler(pool=pool)
    games = []
    
    # Iterate months (April to October/November roughly)
    # Modern seasons usually start ~March/April and end ~Oct/Nov
    months = ["03", "04", "05", "06", "07", "08", "09", "10", "11"]
    
    # KBO Regular Season Series ID: "0,9,6"
    series_regular = "0,9,6"
    
    for month in months:
        try:
            logger.info(f"Crawling schedule for {year}-{month} (Series: {series_regular})...")
            monthly_games = await schedule_crawler.crawl_schedule(year, int(month), series_id=series_regular)
            if monthly_games:
                games.extend(monthly_games)
        except Exception as e:
            logger.error(f"Error crawling schedule for {year}-{month}: {e}")
            
    # Filter for Regular Season only (usually checks game_id or league_id if available)
    # For now, take all returned. GameScheduleCrawler usually gets all.
    # Note: schedule crawler returns dicts with 'game_id', 'game_date', etc.
    
    # FILTER: Skip Exhibition Games (March early/mid)
    # KBO Regular Season usually starts late March (23rd-ish) or April 1st.
    # To be safe and avoid "No Data" errors for exhibition games, we filter.
    # 2024 Start: 03.23
    # 2023 Start: 04.01
    # 2022 Start: 04.02
    # 2021 Start: 04.03
    # 2020 Start: 05.05 (delayed due to COVID)
    
    season_starts = {
        2024: 20240323,
        2023: 20230401,
        2022: 20220402,
        2021: 20210403,
        2020: 20200505
    }
    
    start_date = season_starts.get(year, int(f"{year}0401"))
    
    # Filter by date and deduplicate
    seen_ids = set()
    valid_games = []
    for g in games:
        try:
            game_id = g.get('game_id')
            if not game_id or game_id in seen_ids:
                continue
            seen_ids.add(game_id)
            
            raw_date = g.get('game_date')
            g_date = int(raw_date) if raw_date else 0
            if g_date >= start_date:
                valid_games.append(g)
        except Exception as e:
            logger.error(f"Error parsing date for game {g}: {e}")
            
    logger.info(f"Schedule: {len(games)} raw -> {len(seen_ids)} unique -> {len(valid_games)} after date filter (>= {start_date})")
    return valid_games

async def recover_season(year: int):
    logger.info(f"🚀 Starting recovery for season {year}...")
    
    session = SessionLocal()
    pool = AsyncPlaywrightPool(max_pages=3)
    
    try:
        # Initialize Resolver with DB session
        resolver = PlayerIdResolver(session)
        # Preload index for performance
        resolver.preload_season_index(year)
        
        crawler = GameDetailCrawler(pool=pool, resolver=resolver)
        
        games = await get_games_to_recover(session, year, pool)
        logger.info(f"Found {len(games)} games to recover for {year}")
        
        if not games:
            logger.warning(f"No games found for {year}. Skipping.")
            return

        # Crawl in chunks to avoid memory potential issues
        chunk_size = 20
        for i in range(0, len(games), chunk_size):
            chunk = games[i:i + chunk_size]
            logger.info(f"Processing chunk {i//chunk_size + 1}/{(len(games)-1)//chunk_size + 1} ({len(chunk)} games)...")
            
            results = await crawler.crawl_games(chunk, concurrency=3)
            
            success_count = 0
            for game_data in results:
                if game_data:
                    # Save to DB
                    if save_game_detail(game_data):
                        success_count += 1
                    else:
                        logger.error(f"Failed to save {game_data['game_id']}")
            
            logger.info(f"Chunk complete. Saved {success_count}/{len(chunk)} games.")
            
    except Exception as e:
        logger.error(f"Critical error recovering {year}: {e}", exc_info=True)
    finally:
        session.close()
        await pool.close()
        logger.info(f"🏁 Finished recovery for season {year}")

async def main():
    # Seasons to recover (2024 first for verification, then 2020-2023)
    seasons = [2024, 2023, 2022, 2021, 2020]
    
    for year in seasons:
        await recover_season(year)

if __name__ == "__main__":
    asyncio.run(main())
