#!/usr/bin/env python3
"""
Robust backfill for all missing game metadata.
Features:
- Batched processing
- Automatic retries on DB locks or network errors
- Persistence of failed IDs to logs/failed_orphan_games.log
- Progress tracking
"""
import asyncio
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.crawlers.game_detail_crawler import GameDetailCrawler
from src.repositories.game_repository import save_game_detail
from src.db.engine import SessionLocal

# Setup logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
FAILED_LOG = LOG_DIR / "failed_orphan_games.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "backfill.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def run_backfill(chunk_size: int = 100, max_total: int = 10000):
    logger.info("Starting robust orphan backfill...")
    
    with SessionLocal() as session:
        query = text("""
            SELECT DISTINCT game_id 
            FROM game_batting_stats 
            WHERE game_id NOT IN (SELECT game_id FROM game)
            ORDER BY game_id DESC -- Start from newest missing
            LIMIT :max_total
        """)
        missing_rows = session.execute(query, {"max_total": max_total}).fetchall()
        missing_ids = [row[0] for row in missing_rows]

    if not missing_ids:
        logger.info("No orphan games found to backfill.")
        return

    logger.info(f"Found {len(missing_ids)} orphan games. Processing in chunks of {chunk_size}...")
    
    crawler = GameDetailCrawler(request_delay=1.0)
    success_count = 0
    fail_count = 0
    
    for i in range(0, len(missing_ids), chunk_size):
        chunk = missing_ids[i:i + chunk_size]
        logger.info(f"--- Processing Chunk {i//chunk_size + 1} ({i} to {i+len(chunk)}) ---")
        
        for game_id in chunk:
            date_str = game_id[:8]
            retry_attempts = 2
            
            for attempt in range(retry_attempts + 1):
                try:
                    payload = await crawler.crawl_game(game_id, date_str, lightweight=True)
                    if payload:
                        if save_game_detail(payload):
                            logger.info(f"✅ [{success_count+fail_count+1}/{len(missing_ids)}] Saved {game_id}")
                            success_count += 1
                            break
                        else:
                            logger.error(f"❌ Failed to save {game_id} (DB Error)")
                    else:
                        logger.warning(f"⚠️  No payload for {game_id}")
                except Exception as e:
                    if "locked" in str(e).lower() and attempt < retry_attempts:
                        logger.warning(f"🔒 DB Locked. Retrying {game_id} (Attempt {attempt+1})...")
                        await asyncio.sleep(2)
                        continue
                    logger.error(f"💥 Error crawling {game_id}: {e}")
                
                if attempt == retry_attempts:
                    logger.error(f"🛑 Permanent failure for {game_id}")
                    with open(FAILED_LOG, "a") as f:
                        f.write(f"{game_id}\n")
                    fail_count += 1
            
            # Small delay to prevent KBO blocking
            await asyncio.sleep(0.5)

    logger.info(f"Backfill complete. Success: {success_count}, Failed: {fail_count}")

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    chunk = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    asyncio.run(run_backfill(chunk_size=chunk, max_total=limit))
