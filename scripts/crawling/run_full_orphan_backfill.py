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
from src.repositories.game_repository import repair_game_parent_from_existing_children, save_game_snapshot
from src.db.engine import SessionLocal
from src.utils.game_status import GAME_STATUS_CANCELLED
from src.utils.team_codes import team_code_from_game_id_segment

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


def _cancelled_snapshot_payload(game_id: str, date_str: str) -> dict:
    try:
        season_year = int(date_str[:4])
    except (TypeError, ValueError):
        season_year = None

    away_segment = game_id[8:10] if len(game_id) >= 10 else None
    home_segment = game_id[10:12] if len(game_id) >= 12 else None

    return {
        "game_id": game_id,
        "game_date": date_str,
        "game_status": GAME_STATUS_CANCELLED,
        "season_year": season_year,
        "metadata": {"is_cancelled": True},
        "teams": {
            "away": {
                "code": team_code_from_game_id_segment(away_segment, season_year),
                "score": None,
                "line_score": [],
            },
            "home": {
                "code": team_code_from_game_id_segment(home_segment, season_year),
                "score": None,
                "line_score": [],
            },
        },
    }


async def run_backfill(chunk_size: int = 100, max_total: int = 10000):
    logger.info("Starting robust orphan backfill...")
    
    with SessionLocal() as session:
        query = text("""
            SELECT DISTINCT game_id FROM game_batting_stats WHERE game_id NOT IN (SELECT game_id FROM game)
            UNION
            SELECT DISTINCT game_id FROM game_pitching_stats WHERE game_id NOT IN (SELECT game_id FROM game)
            UNION
            SELECT DISTINCT game_id FROM game_play_by_play WHERE game_id NOT IN (SELECT game_id FROM game)
            ORDER BY game_id DESC
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
                    # We use lightweight=True because we mainly need metadata to fix the 'game' table relationship.
                    payload = await crawler.crawl_game(game_id, date_str, lightweight=True)

                    if payload:
                        if save_game_snapshot(payload):
                            logger.info(f"✅ [{success_count+fail_count+1}/{len(missing_ids)}] Saved {game_id}")
                            success_count += 1
                            break
                        else:
                            logger.error(f"❌ Failed to save {game_id} (DB Error)")
                    else:
                        # KEY FIX: If no payload, it might be CANCELLED. Check crawler's last reason.
                        reason = crawler.get_last_failure_reason(game_id)
                        if repair_game_parent_from_existing_children(game_id):
                            logger.info(f"✅ [{success_count+fail_count+1}/{len(missing_ids)}] Repaired {game_id} from existing child rows")
                            success_count += 1
                            break
                        if reason and "cancelled" in reason.lower():
                            logger.warning(f"ℹ️  Game {game_id} is CANCELLED. Saving as placeholder.")
                            cancel_payload = _cancelled_snapshot_payload(game_id, date_str)
                            if save_game_snapshot(cancel_payload, status=GAME_STATUS_CANCELLED):
                                logger.info(f"✅ [{success_count+fail_count+1}/{len(missing_ids)}] Marked {game_id} as CANCELLED")
                                success_count += 1
                                break
                        logger.warning(f"⚠️  No payload for {game_id} (Reason: {reason})")
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
