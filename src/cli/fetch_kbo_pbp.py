import argparse
import asyncio
import sys
import os
from typing import List, Dict

# Adjust sys.path to run from CLI easily
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.crawlers.pbp_crawler import PBPCrawler
from src.repositories.game_repository import save_relay_data
from src.utils.safe_print import safe_print as print
from src.utils.request_policy import RequestPolicy
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.db.engine import SessionLocal
from src.models.game import Game
from sqlalchemy import text

async def main():
    parser = argparse.ArgumentParser(description="KBO Play-by-Play (game_events) Historical Fetcher")
    parser.add_argument("--season", type=int, help="Season year to fetch (e.g. 2024)")
    parser.add_argument("--month", type=int, help="Optional month to filter games")
    parser.add_argument("--game-id", type=str, help="Specific Game ID to fetch (e.g. 20240323SSHH0)")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of concurrent pages")
    
    args = parser.parse_args()

    if not args.season and not args.game_id:
        print("[ERROR] Must provide --season or --game-id")
        sys.exit(1)

    game_ids = []
    
    # Check if we query from DB
    with SessionLocal() as session:
        if args.game_id:
            game_ids.append(args.game_id)
        else:
            query = session.query(Game.game_id).filter(Game.season_id == args.season)
            query = query.filter(Game.game_status == 'COMPLETED')
            
            if args.month:
                # Naive month filter depending on game_id format (YYYYMMDD...)
                month_str = f"{args.season}{args.month:02d}"
                query = query.filter(Game.game_id.like(f"{month_str}%"))

            results = query.all()
            game_ids = [r[0] for r in results]

    if not game_ids:
        print("[INFO] No COMPLETED games found for the given criteria.")
        return

    print(f"[INFO] Found {len(game_ids)} games to process.")

    # Initialize Crawler Pool
    policy = RequestPolicy(min_delay=1.0, max_delay=1.5)
    pool = AsyncPlaywrightPool(max_pages=args.concurrency, context_kwargs=policy.build_context_kwargs(locale='ko-KR'))
    crawler = PBPCrawler(policy=policy, pool=pool)

    await pool.start()
    try:
        if args.concurrency > 1:
            # Parallel processing wrapper
            sem = asyncio.Semaphore(args.concurrency)
            async def _process(gid: str):
                async with sem:
                    return await _crawl_and_save(crawler, gid)
            
            tasks = [_process(gid) for gid in game_ids]
            await asyncio.gather(*tasks)
        else:
            # Sequential processing
            for idx, gid in enumerate(game_ids, 1):
                print(f"[PROGRESS] Processing {idx}/{len(game_ids)}: {gid}")
                await _crawl_and_save(crawler, gid)
                await asyncio.sleep(0.5) # Soft loop delay
                
    except KeyboardInterrupt:
        print("\n[INFO] Stopped by user.")
    finally:
        await pool.close()
        print("\n[INFO] Fetching completed.")

async def _crawl_and_save(crawler: PBPCrawler, game_id: str):
    res = await crawler.crawl_game_events(game_id)
    if res and res.get('events'):
        events = res['events']
        saved = save_relay_data(game_id, events)
        print(f"[SUCCESS] Saved {saved} events for {game_id}")
    else:
        print(f"[SKIP] No events extracted for {game_id}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
