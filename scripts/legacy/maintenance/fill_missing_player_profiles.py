import argparse
import asyncio
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqlalchemy import text

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.utils.playwright_pool import AsyncPlaywrightPool


async def get_orphan_info() -> dict[int, str]:
    """Find player_ids in stat tables that are missing from player_basic, with suggested position."""
    orphan_map = {}  # id -> position
    with SessionLocal() as session:
        # Pitching orphans
        pitching_query = text("""
            SELECT DISTINCT player_id FROM player_season_pitching
            WHERE player_id NOT IN (SELECT player_id FROM player_basic)
            AND player_id IS NOT NULL
        """)
        for row in session.execute(pitching_query):
            orphan_map[row[0]] = "P"

        # Batting orphans (might overwrite P if they do both, which is fine)
        batting_query = text("""
            SELECT DISTINCT player_id FROM player_season_batting
            WHERE player_id NOT IN (SELECT player_id FROM player_basic)
            AND player_id IS NOT NULL
        """)
        for row in session.execute(batting_query):
            orphan_map[row[0]] = "B"

        print(f"🔍 Found {len(orphan_map)} unique orphan player IDs.")
        return orphan_map


async def fill_missing_profiles(limit: int, delay: float, ids: list[int] | None = None):
    if ids:
        # For manual IDs, we don't know position, so we try both (default behavior)
        target_ids = {pid: None for pid in ids}
    else:
        target_ids = await get_orphan_info()

    if not target_ids:
        print("✅ No orphan player IDs found. All good!")
        return

    sorted_pids = sorted(list(target_ids.keys()))
    if limit > 0 and not ids:
        sorted_pids = sorted_pids[:limit]
        print(f"🎯 Limiting to first {limit} orphans.")

    print(f"🚀 Starting profile collection for {len(sorted_pids)} orphans (delay={delay}s)...")

    repo = PlayerBasicRepository()
    pool = AsyncPlaywrightPool(max_pages=1)
    await pool.start()
    crawler = PlayerProfileCrawler(request_delay=delay, pool=pool)

    success_count = 0
    fail_count = 0

    try:
        for i, pid in enumerate(sorted_pids):
            pos = target_ids[pid]
            print(f"[{i + 1}/{len(sorted_pids)}] Crawling ID: {pid} (pos={pos})...")

            try:
                profile = await crawler.crawl_player_profile(str(pid), position=pos)

                if profile:
                    repo.upsert_players([profile])
                    print(f"  ✅ Success: {profile['name']} ({pid}) saved.")
                    success_count += 1
                else:
                    reason = crawler.get_last_failure_reason(str(pid)) or "NOT_FOUND"
                    print(f"  ⚠️ Failed for {pid}: {reason}")
                    # Optionally mark as NOT_FOUND in DB if we want to avoid retrying
                    # But if it's a transient error, we might want to retry later.
                    # For now, we only save if we find a name.
                    fail_count += 1
            except Exception as e:
                print(f"  ❌ Error for {pid}: {e}")
                fail_count += 1

            if i < len(target_ids) - 1:
                await asyncio.sleep(delay)

    finally:
        await pool.close()

    print("\n✨ Orphan profile backfill complete!")
    print(f"   - Total Processed: {len(target_ids)}")
    print(f"   - New Profiles:    {success_count}")
    print(f"   - Failures/Stubs:  {fail_count}")


def main():
    parser = argparse.ArgumentParser(description="Fill missing player profiles for orphans")
    parser.add_argument("--limit", type=int, default=0, help="Number of orphans to process (0 = all)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests in seconds")
    parser.add_argument("--ids", type=str, help="Comma-separated List of KBO Player IDs")

    args = parser.parse_args()

    target_ids = None
    if args.ids:
        target_ids = [int(i.strip()) for i in args.ids.split(",")]

    asyncio.run(fill_missing_profiles(args.limit, args.delay, target_ids))


if __name__ == "__main__":
    main()
