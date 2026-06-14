"""
Backfill Player Profiles
Collects missing photo_url, salary, draft_info, etc. for existing players in player_basic table.
Usage: python3 scripts/backfill_player_profiles.py --limit 10 --delay 2.0
"""

import argparse
import asyncio
import logging
import os
import sys

logger = logging.getLogger(__name__)

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from sqlalchemy import or_

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.models.player import PlayerBasic
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.utils.playwright_pool import AsyncPlaywrightPool


async def backfill(limit: int, delay: float, ids: list[str] | None = None):
    repo = PlayerBasicRepository()

    # Target players: missing photo_url, NOT a pseudo ID, and NOT already marked as NOT_FOUND
    with SessionLocal() as session:
        if ids:
            query = session.query(PlayerBasic).filter(PlayerBasic.player_id.in_(ids))
            logger.info("🎯 Targeted processing for %s IDs", len(ids))
        else:
            query = session.query(PlayerBasic).filter(
                PlayerBasic.photo_url.is_(None),
                PlayerBasic.player_id >= 10000,
                or_(PlayerBasic.status.is_(None), ~PlayerBasic.status.in_(["NOT_FOUND", "PSEUDO"])),
            )
        if limit > 0:
            query = query.limit(limit)
        targets = query.all()

    if not targets:
        logger.info("✅ No players need backfilling.")
        return

    logger.info("🚀 Starting backfill for %s players (delay=%ss)...", len(targets), delay)

    # Reuse a single pool for efficiency
    pool = AsyncPlaywrightPool(max_pages=1)
    await pool.start()
    crawler = PlayerProfileCrawler(request_delay=delay, pool=pool)

    success_count = 0
    fail_count = 0

    try:
        for i, p in enumerate(targets):
            logger.info("[%s/%s] Processing %s (%s)...", i + 1, len(targets), p.name, p.player_id)

            try:
                profile = await crawler.crawl_player_profile(str(p.player_id), position=p.position)

                if profile:
                    # Fix: upsert_players requires 'name'
                    profile["name"] = p.name
                    # Update DB
                    repo.upsert_players([profile])

                    # Update detailed players table
                    try:
                        from src.parsers.player_profile_parser import PlayerProfileParsed
                        from src.repositories.player_repository import PlayerRepository

                        detailed_repo = PlayerRepository()
                        parsed = PlayerProfileParsed(
                            player_id=int(p.player_id),
                            player_name=p.name,
                            photo_url=profile.get("photo_url"),
                            batting_hand=profile.get("bats"),
                            throwing_hand=profile.get("throws"),
                            height_cm=profile.get("height_cm"),
                            weight_kg=profile.get("weight_kg"),
                            entry_year=profile.get("debut_year"),
                            salary_original=profile.get("salary_original"),
                            signing_bonus_original=profile.get("signing_bonus_original"),
                            salary_amount=profile.get("salary_amount"),
                            salary_currency=profile.get("salary_currency"),
                            signing_bonus_amount=profile.get("signing_bonus_amount"),
                            signing_bonus_currency=profile.get("signing_bonus_currency"),
                            draft_year=profile.get("draft_year"),
                            draft_round=profile.get("draft_round"),
                            draft_pick_overall=profile.get("draft_pick_overall"),
                            draft_type=profile.get("draft_type"),
                            education_or_career_path=profile.get("education_path") or [],
                        )
                        detailed_repo.upsert_player_profile(str(p.player_id), parsed)
                    except Exception as repo_err:  # noqa: BLE001
                        logger.warning("  ⚠️ Detailed player sync warning: %s", repo_err)

                    logger.info("  ✅ Updated: photo=%s, salary=%s", profile["photo_url"], profile["salary_original"])
                    success_count += 1
                else:
                    logger.warning("  ⚠️ No profile found for %s. Marking as NOT_FOUND.", p.player_id)
                    # Mark as NOT_FOUND to avoid re-crawling
                    repo.upsert_players(
                        [{"player_id": p.player_id, "name": p.name, "photo_url": "NOT_FOUND", "status": "NOT_FOUND"}]
                    )
                    fail_count += 1
            except Exception as e:  # noqa: BLE001
                logger.error("  ❌ Error processing %s: %s", p.player_id, e)
                fail_count += 1

            # Additional safety delay (on top of crawler's internal delay if needed)
            if i < len(targets) - 1:
                await asyncio.sleep(delay)

    finally:
        await pool.close()

    logger.info("✨ Backfill complete!")
    logger.info("   - Success: %s", success_count)
    logger.info("   - Failed:  %s", fail_count)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")

    parser = argparse.ArgumentParser(description="Backfill missing player profile details")
    parser.add_argument("--limit", type=int, default=0, help="Number of players to process (0 = all)")
    parser.add_argument("--delay", type=float, default=1.5, help="Delay between requests in seconds")
    parser.add_argument("--ids", type=str, help="Comma-separated List of KBO Player IDs")

    args = parser.parse_args()

    target_ids = None
    if args.ids:
        target_ids = [i.strip() for i in args.ids.split(",")]

    asyncio.run(backfill(args.limit, args.delay, target_ids))


if __name__ == "__main__":
    main()
