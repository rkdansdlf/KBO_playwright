"""
Player Profile Enrichment CLI
Identifies players with missing basic info (e.g. birth_date, debut_year) and crawls them.
"""

import argparse
import asyncio
import logging

from sqlalchemy import or_, select

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.models.player import Player
from src.repositories.player_repository import PlayerRepository
from src.utils.playwright_pool import AsyncPlaywrightPool

logger = logging.getLogger(__name__)


async def collect_profiles(limit: int = 100, target_ids: list[str] | None = None) -> None:
    session = SessionLocal()
    repo = PlayerRepository()
    pool = AsyncPlaywrightPool(max_pages=1)
    crawler = PlayerProfileCrawler(request_delay=1.5, pool=pool)

    try:
        if target_ids:
            stmt = select(Player).where(Player.kbo_person_id.in_(target_ids))
            logger.info(f"🎯 Targeted processing for {len(target_ids)} IDs")
        else:
            stmt = select(Player).where(or_(Player.birth_date is None, Player.debut_year is None)).limit(limit)

        target_players = session.execute(stmt).scalars().all()

        if not target_players:
            logger.info("✅ No matching players found for profile collection.")
            return

        logger.info(f"🎯 Processing {len(target_players)} player profiles...")

        async with pool:
            for idx, player in enumerate(target_players, 1):
                pid = player.kbo_person_id
                if not pid:
                    continue

                logger.info(
                    f"[{idx}/{len(target_players)}] Crawling profile for {pid} ({getattr(player, 'name_kor', 'Unknown')})",
                )

                data = await crawler.crawl_player_profile(str(pid))
                if data:
                    logger.info(f"   ✅ Fetched profile for {pid}")
                    from src.parsers.player_profile_parser import PlayerProfileParsed

                    # Manually populate parsed object since we already have parsed data
                    parsed = PlayerProfileParsed(
                        player_id=int(pid) if pid.isdigit() else None,
                        player_name=data.get("name"),
                        photo_url=data.get("photo_url"),
                        batting_hand=data.get("bats"),
                        throwing_hand=data.get("throws"),
                        height_cm=data.get("height_cm"),
                        weight_kg=data.get("weight_kg"),
                        entry_year=data.get("debut_year"),
                        salary_original=data.get("salary_original"),
                        signing_bonus_original=data.get("signing_bonus_original"),
                        salary_amount=data.get("salary_amount"),
                        salary_currency=data.get("salary_currency"),
                        signing_bonus_amount=data.get("signing_bonus_amount"),
                        signing_bonus_currency=data.get("signing_bonus_currency"),
                        draft_year=data.get("draft_year"),
                        draft_round=data.get("draft_round"),
                        draft_pick_overall=data.get("draft_pick_overall"),
                        draft_type=data.get("draft_type"),
                        education_or_career_path=data.get("education_path") or [],
                    )

                    # The repo.upsert_player_profile expects a PlayerProfileParsed object
                    repo.upsert_player_profile(str(pid), parsed)
                    logger.info(f"   ✅ Saved profile metadata for {pid}")
                else:
                    logger.warning(f"   ⚠️  Crawl skipped or no data for {pid}")

                if idx % 5 == 0:
                    await asyncio.sleep(1)

    except Exception:
        logger.exception("❌ Critical Error")
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect Missing Player Profiles")
    parser.add_argument("--limit", type=int, default=1000, help="Max players to process")
    parser.add_argument("--ids", type=str, help="Comma-separated List of KBO Player IDs")
    args = parser.parse_args()

    target_ids = None
    if args.ids:
        target_ids = [i.strip() for i in args.ids.split(",")]

    asyncio.run(collect_profiles(args.limit, target_ids))


if __name__ == "__main__":
    main()
