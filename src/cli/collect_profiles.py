"""
Player Profile Enrichment CLI.

Identifies players with missing basic info (e.g. birth_date, debut_year) and crawls them.

"""

from __future__ import annotations

import argparse
import asyncio
import logging

from playwright.async_api import Error as PlaywrightError
from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.models.player import Player
from src.repositories.player_repository import PlayerRepository
from src.utils.playwright_pool import AsyncPlaywrightPool

logger = logging.getLogger(__name__)

PROFILE_COLLECTION_EXCEPTIONS = (
    PlaywrightError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    OSError,
)


async def collect_profiles(limit: int = 100, target_ids: list[str] | None = None) -> None:
    """
    Handle the collect profiles operation.

    Args:
        limit: Limit.
        target_ids: Target Ids.
        limit: Limit.
        target_ids: Target Ids.

    """
    session = SessionLocal()

    repo = PlayerRepository()
    pool = AsyncPlaywrightPool(max_pages=1)
    crawler = PlayerProfileCrawler(request_delay=1.5, pool=pool)

    try:
        if target_ids:
            stmt = select(Player).where(Player.kbo_person_id.in_(target_ids))
            logger.info("🎯 Targeted processing for %s IDs", len(target_ids))
        else:
            stmt = select(Player).where(or_(Player.birth_date.is_(None), Player.debut_year.is_(None))).limit(limit)

        target_players = session.execute(stmt).scalars().all()

        if not target_players:
            logger.info("✅ No matching players found for profile collection.")
            return

        logger.info("🎯 Processing %s player profiles...", len(target_players))

        async with pool:
            for idx, player in enumerate(target_players, 1):
                pid = player.kbo_person_id
                if not pid:
                    continue

                logger.info(
                    "[%s/%s] Crawling profile for %s (%s)",
                    idx,
                    len(target_players),
                    pid,
                    getattr(player, "name_kor", "Unknown"),
                )

                data = await crawler.crawl_player_profile(str(pid))
                if data:
                    logger.info("   ✅ Fetched profile for %s", pid)
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
                    logger.info("   ✅ Saved profile metadata for %s", pid)
                else:
                    logger.warning("   ⚠️  Crawl skipped or no data for %s", pid)

                if idx % 5 == 0:
                    await asyncio.sleep(1)

    except PROFILE_COLLECTION_EXCEPTIONS:
        logger.exception("❌ Critical Error")
    finally:
        session.close()


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Collect Missing Player Profiles")
    parser.add_argument("--limit", type=int, default=1000, help="Max players to process")
    parser.add_argument("--ids", type=str, help="Comma-separated List of KBO Player IDs")
    args = parser.parse_args()

    target_ids = None
    if args.ids:
        target_ids = [i.strip() for i in args.ids.split(",")]

    asyncio.run(collect_profiles(args.limit, target_ids))
    return 0


if __name__ == "__main__":
    main()
