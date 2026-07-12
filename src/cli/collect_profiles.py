"""Player Profile Enrichment CLI.

Identifies players with missing basic info (e.g. birth_date, debut_year) and crawls them.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
from typing import TYPE_CHECKING

from playwright.async_api import Error as PlaywrightError
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import SQLAlchemyError

from src.constants import MIN_KBO_PLAYER_ID
from src.crawlers.player_profile_crawler import PlayerProfileCrawler
from src.db.engine import SessionLocal
from src.models.player import Player, PlayerBasic
from src.repositories.player_repository import PlayerRepository
from src.utils.playwright_pool import AsyncPlaywrightPool

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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


def _get_player_name_or_default(session: Session, pid: str) -> str:
    try:
        basic_p = session.query(PlayerBasic).filter_by(player_id=int(pid)).first()
        if basic_p:
            return basic_p.name
    except (ValueError, TypeError):
        pass
    return "Unknown"


async def collect_profiles(
    limit: int = 100,
    target_ids: list[str] | None = None,
    *,
    team_missing: bool = False,
) -> None:
    """Handle the collect profiles operation.

    Args:
        limit: Limit.
        target_ids: Target Ids.
        team_missing: Target players with missing team in PlayerBasic.

    """
    session = SessionLocal()

    repo = PlayerRepository()
    pool = AsyncPlaywrightPool(max_pages=1)
    crawler = PlayerProfileCrawler(request_delay=1.5, pool=pool)

    try:
        pids: list[str] = []
        if target_ids:
            pids = target_ids
            logger.info("🎯 Targeted processing for %s IDs", len(pids))
        elif team_missing:
            stmt = (
                select(PlayerBasic.player_id)
                .where(
                    and_(
                        or_(PlayerBasic.team.is_(None), PlayerBasic.team == ""),
                        PlayerBasic.player_id >= MIN_KBO_PLAYER_ID,
                    )
                )
                .limit(limit)
            )
            results = session.execute(stmt).scalars().all()
            pids = [str(r) for r in results]
            logger.info("🎯 Found %s players with missing team in PlayerBasic", len(pids))
        else:
            stmt = select(Player).where(or_(Player.birth_date.is_(None), Player.debut_year.is_(None))).limit(limit)
            target_players = session.execute(stmt).scalars().all()
            pids = [str(p.kbo_person_id) for p in target_players if p.kbo_person_id]

        if not pids:
            logger.info("✅ No matching players found for profile collection.")
            return

        logger.info("🎯 Processing %s player profiles...", len(pids))

        async with pool:
            for idx, pid in enumerate(pids, 1):
                if not pid:
                    continue

                p_name = _get_player_name_or_default(session, pid)

                logger.info(
                    "[%s/%s] Crawling profile for %s (%s)",
                    idx,
                    len(pids),
                    pid,
                    p_name,
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
                        team=data.get("team"),
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
    parser.add_argument(
        "--team-missing",
        action="store_true",
        help="Collect profiles for players with missing team info in PlayerBasic",
    )
    args = parser.parse_args()

    target_ids = None
    if args.ids:
        target_ids = [i.strip() for i in args.ids.split(",")]

    asyncio.run(collect_profiles(args.limit, target_ids, team_missing=args.team_missing))
    return 0


if __name__ == "__main__":
    main()
