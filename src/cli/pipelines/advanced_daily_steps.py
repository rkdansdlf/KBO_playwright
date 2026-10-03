"""Reusable steps for fetching KBO advanced (fielding, baserunning, team) stats.

These steps used to live inside :mod:`src.cli.pipelines.run_advanced_daily`, which
only GitHub Actions ran. The canonical scheduler needs the same work -- the team
season pages feed the quality gate, and the player fielding/baserunning pages feed
the team defense aggregate -- so the implementations live here and both callers
share them instead of keeping two copies that can drift.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from playwright.sync_api import Error as PlaywrightError
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.baserunning_stats_crawler import crawl_baserunning_stats
from src.crawlers.fielding_stats_crawler import crawl_all_fielding_stats
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler
from src.db.engine import SessionLocal
from src.repositories.player_stats_repository import (
    PlayerSeasonBaserunningRepository,
    PlayerSeasonFieldingRepository,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

#: 5 minutes max per crawl step.
ADVANCED_CRAWL_TIMEOUT = 300

ADVANCED_STEP_EXCEPTIONS = (
    asyncio.TimeoutError,
    PlaywrightError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


def filter_player_rows(records: list[dict], valid_cols: set[str]) -> list[dict]:
    """Keep only known columns and rows that can be attributed to a player."""
    return [
        {key: value for key, value in record.items() if key in valid_cols}
        for record in records
        if record.get("player_id")
    ]


async def run_step(step_label: str, error_message: str, action: Callable[[], Awaitable[None]]) -> bool:
    """Run one step, returning ``True`` when it failed."""
    logger.info("\n%s", step_label)
    try:
        await action()
    except ADVANCED_STEP_EXCEPTIONS:
        logger.exception("   ❌ %s", error_message)
        return True
    else:
        return False


async def crawl_fielding_step(year: int) -> None:
    """Crawl and persist every player's season fielding line."""
    from src.models.player import PlayerSeasonFielding

    records = await asyncio.wait_for(asyncio.to_thread(crawl_all_fielding_stats, year), timeout=ADVANCED_CRAWL_TIMEOUT)
    if records:
        processed = filter_player_rows(records, {column.key for column in PlayerSeasonFielding.__table__.columns})
        with SessionLocal() as session:
            saved = PlayerSeasonFieldingRepository(session).upsert_many(processed)
            session.commit()
        logger.info("   ✅ Saved %s fielding records", saved)


async def crawl_baserunning_step(year: int) -> None:
    """Crawl and persist every player's season baserunning line."""
    from src.models.player import PlayerSeasonBaserunning

    records = await asyncio.wait_for(asyncio.to_thread(crawl_baserunning_stats, year), timeout=ADVANCED_CRAWL_TIMEOUT)
    if records:
        processed = filter_player_rows(records, {column.key for column in PlayerSeasonBaserunning.__table__.columns})
        with SessionLocal() as session:
            saved = PlayerSeasonBaserunningRepository(session).upsert_many(processed)
            session.commit()
        logger.info("   ✅ Saved %s baserunning records", saved)


async def crawl_team_batting_step(year: int, *, headless: bool) -> None:
    """Crawl and persist team season batting from the official team pages."""
    stats = await asyncio.wait_for(
        asyncio.to_thread(TeamBattingStatsCrawler().crawl, year, persist=True, headless=headless),
        timeout=ADVANCED_CRAWL_TIMEOUT,
    )
    logger.info("   ✅ Saved %s team batting records", len(stats))


async def crawl_team_pitching_step(year: int, *, headless: bool) -> None:
    """Crawl and persist team season pitching from the official team pages."""
    stats = await asyncio.wait_for(
        asyncio.to_thread(TeamPitchingStatsCrawler().crawl, year, persist=True, headless=headless),
        timeout=ADVANCED_CRAWL_TIMEOUT,
    )
    logger.info("   ✅ Saved %s team pitching records", len(stats))


async def aggregate_team_defense_step(year: int) -> None:
    """Aggregate team fielding and baserunning from the freshly crawled player lines."""
    from src.aggregators.team_fielding_aggregator import TeamFieldingAggregator
    from src.models.team import Team

    with SessionLocal() as session:
        active_teams = [team.team_id for team in session.query(Team.team_id).filter(Team.is_active).all()]
        TeamFieldingAggregator(session).run_all(year, active_teams)
    logger.info("   ✅ Team defense aggregated for %s teams", len(active_teams))


async def rebuild_rankings_step(year: int) -> None:
    """Recalculate season stat rankings."""
    from src.cli.calc.calculate_rankings import rebuild_rankings

    saved_rankings = await asyncio.wait_for(asyncio.to_thread(rebuild_rankings, year), timeout=ADVANCED_CRAWL_TIMEOUT)
    logger.info("   ✅ Recalculated %s ranking records", saved_rankings)
