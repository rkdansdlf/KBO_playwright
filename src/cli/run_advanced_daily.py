"""
KBO Advanced Daily Data Update Orchestrator.
Fetches fielding, baserunning, and team-level cumulative stats.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from playwright.sync_api import Error as PlaywrightError
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.baserunning_stats_crawler import crawl_baserunning_stats
from src.crawlers.fielding_stats_crawler import crawl_all_fielding_stats
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler
from src.db.engine import SessionLocal
from src.repositories.player_stats_repository import PlayerSeasonBaserunningRepository, PlayerSeasonFieldingRepository
from src.sync.oci_sync import OCISync

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


CRAWL_TIMEOUT = 300  # 5 minutes max per crawl step
ADVANCED_STEP_EXCEPTIONS = (
    asyncio.TimeoutError,
    PlaywrightError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


def _filter_player_rows(records: list[dict], valid_cols: set[str]) -> list[dict]:
    return [
        {key: value for key, value in record.items() if key in valid_cols}
        for record in records
        if record.get("player_id")
    ]


async def _run_step(step_label: str, error_message: str, action) -> bool:
    logger.info("\n%s", step_label)
    try:
        await action()
    except ADVANCED_STEP_EXCEPTIONS:
        logger.exception("   ❌ %s", error_message)
        return True
    else:
        return False


async def _crawl_fielding_step(year: int) -> None:
    from src.models.player import PlayerSeasonFielding

    records = await asyncio.wait_for(asyncio.to_thread(crawl_all_fielding_stats, year), timeout=CRAWL_TIMEOUT)
    if records:
        processed = _filter_player_rows(records, {column.key for column in PlayerSeasonFielding.__table__.columns})
        logger.info("   ✅ Saved %s fielding records", PlayerSeasonFieldingRepository().upsert_many(processed))


async def _crawl_baserunning_step(year: int) -> None:
    from src.models.player import PlayerSeasonBaserunning

    records = await asyncio.wait_for(asyncio.to_thread(crawl_baserunning_stats, year), timeout=CRAWL_TIMEOUT)
    if records:
        processed = _filter_player_rows(records, {column.key for column in PlayerSeasonBaserunning.__table__.columns})
        logger.info("   ✅ Saved %s baserunning records", PlayerSeasonBaserunningRepository().upsert_many(processed))


async def _crawl_team_batting_step(year: int, headless: bool) -> None:
    stats = await asyncio.wait_for(
        asyncio.to_thread(TeamBattingStatsCrawler().crawl, year, persist=True, headless=headless),
        timeout=CRAWL_TIMEOUT,
    )
    logger.info("   ✅ Saved %s team batting records", len(stats))


async def _crawl_team_pitching_step(year: int, headless: bool) -> None:
    stats = await asyncio.wait_for(
        asyncio.to_thread(TeamPitchingStatsCrawler().crawl, year, persist=True, headless=headless),
        timeout=CRAWL_TIMEOUT,
    )
    logger.info("   ✅ Saved %s team pitching records", len(stats))


async def _aggregate_team_defense_step(year: int) -> None:
    from src.aggregators.team_fielding_aggregator import TeamFieldingAggregator
    from src.models.team import Team

    with SessionLocal() as session:
        active_teams = [team.team_id for team in session.query(Team.team_id).filter(Team.is_active).all()]
        TeamFieldingAggregator(session).run_all(year, active_teams)
    logger.info("   ✅ Team defense aggregated for %s teams", len(active_teams))


async def _rebuild_rankings_step(year: int) -> None:
    from src.cli.calculate_rankings import rebuild_rankings

    saved_rankings = await asyncio.wait_for(asyncio.to_thread(rebuild_rankings, year), timeout=CRAWL_TIMEOUT)
    logger.info("   ✅ Recalculated %s ranking records", saved_rankings)


def _sync_advanced_to_oci(year: int) -> bool:
    logger.info("\n☁️ Step 7: Synchronizing to OCI...")
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        logger.warning("   ⚠️ OCI_DB_URL not set, skipping sync")
        return False
    with SessionLocal() as session:
        syncer = OCISync(oci_url, session)
        try:
            syncer.sync_fielding_stats(year)
            syncer.sync_baserunning_stats(year)
            syncer.sync_team_season_batting(year)
            syncer.sync_team_season_pitching(year)
            syncer.sync_team_season_fielding(year)
            syncer.sync_team_season_baserunning(year)
            syncer.sync_stat_rankings(year)
        except ADVANCED_STEP_EXCEPTIONS:
            logger.exception("   ❌ OCI sync error")
            return True
        else:
            logger.info("   ✅ OCI synchronization completed")
            return False
        finally:
            syncer.close()


async def run_advanced_update(
    year: int,
    sync: bool = False,
    headless: bool = True,
) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("🚀 KBO Advanced Daily Sync Started for Year: %s", year)
    logger.info("%s", "=" * 60)

    any_error = False

    any_error |= await _run_step(
        "🛡️ Step 1: Crawling Fielding Stats...", "Error crawling fielding stats", lambda: _crawl_fielding_step(year)
    )
    any_error |= await _run_step(
        "🏃 Step 2: Crawling Baserunning Stats...",
        "Error crawling baserunning stats",
        lambda: _crawl_baserunning_step(year),
    )
    any_error |= await _run_step(
        "🏏 Step 3: Crawling Team Batting Stats...",
        "Error crawling team batting stats",
        lambda: _crawl_team_batting_step(year, headless),
    )
    any_error |= await _run_step(
        "⚾ Step 4: Crawling Team Pitching Stats...",
        "Error crawling team pitching stats",
        lambda: _crawl_team_pitching_step(year, headless),
    )
    any_error |= await _run_step(
        "🏰 Step 5: Aggregating Team Fielding & Baserunning...",
        "Error aggregating team defense stats",
        lambda: _aggregate_team_defense_step(year),
    )
    any_error |= await _run_step(
        "🏷️ Step 6: Recalculating Stat Rankings...", "Error recalculating rankings", lambda: _rebuild_rankings_step(year)
    )

    if sync:
        any_error |= _sync_advanced_to_oci(year)

    logger.info("\n%s", "=" * 60)
    logger.info("🏁 Advanced Daily Sync Finished for %s", year)
    logger.info("%s\n", "=" * 60)

    if any_error:
        raise RuntimeError(f"Advanced Daily Sync finished with errors for {year}")


def main() -> int:
    parser = argparse.ArgumentParser(description="KBO Advanced Daily Data Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with browser UI")

    args = parser.parse_args()

    year = args.year or datetime.now(KST).year
    asyncio.run(run_advanced_update(year, sync=args.sync, headless=args.headless))


if __name__ == "__main__":
    main()
