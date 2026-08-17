"""KBO Weekly Maintenance Orchestrator.

Performs player profile enrichment, DB health checks, team events, and fan culture crawling.

"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from playwright.async_api import Error as PlaywrightError
from sqlalchemy.exc import SQLAlchemyError

from src.cli.collect_profiles import collect_profiles
from src.cli.db_healthcheck import main as healthcheck_main
from src.db.engine import SessionLocal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")
WEEKLY_MAINTENANCE_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    SQLAlchemyError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


async def _run_weekly_step(step_label: str, error_message: str, action: Callable[[], Awaitable[None]]) -> None:
    logger.info("\n%s", step_label)
    try:
        await action()
    except WEEKLY_MAINTENANCE_EXCEPTIONS:
        logger.exception("   ❌ %s", error_message)


def _profile_delay() -> float:
    try:
        return float(os.getenv("PROFILE_BACKFILL_DELAY", "1.5"))
    except ValueError:
        logger.warning("Invalid PROFILE_BACKFILL_DELAY=%r; using default=1.5", os.getenv("PROFILE_BACKFILL_DELAY"))
        return 1.5


async def _profile_enrichment_step(profile_limit: int) -> None:
    from scripts.backfill_player_profiles import backfill as backfill_player_basic_profiles

    logger.info("   - Backfilling player_basic profile photos/details...")
    await backfill_player_basic_profiles(limit=profile_limit, delay=_profile_delay())
    logger.info("   - Enriching master player profile records...")
    await collect_profiles(limit=profile_limit)
    logger.info("   ✅ Profile enrichment complete")


async def _healthcheck_step() -> None:
    try:
        healthcheck_main([])
    except SystemExit as e:
        if e.code != 0:
            msg = f"Database healthcheck failed with exit code {e.code}"
            raise RuntimeError(msg) from e
    logger.info("   ✅ Healthcheck complete")


async def _resolve_null_team_codes_step() -> None:
    from scripts.maintenance.resolve_null_team_codes import resolve_team_codes

    logger.info("   - Resolving NULL team codes in player_season tables...")
    with SessionLocal() as session:
        stats = resolve_team_codes(session, apply=True)
        session.commit()
        logger.info("   ✅ NULL team codes resolution complete: %s updated", stats["updated"])


async def _resolve_roster_player_links_step() -> None:
    from sqlalchemy import text

    logger.info("   - Linking player_basic_id in team_daily_roster...")
    with SessionLocal() as session:
        sql = """
            UPDATE team_daily_roster
            SET player_basic_id = player_id, updated_at = :now
            WHERE person_type = 'player'
              AND player_basic_id IS NULL
              AND player_id IN (SELECT player_id FROM player_basic)
        """
        res = session.execute(text(sql), {"now": datetime.now(KST)})
        session.commit()
        logger.info("   ✅ Team daily roster player links resolved: %s updated", res.rowcount)  # type: ignore[attr-defined]


async def _resolve_null_player_ids_step() -> None:
    from scripts.maintenance.resolve_null_player_ids_conservative import (
        DEFAULT_OUTPUT_DIR,
        DEFAULT_OVERRIDES_CSV,
        DEFAULT_ROW_OVERRIDES_CSV,
        DEFAULT_TABLES,
        DEFAULT_YEARS,
        resolve_null_player_ids,
    )

    logger.info("   - Conservatively resolving NULL player_id values...")
    result = resolve_null_player_ids(
        years=DEFAULT_YEARS,
        tables=DEFAULT_TABLES,
        overrides_csv=DEFAULT_OVERRIDES_CSV,
        row_overrides_csv=DEFAULT_ROW_OVERRIDES_CSV,
        output_dir=DEFAULT_OUTPUT_DIR,
        apply=True,
        backup=False,
        delete_duplicates=True,
    )
    logger.info(
        "   ✅ NULL player_id resolution complete: %s groups resolved (%s rows updated, %s duplicates removed)",
        result.get("resolved_groups", 0),
        result.get("updated_rows", 0),
        result.get("duplicate_null_rows", 0),
    )


async def _team_events_step() -> None:
    from src.crawlers.team_event_crawler import TeamEventCrawler

    await TeamEventCrawler(days_back=14).run(save=True)
    logger.info("   ✅ Team events crawl complete")


async def _fan_culture_step() -> None:
    from src.crawlers.fan_culture_crawler import FanCultureCrawler

    await FanCultureCrawler().run(save=True)
    logger.info("   ✅ Fan culture crawl complete")


async def run_weekly_maintenance(
    profile_limit: int = 100,
) -> None:
    """Run weekly maintenance.

    Args:
        profile_limit: Profile Limit.

    """
    logger.info("\n%s", "=" * 60)

    logger.info("🚀 KBO Weekly Maintenance Started: %s", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("%s", "=" * 60)

    await _run_weekly_step(
        "👤 Step 1: Enriching Player Profiles...",
        "Error during profile enrichment",
        lambda: _profile_enrichment_step(profile_limit),
    )
    await _run_weekly_step("🩺 Step 2: Running Database Healthcheck...", "Error during healthcheck", _healthcheck_step)
    await _run_weekly_step(
        "🛠️  Step 2.5: Resolving NULL team_code entries...",
        "Error during team_code resolution",
        _resolve_null_team_codes_step,
    )
    await _run_weekly_step(
        "🛠️  Step 2.6: Resolving NULL player_basic_id in team_daily_roster...",
        "Error during player_basic_id resolution",
        _resolve_roster_player_links_step,
    )
    await _run_weekly_step(
        "🛠️  Step 2.7: Conservatively resolving NULL player_id values...",
        "Error during NULL player_id resolution",
        _resolve_null_player_ids_step,
    )
    await _run_weekly_step("📅 Step 3: Crawling Team Events & News...", "Error crawling team events", _team_events_step)
    await _run_weekly_step(
        "🎵 Step 4: Crawling Fan Culture & Cheer Songs...",
        "Error crawling fan culture",
        _fan_culture_step,
    )

    logger.info("\n%s", "=" * 60)
    logger.info("🏁 Weekly Maintenance Finished")
    logger.info("%s\n", "=" * 60)


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Weekly Maintenance Orchestrator")
    parser.add_argument("--profile-limit", type=int, default=200, help="Max profiles to enrich")

    args = parser.parse_args()
    asyncio.run(run_weekly_maintenance(profile_limit=args.profile_limit))
    return 0


if __name__ == "__main__":
    main()
