"""KBO Weekly Maintenance Orchestrator.
Performs player profile enrichment, DB health checks, team events, fan culture crawling, and OCI cleanup/sync.
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

from scripts.maintenance.cleanup_oci import cleanup_oci_duplicates
from src.cli.collect_profiles import collect_profiles
from src.cli.db_healthcheck import main as healthcheck_main
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync

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
    healthcheck_main([])
    logger.info("   ✅ Healthcheck complete")


async def _team_events_step() -> None:
    from src.crawlers.team_event_crawler import TeamEventCrawler

    await TeamEventCrawler(days_back=14).run(save=True)
    logger.info("   ✅ Team events crawl complete")


async def _fan_culture_step() -> None:
    from src.crawlers.fan_culture_crawler import FanCultureCrawler

    await FanCultureCrawler().run(save=True)
    logger.info("   ✅ Fan culture crawl complete")


def _cleanup_oci_duplicates(oci_url: str | None) -> None:
    logger.info("\n🧹 Step 5: Cleaning up OCI Duplicates...")
    if not oci_url:
        logger.warning("   ⚠️ OCI_DB_URL not set, skipping cleanup")
        return
    try:
        counts = cleanup_oci_duplicates(database_url=oci_url, apply=True)
        logger.info("   ✅ OCI Cleanup committed:")
        for key, value in counts.items():
            logger.info("      %s: %s", key, value)
    except WEEKLY_MAINTENANCE_EXCEPTIONS:
        logger.exception("   ❌ Error during OCI cleanup")


def _sync_weekly_to_oci(oci_url: str | None) -> None:
    logger.info("\n☁️ Step 6: Synchronizing Updated Data to OCI...")
    if not oci_url:
        logger.warning("   ⚠️ OCI_DB_URL not set, skipping sync")
        return
    with SessionLocal() as session:
        syncer = OCISync(oci_url, session)
        try:
            logger.info("   - Syncing player basics...")
            syncer.sync_player_basic()
            logger.info("   - Syncing players...")
            syncer.sync_players()
            logger.info("   - Syncing team events...")
            syncer.sync_team_events()
            logger.info("   - Syncing fan culture (rivalries, songs, chants)...")
            syncer.sync_team_rivalries()
            syncer.sync_cheer_songs()
            syncer.sync_cheer_chants()
            logger.info("   ✅ OCI synchronization completed")
        except WEEKLY_MAINTENANCE_EXCEPTIONS:
            logger.exception("   ❌ OCI sync error")
        finally:
            syncer.close()


async def run_weekly_maintenance(
    profile_limit: int = 100,
    *,
    sync: bool = False,
) -> None:
    """Runs weekly maintenance.

    Args:
        profile_limit: Profile Limit.

    """
    logger.info("\n%s", "=" * 60)
    logger.info("🚀 KBO Weekly Maintenance Started: %s", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("%s", "=" * 60)

    oci_url = os.getenv("OCI_DB_URL")
    await _run_weekly_step(
        "👤 Step 1: Enriching Player Profiles...",
        "Error during profile enrichment",
        lambda: _profile_enrichment_step(profile_limit),
    )
    await _run_weekly_step("🩺 Step 2: Running Database Healthcheck...", "Error during healthcheck", _healthcheck_step)
    await _run_weekly_step("📅 Step 3: Crawling Team Events & News...", "Error crawling team events", _team_events_step)
    await _run_weekly_step(
        "🎵 Step 4: Crawling Fan Culture & Cheer Songs...",
        "Error crawling fan culture",
        _fan_culture_step,
    )
    _cleanup_oci_duplicates(oci_url)

    if sync:
        _sync_weekly_to_oci(oci_url)

    logger.info("\n%s", "=" * 60)
    logger.info("🏁 Weekly Maintenance Finished")
    logger.info("%s\n", "=" * 60)


def main() -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Weekly Maintenance Orchestrator")
    parser.add_argument("--profile-limit", type=int, default=200, help="Max profiles to enrich")
    parser.add_argument("--sync", action="store_true", help="Sync updated profiles to OCI")

    args = parser.parse_args()
    asyncio.run(run_weekly_maintenance(profile_limit=args.profile_limit, sync=args.sync))


if __name__ == "__main__":
    main()
