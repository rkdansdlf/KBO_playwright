"""
KBO Weekly Maintenance Orchestrator.
Performs player profile enrichment, DB health checks, team events, fan culture crawling, and OCI cleanup/sync.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from scripts.legacy.maintenance.cleanup_oci import cleanup_oci_duplicates
from src.cli.collect_profiles import collect_profiles
from src.cli.db_healthcheck import main as healthcheck_main
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


async def run_weekly_maintenance(
    profile_limit: int = 100,
    sync: bool = False,
) -> None:
    logger.info(f"\n{'=' * 60}")  # noqa: G004
    logger.info(f"🚀 KBO Weekly Maintenance Started: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")  # noqa: G004
    logger.info(f"{'=' * 60}")  # noqa: G004

    # 1. Player Profile Enrichment
    logger.info("\n👤 Step 1: Enriching Player Profiles...")
    try:
        await collect_profiles(limit=profile_limit)
        logger.info("   ✅ Profile enrichment complete")
    except Exception:
        logger.exception("   ❌ Error during profile enrichment")

    # 2. Database Healthcheck
    logger.info("\n🩺 Step 2: Running Database Healthcheck...")
    try:
        healthcheck_main([])
        logger.info("   ✅ Healthcheck complete")
    except Exception:
        logger.exception("   ❌ Error during healthcheck")

    # 3. Crawl Team Events/News
    logger.info("\n📅 Step 3: Crawling Team Events & News...")
    try:
        from src.crawlers.team_event_crawler import TeamEventCrawler

        await TeamEventCrawler(days_back=14).run(save=True)
        logger.info("   ✅ Team events crawl complete")
    except Exception:
        logger.exception("   ❌ Error crawling team events")

    # 4. Crawl Fan Culture (Cheer Songs)
    logger.info("\n🎵 Step 4: Crawling Fan Culture & Cheer Songs...")
    try:
        from src.crawlers.fan_culture_crawler import FanCultureCrawler

        await FanCultureCrawler().run(save=True)
        logger.info("   ✅ Fan culture crawl complete")
    except Exception:
        logger.exception("   ❌ Error crawling fan culture")

    # 5. OCI Cleanup (Duplicates)
    logger.info("\n🧹 Step 5: Cleaning up OCI Duplicates...")
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        logger.warning("   ⚠️ OCI_DB_URL not set, skipping cleanup")
    else:
        try:
            counts = cleanup_oci_duplicates(database_url=oci_url, apply=True)
            logger.info("   ✅ OCI Cleanup committed:")
            for key, value in counts.items():
                logger.info("      %s: %s", key, value)
        except Exception:
            logger.exception("   ❌ Error during OCI cleanup")

    if sync:
        logger.info("\n☁️ Step 6: Synchronizing Updated Data to OCI...")
        if not oci_url:
            logger.warning("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    # Sync player_basic before master/profile records to match publish dependency order.
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
                except Exception:
                    logger.exception("   ❌ OCI sync error")
                finally:
                    syncer.close()

    logger.info(f"\n{'=' * 60}")  # noqa: G004
    logger.info("🏁 Weekly Maintenance Finished")
    logger.info(f"{'=' * 60}\n")  # noqa: G004


def main() -> int:
    parser = argparse.ArgumentParser(description="KBO Weekly Maintenance Orchestrator")
    parser.add_argument("--profile-limit", type=int, default=200, help="Max profiles to enrich")
    parser.add_argument("--sync", action="store_true", help="Sync updated profiles to OCI")

    args = parser.parse_args()
    asyncio.run(run_weekly_maintenance(profile_limit=args.profile_limit, sync=args.sync))


if __name__ == "__main__":
    main()
