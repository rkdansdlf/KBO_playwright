"""
KBO Weekly Maintenance Orchestrator.
Performs player profile enrichment, DB health checks, team events, fan culture crawling, and OCI cleanup/sync.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from src.cli.collect_profiles import collect_profiles
from src.cli.db_healthcheck import main as healthcheck_main
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


async def run_weekly_maintenance(
    profile_limit: int = 100,
    sync: bool = False,
):
    print(f"\n{'=' * 60}")
    print(f"🚀 KBO Weekly Maintenance Started: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    # 1. Player Profile Enrichment
    print("\n👤 Step 1: Enriching Player Profiles...")
    try:
        await collect_profiles(limit=profile_limit)
        print("   ✅ Profile enrichment complete")
    except Exception:
        logger.exception("   ❌ Error during profile enrichment")

    # 2. Database Healthcheck
    print("\n🩺 Step 2: Running Database Healthcheck...")
    try:
        healthcheck_main([])
        print("   ✅ Healthcheck complete")
    except Exception:
        logger.exception("   ❌ Error during healthcheck")

    # 3. Crawl Team Events/News
    print("\n📅 Step 3: Crawling Team Events & News...")
    try:
        from src.crawlers.team_event_crawler import TeamEventCrawler

        await TeamEventCrawler(days_back=14).run(save=True)
        print("   ✅ Team events crawl complete")
    except Exception:
        logger.exception("   ❌ Error crawling team events")

    # 4. Crawl Fan Culture (Cheer Songs)
    print("\n🎵 Step 4: Crawling Fan Culture & Cheer Songs...")
    try:
        from src.crawlers.fan_culture_crawler import FanCultureCrawler

        await FanCultureCrawler().run(save=True)
        print("   ✅ Fan culture crawl complete")
    except Exception:
        logger.exception("   ❌ Error crawling fan culture")

    # 5. OCI Cleanup (Duplicates)
    print("\n🧹 Step 5: Cleaning up OCI Duplicates...")
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        print("   ⚠️ OCI_DB_URL not set, skipping cleanup")
    else:
        try:
            # We call the script via subprocess to handle its own imports/setup if needed,
            # or just call its logic if easily importable.
            # cleanup_oci.py is in scripts/maintenance/
            cmd = [sys.executable, "scripts/maintenance/cleanup_oci.py", "--apply"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"   ✅ OCI Cleanup output:\n{result.stdout}")
            else:
                print(f"   ❌ OCI Cleanup failed:\n{result.stderr}")
        except Exception:
            logger.exception("   ❌ Error during OCI cleanup")

    if sync:
        print("\n☁️ Step 6: Synchronizing Updated Data to OCI...")
        if not oci_url:
            print("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    # Sync player_basic before master/profile records to match publish dependency order.
                    print("   - Syncing player basics...")
                    syncer.sync_player_basic()
                    print("   - Syncing players...")
                    syncer.sync_players()
                    print("   - Syncing team events...")
                    syncer.sync_team_events()
                    print("   - Syncing fan culture (rivalries, songs, chants)...")
                    syncer.sync_team_rivalries()
                    syncer.sync_cheer_songs()
                    syncer.sync_cheer_chants()
                    print("   ✅ OCI synchronization completed")
                except Exception:
                    logger.exception("   ❌ OCI sync error")
                finally:
                    syncer.close()

    print(f"\n{'=' * 60}")
    print("🏁 Weekly Maintenance Finished")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="KBO Weekly Maintenance Orchestrator")
    parser.add_argument("--profile-limit", type=int, default=200, help="Max profiles to enrich")
    parser.add_argument("--sync", action="store_true", help="Sync updated profiles to OCI")

    args = parser.parse_args()
    asyncio.run(run_weekly_maintenance(profile_limit=args.profile_limit, sync=args.sync))


if __name__ == "__main__":
    main()
