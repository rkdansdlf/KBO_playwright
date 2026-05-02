"""
KBO Advanced Daily Data Update Orchestrator.
Fetches fielding, baserunning, and team-level cumulative stats.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from src.crawlers.fielding_stats_crawler import crawl_all_fielding_stats
from src.crawlers.baserunning_stats_crawler import crawl_baserunning_stats
from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler
from src.repositories.player_stats_repository import PlayerSeasonFieldingRepository, PlayerSeasonBaserunningRepository
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.safe_print import safe_print as print

KST = ZoneInfo("Asia/Seoul")

async def run_advanced_update(
    year: int,
    sync: bool = False,
    headless: bool = True,
):
    print(f"\n{'=' * 60}")
    print(f"🚀 KBO Advanced Daily Sync Started for Year: {year}")
    print(f"{'=' * 60}")

    # 1. Fielding Stats
    print("\n🛡️ Step 1: Crawling Fielding Stats...")
    try:
        fielding_records = crawl_all_fielding_stats(year)
        if fielding_records:
            # Adapt record keys to match ORM if necessary (team_id, year, position_id are already there)
            # Filter out records without player_id
            fielding_records = [r for r in fielding_records if r.get('player_id')]
            fielding_repo = PlayerSeasonFieldingRepository()
            saved = fielding_repo.upsert_many(fielding_records)
            print(f"   ✅ Saved {saved} fielding records")
    except Exception as exc:
        print(f"   ❌ Error crawling fielding stats: {exc}")

    # 2. Baserunning Stats
    print("\n🏃 Step 2: Crawling Baserunning Stats...")
    try:
        baserunning_records = crawl_baserunning_stats(year)
        if baserunning_records:
            # Filter out records without player_id
            baserunning_records = [r for r in baserunning_records if r.get('player_id')]
            baserunning_repo = PlayerSeasonBaserunningRepository()
            saved = baserunning_repo.upsert_many(baserunning_records)
            print(f"   ✅ Saved {saved} baserunning records")
    except Exception as exc:
        print(f"   ❌ Error crawling baserunning stats: {exc}")

    # 3. Team Batting Stats
    print("\n🏏 Step 3: Crawling Team Batting Stats...")
    try:
        batting_crawler = TeamBattingStatsCrawler()
        batting_stats = batting_crawler.crawl(year, persist=True, headless=headless)
        print(f"   ✅ Saved {len(batting_stats)} team batting records")
    except Exception as exc:
        print(f"   ❌ Error crawling team batting stats: {exc}")

    # 4. Team Pitching Stats
    print("\n⚾ Step 4: Crawling Team Pitching Stats...")
    try:
        pitching_crawler = TeamPitchingStatsCrawler()
        pitching_stats = pitching_crawler.crawl(year, persist=True, headless=headless)
        print(f"   ✅ Saved {len(pitching_stats)} team pitching records")
    except Exception as exc:
        print(f"   ❌ Error crawling team pitching stats: {exc}")

    if sync:
        print("\n☁️ Step 5: Synchronizing to OCI...")
        oci_url = os.getenv("OCI_DB_URL")
        if not oci_url:
            print("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    syncer.sync_fielding_stats(year)
                    syncer.sync_baserunning_stats(year)
                    syncer.sync_team_batting_stats(year)
                    syncer.sync_team_pitching_stats(year)
                    print("   ✅ OCI synchronization completed")
                except Exception as exc:
                    print(f"   ❌ OCI sync error: {exc}")
                finally:
                    syncer.close()

    print(f"\n{'=' * 60}")
    print(f"🏁 Advanced Daily Sync Finished for {year}")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="KBO Advanced Daily Data Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Run with browser UI")
    
    args = parser.parse_args()
    
    year = args.year or datetime.now(KST).year
    asyncio.run(run_advanced_update(year, sync=args.sync, headless=args.headless))

if __name__ == "__main__":
    main()
