"""
KBO Periodic Extras Orchestrator.
Fetches Futures league data and retired player listings.
"""
from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.safe_print import safe_print as print

KST = ZoneInfo("Asia/Seoul")

async def run_periodic_extras(
    year: int,
    sync: bool = False,
):
    print(f"\n{'=' * 60}")
    print(f"🚀 KBO Periodic Extras Started for Year: {year}")
    print(f"{'=' * 60}")

    # 1. Futures League Data (Hitter)
    # Note: We assume these crawlers have a main() or similar entrypoint
    print("\n🔮 Step 1: Crawling Futures League Batting Stats...")
    try:
        import subprocess
        import sys
        cmd = [sys.executable, "-m", "src.crawlers.futures.futures_batting", "--year", str(year), "--save"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ Futures Hitter output:\n{result.stdout}")
        else:
            print(f"   ❌ Futures Hitter failed:\n{result.stderr}")
    except Exception as exc:
        print(f"   ❌ Error crawling futures stats: {exc}")

    # 2. Retired Player Listing
    print("\n👴 Step 2: Crawling Retired Player Listings...")
    try:
        # retired listing usually doesn't need a year, or it's for all
        cmd = [sys.executable, "-m", "src.crawlers.retire.listing", "--save"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ Retired Listing output:\n{result.stdout}")
        else:
            print(f"   ❌ Retired Listing failed:\n{result.stderr}")
    except Exception as exc:
        print(f"   ❌ Error crawling retired players: {exc}")

    if sync:
        print("\n☁️ Step 3: Synchronizing to OCI...")
        oci_url = os.getenv("OCI_DB_URL")
        if not oci_url:
            print("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    # Sync batting/pitching (includes futures if level='FUTURES')
                    syncer.sync_player_season_batting(year=year)
                    syncer.sync_player_season_pitching(year=year)
                    syncer.sync_player_basic()
                    print("   ✅ OCI synchronization completed")
                except Exception as exc:
                    print(f"   ❌ OCI sync error: {exc}")
                finally:
                    syncer.close()

    print(f"\n{'=' * 60}")
    print(f"🏁 Periodic Extras Finished")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="KBO Periodic Extras Orchestrator")
    parser.add_argument("--year", type=int, help="Target year. Defaults to current year.")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI")
    
    args = parser.parse_args()
    
    year = args.year or datetime.now(KST).year
    asyncio.run(run_periodic_extras(year, sync=args.sync))

if __name__ == "__main__":
    main()
