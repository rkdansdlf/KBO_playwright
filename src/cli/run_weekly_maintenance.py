"""
KBO Weekly Maintenance Orchestrator.
Performs player profile enrichment, DB health checks, and OCI cleanup.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

from src.cli.collect_profiles import collect_profiles
from src.cli.db_healthcheck import main as healthcheck_main
from src.db.engine import SessionLocal
from src.sync.oci_sync import OCISync
from src.utils.safe_print import safe_print as print

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
    except Exception as exc:
        print(f"   ❌ Error during profile enrichment: {exc}")

    # 2. Database Healthcheck
    print("\n🩺 Step 2: Running Database Healthcheck...")
    try:
        healthcheck_main([])
        print("   ✅ Healthcheck complete")
    except Exception as exc:
        print(f"   ❌ Error during healthcheck: {exc}")

    # 3. OCI Cleanup (Duplicates)
    print("\n🧹 Step 3: Cleaning up OCI Duplicates...")
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
        except Exception as exc:
            print(f"   ❌ Error during OCI cleanup: {exc}")

    if sync:
        print("\n☁️ Step 4: Synchronizing Updated Profiles to OCI...")
        if not oci_url:
            print("   ⚠️ OCI_DB_URL not set, skipping sync")
        else:
            with SessionLocal() as session:
                syncer = OCISync(oci_url, session)
                try:
                    # Sync both players (master) and player_basic
                    syncer.sync_players()
                    syncer.sync_player_basic()
                    print("   ✅ OCI synchronization completed")
                except Exception as exc:
                    print(f"   ❌ OCI sync error: {exc}")
                finally:
                    syncer.close()

    print(f"\n{'=' * 60}")
    print(f"🏁 Weekly Maintenance Finished")
    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="KBO Weekly Maintenance Orchestrator")
    parser.add_argument("--profile-limit", type=int, default=200, help="Max profiles to enrich")
    parser.add_argument("--sync", action="store_true", help="Sync updated profiles to OCI")
    
    args = parser.parse_args()
    asyncio.run(run_weekly_maintenance(profile_limit=args.profile_limit, sync=args.sync))

if __name__ == "__main__":
    main()
