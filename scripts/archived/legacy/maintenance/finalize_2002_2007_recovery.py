import os
import subprocess
import sys
import time

# Define the years to process
YEARS = range(2002, 2008)


def is_process_running(process_name_pattern):
    try:
        # Check if recover_historical_games.py is running
        res = subprocess.run(["ps", "aux"], capture_output=True, text=True)
        return process_name_pattern in res.stdout
    except Exception:  # noqa: BLE001
        return False


def run_command(cmd):
    print(f"🚀 Running: {cmd}")
    ret = os.system(cmd)
    if ret != 0:
        print(f"❌ Command failed: {cmd}")
        # We generally continue or exit?
        # For pipeline, better to exit if backfill fails.
        if "backfill" in cmd:
            sys.exit(1)
    else:
        print(f"✅ Success: {cmd}")


def main():
    print("⏳ Waiting for `recover_historical_games.py` to finish...")

    # Wait loop
    while is_process_running("recover_historical_games.py"):
        time.sleep(60)
        print("... still running (checked at " + time.strftime("%H:%M:%S") + ")")

    print("\n🎉 Recovery process appears finished. Starting post-processing...")

    # 1. Backfill Season ID
    print("\n🔄 Step 1: Backfilling Season ID (2002-2007)...")
    run_command("venv/bin/python3 scripts/maintenance/backfill_season_id.py --start-year 2002 --end-year 2007")

    # 2. Audit
    print("\n🔍 Step 2: Auditing Data Integrity (2002-2007)...")
    run_command("venv/bin/python3 scripts/maintenance/audit_game_stats.py --start-year 2002 --end-year 2007")

    # 3. Sync to OCI
    print("\n☁️ Step 3: Syncing to OCI (2002-2007)...")
    for year in YEARS:
        print(f"   Syncing {year}...")
        run_command(f"venv/bin/python3 -m src.cli.sync_oci --game-details --year {year}")

    print("\n✅ All tasks completed for 2002-2007 Recovery Pipeline!")


if __name__ == "__main__":
    main()
