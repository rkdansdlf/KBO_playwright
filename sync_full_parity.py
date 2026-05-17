import subprocess
import sys

years = list(range(2001, 2027))
for year in years:
    print(f"🚀 FULL PARITY Syncing year {year}...")
    # Without --unsynced-only to trigger _purge_game_detail_children_for_year
    cmd = [".venv/bin/python3", "-m", "src.cli.sync_oci", "--game-details", "--year", str(year)]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ Failed to sync year {year}")
        # sys.exit(1)

print("✅ Full parity sync complete.")
