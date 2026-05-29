import subprocess

for year in range(2001, 2026):
    print(f"🚀 Syncing year {year}...")
    cmd = [".venv/bin/python3", "-m", "src.cli.sync_oci", "--game-details", "--year", str(year), "--unsynced-only"]
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ Failed to sync year {year}")
        # sys.exit(1) # Continue with next year even if one fails
