import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cli.daily_preview_batch import run_preview_batch

async def main():
    # Only March dates that had KBO games in 2026
    # Let's just loop from Mar 01 to Mar 31
    for day in range(1, 32):
        target_date = f"202603{day:02d}"
        print(f"--- Regenerating previews for {target_date} ---")
        try:
            # Rehydrate preview aggregating Matchups/Starters data
            # No sync here to speed it up. Will sync all later via sync_oci
            await run_preview_batch(target_date, sync_to_oci=False)
        except Exception as e:
            print(f"Failed for {target_date}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
