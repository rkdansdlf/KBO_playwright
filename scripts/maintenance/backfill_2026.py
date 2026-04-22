"""
KBO 2026 Season Backfill Script.
Runs the daily update process for a list of target dates to recover missing data.
"""
import asyncio
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from src.cli.run_daily_update import run_update

async def main():
    # Dates identified in the audit as missing data (Exhibition + Regular season gaps)
    target_dates = [
        # April Regular Season Gaps
        "20260417",
        
        # March Exhibition Games
        "20260312", "20260313", "20260314", "20260315", "20260316",
        "20260317", "20260319", "20260320", "20260321", "20260322",
        "20260323", "20260324", "20260328", "20260329", "20260331"
    ]
    
    print(f"🚀 Starting backfill for {len(target_dates)} dates...")
    
    for date_str in target_dates:
        print(f"\n{'='*60}")
        print(f"📅 Processing Date: {date_str}")
        print(f"{'='*60}")
        
        try:
            # Run the full daily update pipeline for each date
            # sync=True to push to OCI immediately
            await run_update(
                target_date=date_str,
                sync=True,
                headless=True,
                limit=None,
                run_auto_healer=False,
            )
            print(f"✅ Finished processing {date_str}")
        except Exception as e:
            print(f"❌ Error processing {date_str}: {e}")
            
    print("\n✨ All backfill tasks completed!")

if __name__ == "__main__":
    asyncio.run(main())
