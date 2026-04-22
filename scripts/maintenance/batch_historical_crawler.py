"""
Batch Historical Crawler Wrapper
Iterates through a given year and month to crawl full KBO season data.
Features checkpointing to resume from the last successful date.
"""
import os
import sys
import json
import argparse
import asyncio
from datetime import datetime, timedelta

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.cli.run_daily_update import run_update

STATUS_FILE = "data/backfill_status.json"

def load_status():
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_status(year, last_date):
    status = load_status()
    status[str(year)] = last_date
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)

async def process_batch(year: int, start_month: int, end_month: int, sync: bool):
    status = load_status()
    current_stored_date = status.get(str(year))
    
    # Simple strategy: iterate months, but run_daily_update handles schedule fetching.
    # We can call run_daily_update for the 15th of each month to fetch that month's schedule
    # then iterate through days (or just let run_daily_update handle monthly logic).
    
    # Actually, run_daily_update --date YYYYMMDD --sync does:
    # 1. Fetch monthly schedule for that date's month
    # 2. Finalize the specific game(s) on that date
    
    # Better Batch Strategy:
    # Iterate through all dates in the range. 
    # run_update is efficient enough to skip if already completed (with some tweaks if needed).
    
    start_date = datetime(year, start_month, 1)
    # End date is last day of end_month
    if end_month == 12:
        end_date = datetime(year, 12, 31)
    else:
        end_date = datetime(year, end_month + 1, 1) - timedelta(days=1)
        
    delta = timedelta(days=1)
    curr = start_date
    
    # If checkpoint exists, resume from the day after
    if current_stored_date:
        resume_date = datetime.strptime(current_stored_date, "%Y%m%d") + delta
        if resume_date > curr:
            curr = resume_date
            print(f"🔄 Resuming from {curr.strftime('%Y-%m-%d')}...")

    while curr <= end_date:
        date_str = curr.strftime("%Y%m%d")
        print(f"\n📅 [BATCH] Processing {date_str}...")
        
        try:
            # We call the CLI logic
            # Note: run_update is an async function in run_daily_update.py
            await run_update(date_str, sync=sync, run_auto_healer=False)
            save_status(year, date_str)
        except Exception as e:
            print(f"❌ Error on {date_str}: {e}")
            # We don't stop unless it's a critical error (optional: add retry logic)
            
        curr += delta

async def main():
    parser = argparse.ArgumentParser(description="KBO Multi-year Batch Backfill")
    parser.add_argument("--year", type=int, required=True, help="Year to backfill (e.g. 2024)")
    parser.add_argument("--start-month", type=int, default=3, help="Month to start (3 = March)")
    parser.add_argument("--end-month", type=int, default=11, help="Month to end (11 = Nov)")
    parser.add_argument("--sync", action="store_true", help="Sync to OCI after each day")
    
    args = parser.parse_args()
    
    await process_batch(args.year, args.start_month, args.end_month, args.sync)

if __name__ == "__main__":
    asyncio.run(main())
