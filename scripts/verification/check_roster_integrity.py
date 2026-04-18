
import os
import sys
import argparse
from datetime import datetime, date, timedelta
from typing import List

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.db.engine import SessionLocal
from src.models.team import TeamDailyRoster
from sqlalchemy import func

def check_integrity(year: int):
    print(f"🧐 Auditing Team Daily Roster Integrity for Year: {year}...")
    
    # 2015 onwards should have 10 teams
    expected_team_count = 10
    standard_teams = ["LG", "HH", "SS", "KT", "OB", "LT", "HT", "NC", "SK", "WO"]
    
    # Season start and end dates (approximate for verification)
    start_date = date(year, 3, 23)
    end_date = date(year, 10, 31)
    
    # Current date for 2026 check
    if year == 2026:
        end_date = date(2026, 4, 15)

    missing_days = []
    partial_days = []
    
    with SessionLocal() as session:
        current_date = start_date
        while current_date <= end_date:
            # Check distinct team codes for this date
            teams_on_date = session.query(TeamDailyRoster.team_code).filter(
                TeamDailyRoster.roster_date == current_date
            ).distinct().all()
            
            team_codes = [t[0] for t in teams_on_date]
            count = len(team_codes)
            
            if count == 0:
                missing_days.append(current_date)
                # print(f"  ❌ {current_date}: COMPLETELY MISSING")
            elif count < expected_team_count:
                missing = [t for t in standard_teams if t not in team_codes]
                partial_days.append((current_date, missing))
                # print(f"  ⚠️ {current_date}: PARTIAL ({count}/{expected_team_count}) - Missing: {missing}")
            
            current_date += timedelta(days=1)

    # Print Summary
    total_days = (end_date - start_date).days + 1
    complete_days = total_days - len(missing_days) - len(partial_days)
    
    print(f"\n{'='*40}")
    print(f"📊 SUMMARY FOR {year}")
    print(f"{'='*40}")
    print(f"✅ Complete Days: {complete_days}/{total_days}")
    print(f"❌ Missing Days:  {len(missing_days)}")
    print(f"⚠️ Partial Days:  {len(partial_days)}")
    print(f"{'='*40}")
    
    if partial_days:
        print("\n🔍 Details of Partial Days:")
        for d, missing in partial_days[:10]:
            print(f"  - {d}: Missing teams {missing}")
        if len(partial_days) > 10:
            print(f"  ... and {len(partial_days)-10} more.")

    if missing_days:
        print("\n🚫 Sample of Missing Days:")
        for d in missing_days[:10]:
            print(f"  - {d}")
        if len(missing_days) > 10:
            print(f"  ... and {len(missing_days)-10} more.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KBO Roster Data Integrity Checker")
    parser.add_argument("--year", type=int, help="Target year to check", default=datetime.now().year)
    args = parser.parse_args()
    
    check_integrity(args.year)
