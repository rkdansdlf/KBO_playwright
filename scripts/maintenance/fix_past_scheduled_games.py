#!/usr/bin/env python3
"""
Fix past games stuck in 'SCHEDULED' status.
Updates them to 'UNRESOLVED_MISSING' so the refresh logic will attempt to find their results.
"""
import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

def fix_scheduled_games():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"Checking for games scheduled on or before {today} that are still 'SCHEDULED'...")
    
    with SessionLocal() as session:
        # Find affected games
        select_sql = text("""
            SELECT game_id, game_date FROM game 
            WHERE game_status = 'SCHEDULED' AND game_date <= :today
        """)
        rows = session.execute(select_sql, {"today": today}).fetchall()
        
        if not rows:
            print("No stale scheduled games found.")
            return

        print(f"Found {len(rows)} stale games. Updating to 'UNRESOLVED_MISSING'...")
        
        update_sql = text("""
            UPDATE game 
            SET game_status = 'UNRESOLVED_MISSING', updated_at = CURRENT_TIMESTAMP
            WHERE game_status = 'SCHEDULED' AND game_date <= :today
        """)
        result = session.execute(update_sql, {"today": today})
        session.commit()
        
        print(f"Successfully updated {result.rowcount} rows.")
        for row in rows:
            print(f"  - {row[0]} ({row[1]})")

if __name__ == "__main__":
    fix_scheduled_games()
