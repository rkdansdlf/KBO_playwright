
import sys
import os
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from src.db.engine import Engine
from src.models.game import Game

def backfill_season_id(year: int):
    Session = sessionmaker(bind=Engine)
    session = Session()
    
    try:
        print(f"🔄 Backfilling season_id for year {year}...")
        
        # Target season_id format: e.g. 2001 -> 20010
        # NOTE: This assumes Regular Season (0). If post-season, might be different.
        # But for now, let's set 20010 as default.
        target_season_id = int(f"{year}0")
        
        # Update query
        # We update all games starting with {year} regardless of current value if it's None
        # But to be safe, let's update all.
        
        stmt = text(f"""
            UPDATE game 
            SET season_id = :sid 
            WHERE game_id LIKE '{year}%' AND (season_id IS NULL OR season_id != :sid)
        """)
        
        result = session.execute(stmt, {"sid": target_season_id})
        session.commit()
        
        print(f"✅ Updated {result.rowcount} games to season_id={target_season_id}")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument("--end-year", type=int, default=2001)
    args = parser.parse_args()
    
    for y in range(args.start_year, args.end_year + 1):
        backfill_season_id(y)
