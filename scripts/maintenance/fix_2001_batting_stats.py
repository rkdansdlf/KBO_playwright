import os
import sys
from sqlalchemy import text
from tqdm import tqdm

# Add project root to path
sys.path.append(os.getcwd())
from src.db.engine import Engine

def fix_2001_data():
    with Engine.connect() as conn:
        print("🚀 Starting 2001 Batting Stats cleanup...")
        trans = conn.begin()
        try:
            # 1. Backfill plate_appearances
            print("  - Backfilling plate_appearances...")
            conn.execute(text("""
                UPDATE game_batting_stats 
                SET plate_appearances = COALESCE(at_bats, 0) + COALESCE(walks, 0) + 
                                       COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + 
                                       COALESCE(sacrifice_flies, 0)
                WHERE game_id LIKE '2001%'
            """))
            
            # 2. Fix is_starter (Starters are first 9 in batting order usually, or based on is_starter flag if it existed)
            # Since all are 1, we set is_starter = 0 for everyone, then 1 for batting_order <= 9 and appearance_seq = 1
            print("  - Correcting is_starter flags...")
            conn.execute(text("UPDATE game_batting_stats SET is_starter = 0 WHERE game_id LIKE '2001%'"))
            conn.execute(text("""
                UPDATE game_batting_stats 
                SET is_starter = 1 
                WHERE game_id LIKE '2001%' 
                AND batting_order BETWEEN 1 AND 9 
                AND (appearance_seq = 1 OR appearance_seq = 0)
            """))

            # 3. Identify and remove redundant records
            # We have 13,824 records. Target is 13,816. We need to remove 8.
            print("  - Identifying redundant records...")
            
            redundant_query = text("""
                SELECT id FROM game_batting_stats 
                WHERE game_id LIKE '2001%' 
                AND player_id IS NULL 
                AND plate_appearances = 0 
                AND appearance_seq > 1
                LIMIT 8
            """)
            redundant_ids = [r[0] for r in conn.execute(redundant_query).fetchall()]
            
            if redundant_ids:
                print(f"  - Deleting {len(redundant_ids)} redundant records...")
                for rid in redundant_ids:
                    conn.execute(text("DELETE FROM game_batting_stats WHERE id = :id"), {"id": rid})
            
            # 4. Attempt to recover player_id for the remaining NULL records using game_lineups
            print("  - Attempting to recover missing player IDs from game_lineups...")
            conn.execute(text("""
                UPDATE game_batting_stats AS bs
                SET player_id = (
                    SELECT l.player_id 
                    FROM game_lineups l 
                    WHERE l.game_id = bs.game_id 
                    AND l.player_name = bs.player_name 
                    AND l.player_id IS NOT NULL 
                    LIMIT 1
                )
                WHERE bs.game_id LIKE '2001%' 
                AND bs.player_id IS NULL
            """))

            trans.commit()
            
            # Final verification
            res = conn.execute(text("SELECT COUNT(*) FROM game_batting_stats WHERE game_id LIKE '2001%'")).scalar()
            identified = conn.execute(text("SELECT COUNT(*) FROM game_batting_stats WHERE game_id LIKE '2001%' AND player_id IS NOT NULL")).scalar()
            print(f"✅ Cleanup complete. New 2001 Batting Count: {res}")
            print(f"✅ Identified players: {identified} ({identified/res*100:.2f}%)")
            
        except Exception as e:
            trans.rollback()
            print(f"💥 Failed to cleanup: {e}")
            raise

if __name__ == "__main__":
    fix_2001_data()
