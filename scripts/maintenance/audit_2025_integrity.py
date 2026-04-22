"""
Audit script for 2025 historical data integrity.
Checks for score mismatches, missing PBP data, and unresolved player IDs.
"""
import os
import sys
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.engine import SessionLocal

def audit_2025():
    load_dotenv()
    print("🛡️ Starting 2025 Data Integrity Audit...")
    
    with SessionLocal() as session:
        # 1. Total Games Check
        res = session.execute(text("SELECT count(*) FROM game WHERE game_id LIKE '2025%'")).scalar()
        print(f"📊 Total 2025 games in DB: {res}")
        
        # 2. Score Mismatch Check (Final vs Inning Sum)
        print("\n🔍 Checking for score mismatches...")
        mismatch_query = """
        SELECT g.game_id, g.home_score, g.away_score, 
               SUM(CASE WHEN i.team_side = 'home' THEN i.runs ELSE 0 END) as calc_home,
               SUM(CASE WHEN i.team_side = 'away' THEN i.runs ELSE 0 END) as calc_away
        FROM game g
        JOIN game_inning_scores i ON g.game_id = i.game_id
        WHERE g.game_id LIKE '2025%' AND g.game_status != 'CANCELLED'
        GROUP BY g.game_id
        HAVING g.home_score != calc_home OR g.away_score != calc_away
        """
        mismatches = session.execute(text(mismatch_query)).fetchall()
        if mismatches:
            print(f"  ❌ Found {len(mismatches)} score mismatches:")
            for m in mismatches:
                print(f"    - {m.game_id}: Final({m.home_score}-{m.away_score}) vs Calc({m.calc_home}-{m.calc_away})")
        else:
            print("  ✅ All scores match inning sums.")
            
        # 3. Missing PBP Check
        print("\n🔍 Checking for missing PBP/Events...")
        missing_pbp_query = """
        SELECT game_id FROM game 
        WHERE game_id LIKE '2025%' 
        AND game_status IN ('COMPLETED', 'DRAW')
        AND game_id NOT IN (SELECT DISTINCT game_id FROM game_play_by_play)
        """
        missing_pbps = session.execute(text(missing_pbp_query)).fetchall()
        if missing_pbps:
            print(f"  ❌ Found {len(missing_pbps)} games missing PBP data:")
            for p in missing_pbps:
                print(f"    - {p.game_id}")
        else:
            print("  ✅ All completed games have PBP data.")
            
        # 4. Unresolved Players Check (player_id=0 or NULL)
        print("\n🔍 Checking for unresolved player IDs in batting stats...")
        unresolved_query = """
        SELECT game_id, player_id FROM game_batting_stats 
        WHERE game_id LIKE '2025%' AND (player_id IS NULL OR player_id = 0)
        """
        unresolved = session.execute(text(unresolved_query)).fetchall()
        if unresolved:
            print(f"  ❌ Found {len(unresolved)} unresolved player records across various games.")
        else:
            print("  ✅ All batting stats have resolved player IDs.")

    print("\n✨ Audit Complete.")

if __name__ == "__main__":
    audit_2025()
