"""
Player Status Automation Script

Updates `status` and `retire_year` in `player_basic` and `players` tables
based on empirical KBO game appearances historically collected.
"""
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.db.engine import SessionLocal

def run_update():
    load_dotenv()
    print("🧹 Starting Player Status Automation...")

    with SessionLocal() as session:
        # 1. Identify max season for all players
        agg_query = """
        WITH player_max_season AS (
            SELECT 
                player_id,
                MAX(max_season) as final_season
            FROM (
                SELECT player_id, MAX(season) as max_season FROM player_season_batting GROUP BY player_id
                UNION ALL
                SELECT player_id, MAX(season) as max_season FROM player_season_pitching GROUP BY player_id
            ) combined
            GROUP BY player_id
        )
        SELECT player_id, final_season FROM player_max_season
        """
        
        results = session.execute(text(agg_query)).fetchall()
        print(f"📊 Found appearance history for {len(results)} players.")

        active_count = 0
        retired_count = 0
        
        # We will bulk update using SQLite CTE or individual updates.
        # Given SQLite, explicit iteration or a fast bulk mapping is best.
        active_ids = [r.player_id for r in results if r.final_season >= 2024]
        retired_mappings = [{"p_id": r.player_id, "ryear": r.final_season} for r in results if r.final_season < 2024]
        
        active_count = len(active_ids)
        retired_count = len(retired_mappings)
        
        print(f"  - Calculated Active (>=2024): {active_count}")
        print(f"  - Calculated Retired (< 2024): {retired_count}")

        if active_ids:
            # Batch update for SQLite
            for p_id in active_ids:
                session.execute(text("""
                    UPDATE player_basic 
                    SET status = 'active' 
                    WHERE player_id = :p_id AND (status != 'staff' OR status IS NULL)
                """), {"p_id": p_id})
                
                session.execute(text("""
                    UPDATE players 
                    SET status = 'ACTIVE' 
                    WHERE kbo_person_id = CAST(:p_id AS TEXT)
                """), {"p_id": p_id})

        if retired_mappings:
            # Batch update for SQLite
            for mapping in retired_mappings:
                session.execute(text("""
                    UPDATE player_basic 
                    SET status = 'retired' 
                    WHERE player_id = :p_id AND (status != 'staff' OR status IS NULL)
                """), mapping)
                
                session.execute(text("""
                    UPDATE players 
                    SET status = 'RETIRED', retire_year = :ryear 
                    WHERE kbo_person_id = CAST(:p_id AS TEXT)
                """), mapping)

        # Commit all changes
        session.commit()
        print("✅ Status successfully written to local DB.")

        # Optional: Sync to OCI if needed right now. Or we can just let OCI Sync handle it 
        # by calling the dedicated sync scripts.
        print("\n✨ Automation complete. Run OCI Sync to update the production database.")

if __name__ == "__main__":
    run_update()
