"""
Player Data Integrity Audit Tool.
Performs: 
1. Stub detection (Missing metadata)
2. Statistical Reconciliation (Game vs Season Totals)
3. Retirement Audit (Stale 'ACTIVE' status)
"""
import os
import sys
from sqlalchemy import text
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.db.engine import SessionLocal

def run_audit():
    load_dotenv()
    print("🔍 Starting Player Master Data Integrity Audit...")
    
    with SessionLocal() as session:
        # 1. Stub Detection: Records with zero height or no photo_url
        print("\n[STUB DETECTION]")
        stubs = session.execute(text("""
            SELECT player_id, name, team FROM player_basic 
            WHERE status = 'active' AND (photo_url IS NULL OR height_cm IS NULL)
        """)).fetchall()
        print(f"  ⚠️  Found {len(stubs)} active players with missing profile metadata.")
        if stubs:
            for s in stubs[:5]:
                print(f"    - {s.player_id}: {s.name} ({s.team})")
            if len(stubs) > 5:
                print(f"    ... and {len(stubs)-5} more.")

        # 2. Statistical Reconciliation: 2024 Hits Check
        # Comparing sum of game_batting_stats with player_season_batting
        print("\n[STATISTICAL RECONCILIATION - 2024 HITS]")
        recon_query = """
        WITH game_sums AS (
            SELECT gbs.player_id, SUM(gbs.hits) as total_hits
            FROM game_batting_stats gbs
            JOIN game g ON gbs.game_id = g.game_id
            JOIN kbo_seasons ks ON g.season_id = ks.season_id
            WHERE ks.season_year = 2024 AND ks.league_type_code = 0
            GROUP BY gbs.player_id
        )
        SELECT s.player_id, pb.name, s.hits as season_hits, g.total_hits as calc_hits
        FROM player_season_batting s
        JOIN game_sums g ON s.player_id = g.player_id
        JOIN player_basic pb ON s.player_id = pb.player_id
        WHERE s.season = 2024 AND s.hits != g.total_hits
        """
        mismatches = session.execute(text(recon_query)).fetchall()
        if mismatches:
            print(f"  ❌ Found {len(mismatches)} players with 2024 hits mismatch between games and season table:")
            for m in mismatches[:5]:
                print(f"    - {m.name} ({m.player_id}): Season={m.season_hits}, Calculated={m.calc_hits}")
        else:
            print("  ✅ 2024 Hits reconciliation passed for all players.")

        # 3. Retirement Audit (Heuristic)
        # Players marked 'active' but no games played in 2025 or 2026
        print("\n[RETIREMENT AUDIT]")
        stale_query = """
        SELECT player_id, name, team FROM player_basic 
        WHERE status = 'active' AND player_id NOT IN (
            SELECT DISTINCT player_id FROM game_batting_stats WHERE game_id LIKE '2025%' OR game_id LIKE '2026%'
        ) AND player_id NOT IN (
            SELECT DISTINCT player_id FROM game_pitching_stats WHERE game_id LIKE '2025%' OR game_id LIKE '2026%'
        )
        """
        stale_players = session.execute(text(stale_query)).fetchall()
        print(f"  ⚠️  Found {len(stale_players)} players marked 'active' who haven't played since 2024.")
        if stale_players:
            print("     (These are likely retired but status hasn't been updated)")

    print("\n✨ Audit Finished.")

if __name__ == "__main__":
    run_audit()
