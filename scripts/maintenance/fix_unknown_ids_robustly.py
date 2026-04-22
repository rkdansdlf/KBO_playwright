import sqlite3
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal

def fix_unknown_ids_robustly():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    with SessionLocal() as session:
        resolver = PlayerIdResolver(session)
        
        # 1. Target Batting Stats (NULL or temporary IDs >= 900000)
        print("🔍 Scanning for unknown batters...")
        cursor.execute("""
            SELECT b.id, b.game_id, b.player_name, b.team_code, strftime('%Y', g.game_date) as year, b.uniform_no, b.player_id
            FROM game_batting_stats b
            JOIN game g ON b.game_id = g.game_id
            WHERE (b.player_id IS NULL OR b.player_id >= 900000)
        """)
        
        rows = cursor.fetchall()
        print(f"Found {len(rows)} records to resolve.")
        
        for b_id, g_id, name, team, year, uniform, old_id in rows:
            new_id = resolver.resolve_id(name, team, int(year), uniform_no=uniform)
            
            if new_id and new_id != old_id:
                # Check if this player already exists in THIS game with the new_id
                # (To avoid UNIQUE constraint violation)
                cursor.execute("SELECT id FROM game_batting_stats WHERE game_id = ? AND player_id = ?", (g_id, new_id))
                existing = cursor.fetchone()
                
                if existing:
                    # Player already exists in this game. Delete the redundant 'Unknown' record.
                    # In a real scenario, we might want to merge stats, but usually these are duplicates.
                    cursor.execute("DELETE FROM game_batting_stats WHERE id = ?", (b_id,))
                else:
                    # Safe to update
                    try:
                        cursor.execute("UPDATE game_batting_stats SET player_id = ? WHERE id = ?", (new_id, b_id))
                    except sqlite3.IntegrityError:
                        # Fallback: if update still fails, just delete to keep DB clean
                        cursor.execute("DELETE FROM game_batting_stats WHERE id = ?", (b_id,))
        
        conn.commit()
        print("✅ Batting stats resolution complete.")

        # 2. Target Pitching Stats
        print("🔍 Scanning for unknown pitchers...")
        cursor.execute("""
            SELECT p.id, p.game_id, p.player_name, p.team_code, strftime('%Y', g.game_date) as year, p.uniform_no, p.player_id
            FROM game_pitching_stats p
            JOIN game g ON p.game_id = g.game_id
            WHERE (p.player_id IS NULL OR p.player_id >= 900000)
        """)
        
        rows_p = cursor.fetchall()
        for p_id_db, g_id, name, team, year, uniform, old_id in rows_p:
            new_id = resolver.resolve_id(name, team, int(year), uniform_no=uniform)
            if new_id and new_id != old_id:
                cursor.execute("SELECT id FROM game_pitching_stats WHERE game_id = ? AND player_id = ?", (g_id, new_id))
                if cursor.fetchone():
                    cursor.execute("DELETE FROM game_pitching_stats WHERE id = ?", (p_id_db,))
                else:
                    try:
                        cursor.execute("UPDATE game_pitching_stats SET player_id = ? WHERE id = ?", (new_id, p_id_db))
                    except sqlite3.IntegrityError:
                        cursor.execute("DELETE FROM game_pitching_stats WHERE id = ?", (p_id_db,))
        
        conn.commit()
        print("✅ Pitching stats resolution complete.")

    conn.close()

if __name__ == "__main__":
    fix_unknown_ids_robustly()
