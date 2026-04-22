import sqlite3
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal

def fix_null_ids():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    with SessionLocal() as session:
        resolver = PlayerIdResolver(session)
        
        print("Finding batting stats with NULL or Unknown (>=900000) player_id...")
        cursor.execute("""
            SELECT b.id, b.player_name, b.team_code, strftime('%Y', g.game_date) as year, b.uniform_no
            FROM game_batting_stats b
            JOIN game g ON b.game_id = g.game_id
            WHERE (b.player_id IS NULL OR b.player_id >= 900000) AND b.player_name IS NOT NULL
        """)

        
        rows = cursor.fetchall()
        print(f"Found {len(rows)} records to fix.")
        
        updates = []
        resolved_count = 0
        for b_id, name, team, year, uniform in rows:
            p_id = resolver.resolve_id(name, team, int(year), uniform_no=uniform)
            if p_id:
                updates.append((p_id, b_id))
                resolved_count += 1
                if resolved_count % 1000 == 0:
                    print(f"Resolved {resolved_count}...")

        if updates:
            cursor.executemany("UPDATE game_batting_stats SET player_id = ? WHERE id = ?", updates)
            conn.commit()
            print(f"Successfully updated {len(updates)} player_ids in game_batting_stats.")

        # Repeat for pitching
        print("Finding pitching stats with NULL player_id...")
        cursor.execute("""
            SELECT p.id, p.player_name, p.team_code, strftime('%Y', g.game_date) as year, p.uniform_no
            FROM game_pitching_stats p
            JOIN game g ON p.game_id = g.game_id
            WHERE p.player_id IS NULL AND p.player_name IS NOT NULL
        """)
        
        rows_p = cursor.fetchall()
        print(f"Found {len(rows_p)} pitching records to fix.")
        
        updates_p = []
        resolved_p = 0
        for p_id_db, name, team, year, uniform in rows_p:
            p_id = resolver.resolve_id(name, team, int(year), uniform_no=uniform)
            if p_id:
                updates_p.append((p_id, p_id_db))
                resolved_p += 1
        
        if updates_p:
            cursor.executemany("UPDATE game_pitching_stats SET player_id = ? WHERE id = ?", updates_p)
            conn.commit()
            print(f"Successfully updated {len(updates_p)} player_ids in game_pitching_stats.")

    conn.close()

if __name__ == "__main__":
    fix_null_ids()
