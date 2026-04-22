import sqlite3
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.player_id_resolver import PlayerIdResolver
from src.db.engine import SessionLocal

def finalize_id_resolution():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    with SessionLocal() as session:
        resolver = PlayerIdResolver(session)
        
        for table in ['game_batting_stats', 'game_pitching_stats']:
            print(f"🛠 Processing {table} for final ID resolution...")
            cursor.execute(f"""
                SELECT id, game_id, player_name, team_code, strftime('%Y', (SELECT game_date FROM game WHERE game_id = {table}.game_id)) as year, uniform_no, player_id
                FROM {table}
                WHERE (player_id >= 900000 OR player_id IS NULL)
            """)
            
            rows = cursor.fetchall()
            print(f"Found {len(rows)} records to resolve in {table}.")
            
            for row_id, g_id, name, team, year, uniform, old_id in rows:
                if not year: continue
                new_id = resolver.resolve_id(name, team, int(year), uniform_no=uniform)
                
                if new_id and new_id != old_id:
                    # Check if this player already exists in THIS game with the new_id
                    cursor.execute(f"SELECT id FROM {table} WHERE game_id = ? AND player_id = ?", (g_id, new_id))
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Duplicate found! Delete the unknown record to satisfy UNIQUE constraint
                        cursor.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))
                    else:
                        # Safe to update
                        try:
                            cursor.execute(f"UPDATE {table} SET player_id = ? WHERE id = ?", (new_id, row_id))
                        except sqlite3.IntegrityError:
                            cursor.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))
            
            conn.commit()
    
    print("✅ ID Finalization complete.")
    conn.close()

if __name__ == "__main__":
    finalize_id_resolution()
