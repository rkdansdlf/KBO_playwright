import sqlite3
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")

def smart_deduplicate():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Resetting is_primary to 0...")
    cursor.execute("UPDATE game SET is_primary = 0")
    
    # Target only Standard Regular Season games (Suffix 0, 1, 2)
    # We group by Date, Home Franchise, Away Franchise, and DH Suffix
    # For each group, we pick the game_id that has the MAX count of batting stats
    
    query = """
    SELECT game_date, home_franchise_id, away_franchise_id, SUBSTR(game_id, -1, 1) as suffix
    FROM game
    WHERE (SUBSTR(game_id, -1, 1) IN ('0', '1', '2'))
      AND home_franchise_id IS NOT NULL 
      AND away_franchise_id IS NOT NULL
      AND (
          (game_date <= '2024-10-01' AND strftime('%Y', game_date) = '2024') OR
          (game_date <= '2025-09-30' AND strftime('%Y', game_date) = '2025') OR
          (strftime('%Y', game_date) = '2026')
      )
    GROUP BY game_date, home_franchise_id, away_franchise_id, suffix
    """

    
    groups = cursor.execute(query).fetchall()
    print(f"Analyzing {len(groups)} unique game slots...")
    
    updates = 0
    for g_date, h_fid, a_fid, suffix in groups:
        # Find all candidates for this slot
        # Strict for 2024-2025, more relaxed for 2026 (Live season)
        if '2026' in g_date:
            cursor.execute("""
                SELECT game_id FROM game 
                WHERE game_date = ? AND home_franchise_id = ? AND away_franchise_id = ? AND game_id LIKE ?
            """, (g_date, h_fid, a_fid, f"%{suffix}"))
        else:
            cursor.execute("""
                SELECT g.game_id FROM game g
                WHERE g.game_date = ? AND g.home_franchise_id = ? AND g.away_franchise_id = ? AND g.game_id LIKE ?
                  AND EXISTS (SELECT 1 FROM game_batting_stats b WHERE b.game_id = g.game_id)
            """, (g_date, h_fid, a_fid, f"%{suffix}"))

        candidates = [r[0] for r in cursor.fetchall()]

        # If no candidates with stats (for 2024-2025), try getting ANY candidate as fallback
        if not candidates:
            cursor.execute("""
                SELECT game_id FROM game 
                WHERE game_date = ? AND home_franchise_id = ? AND away_franchise_id = ? AND game_id LIKE ?
            """, (g_date, h_fid, a_fid, f"%{suffix}"))
            candidates = [r[0] for r in cursor.fetchall()]


        
        if len(candidates) == 1:
            cursor.execute("UPDATE game SET is_primary = 1 WHERE game_id = ?", (candidates[0],))
        else:
            # Multiple candidates (e.g. SK vs SSG or KH vs WO)
            # Pick the one with the most records in game_batting_stats
            best_id = None
            max_stat = -1
            
            # Sub-check: only consider IDs that actually have stats
            stat_counts = []
            for cid in candidates:
                cnt = cursor.execute("SELECT COUNT(*) FROM game_batting_stats WHERE game_id = ?", (cid,)).fetchone()[0]
                stat_counts.append((cnt, cid))
            
            # Sort by count desc, then preferred team code
            stat_counts.sort(key=lambda x: (x[0], any(p in x[1] for p in ['SSG', 'KH', 'DB', 'KIA'])), reverse=True)
            
            if stat_counts[0][0] > 0:
                best_id = stat_counts[0][1]
            else:
                # If all are 0, just pick the first standard one
                best_id = candidates[0]
            
            if best_id:
                cursor.execute("UPDATE game SET is_primary = 1 WHERE game_id = ?", (best_id,))

        updates += 1

    conn.commit()
    print(f"Deduplication complete. {updates} primary games marked.")
    
    # Generic safety check: Remove primary from obviously extreme dates
    # But leave 2026 untouched as it's the live season
    cursor.execute("UPDATE game SET is_primary = 0 WHERE (strftime('%m%d', game_date) < '0301' OR strftime('%m%d', game_date) > '1231') AND strftime('%Y', game_date) != '2026'")
    conn.commit()



    
    conn.close()

if __name__ == "__main__":
    smart_deduplicate()
