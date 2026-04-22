import sqlite3
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")

def hard_deduplicate_all():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Hard reset: Setting all is_primary to 0...")
    cursor.execute("UPDATE game SET is_primary = 0")
    
    # Precise mappings for Regular Seasons only
    # (year, season_id, start_date, end_date)
    REGULAR_SEASONS = [
        (2024, 253, '2024-03-23', '2024-10-31'), 
        (2025, 259, '2025-03-22', '2025-10-31'), 
        (2026, 2026,  '2026-03-28', '2026-10-31')
    ]

    
    total_marked = 0
    for year, sid, start, end in REGULAR_SEASONS:
        print(f"Deduplicating {year} Regular Season...")
        
        # Group by Date, Franchise (Normalized), and DH Suffix
        # We pick the ONE game_id that has the most batting records
        query = """
        SELECT game_date, home_franchise_id, away_franchise_id, SUBSTR(game_id, -1, 1) as suffix
        FROM game
        WHERE game_date BETWEEN ? AND ?
          AND home_franchise_id IS NOT NULL 
          AND away_franchise_id IS NOT NULL
        GROUP BY game_date, home_franchise_id, away_franchise_id, suffix
        """
        
        groups = cursor.execute(query, (start, end)).fetchall()
        
        for g_date, h_fid, a_fid, suffix in groups:
            # Postseason exclusion: KBO regular season IDs usually end in 0, 1, or 2
            # and have standard 4-letter team segments like 'SSHT0'.
            # Some PS games end in '5' or other characters.
            
            cursor.execute("""
                SELECT game_id FROM game 
                WHERE game_date BETWEEN ? AND ?
                  AND game_date = ? AND home_franchise_id = ? AND away_franchise_id = ? AND game_id LIKE ?
                  AND (SUBSTR(game_id, -1, 1) IN ('0', '1', '2')) -- Standard regular/DH suffixes
            """, (start, end, g_date, h_fid, a_fid, f"%{suffix}"))

            
            candidates = [r[0] for r in cursor.fetchall()]
            
            if len(candidates) == 1:
                cursor.execute("UPDATE game SET is_primary = 1 WHERE game_id = ?", (candidates[0],))
            else:
                # Multiple candidates (e.g. SK vs SSG or KH vs WO)
                # Pick the one with the most records in game_batting_stats
                best_id = None
                max_cnt = -1
                for cid in candidates:
                    cnt = cursor.execute("SELECT COUNT(*) FROM game_batting_stats WHERE game_id = ?", (cid,)).fetchone()[0]
                    if cnt > max_cnt:
                        max_cnt = cnt
                        best_id = cid
                    elif cnt == max_cnt:
                        # Preference: SSG > SK, KH > WO, DB > OB
                        if any(pref in cid for pref in ['SSG', 'KH', 'DB', 'KIA']):
                            best_id = cid
                
                if best_id:
                    cursor.execute("UPDATE game SET is_primary = 1 WHERE game_id = ?", (best_id,))
            total_marked += 1

    conn.commit()
    print(f"Successfully marked {total_marked} primary games across 2024-2026.")
    
    # Double check total games per year
    for year in [2024, 2025, 2026]:
        cnt = cursor.execute("SELECT COUNT(*) FROM game WHERE strftime('%Y', game_date) = ? AND is_primary = 1", (str(year),)).fetchone()[0]
        print(f"Year {year} primary games: {cnt}")
        
    conn.close()

if __name__ == "__main__":
    hard_deduplicate_all()
