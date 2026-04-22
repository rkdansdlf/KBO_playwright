import sqlite3
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")

def fix_all_season_ids():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get season mappings from kbo_seasons
    cursor.execute("SELECT season_id, season_year, league_type_code, start_date, end_date FROM kbo_seasons WHERE season_year IN (2024, 2025, 2026)")
    seasons = cursor.fetchall()
    
    # 0: Regular, 1: Exhibition
    print("Mapping games to correct season_ids...")
    
    # Precise Regular Season date ranges
    REGULAR_DATES = {
        2024: ('2024-03-23', '2024-10-31'), # 2024 actual end was late Oct due to rain
        2025: ('2025-03-22', '2025-10-31'),
        2026: ('2026-03-28', '2026-10-31')
    }

    
    updates = 0
    for sid, year, league_code, start_date, end_date in seasons:
        if league_code == 0: # Regular
            start, end = REGULAR_DATES.get(year, (f"{year}-03-20", f"{year}-11-30"))
            
            cursor.execute("""
                UPDATE game 
                SET season_id = ? 
                WHERE game_date BETWEEN ? AND ? 
                  AND strftime('%Y', game_date) = ?
            """, (sid, start, end, str(year)))
            updates += cursor.rowcount
            
        elif league_code == 1: # Exhibition
            if year == 2024:
                start, end = '2024-03-09', '2024-03-19'
            elif year == 2025:
                start, end = '2025-03-08', '2025-03-18'
            else: # 2026
                start, end = '2026-03-12', '2026-03-24'
            
            cursor.execute("""
                UPDATE game 
                SET season_id = ? 
                WHERE game_date BETWEEN ? AND ? 
                  AND strftime('%Y', game_date) = ?
            """, (sid, start, end, str(year)))
            updates += cursor.rowcount
            
        elif league_code == 5: # Korean Series / Postseason (ID 258 etc)
            cursor.execute("""
                UPDATE game 
                SET season_id = ? 
                WHERE game_date > ? 
                  AND strftime('%Y', game_date) = ?
                  AND game_id LIKE '%%5' -- Postseason IDs often end with 5 in KBO naming
            """, (sid, end_date or f"{year}-10-02", str(year)))
            updates += cursor.rowcount


    conn.commit()
    print(f"Successfully updated {updates} game season_ids.")
    conn.close()

if __name__ == "__main__":
    fix_all_season_ids()
