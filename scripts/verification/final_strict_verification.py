import sqlite3
import pandas as pd

def final_strict_verification():
    conn = sqlite3.connect("data/kbo_dev.db")
    
    # Precise Regular Season Boundaries (Official KBO)
    SEASONS = {
        2024: ('2024-03-23', '2024-10-31'), 
        2025: ('2025-03-22', '2025-10-31'), 
        2026: ('2026-03-28', '2026-04-19')  # Today's date (Live)
    }

    
    print("=== FINAL STRICT INTEGRITY VERIFICATION (2024-2026) ===")
    
    for year, (start, end) in SEASONS.items():
        print(f"\n--- {year} Regular Season ({start} to {end}) ---")
        
        query = f"""
        WITH game_agg AS (
            SELECT 
                b.player_id,
                COUNT(DISTINCT b.game_id) as g_games,
                SUM(b.hits) as g_hits
            FROM game_batting_stats b
            JOIN game g ON b.game_id = g.game_id
            WHERE g.game_date BETWEEN '{start}' AND '{end}'
              AND g.is_primary = 1
              AND (SUBSTR(g.game_id, -1, 1) IN ('0', '1', '2'))
            GROUP BY b.player_id
        ),
        season_agg AS (
            SELECT 
                player_id,
                games as s_games,
                hits as s_hits
            FROM player_season_batting
            WHERE season = {year} AND league = 'REGULAR'
        )
        SELECT 
            p.name,
            s.s_games, g.g_games,
            s.s_hits, g.g_hits
        FROM season_agg s
        JOIN game_agg g ON s.player_id = g.player_id
        JOIN player_basic p ON s.player_id = p.player_id
        WHERE s.s_hits != COALESCE(g.g_hits, 0) OR s.s_games != COALESCE(g.g_games, 0)
        ORDER BY ABS(s.s_hits - COALESCE(g.g_hits, 0)) DESC
        LIMIT 10;
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Stats
        total_p = conn.execute(f"SELECT COUNT(*) FROM player_season_batting WHERE season={year} AND league='REGULAR'").fetchone()[0]
        
        # Mismatch count in this strict range
        m_query = f"""
            SELECT COUNT(*) FROM (
                SELECT s.player_id FROM player_season_batting s 
                JOIN (
                    SELECT b.player_id, COUNT(DISTINCT b.game_id) as g_games, SUM(b.hits) as g_hits
                    FROM game_batting_stats b JOIN game g ON b.game_id = g.game_id
                    WHERE g.game_date BETWEEN '{start}' AND '{end}' AND g.is_primary = 1
                    GROUP BY b.player_id
                ) g ON s.player_id = g.player_id
                WHERE s.season = {year} AND s.league = 'REGULAR' AND (s.hits != g.g_hits OR s.games != g.g_games)
            )
        """
        mismatches = conn.execute(m_query).fetchone()[0]
        
        if mismatches == 0:
            print(f"✅ PERFECT MATCH: All {total_p} players match 100% in this range!")
        else:
            accuracy = ((total_p - mismatches) / total_p) * 100
            print(f"📊 ACCURACY: {accuracy:.1f}% ({total_p - mismatches} / {total_p} players match)")
            print("Sample Mismatches (Sorted by hit difference):")
            print(df.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    final_strict_verification()
