import sqlite3
import pandas as pd

def verify_pitching_integrity():
    conn = sqlite3.connect("data/kbo_dev.db")
    
    # Yearly Regular Season Windows (Match with batting verification)
    SEASONS = {
        2024: (253, '2024-03-23', '2024-10-01'), 
        2025: (259, '2025-03-22', '2025-10-31'), 
        2026: (2026, '2026-03-28', '2026-04-19')
    }
    
    print("=== PITCHING STATS INTEGRITY VERIFICATION (2024-2026) ===")
    
    for year, (sid, start, end) in SEASONS.items():
        print(f"\n--- {year} Regular Season (ID: {sid}) ---")
        
        query = f"""
        WITH game_agg AS (
            SELECT 
                p.player_id,
                COUNT(DISTINCT p.game_id) as g_games,
                SUM(p.innings_outs) as g_outs,
                SUM(p.earned_runs) as g_er,
                SUM(p.wins) as g_w
            FROM game_pitching_stats p
            JOIN game g ON p.game_id = g.game_id
            WHERE g.game_date BETWEEN '{start}' AND '{end}'
              AND g.is_primary = 1
            GROUP BY p.player_id
        ),
        season_agg AS (
            SELECT 
                player_id,
                games as s_games,
                innings_outs as s_outs,
                earned_runs as s_er,
                wins as s_w
            FROM player_season_pitching
            WHERE season = {year} AND league = 'REGULAR'
        )
        SELECT 
            pb.name,
            s.s_games, g.g_games,
            s.s_outs, g.g_outs,
            s.s_er, g.g_er
        FROM season_agg s
        JOIN game_agg g ON s.player_id = g.player_id
        JOIN player_basic pb ON s.player_id = pb.player_id
        WHERE s.s_outs != COALESCE(g.g_outs, 0) OR s.s_er != COALESCE(g.g_er, 0)
        ORDER BY ABS(s.s_outs - COALESCE(g.g_outs, 0)) DESC
        LIMIT 10;
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Stats
        total_p = conn.execute(f"SELECT COUNT(*) FROM player_season_pitching WHERE season={year} AND league='REGULAR'").fetchone()[0]
        
        # Mismatch count
        m_query = f"""
            SELECT COUNT(*) FROM (
                SELECT s.player_id FROM player_season_pitching s 
                JOIN (
                    SELECT p.player_id, SUM(p.innings_outs) as g_outs, SUM(p.earned_runs) as g_er
                    FROM game_pitching_stats p JOIN game g ON p.game_id = g.game_id
                    WHERE g.game_date BETWEEN '{start}' AND '{end}' AND g.is_primary = 1
                    GROUP BY p.player_id
                ) g ON s.player_id = g.player_id
                WHERE s.season = {year} AND s.league = 'REGULAR' AND (s.innings_outs != g.g_outs OR s.earned_runs != g.g_er)
            )
        """
        mismatches = conn.execute(m_query).fetchone()[0]
        
        if mismatches == 0:
            print(f"✅ PERFECT MATCH: All {total_p} pitchers match 100%!")
        else:
            accuracy = ((total_p - mismatches) / total_p) * 100
            print(f"📊 ACCURACY: {accuracy:.1f}% ({total_p - mismatches} / {total_p} pitchers match)")
            print("Sample Mismatches (Sorted by innings/ER difference):")
            print(df.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    verify_pitching_integrity()
