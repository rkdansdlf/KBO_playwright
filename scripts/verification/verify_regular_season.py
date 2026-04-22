import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")

def verify_regular_season():
    conn = sqlite3.connect(DB_PATH)
    
    years = [2024, 2025, 2026]

    
    print("=== KBO Regular Season Integrity Verification (2025-2026) ===")
    
    for year in years:
        # Resolve the active season_id for this year's regular season
        # Using hardcoded common IDs for stability in this report
        sid_map = {2024: 253, 2025: 259, 2026: 2026}
        sid = sid_map.get(year)
        
        print(f"\n--- Testing Year: {year} (Regular Season, ID: {sid}) ---")
        
        query = f"""
        WITH game_agg AS (
            SELECT 
                b.player_id,
                COUNT(DISTINCT b.game_id) as g_games,
                SUM(b.hits) as g_hits,
                SUM(b.at_bats) as g_ab
            FROM game_batting_stats b
            JOIN game g ON b.game_id = g.game_id
            WHERE g.season_id = {sid} AND g.is_primary = 1
            GROUP BY b.player_id
        ),
        season_agg AS (
            SELECT 
                player_id,
                games as s_games,
                hits as s_hits,
                at_bats as s_ab
            FROM player_season_batting
            WHERE season = {year} AND league = 'REGULAR'
        )
        SELECT 
            p.name,
            s.player_id,
            s.s_games, g.g_games,
            s.s_hits, g.g_hits
        FROM season_agg s
        LEFT JOIN game_agg g ON s.player_id = g.player_id
        JOIN player_basic p ON s.player_id = p.player_id
        WHERE s.s_hits != COALESCE(g.g_hits, 0) 
           OR s.s_games != COALESCE(g.g_games, 0)
        ORDER BY s.s_games DESC
        LIMIT 10;
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Total counts
        total_players_query = f"SELECT COUNT(*) FROM player_season_batting WHERE season = {year} AND league = 'REGULAR'"
        total_players = conn.execute(total_players_query).fetchone()[0]
        
        mismatch_count_query = f"""
        SELECT COUNT(*) FROM (
            WITH game_agg AS (
                SELECT b.player_id, COUNT(DISTINCT b.game_id) as g_games, SUM(b.hits) as g_hits
                FROM game_batting_stats b
                JOIN game g ON b.game_id = g.game_id
                WHERE g.season_id = {sid} AND g.is_primary = 1
                GROUP BY b.player_id
            ),
            season_agg AS (
                SELECT player_id, games as s_games, hits as s_hits
                FROM player_season_batting
                WHERE season = {year} AND league = 'REGULAR'
            )
            SELECT s.player_id
            FROM season_agg s
            LEFT JOIN game_agg g ON s.player_id = g.player_id
            WHERE s.s_hits != COALESCE(g.g_hits, 0) OR s.s_games != COALESCE(g.g_games, 0)
        )
        """
        mismatches = conn.execute(mismatch_count_query).fetchone()[0]
        
        if mismatches == 0:
            print(f"✅ SUCCESS: All {total_players} players match perfectly!")
        else:
            print(f"❌ MISMATCH: {mismatches} / {total_players} players have discrepancies.")
            print("Sample Mismatches (Top 10 by Games Played):")
            print(df.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    verify_regular_season()
