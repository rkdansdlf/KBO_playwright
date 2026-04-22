import sqlite3
import pandas as pd
from datetime import datetime

def find_missing_games_2025():
    conn = sqlite3.connect("data/kbo_dev.db")
    
    # 1. Get players who have 144 games in season stats but < 144 in DB
    query = """
    SELECT s.player_id, p.name, s.team_code, s.games as s_games, COUNT(DISTINCT b.game_id) as g_games
    FROM player_season_batting s
    JOIN player_basic p ON s.player_id = p.player_id
    LEFT JOIN game_batting_stats b ON s.player_id = b.player_id
    LEFT JOIN game g ON b.game_id = g.game_id AND g.is_primary = 1 AND g.season_id = 20250
    WHERE s.season = 2025 AND s.league = 'REGULAR' AND s.games >= 140
    GROUP BY s.player_id
    HAVING g_games < s.games;
    """
    
    mismatched_players = pd.read_sql_query(query, conn)
    print(f"Found {len(mismatched_players)} high-usage players with missing games in 2025.")
    
    missing_game_ids = set()
    
    for _, player in mismatched_players.iterrows():
        pid = player['player_id']
        team = player['team_code']
        
        # Get all games of this player's team in 2025
        # We look for games where this team played but this player has no record
        # and the game is terminal.
        cursor = conn.cursor()
        cursor.execute("""
            SELECT g.game_id, g.game_date
            FROM game g
            WHERE g.season_id = 20250 AND g.is_primary = 1
              AND (g.home_team = ? OR g.away_team = ?)
              AND g.game_id NOT IN (SELECT game_id FROM game_batting_stats WHERE player_id = ?)
              AND g.game_status NOT IN ('취소', '경기취소', '우천취소')
        """, (team, team, pid))
        
        missing = cursor.fetchall()
        for gid, gdate in missing:
            missing_game_ids.add((gid, gdate))
            
    print(f"Identified {len(missing_game_ids)} potentially missing or incomplete games.")
    
    # Save to a text file for the crawler
    with open("scratch/missing_2025_games.txt", "w") as f:
        for gid, gdate in sorted(list(missing_game_ids)):
            f.write(f"{gid},{gdate}\n")
            
    conn.close()

if __name__ == "__main__":
    find_missing_games_2025()
