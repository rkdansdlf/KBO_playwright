import sqlite3
import requests
from bs4 import BeautifulSoup

def analyze_mismatch(player_id, year, target_hits):
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    # Get all primary games for this player
    cursor.execute("""
        SELECT g.game_id, g.game_date, b.hits, b.at_bats
        FROM game_batting_stats b
        JOIN game g ON b.game_id = g.game_id
        WHERE b.player_id = ? AND strftime('%Y', g.game_date) = ? AND g.is_primary = 1
    """, (player_id, str(year)))
    
    games = cursor.fetchall()
    db_hits = sum(g[2] for g in games)
    
    print(f"--- Analysis for Player {player_id} in {year} ---")
    print(f"DB Total Hits: {db_hits} vs Target: {target_hits}")
    print(f"Diff: {db_hits - target_hits}")
    
    # We can't easily check KBO side without a lot of HTTP calls here,
    # but we can look for "suspicious" games in DB where hits are 0 but score was high.
    suspicious = [g for g in games if g[2] == 0 and g[3] > 3]
    if suspicious:
        print(f"Found {len(suspicious)} suspicious 0-hit games with >3 ABs.")
        for s in suspicious:
            print(f"  Game: {s[0]} Date: {s[1]}")

    conn.close()

if __name__ == "__main__":
    # Reyes 2025
    analyze_mismatch(54529, 2025, 187)
    # Rojas 2024
    analyze_mismatch(67025, 2024, 188)
