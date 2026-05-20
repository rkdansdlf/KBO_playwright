import sqlite3
import pandas as pd
import json
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def verify_2024_batters():
    print("=== 2024 Batter Sabermetrics Verification ===")
    conn = sqlite3.connect('data/kbo_dev.db')
    
    query = """
    SELECT 
        p.name, 
        b.plate_appearances as PA,
        b.extra_stats
    FROM player_season_batting b
    JOIN player_basic p ON b.player_id = p.player_id
    WHERE b.season = 2024 AND b.league = 'REGULAR' AND b.plate_appearances > 400
    """
    
    rows = conn.execute(query).fetchall()
    results = []
    for name, pa, extra_json in rows:
        extra = json.loads(extra_json) if extra_json else {}
        results.append({
            "Name": name,
            "PA": pa,
            "wOBA": extra.get("woba"),
            "wRC+": extra.get("wrc_plus"),
            "WAR": extra.get("war")
        })
    
    df = pd.DataFrame(results)
    if not df.empty and "WAR" in df.columns:
        df = df.sort_values("WAR", ascending=False).head(10)
        print(df.to_string(index=False))
    else:
        print("No results found for league='REGULAR'.")
    
    print("\nReference Points (STATIZ 2024):")
    print("- Kim Do-yeong: wOBA 0.463, wRC+ 173.0, WAR 8.32")
    print("- Austin Dean:  wOBA 0.428, wRC+ 152.0, WAR 5.56")
    
    conn.close()

def verify_2024_pitchers():
    print("\n=== 2024 Pitcher Sabermetrics Verification ===")
    conn = sqlite3.connect('data/kbo_dev.db')
    
    query = """
    SELECT 
        p.name, 
        ps.innings_outs,
        ps.extra_stats
    FROM player_season_pitching ps
    JOIN player_basic p ON ps.player_id = p.player_id
    WHERE ps.season = 2024 AND ps.league = 'REGULAR' AND ps.innings_outs > 300
    """
    
    rows = conn.execute(query).fetchall()
    results = []
    for name, outs, extra_json in rows:
        extra = json.loads(extra_json) if extra_json else {}
        results.append({
            "Name": name,
            "IP": round(outs / 3.0, 1),
            "FIP": extra.get("fip_adj"),
            "WAR": extra.get("war")
        })
    
    df = pd.DataFrame(results)
    if not df.empty and "WAR" in df.columns:
        df = df.sort_values("WAR", ascending=False).head(10)
        print(df.to_string(index=False))
    else:
        print("No results found for league='REGULAR'.")
    
    print("\nReference Points (STATIZ 2024):")
    print("- Hart (NC):  FIP 2.50, WAR 7.01")
    print("- Won Tae-in: FIP 3.90, WAR 3.49")
    
    conn.close()

if __name__ == "__main__":
    verify_2024_batters()
    verify_2024_pitchers()
