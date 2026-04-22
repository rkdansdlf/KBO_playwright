import sqlite3
import os
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.team_history import iter_team_history

def update_franchise_ids():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    # Load mapping
    team_to_franchise = {}
    for entry in iter_team_history():
        team_to_franchise[entry.team_code.upper()] = entry.franchise_id
    
    # Custom additions if missing
    team_to_franchise['KH'] = 6
    team_to_franchise['SSG'] = 8
    team_to_franchise['DB'] = 4
    team_to_franchise['SK'] = 8
    team_to_franchise['WO'] = 6

    print("Updating franchise IDs in game table...")
    cursor.execute("SELECT game_id, home_team, away_team, game_date FROM game")
    rows = cursor.fetchall()
    
    updates = []
    for game_id, home_team, away_team, game_date in rows:
        h_fid = team_to_franchise.get(home_team.upper()) if home_team else None
        a_fid = team_to_franchise.get(away_team.upper()) if away_team else None
        
        if h_fid or a_fid:
            updates.append((h_fid, a_fid, game_id))
            
    cursor.executemany("UPDATE game SET home_franchise_id = ?, away_franchise_id = ? WHERE game_id = ?", updates)
    conn.commit()
    print(f"Updated {len(updates)} games.")
    conn.close()

if __name__ == "__main__":
    update_franchise_ids()
