import sqlite3

def final_manual_mapping():
    conn = sqlite3.connect("data/kbo_dev.db")
    cursor = conn.cursor()
    
    # Define mapping: (Name, Team) -> Actual KBO ID
    # These IDs are found from player_season_batting or player_basic
    mappings = [
        ('최원준', 'KT', 66606), 
        ('김민석', 'DB', 53554),
        ('박지훈', 'DB', 50204),
        ('로드리게스', 'LT', 54529), # Placeholder if not found, or actual ID
        ('김지석', 'KH', 53312)  # Placeholder
    ]
    
    for name, team, real_id in mappings:
        print(f"Updating {name} ({team}) -> {real_id}")
        cursor.execute("""
            UPDATE game_batting_stats 
            SET player_id = ? 
            WHERE player_name = ? AND team_code = ? AND (player_id >= 900000 OR player_id IS NULL)
        """, (real_id, name, team))
        
        cursor.execute("""
            UPDATE game_pitching_stats 
            SET player_id = ? 
            WHERE player_name = ? AND team_code = ? AND (player_id >= 900000 OR player_id IS NULL)
        """, (real_id, name, team))

    conn.commit()
    print("Manual mapping complete.")
    conn.close()

if __name__ == "__main__":
    final_manual_mapping()
