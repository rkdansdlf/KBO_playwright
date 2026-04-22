import sqlite3
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")

# Canonical mappings for current seasons (2021+)
CANONICAL_PAIRS = {
    8: "SSG",
    6: "KH",
    4: "DB",
    5: "KIA"
}

def mark_primary_games():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Identifying duplicates...")
    
    # Get all potential duplicate groups for 2025-2026
    # Group by Date, Franchise, AND game_id suffix (last char)
    query = """
    SELECT game_date, home_franchise_id, away_franchise_id, SUBSTR(game_id, -1, 1) as suffix, COUNT(*) as cnt
    FROM game
    WHERE home_franchise_id IS NOT NULL 
      AND away_franchise_id IS NOT NULL
    GROUP BY game_date, home_franchise_id, away_franchise_id, suffix
    HAVING cnt > 1
    """
    
    duplicates = cursor.execute(query).fetchall()
    print(f"Found {len(duplicates)} duplicate game groups (considering DH suffix).")
    
    updates = 0
    for g_date, home_fid, away_fid, suffix, count in duplicates:
        # Get all game_ids in this group
        cursor.execute("""
            SELECT game_id, home_team, away_team 
            FROM game 
            WHERE game_date = ? AND home_franchise_id = ? AND away_franchise_id = ? AND game_id LIKE ?
        """, (g_date, home_fid, away_fid, f"%{suffix}"))

        
        games = cursor.fetchall()
        
        # Decide which one is primary
        primary_id = None
        
        # Priority 1: Check canonical codes
        for gid, h_code, a_code in games:
            if (home_fid in CANONICAL_PAIRS and CANONICAL_PAIRS[home_fid] in gid) or \
               (away_fid in CANONICAL_PAIRS and CANONICAL_PAIRS[away_fid] in gid):
                primary_id = gid
                break
        
        # Priority 2: If multiple matches or no match, pick the one that has the most batting stats
        if not primary_id or len(games) > 1:
            best_id = None
            max_stats = -1
            for gid, h_code, a_code in games:
                stat_count = cursor.execute("SELECT COUNT(*) FROM game_batting_stats WHERE game_id = ?", (gid,)).fetchone()[0]
                if stat_count > max_stats:
                    max_stats = stat_count
                    best_id = gid
                elif stat_count == max_stats:
                    # Tie-break with game_id length or lexicographical (prefer longer IDs usually like SSG vs SK)
                    if len(gid) > len(str(best_id)):
                        best_id = gid
            primary_id = best_id
            
        # Set is_primary: Reset ALL in this group first to 0
        cursor.execute("UPDATE game SET is_primary = 0 WHERE game_date = ? AND home_franchise_id = ? AND away_franchise_id = ?", 
                       (g_date, home_fid, away_fid))
        # Set the CHOSEN one to 1
        cursor.execute("UPDATE game SET is_primary = 1 WHERE game_id = ?", (primary_id,))
        updates += 1


    conn.commit()
    print(f"Successfully marked {updates} groups. Non-primary records are now set to is_primary = 0.")
    
    # Verification
    cursor.execute("SELECT COUNT(*) FROM game WHERE is_primary = 0")
    non_primary_count = cursor.fetchone()[0]
    print(f"Total non-primary (duplicate) games: {non_primary_count}")
    
    conn.close()

if __name__ == "__main__":
    mark_primary_games()
