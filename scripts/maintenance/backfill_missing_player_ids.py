"""
Backfill missing player_ids in game stats tables using resolutions from CSV.

Reads data/unresolved_players.csv.
Applies updates for rows with match_type = 'UNIQUE_MATCH'.
Updates both game_batting_stats and game_pitching_stats.
"""
import sys
import os
import csv
from sqlalchemy import text

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.db.engine import get_db_session

def backfill_ids():
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'unresolved_players.csv')
    csv_path = os.path.normpath(csv_path)
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return

    updates = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['match_type'].startswith('UNIQUE_MATCH'):
                updates.append({
                    'name': row['player_name'],
                    'team': row['team_code'],
                    'season': row['season'],
                    'pid': int(row['suggested_player_id'])
                })
    
    print(f"Loaded {len(updates)} unique matches to apply.")
    
    with get_db_session() as session:
        batting_count = 0
        pitching_count = 0
        
        for up in updates:
            # 1. Update Batting Stats
            stmt = text("""
                UPDATE game_batting_stats
                SET player_id = :pid
                WHERE player_name = :name
                  AND team_code = :team
                  AND game_id LIKE :season || '%'
                  AND player_id IS NULL
            """)
            result = session.execute(stmt, {
                'pid': up['pid'],
                'name': up['name'],
                'team': up['team'],
                'season': up['season']
            })
            batting_count += result.rowcount
            
            # 2. Update Pitching Stats
            stmt = text("""
                UPDATE game_pitching_stats
                SET player_id = :pid
                WHERE player_name = :name
                  AND team_code = :team
                  AND game_id LIKE :season || '%'
                  AND player_id IS NULL
            """)
            result = session.execute(stmt, {
                'pid': up['pid'],
                'name': up['name'],
                'team': up['team'],
                'season': up['season']
            })
            pitching_count += result.rowcount
            
        print(f"✅ Backfill complete.")
        print(f"   Updated {batting_count} batting records.")
        print(f"   Updated {pitching_count} pitching records.")

if __name__ == "__main__":
    backfill_ids()
