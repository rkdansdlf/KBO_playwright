"""
Disambiguate players with multiple ID candidates.

Reads data/unresolved_players.csv (rows with 'MULTIPLE').
For each candidate ID, checks if they have a record in:
- player_season_batting
- player_season_pitching
for the specific (season, team_code).

If exactly ONE candidate has a record, we resolve it.
"""
import sys
import os
import csv
from sqlalchemy import text, select, and_

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.db.engine import get_db_session
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

def disambiguate():
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'unresolved_players.csv')
    csv_path = os.path.normpath(csv_path)
    
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return

    resolved_updates = []
    
    
    # Mapping CSV/Game codes to DB Franchise codes
    # DB uses historical codes: OB, HT, SK, WO for all seasons
    CSV_CODE_TO_DB_CODE = {
        'DB': 'OB', 'OB': 'OB', 'DO': 'OB', 'DOOSAN': 'OB',
        'KIA': 'HT', 'HT': 'HT', 'KI': 'HT',
        'SSG': 'SK', 'SK': 'SK',
        'KH': 'WO', 'WO': 'WO', 'NX': 'WO', 'NEXEN': 'WO', 'HEROES': 'WO',
        'SS': 'SS', 'SA': 'SS', 'SAMSUNG': 'SS',
        'LT': 'LT', 'LOTTE': 'LT',
        'LG': 'LG',
        'HH': 'HH', 'HANWHA': 'HH',
        'KT': 'KT',
        'NC': 'NC'
    }

    with get_db_session() as session:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        print(f"Processing {len(rows)} rows...")
        
        for row in rows:
            if not row['match_type'].startswith('MULTIPLE'):
                continue
                
            name = row['player_name']
            raw_team = row['team_code']
            # Normalize team code
            team = CSV_CODE_TO_DB_CODE.get(raw_team, raw_team)
            
            season_str = row['season']
            candidates_str = row['suggested_player_id']
            
            if not season_str or not candidates_str:
                continue
                
            season = int(season_str)
            candidate_ids = [int(pid) for pid in candidates_str.replace('"', '').split(',') if pid]
            
            # Check active status for each candidate
            active_ids = []
            
            for pid in candidate_ids:
                # Check Batting
                stmt_bat = select(PlayerSeasonBatting).where(
                    PlayerSeasonBatting.player_id == pid,
                    PlayerSeasonBatting.season == season,
                    PlayerSeasonBatting.team_code == team
                )
                if session.execute(stmt_bat).first():
                    active_ids.append(pid)
                    continue
                    
                # Check Pitching
                stmt_pit = select(PlayerSeasonPitching).where(
                    PlayerSeasonPitching.player_id == pid,
                    PlayerSeasonPitching.season == season,
                    PlayerSeasonPitching.team_code == team
                )
                if session.execute(stmt_pit).first():
                    active_ids.append(pid)
            
            # Decision Logic
            if len(active_ids) == 1:
                resolved_pid = active_ids[0]
                print(f"✅ Resolved {name} ({team}, {season}): {resolved_pid} (Candidates: {candidate_ids})")
                resolved_updates.append({
                    'name': name,
                    'team': raw_team, # Use RAW team code for the DB update to match game_batting_stats
                    'season': str(season),
                    'pid': resolved_pid
                })
            elif len(active_ids) > 1:
                print(f"⚠️ Still ambiguous {name} ({team}, {season}): Multiple active records {active_ids}")
            else:
                # No active records found match the team/season exact constraint
                # Could be a trade mid-season? Or team code mismatch?
                # Try relaxed team check? (Skipping for now to be safe)
                print(f"❌ No match {name} ({team}, {season}): None of {candidate_ids} have stats for specific team")

    print(f"\nFound {len(resolved_updates)} resolvable ambiguous cases.")
    
    if not resolved_updates:
        return

    # Apply updates
    with get_db_session() as session:
        batting_count = 0
        pitching_count = 0
        
        for up in resolved_updates:
            # 1. Update Batting
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
            
            # 2. Update Pitching
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

        print(f"✅ Disambiguation Backfill Complete.")
        print(f"   Updated {batting_count} batting records.")
        print(f"   Updated {pitching_count} pitching records.")

if __name__ == "__main__":
    disambiguate()
