"""
Detect unresolved player names in game data.

Scans game_batting_stats and game_pitching_stats for player_id=NULL entries,
cross-references with player_basic, and outputs candidates for alias mapping.

Output: data/unresolved_players.csv
"""
import sys
import os
import csv

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.db.engine import get_db_session, Engine


def detect_unresolved():
    """Find game records with NULL player_id and try to suggest matches."""
    
    output_rows = []
    
    with get_db_session() as session:
        # 1. Check game_batting_stats for NULL player_id
        try:
            batting_nulls = session.execute(
                Engine.raw_connection().cursor().execute("""
                    SELECT DISTINCT player_name, team_code, 
                           substr(game_id, 1, 4) as season
                    FROM game_batting_stats 
                    WHERE player_id IS NULL AND player_name IS NOT NULL
                """)
            ).fetchall()
        except Exception:
            batting_nulls = []
            
        # Use raw SQL for compatibility
        from sqlalchemy import text
        
        # Batting stats with NULL player_id
        try:
            result = session.execute(text("""
                SELECT DISTINCT player_name, team_code, 
                       substr(game_id, 1, 4) as season
                FROM game_batting_stats 
                WHERE player_id IS NULL AND player_name IS NOT NULL
                ORDER BY season, team_code, player_name
            """))
            batting_nulls = result.fetchall()
            print(f"Found {len(batting_nulls)} unique unresolved batters.")
        except Exception as e:
            batting_nulls = []
            print(f"No game_batting_stats table or error: {e}")
        
        # Pitching stats with NULL player_id
        try:
            result = session.execute(text("""
                SELECT DISTINCT player_name, team_code,
                       substr(game_id, 1, 4) as season
                FROM game_pitching_stats 
                WHERE player_id IS NULL AND player_name IS NOT NULL
                ORDER BY season, team_code, player_name
            """))
            pitching_nulls = result.fetchall()
            print(f"Found {len(pitching_nulls)} unique unresolved pitchers.")
        except Exception as e:
            pitching_nulls = []
            print(f"No game_pitching_stats table or error: {e}")
            
        # Combine and look for potential matches in player_basic
        all_unresolved = set()
        for row in batting_nulls + pitching_nulls:
            name, team, season = row
            all_unresolved.add((name, team, season))
        
        print(f"\nTotal unique unresolved (name, team, season): {len(all_unresolved)}")
        
        # Try to suggest player_basic matches
        for name, team, season in sorted(all_unresolved):
            # Check if exact name exists in player_basic
            result = session.execute(text(
                "SELECT player_id, name, team, uniform_no FROM player_basic WHERE name = :name"
            ), {"name": name}).fetchall()
            
            if len(result) == 0:
                suggestion = "NO_MATCH"
                suggested_id = ""
            elif len(result) == 1:
                suggested_id = result[0][0]
                suggestion = f"UNIQUE_MATCH (team={result[0][2]}, uni={result[0][3]})"
            else:
                suggested_id = ",".join(str(r[0]) for r in result)
                suggestion = f"MULTIPLE ({len(result)} candidates)"
            
            output_rows.append({
                "player_name": name,
                "team_code": team,
                "season": season,
                "suggested_player_id": suggested_id,
                "match_type": suggestion,
            })
    
    # Write output CSV
    output_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'unresolved_players.csv')
    output_path = os.path.normpath(output_path)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["player_name", "team_code", "season", "suggested_player_id", "match_type"])
        writer.writeheader()
        writer.writerows(output_rows)
    
    print(f"\n✅ Wrote {len(output_rows)} entries to {output_path}")
    
    # Summary stats
    no_match = sum(1 for r in output_rows if r["match_type"] == "NO_MATCH")
    unique = sum(1 for r in output_rows if "UNIQUE_MATCH" in r["match_type"])
    multiple = sum(1 for r in output_rows if "MULTIPLE" in r["match_type"])
    print(f"   NO_MATCH: {no_match} | UNIQUE_MATCH: {unique} | MULTIPLE: {multiple}")


if __name__ == "__main__":
    detect_unresolved()
