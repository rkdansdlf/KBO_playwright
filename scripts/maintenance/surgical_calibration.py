import sqlite3
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")

def surgical_calibration():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    years = [2024, 2025, 2026]
    
    print("=== Surgical Integrity Calibration (Target: 100% Match for Top Players) ===")
    
    for year in years:
        print(f"\n--- Calibrating Year: {year} ---")
        
        # 1. Get Top 100 players by hits in this year
        query_top_players = """
        SELECT player_id, hits, games, at_bats, team_code
        FROM player_season_batting
        WHERE season = ? AND league = 'REGULAR'
        ORDER BY hits DESC
        LIMIT 100
        """
        top_players = cursor.execute(query_top_players, (year,)).fetchall()
        
        corrected_count = 0
        for pid, s_hits, s_games, s_ab, team in top_players:
            # 2. Get ALL available records for this player in this year (ignoring is_primary for a moment)
            cursor.execute("""
                SELECT b.id, g.game_id, g.game_date, b.hits, b.at_bats, g.season_id, g.is_primary
                FROM game_batting_stats b
                JOIN game g ON b.game_id = g.game_id
                WHERE b.player_id = ? AND strftime('%Y', g.game_date) = ?
                ORDER BY g.game_date, g.game_id
            """, (pid, str(year)))
            
            all_records = cursor.fetchall()
            
            # Group by date to handle duplicates/DH
            by_date = {}
            for row in all_records:
                d = row[2]
                if d not in by_date: by_date[d] = []
                by_date[d].append(row)
            
            # Simple Selection Strategy:
            # We want to pick a subset of these records that SUM to s_hits and s_games.
            # Usually, if s_games is 144 and we have 144 unique dates, we just pick one per date.
            
            chosen_ids = []
            current_hits = 0
            current_games = 0
            
            # Priority: Games already marked is_primary=1, then by season_id, then by hits
            for d in sorted(by_date.keys()):
                records = by_date[d]
                
                # If only one record on this date, it's likely the one
                if len(records) == 1:
                    chosen_ids.append(records[0][0])
                    current_hits += records[0][3]
                    current_games += 1
                else:
                    # Multiple records (DH or Team Code duplicate)
                    # For DH, we usually want both if s_games is high.
                    # For SK/SSG duplicates, we want ONLY ONE.
                    
                    # Heuristic: If s_games is high (like 144) and this is a known DH date, take both.
                    # Otherwise, take the one with better/modern game_id.
                    
                    # Let's try to match the season total
                    # If we need 144 games and we have 142 dates, some dates MUST have 2 games (DH).
                    
                    # For now, let's pick the one already marked primary if available
                    primary = [r for r in records if r[6] == 1]
                    if primary:
                        for p_rec in primary:
                            chosen_ids.append(p_rec[0])
                            current_hits += p_rec[3]
                            current_games += 1
                    else:
                        # Pick the first one
                        chosen_ids.append(records[0][0])
                        current_hits += records[0][3]
                        current_games += 1
            
            # Final check for this player
            if current_hits == s_hits and current_games == s_games:
                # This subset is perfect! Ensure ONLY these are marked is_primary for THIS player
                # but is_primary is a GAME level flag, not PLAYER level.
                # This is the tricky part. If we change is_primary, it affects other players.
                corrected_count += 1
            else:
                # If mismatch persists, we may need to print it for manual review or further logic
                pass

        print(f"Verified {corrected_count} / 100 players match perfectly.")

    conn.close()

if __name__ == "__main__":
    surgical_calibration()
