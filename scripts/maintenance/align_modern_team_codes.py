import sys
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# Add the project root to the python path
sys.path.append(os.getcwd())

from src.utils.team_history import resolve_team_code_for_season

def main():
    db_url = "sqlite:///./data/kbo_dev.db"
    if not os.path.exists("./data/kbo_dev.db"):
        print("Database not found.")
        return

    print(f"Connecting to database: {db_url}")
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        print("Starting team code alignment migration...")
        
        # 1. Fetch all games to check their dates and codes
        games = conn.execute(text("SELECT id, game_id, game_date, home_team, away_team FROM game")).fetchall()
        
        update_count = 0
        for game in games:
            game_id_val, game_date_str, home_team, away_team = game.id, game.game_date, game.home_team, game.away_team
            
            # Extract year from date
            try:
                if isinstance(game_date_str, str):
                    year = int(game_date_str[:4])
                else:
                    # might be a date object
                    year = game_date_str.year
            except:
                continue
                
            new_home = resolve_team_code_for_season(home_team, year) or home_team
            new_away = resolve_team_code_for_season(away_team, year) or away_team
            
            if new_home != home_team or new_away != away_team:
                conn.execute(text("UPDATE game SET home_team = :home, away_team = :away WHERE id = :id"),
                             {"home": new_home, "away": new_away, "id": game_id_val})
                
                # Update related tables for this game
                for table in ["game_inning_scores", "game_batting_stats", "game_pitching_stats", "game_lineups"]:
                    conn.execute(text(f"UPDATE {table} SET team_code = :new WHERE game_id = (SELECT game_id FROM game WHERE id = :id) AND team_code = :old"),
                                 {"new": new_home, "id": game_id_val, "old": home_team})
                    conn.execute(text(f"UPDATE {table} SET team_code = :new WHERE game_id = (SELECT game_id FROM game WHERE id = :id) AND team_code = :old"),
                                 {"new": new_away, "id": game_id_val, "old": away_team})
                
                update_count += 1
                if update_count % 100 == 0:
                    print(f"  Processed {update_count} updates...")

        conn.commit()
        print(f"\nMigration completed. Updated {update_count} games.")

if __name__ == "__main__":
    main()
