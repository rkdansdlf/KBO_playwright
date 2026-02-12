import sys
import os
from sqlalchemy import create_engine, text

# Add the project root to the python path
sys.path.append(os.getcwd())

def migrate():
    db_url = "sqlite:///./data/kbo_dev.db"
    if not os.path.exists("./data/kbo_dev.db"):
        print("Database not found.")
        return

    print(f"Connecting to database: {db_url}")
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        print("Starting team code renaming migration (KI->KH, DO->DB)...")
        
        # 1. Update teams table
        print("Updating teams table...")
        res = conn.execute(text("UPDATE teams SET team_id = 'KH' WHERE team_id = 'KI'"))
        print(f"  - Updated KI -> KH in teams: {res.rowcount} rows")
        res = conn.execute(text("UPDATE teams SET team_id = 'DB' WHERE team_id = 'DO'"))
        print(f"  - Updated DO -> DB in teams: {res.rowcount} rows")

        # 2. Update game table (home_team, away_team, winning_team)
        print("Updating game table...")
        for field in ["home_team", "away_team", "winning_team"]:
            res = conn.execute(text(f"UPDATE game SET {field} = 'KH' WHERE {field} = 'KI'"))
            print(f"  - Updated KI -> KH in game.{field}: {res.rowcount} rows")
            res = conn.execute(text(f"UPDATE game SET {field} = 'DB' WHERE {field} = 'DO'"))
            print(f"  - Updated DO -> DB in game.{field}: {res.rowcount} rows")

        # 3. Update related tables with team_code column
        team_code_tables = [
            "game_inning_scores",
            "game_batting_stats",
            "game_pitching_stats",
            "game_lineups",
            "player_season_batting",
            "player_season_pitching",
            "team_season_batting",
            "team_season_pitching",
            "player_basic",
            "stat_rankings",
            "team_daily_roster",
            "player_movements"
        ]
        
        for table in team_code_tables:
            print(f"Updating {table} table...")
            try:
                res = conn.execute(text(f"UPDATE {table} SET team_code = 'KH' WHERE team_code = 'KI'"))
                print(f"  - Updated KI -> KH in {table}: {res.rowcount} rows")
                res = conn.execute(text(f"UPDATE {table} SET team_code = 'DB' WHERE team_code = 'DO'"))
                print(f"  - Updated DO -> DB in {table}: {res.rowcount} rows")
            except Exception as e:
                print(f"  ⚠️ Skipping {table} (or error): {e}")

        conn.commit()
        print("\nMigration completed successfully.")

if __name__ == "__main__":
    migrate()
