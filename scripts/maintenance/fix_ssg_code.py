import sys
import os
from sqlalchemy import create_engine, text

# Add the project root to the python path
sys.path.append(os.getcwd())

def main():
    db_url = "sqlite:///./data/kbo_dev.db"
    if not os.path.exists("./data/kbo_dev.db"):
        print("Database not found.")
        return

    print(f"Connecting to database: {db_url}")
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        print("Starting SSG -> SK migration...")
        
        # 1. Update Game Table
        print("\nUpdating 'game' table...")
        
        # Home Team
        result = conn.execute(text("UPDATE game SET home_team = 'SK' WHERE home_team = 'SSG'"))
        print(f"  - Updated home_team SSG -> SK: {result.rowcount} rows")
        
        # Away Team
        result = conn.execute(text("UPDATE game SET away_team = 'SK' WHERE away_team = 'SSG'"))
        print(f"  - Updated away_team SSG -> SK: {result.rowcount} rows")
        
        # Winning Team
        result = conn.execute(text("UPDATE game SET winning_team = 'SK' WHERE winning_team = 'SSG'"))
        print(f"  - Updated winning_team SSG -> SK: {result.rowcount} rows")

        # 2. Update Related Tables
        tables_with_team_code = [
            "game_inning_scores",
            "game_batting_stats",
            "game_pitching_stats",
            "game_lineups"
        ]
        
        for table in tables_with_team_code:
            print(f"\nUpdating '{table}' table...")
            try:
                # SSG -> SK
                result = conn.execute(text(f"UPDATE {table} SET team_code = 'SK' WHERE team_code = 'SSG'"))
                print(f"  - Updated SSG -> SK: {result.rowcount} rows")
            except Exception as e:
                print(f"  ⚠️ Error updating {table}: {e}")

        conn.commit()
        print("\nMigration completed successfully.")

if __name__ == "__main__":
    main()
