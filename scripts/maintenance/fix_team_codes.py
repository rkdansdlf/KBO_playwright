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
        print("Starting migration...")
        
        # 1. Update Game Table
        print("\nUpdating 'game' table...")
        
        # Home Team
        result = conn.execute(text("UPDATE game SET home_team = 'WO' WHERE home_team = 'KI'"))
        print(f"  - Updated home_team KI -> WO: {result.rowcount} rows")
        
        # Away Team
        result = conn.execute(text("UPDATE game SET away_team = 'WO' WHERE away_team = 'KI'"))
        print(f"  - Updated away_team KI -> WO: {result.rowcount} rows")
        
        # Winning Team
        result = conn.execute(text("UPDATE game SET winning_team = 'WO' WHERE winning_team = 'KI'"))
        print(f"  - Updated winning_team KI -> WO: {result.rowcount} rows")

        # DO -> OB (Just in case)
        result = conn.execute(text("UPDATE game SET home_team = 'OB' WHERE home_team = 'DO'"))
        if result.rowcount > 0: print(f"  - Updated home_team DO -> OB: {result.rowcount} rows")
        
        result = conn.execute(text("UPDATE game SET away_team = 'OB' WHERE away_team = 'DO'"))
        if result.rowcount > 0: print(f"  - Updated away_team DO -> OB: {result.rowcount} rows")
        
        result = conn.execute(text("UPDATE game SET winning_team = 'OB' WHERE winning_team = 'DO'"))
        if result.rowcount > 0: print(f"  - Updated winning_team DO -> OB: {result.rowcount} rows")

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
                # KI -> WO
                result = conn.execute(text(f"UPDATE {table} SET team_code = 'WO' WHERE team_code = 'KI'"))
                print(f"  - Updated KI -> WO: {result.rowcount} rows")
                
                # DO -> OB
                result = conn.execute(text(f"UPDATE {table} SET team_code = 'OB' WHERE team_code = 'DO'"))
                if result.rowcount > 0: print(f"  - Updated DO -> OB: {result.rowcount} rows")
            except Exception as e:
                print(f"  ⚠️ Error updating {table}: {e}")

        conn.commit()
        print("\nMigration completed successfully.")

if __name__ == "__main__":
    main()
