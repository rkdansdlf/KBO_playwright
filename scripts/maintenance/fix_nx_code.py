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
        print("Starting NX -> WO migration...")
        
        # 1. Update Game Table
        print("\nUpdating 'game' table...")
        
        # Home Team
        result = conn.execute(text("UPDATE game SET home_team = 'WO' WHERE home_team = 'NX'"))
        print(f"  - Updated home_team NX -> WO: {result.rowcount} rows")
        
        # Away Team
        result = conn.execute(text("UPDATE game SET away_team = 'WO' WHERE away_team = 'NX'"))
        print(f"  - Updated away_team NX -> WO: {result.rowcount} rows")
        
        # Winning Team
        result = conn.execute(text("UPDATE game SET winning_team = 'WO' WHERE winning_team = 'NX'"))
        print(f"  - Updated winning_team NX -> WO: {result.rowcount} rows")

        conn.commit()
        print("\nNX -> WO Migration completed successfully.")

if __name__ == "__main__":
    main()
