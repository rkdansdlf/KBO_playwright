import sys
import os
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import Session

# Add the project root to the python path
sys.path.append(os.getcwd())

from src.utils.team_codes import resolve_team_code
from src.models.base import Base

# Define awards model locally if needed to avoid imports or use raw SQL
# Since we need to update rows, raw SQL might be easier for a quick cleanup script

def main():
    db_url = "sqlite:///./data/kbo_dev.db"
    if not os.path.exists("./data/kbo_dev.db"):
        print("Database not found.")
        return

    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        print("=== Awards Table Cleanup ===\n")
        
        # 1. Get all unique team_names from awards table
        result = conn.execute(text("SELECT DISTINCT team_name FROM awards"))
        unique_names = [row[0] for row in result if row[0]]
        
        print(f"Found {len(unique_names)} unique team names/codes in awards table.")
        
        mapping_stats = {}
        
        for name in unique_names:
            resolved = resolve_team_code(name)
            if resolved:
                if name != resolved:
                    print(f"  Mapping: '{name}' -> '{resolved}'")
                    # Update all rows with this name to the new code
                    update_stmt = text("UPDATE awards SET team_name = :new_code WHERE team_name = :old_name")
                    res = conn.execute(update_stmt, {"new_code": resolved, "old_name": name})
                    mapping_stats[name] = (resolved, res.rowcount)
            else:
                print(f"  ⚠️ Could not resolve: '{name}'")

        conn.commit()
        
        print("\n--- Summary ---")
        for old, (new, count) in mapping_stats.items():
            print(f"  - '{old}' -> '{new}': {count} rows updated")
            
        print("\nAwards table cleanup completed.")

if __name__ == "__main__":
    main()
