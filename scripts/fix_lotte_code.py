import sys
import os

# Add the project root to the python path
sys.path.append(os.getcwd())

from sqlalchemy import text
from src.db.engine import SessionLocal

def fix_lotte_code():
    """
    Updates the Lotte Giants team code in the team_franchises table from 'LOT' to 'LT'.
    """
    print("Starting Lotte team code fix...")
    
    with SessionLocal() as session:
        try:
            # Check current state
            check_sql = text("SELECT id, name, original_code, current_code FROM team_franchises WHERE original_code = 'LT'")
            result = session.execute(check_sql).fetchone()
            
            if not result:
                print("Error: Could not find Lotte franchise with original_code='LT'")
                return
            
            print(f"Current state: ID={result.id}, Name={result.name}, Original={result.original_code}, Current={result.current_code}")
            
            if result.current_code == 'LT':
                print("Code is already 'LT'. No action needed.")
                return

            # Update
            update_sql = text("UPDATE team_franchises SET current_code = 'LT' WHERE original_code = 'LT'")
            session.execute(update_sql)
            session.commit()
            
            # Verify
            result_after = session.execute(check_sql).fetchone()
            print(f"New state: ID={result_after.id}, Name={result_after.name}, Original={result_after.original_code}, Current={result_after.current_code}")
            
            if result_after.current_code == 'LT':
                print("SUCCESS: Lotte team code updated to 'LT'.")
            else:
                print("FAILURE: Lotte team code was not updated.")
                
        except Exception as e:
            session.rollback()
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    fix_lotte_code()
