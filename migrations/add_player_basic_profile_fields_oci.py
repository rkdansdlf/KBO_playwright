"""
Migration for OCI Postgres: Add extended profile fields to player_basic table.
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def run():
    load_dotenv()
    url = os.getenv('OCI_DB_URL')
    if not url:
        print("[ERROR] OCI_DB_URL not found in environment")
        return

    engine = create_engine(url)
    
    NEW_COLUMNS = [
        ("photo_url",               "VARCHAR(500)"),
        ("bats",                    "VARCHAR(4)"),
        ("throws",                  "VARCHAR(4)"),
        ("debut_year",              "INTEGER"),
        ("salary_original",         "VARCHAR(50)"),
        ("signing_bonus_original",  "VARCHAR(50)"),
        ("draft_info",              "VARCHAR(100)"),
    ]

    with engine.connect() as conn:
        for col_name, col_type in NEW_COLUMNS:
            try:
                # Check if column exists
                check_sql = text(f"SELECT column_name FROM information_schema.columns WHERE table_name='player_basic' AND column_name='{col_name}'")
                result = conn.execute(check_sql).fetchone()
                
                if result:
                    print(f"[SKIP] Column already exists: {col_name}")
                else:
                    # Add column
                    print(f"[ADD] Adding column: {col_name}")
                    conn.execute(text(f"ALTER TABLE player_basic ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
            except Exception as e:
                print(f"[ERROR] Failed to add {col_name}: {e}")

    print("\n✅ OCI Migration complete.")

if __name__ == "__main__":
    run()
