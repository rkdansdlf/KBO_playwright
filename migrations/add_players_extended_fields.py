"""
Migration: Add extended profile fields to the relational 'players' table.
"""
import sqlite3
import os

def run():
    db_path = "data/kbo_dev.db"
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    NEW_COLUMNS = [
        ("photo_url", "VARCHAR(500)"),
        ("salary_original", "VARCHAR(50)"),
        ("signing_bonus_original", "VARCHAR(50)"),
        ("draft_info", "VARCHAR(100)"),
    ]

    for col_name, col_type in NEW_COLUMNS:
        try:
            print(f"Adding column {col_name} to players table...")
            cursor.execute(f"ALTER TABLE players ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            print(f"Column {col_name} already exists.")

    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    run()
