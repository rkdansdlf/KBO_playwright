
from sqlalchemy import text
from src.db.engine import Engine

def migrate_sqlite_teams():
    print("🛠️  Migrating SQLite 'teams' table to add 'franchise_id' column...")
    with Engine.connect() as conn:
        try:
            # Check if column exists first? No easy way in raw SQL + SQLite without parsing table_info
            # Just try adding it. ALter table add column if not exists is not supported in all sqlite versions
            # But standard ADD COLUMN is supported.
            conn.execute(text("ALTER TABLE teams ADD COLUMN franchise_id INTEGER"))
            print("✅ Successfully added 'franchise_id' column.")
        except Exception as e:
            if "duplicate column name" in str(e):
                print("ℹ️  Column 'franchise_id' already exists.")
            else:
                print(f"❌ Error adding column: {e}")
                
if __name__ == "__main__":
    migrate_sqlite_teams()
