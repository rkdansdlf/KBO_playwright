from sqlalchemy import text

from src.db.engine import Engine
from src.models.base import Base


def run_migration():
    print("🛠️  Starting Team Schema Verification & Migration...")
    print(f"📊 Database URL: {Engine.url}")

    with Engine.connect() as conn:
        tables = conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        print(f"📋 Tables in DB: {[t[0] for t in tables]}")

        # 1. Alter team_franchises
        try:
            print("Checking team_franchises columns...")
            conn.execute(text("ALTER TABLE team_franchises ADD COLUMN metadata_json JSON"))
            print("✅ Added metadata_json to team_franchises")
        except Exception as e:  # noqa: BLE001
            if "duplicate column" in str(e).lower():
                print("ℹ️  metadata_json already exists")
            else:
                print(f"⚠️  Alter error (metadata_json): {e}")

        try:
            conn.execute(text("ALTER TABLE team_franchises ADD COLUMN web_url VARCHAR(255)"))
            print("✅ Added web_url to team_franchises")
        except Exception as e:  # noqa: BLE001
            if "duplicate column" in str(e).lower():
                print("ℹ️  web_url already exists")
            else:
                print(f"⚠️  Alter error (web_url): {e}")

        # 2. Alter teams
        try:
            print("Checking teams columns...")
            conn.execute(text("ALTER TABLE teams ADD COLUMN is_active BOOLEAN DEFAULT 1"))
            print("✅ Added is_active to teams")
        except Exception as e:  # noqa: BLE001
            if "duplicate column" in str(e).lower():
                print("ℹ️  is_active already exists")
            else:
                print(f"⚠️  Alter error (is_active): {e}")

        try:
            conn.execute(text("ALTER TABLE teams ADD COLUMN aliases JSON"))
            print("✅ Added aliases to teams")
        except Exception as e:  # noqa: BLE001
            if "duplicate column" in str(e).lower():
                print("ℹ️  aliases already exists")
            else:
                print(f"⚠️  Alter error (aliases): {e}")

    # 3. Create team_history table
    # Using SQLAlchemy create_all feature which only creates missing tables
    print("Creating missing tables (team_history)...")
    Base.metadata.create_all(bind=Engine)
    print("✅ Schema Migration Complete.")


if __name__ == "__main__":
    run_migration()
