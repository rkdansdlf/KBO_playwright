import glob
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def apply_migrations():
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return

    engine = create_engine(oci_url)
    Session = sessionmaker(bind=engine)
    migration_files = sorted(glob.glob("migrations/oci/*.sql"))

    if not migration_files:
        print("ℹ️ No OCI migration files found.")
        return

    with Session() as session:
        # Create migration tracking table if it doesn't exist
        session.execute(
            text("""
            CREATE TABLE IF NOT EXISTS _schema_migrations (
                filename TEXT PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        )
        session.commit()

        for file_path in migration_files:
            filename = os.path.basename(file_path)
            # Check if migration has already been applied
            result = session.execute(
                text("SELECT 1 FROM _schema_migrations WHERE filename = :filename"), {"filename": filename}
            )
            if result.fetchone() is not None:
                print(f"⏭️  Skipping already applied migration: {filename}")
                continue

            print(f"🚀 Applying migration: {filename}")
            try:
                with open(file_path, encoding="utf-8") as f:
                    sql = f.read()
                session.execute(text(sql))
                # Record that this migration has been applied
                session.execute(
                    text("INSERT INTO _schema_migrations (filename) VALUES (:filename)"), {"filename": filename}
                )
                session.commit()
                print(f"✅ Successfully applied {filename}")
            except Exception as exc:
                session.rollback()
                print(f"❌ Failed to apply {filename}: {exc}")
                raise

    engine.dispose()


if __name__ == "__main__":
    apply_migrations()
