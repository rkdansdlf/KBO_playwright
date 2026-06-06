import argparse
import glob
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


def _get_oci_url() -> str | None:
    return os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")


def check_migrations():
    oci_url = _get_oci_url()
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return 1

    engine = create_engine(oci_url)
    Session = sessionmaker(bind=engine)
    local_files = sorted(os.path.basename(f) for f in glob.glob("migrations/oci/*.sql"))

    print(f"OCI Migration Check — {len(local_files)} local migration files")
    print()

    with Session() as session:
        try:
            result = session.execute(
                text("SELECT filename, applied_at FROM _schema_migrations ORDER BY filename")
            ).fetchall()
            applied: dict[str, str] = {row[0]: str(row[1]) for row in result}
        except SQLAlchemyError:
            applied = {}

        all_passed = True
        for fname in local_files:
            if fname in applied:
                print(f"  ✅ {fname:50} {applied[fname]}")
            else:
                print(f"  ❌ {fname:50} NOT APPLIED")
                all_passed = False

        extras = [f for f in applied if f not in set(local_files)]
        if extras:
            print("\n⚠️   Migration(s) applied in OCI but missing locally:")
            for f in extras:
                print(f"     {f:50} {applied[f]}")
            all_passed = False

        print(f"\nTotal: {len(local_files)} local, {len(applied)} applied in OCI")
        if all_passed:
            print("✅ All migrations are in sync.")
            return 0
        else:
            print("❌ Some migrations are out of sync.")
            return 1


def apply_migrations():
    load_dotenv()
    oci_url = _get_oci_url()
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
    parser = argparse.ArgumentParser(description="Apply or check OCI migrations")
    parser.add_argument("--check", action="store_true", help="Check migration status without applying")
    args = parser.parse_args()

    if args.check:
        sys.exit(check_migrations())
    else:
        apply_migrations()
