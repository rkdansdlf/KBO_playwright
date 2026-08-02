import argparse
import hashlib
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.engine import create_engine_for_url

MIGRATION_DIRECTORIES = {
    "postgres": Path("migrations/oci"),
    "oracle": Path("migrations/oracle"),
}
MIGRATION_TABLES = {
    "postgres": "_schema_migrations",
    "oracle": "KBO_SCHEMA_MIGRATIONS",
}


def _get_oci_url() -> str | None:
    return os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")


def _migration_files(
    dialect: str,
    migration_dir: str | None = None,
    only: set[str] | None = None,
) -> list[Path]:
    root = Path(migration_dir) if migration_dir else MIGRATION_DIRECTORIES[dialect]
    files = sorted(root.glob("*.sql"))
    if not only:
        return files
    available = {path.name for path in files}
    unknown = sorted(only - available)
    if unknown:
        message = f"Unknown {dialect} migration file(s): {', '.join(unknown)}"
        raise ValueError(message)
    return [path for path in files if path.name in only]


def _split_migration_statements(sql: str, dialect: str) -> list[str]:
    """Split Oracle slash-delimited PL/SQL blocks without splitting SQL bodies."""
    if dialect != "oracle":
        return [sql.strip()] if sql.strip() else []
    return [part.strip() for part in re.split(r"(?m)^\s*/\s*$", sql) if part.strip()]


def _file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _execute_migration_statements(session, filename: str, statements: list[str]) -> None:
    for index, statement in enumerate(statements, start=1):
        try:
            session.execute(text(statement))
        except SQLAlchemyError as exc:
            session.rollback()
            message = f"Migration {filename} statement {index}/{len(statements)} failed; transaction rolled back: {exc}"
            raise RuntimeError(message) from exc


def _oracle_metadata_ddl() -> str:
    return """
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE KBO_SCHEMA_MIGRATIONS (
            FILENAME VARCHAR2(255) PRIMARY KEY,
            APPLIED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            DIALECT VARCHAR2(16) NOT NULL,
            CHECKSUM VARCHAR2(64) NOT NULL
        )';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -955 THEN
                RAISE;
            END IF;
    END;
    """


def _ensure_metadata_table(session, dialect: str) -> None:
    table_name = MIGRATION_TABLES[dialect]
    if dialect == "oracle":
        session.execute(text(_oracle_metadata_ddl()))
    else:
        session.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                filename TEXT PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """),
        )
    if dialect == "oracle":
        session.execute(
            text(
                """
                DECLARE
                    v_exists NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO v_exists FROM user_tab_columns
                     WHERE table_name = 'KBO_SCHEMA_MIGRATIONS' AND column_name = 'DIALECT';
                    IF v_exists = 0 THEN
                        EXECUTE IMMEDIATE 'ALTER TABLE KBO_SCHEMA_MIGRATIONS ADD (DIALECT VARCHAR2(16))';
                    END IF;
                    SELECT COUNT(*) INTO v_exists FROM user_tab_columns
                     WHERE table_name = 'KBO_SCHEMA_MIGRATIONS' AND column_name = 'CHECKSUM';
                    IF v_exists = 0 THEN
                        EXECUTE IMMEDIATE 'ALTER TABLE KBO_SCHEMA_MIGRATIONS ADD (CHECKSUM VARCHAR2(64))';
                    END IF;
                END;
                """,
            ),
        )
    else:
        session.execute(text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS dialect TEXT"))
        session.execute(text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS checksum VARCHAR(64)"))


def _read_applied_migrations(session, dialect: str) -> dict[str, tuple[str, str | None]]:
    table_name = MIGRATION_TABLES[dialect]
    try:
        result = session.execute(
            text(f"SELECT filename, applied_at, checksum FROM {table_name} ORDER BY filename"),
        ).fetchall()
    except SQLAlchemyError:
        if dialect != "postgres":
            raise
        session.rollback()
        result = session.execute(
            text(f"SELECT filename, applied_at FROM {table_name} ORDER BY filename"),
        ).fetchall()
        return {row[0]: (str(row[1]), None) for row in result}
    return {row[0]: (str(row[1]), row[2]) for row in result}


def check_migrations(
    dialect: str = "postgres",
    migration_dir: str | None = None,
    only: set[str] | None = None,
) -> int:
    oci_url = _get_oci_url()
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return 1

    engine = create_engine_for_url(oci_url)
    Session = sessionmaker(bind=engine)
    local_paths = _migration_files(dialect, migration_dir, only)
    local_files = [path.name for path in local_paths]
    local_paths_by_name = {path.name: path for path in local_paths}
    table_name = MIGRATION_TABLES[dialect]

    print(f"{dialect.title()} Migration Check — {len(local_files)} local migration files")
    print()

    try:
        with Session() as session:
            try:
                applied = _read_applied_migrations(session, dialect)
            except SQLAlchemyError as exc:
                print(f"⚠️ Migration tracking table {table_name} is unavailable: {exc}")
                applied = {}

            all_passed = True
            for fname in local_files:
                if fname in applied:
                    applied_at, checksum = applied[fname]
                    expected_checksum = _file_checksum(local_paths_by_name[fname])
                    if checksum and checksum != expected_checksum:
                        print(f"  ⚠️ {fname:50} CHECKSUM MISMATCH")
                        all_passed = False
                    else:
                        suffix = "" if checksum else " (checksum unavailable)"
                        print(f"  ✅ {fname:50} {applied_at}{suffix}")
                else:
                    print(f"  ❌ {fname:50} NOT APPLIED")
                    all_passed = False

            extras = [] if only else [f for f in applied if f not in set(local_files)]
            if extras:
                print("\n⚠️   Migration(s) applied in OCI but missing locally:")
                for f in extras:
                    print(f"     {f:50} {applied[f]}")
                all_passed = False

            selected_applied = sum(1 for filename in local_files if filename in applied)
            print(f"\nTotal: {len(local_files)} local, {selected_applied} selected migration(s) applied in OCI")
            if all_passed:
                print("✅ All migrations are in sync.")
                return 0
            print("❌ Some migrations are out of sync.")
            return 1
    finally:
        engine.dispose()


def apply_migrations(
    dialect: str = "postgres",
    migration_dir: str | None = None,
    only: set[str] | None = None,
) -> int:
    load_dotenv()
    oci_url = _get_oci_url()
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return 1

    engine = create_engine_for_url(oci_url)
    Session = sessionmaker(bind=engine)
    migration_files = _migration_files(dialect, migration_dir, only)
    table_name = MIGRATION_TABLES[dialect]

    if not migration_files:
        print("ℹ️ No OCI migration files found.")
        engine.dispose()
        return 0

    try:
        with Session() as session:
            _ensure_metadata_table(session, dialect)
            session.commit()

            for file_path in migration_files:
                filename = file_path.name
                result = session.execute(
                    text(f"SELECT 1 FROM {table_name} WHERE filename = :filename"),
                    {"filename": filename},
                )
                if result.fetchone() is not None:
                    print(f"⏭️  Skipping already applied migration: {filename}")
                    continue

                print(f"🚀 Applying migration: {filename}")
                with file_path.open(encoding="utf-8") as f:
                    sql = f.read()
                _execute_migration_statements(
                    session,
                    filename,
                    _split_migration_statements(sql, dialect),
                )
                session.execute(
                    text(
                        f"INSERT INTO {table_name} (filename, dialect, checksum) "
                        "VALUES (:filename, :dialect, :checksum)",
                    ),
                    {"filename": filename, "dialect": dialect, "checksum": _file_checksum(file_path)},
                )
                session.commit()
                print(f"✅ Successfully applied {filename}")
    except (SQLAlchemyError, OSError, RuntimeError) as exc:
        print(f"❌ Migration apply failed: {exc}")
        raise
    finally:
        engine.dispose()

    return 0


def refresh_checksums(
    dialect: str = "postgres",
    migration_dir: str | None = None,
    only: set[str] | None = None,
) -> int:
    """Adopt current file checksums for migrations already applied in the target."""
    load_dotenv()
    oci_url = _get_oci_url()
    if not oci_url:
        print("❌ OCI_DB_URL is not set.")
        return 1
    if not only:
        raise ValueError("--refresh-checksum requires at least one --only migration filename")

    engine = create_engine_for_url(oci_url)
    Session = sessionmaker(bind=engine)
    migration_files = _migration_files(dialect, migration_dir, only)
    table_name = MIGRATION_TABLES[dialect]
    try:
        with Session() as session:
            applied = _read_applied_migrations(session, dialect)
            missing = [path.name for path in migration_files if path.name not in applied]
            if missing:
                message = f"Cannot refresh unapplied migration(s): {', '.join(missing)}"
                raise ValueError(message)
            for file_path in migration_files:
                session.execute(
                    text(f"UPDATE {table_name} SET checksum = :checksum WHERE filename = :filename"),
                    {"filename": file_path.name, "checksum": _file_checksum(file_path)},
                )
                print(f"🔄 Refreshed migration checksum: {file_path.name}")
            session.commit()
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(description="Apply or check OCI migrations")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Check migration status without applying")
    mode.add_argument(
        "--refresh-checksum",
        action="store_true",
        help="Refresh checksum metadata for already applied --only migrations",
    )
    parser.add_argument("--dialect", choices=sorted(MIGRATION_DIRECTORIES), default="postgres")
    parser.add_argument("--migration-dir", default=None, help="Override the migration directory")
    parser.add_argument(
        "--only",
        action="append",
        default=None,
        help="Select one migration filename; repeat for a batch",
    )
    args = parser.parse_args()
    selected = set(args.only or [])

    if args.refresh_checksum:
        if not selected:
            parser.error("--refresh-checksum requires at least one --only migration filename")
        sys.exit(refresh_checksums(args.dialect, args.migration_dir, selected))
    elif args.check:
        sys.exit(check_migrations(args.dialect, args.migration_dir, selected or None))
    else:
        sys.exit(apply_migrations(args.dialect, args.migration_dir, selected or None))
