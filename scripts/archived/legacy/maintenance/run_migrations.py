"""
SQLite migration runner.

Scans migration files (migrations/sqlite/{NNN}_*.sql or .py), tracks
applied migrations in a _migrations table, and applies unapplied ones
in sorted order.
"""

import importlib.util
import os
import re
import sqlite3
import sys
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "migrations" / "sqlite"
MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "data",
    "kbo_dev.db",
)


def get_db_path():
    from dotenv import load_dotenv

    load_dotenv()
    db_url = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")
    if db_url.startswith("sqlite:///"):
        return db_url[len("sqlite:///") :]
    return DB_PATH


def get_conn():
    path = get_db_path()
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _regexp_replace(s, pattern, repl):
    try:
        return re.sub(pattern, repl or "", s or "")
    except Exception as e:
        raise RuntimeError(f"regexp_replace({s!r}, {pattern!r}, {repl!r}): {e}") from e


def _install_functions(conn):
    conn.create_function("regexp_replace", 3, _regexp_replace)


def get_applied(conn):
    rows = conn.execute("SELECT filename FROM _migrations ORDER BY filename").fetchall()
    return {r[0] for r in rows}


def ensure_tracking_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            filename    TEXT PRIMARY KEY,
            applied_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


def discover_migrations():
    files = []
    for f in sorted(MIGRATIONS_DIR.iterdir()):
        m = re.match(r"(\d{3})_.+\.(sql|py)$", f.name)
        if m:
            files.append((m.group(1), f.name, f.suffix))
    return files


def seed_pre_existing(conn, all_migrations):
    pre_existing = [m for m in all_migrations if 2 <= int(m[0]) <= 18]
    if not pre_existing:
        return
    applied = get_applied(conn)
    for _, fname, _ in pre_existing:
        if fname not in applied:
            conn.execute(
                "INSERT INTO _migrations (filename) VALUES (?)",
                (fname,),
            )
    conn.commit()
    print(f"  Pre-marked {len(pre_existing)} migration(s) (002-018) as applied.")


def run_sql_migration(conn, path):
    sql = path.read_text().strip()
    if not sql:
        return
    conn.executescript(sql)
    conn.commit()


def run_py_migration(conn, path):
    mod_name = path.stem
    spec = importlib.util.spec_from_file_location(mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    mod.run_migrations_conn = conn
    spec.loader.exec_module(mod)
    if hasattr(mod, "upgrade"):
        mod.upgrade()


def run():
    conn = get_conn()
    _install_functions(conn)

    ensure_tracking_table(conn)
    migrations = discover_migrations()
    seed_pre_existing(conn, migrations)
    applied = get_applied(conn)

    if not migrations:
        print("No migrations found in migrations/sqlite/")
        conn.close()
        return

    count = 0
    for _, fname, ext in migrations:
        if fname in applied:
            print(f"  [SKIP] {fname} already applied")
            continue

        path = MIGRATIONS_DIR / fname
        print(f"  [RUN]  {fname}...", end="", flush=True)

        try:
            if ext == ".sql":
                run_sql_migration(conn, path)
            elif ext == ".py":
                run_py_migration(conn, path)

            conn.execute(
                "INSERT INTO _migrations (filename) VALUES (?)",
                (fname,),
            )
            conn.commit()
            print(" OK")
            count += 1
        except Exception as e:
            conn.rollback()
            print(f" FAILED: {e}")
            conn.close()
            raise

    conn.close()
    if count == 0:
        print("All migrations already applied.")
    else:
        print(f"Applied {count} migration(s).")


if __name__ == "__main__":
    run()
