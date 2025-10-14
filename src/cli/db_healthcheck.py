"""
Database healthcheck CLI.

Prints current DATABASE_URL, dialect, connectivity, and basic table stats.

Usage:
  python -m src.cli.db_healthcheck
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import List

from sqlalchemy import text, inspect

from src.db.engine import Engine, DATABASE_URL
from src.utils.safe_print import safe_print as print


def main(argv: List[str] | None = None) -> None:
    url = os.getenv("DATABASE_URL", DATABASE_URL)
    dialect = Engine.url.get_backend_name()

    print("\n=== DB Healthcheck ===")
    print(f"URL: {url}")
    print(f"Dialect: {dialect}")

    # Check connectivity
    try:
        with Engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connectivity: OK")
    except Exception as e:
        print(f"Connectivity: FAILED -> {e}")
        return

    # Introspect tables
    try:
        insp = inspect(Engine)
        tables = insp.get_table_names()
        print(f"Tables: {len(tables)} found")
        if tables:
            # Show up to 10 table names
            for t in tables[:10]:
                print(f"  - {t}")
    except Exception as e:
        print(f"Introspection failed: {e}")

    # Basic counts for common tables if they exist
    for table in [
        "players",
        "teams",
        "game_schedules",
        "player_season_batting",
        "player_game_batting",
        "player_game_pitching",
        "game_play_by_play",
    ]:
        try:
            with Engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar_one()
                print(f"{table}: {count}")
        except Exception:
            # Table may not exist; skip quietly
            continue

    print("\nHealthcheck complete.\n")


if __name__ == "__main__":
    main()

