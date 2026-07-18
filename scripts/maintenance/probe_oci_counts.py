"""Read-only probe: compare local vs Postgres(TARGET) row counts for a few years."""

from __future__ import annotations

import os
import signal
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from dotenv import load_dotenv

load_dotenv()

TABLES = ["game_lineups", "game_batting_stats", "player_game_batting", "game_events"]
YEARS = ["2009", "2010", "2021", "2024"]


def counts(engine, label):
    if engine is None:
        print(f"[{label}] SKIP (no url)", flush=True)
        return
    try:
        with engine.connect() as conn:
            for tbl in TABLES:
                try:
                    per = []
                    for y in YEARS:
                        n = conn.execute(
                            text(f"SELECT COUNT(*) FROM {tbl} WHERE CAST(game_id AS TEXT) LIKE :p"),
                            {"p": f"{y}%"},
                        ).scalar()
                        per.append(f"{y}:{n}")
                    print(f"  [{label}] {tbl}: " + " ".join(per), flush=True)
                except SQLAlchemyError as e:
                    print(f"  [{label}] {tbl}: ERR {e}", flush=True)
    except SQLAlchemyError as e:
        print(f"[{label}] CONNECT FAILED: {type(e).__name__}: {e}", flush=True)


def make_engine(url):
    if not url:
        return None
    if url.startswith("postgresql"):
        url = url + ("&" if "?" in url else "?") + "connect_timeout=10"
    return sqlalchemy.create_engine(url, pool_pre_ping=True)


print("=== LOCAL ===", flush=True)
counts(make_engine(os.getenv("DATABASE_URL")), "local")

print("=== POSTGRES (TARGET_DATABASE_URL) ===", flush=True)
signal.alarm(45)
counts(make_engine(os.getenv("TARGET_DATABASE_URL")), "pg")
signal.alarm(0)
print("DONE", flush=True)
