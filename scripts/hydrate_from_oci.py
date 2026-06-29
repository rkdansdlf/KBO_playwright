#!/usr/bin/env python3
"""Hydrate local DB from OCI for core tables."""

import json
import os
import sys
from datetime import date, time, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from sqlalchemy import create_engine, inspect, text

oci_url = os.environ.get("TARGET_DATABASE_URL") or os.environ.get("OCI_DB_URL")
local_url = f"sqlite:///{PROJECT_ROOT}/data/kbo_dev.db"

oci_engine = create_engine(oci_url)
local_engine = create_engine(local_url)

tables = [
    "teams",
    "kbo_seasons",
    "team_franchises",
    "team_code_map",
    "team_history",
    "player_season_batting",
    "player_season_pitching",
    "player_season_fielding",
    "player_season_baserunning",
]

oci_inspector = inspect(oci_engine)
local_inspector = inspect(local_engine)

oci_tables = set(oci_inspector.get_table_names())
local_tables = set(local_inspector.get_table_names())


def hydrate_table(table: str, oci_conn, local_conn) -> int:
    if table not in oci_tables:
        print(f"{table}: NOT IN OCI")
        return -1
    if table not in local_tables:
        print(f"{table}: NOT IN LOCAL")
        return -1

    oci_cols = [c["name"] for c in oci_inspector.get_columns(table)]
    local_cols = [c["name"] for c in local_inspector.get_columns(table)]
    common_cols = [c for c in oci_cols if c in local_cols]

    col_str = ",".join(common_cols)
    val_str = ",".join([f":{c}" for c in common_cols])

    rows = oci_conn.execute(text(f"SELECT {col_str} FROM {table}")).fetchall()
    if not rows:
        print(f"{table}: no data")
        return 0

    local_conn.execute(text(f"DELETE FROM {table}"))
    inserted = 0
    for row in rows:
        vals = {}
        for i, c in enumerate(common_cols):
            v = row[i]
            if isinstance(v, (list, dict)):
                v = json.dumps(v, ensure_ascii=False)
            elif isinstance(v, datetime) and v.tzinfo is not None:
                v = v.replace(tzinfo=None)
            elif isinstance(v, (time, date)):
                v = v.isoformat()
            elif v is None and c in ("created_at", "updated_at"):
                v = datetime.utcnow()
            vals[c] = v
        local_conn.execute(
            text(f"INSERT INTO {table} ({col_str}) VALUES ({val_str})"),
            vals,
        )
        inserted += 1

    print(f"{table}: {inserted} rows")
    return inserted


with oci_engine.connect() as oci_conn, local_engine.begin() as local_conn:
    for table in tables:
        hydrate_table(table, oci_conn, local_conn)

print("\nDone!")
