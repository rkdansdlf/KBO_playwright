#!/usr/bin/env python3
"""Sync player_game and game_stats tables from OCI to local."""

import os
import sys
import json
from datetime import date, time, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from sqlalchemy import create_engine, inspect, text

oci_url = os.environ.get("TARGET_DATABASE_URL") or os.environ.get("OCI_DB_URL")
assert oci_url is not None, "OCI_DB_URL or TARGET_DATABASE_URL must be set"
local_url = f"sqlite:///{PROJECT_ROOT}/data/kbo_dev.db"

oci_engine = create_engine(oci_url)
local_engine = create_engine(local_url)

oci_inspector = inspect(oci_engine)
local_inspector = inspect(local_engine)


def _serialize_value(v):
    from decimal import Decimal

    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, datetime) and v.tzinfo:
        return v.replace(tzinfo=None)
    if isinstance(v, (time, date)):
        return v.isoformat()
    return v


def _validate_table(table: str) -> list[str] | None:
    if table not in set(oci_inspector.get_table_names()):
        print(f"{table}: NOT IN OCI")
        return None
    if table not in set(local_inspector.get_table_names()):
        print(f"{table}: NOT IN LOCAL")
        return None
    oci_cols = [c["name"] for c in oci_inspector.get_columns(table)]
    local_cols = [c["name"] for c in local_inspector.get_columns(table)]
    return [c for c in oci_cols if c in local_cols]


def sync_table(table: str, oci_conn, local_conn, batch_size: int = 2000) -> int:
    common_cols = _validate_table(table)
    if common_cols is None:
        return -1
    rows = oci_conn.execute(text(f"SELECT {','.join(common_cols)} FROM {table}")).fetchall()
    if not rows:
        print(f"{table}: no data")
        return 0
    local_conn.execute(text(f"DELETE FROM {table}"))
    col_str = ",".join(common_cols)
    val_str = ",".join([f":{c}" for c in common_cols])
    inserted = 0
    batch = []
    for row in rows:
        batch.append({c: _serialize_value(row[i]) for i, c in enumerate(common_cols)})
        if len(batch) >= batch_size:
            local_conn.execute(text(f"INSERT INTO {table} ({col_str}) VALUES ({val_str})"), batch)
            inserted += len(batch)
            batch = []
    if batch:
        local_conn.execute(text(f"INSERT INTO {table} ({col_str}) VALUES ({val_str})"), batch)
        inserted += len(batch)
    print(f"{table}: {inserted:,} rows synced")
    return inserted


tables = [
    "player_game_batting",
    "player_game_pitching",
    "game_batting_stats",
    "game_pitching_stats",
    "game_inning_scores",
    "game_lineups",
]

with oci_engine.connect() as oci_conn, local_engine.begin() as local_conn:
    for table in tables:
        sync_table(table, oci_conn, local_conn)

print("\nDone!")
