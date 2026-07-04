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
local_url = f"sqlite:///{PROJECT_ROOT}/data/kbo_dev.db"

oci_engine = create_engine(oci_url)
local_engine = create_engine(local_url)

oci_inspector = inspect(oci_engine)
local_inspector = inspect(local_engine)


def sync_table(table: str, oci_conn, local_conn, batch_size: int = 2000) -> int:
    if table not in set(oci_inspector.get_table_names()):
        print(f"{table}: NOT IN OCI")
        return -1
    if table not in set(local_inspector.get_table_names()):
        print(f"{table}: NOT IN LOCAL")
        return -1

    oci_cols = [c["name"] for c in oci_inspector.get_columns(table)]
    local_cols = [c["name"] for c in local_inspector.get_columns(table)]
    common_cols = [c for c in oci_cols if c in local_cols]

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
        vals = {}
        for i, c in enumerate(common_cols):
            v = row[i]
            from decimal import Decimal

            if isinstance(v, Decimal):
                v = float(v)
            elif isinstance(v, (list, dict)):
                v = json.dumps(v, ensure_ascii=False)
            elif isinstance(v, datetime) and v.tzinfo:
                v = v.replace(tzinfo=None)
            elif isinstance(v, (time, date)):
                v = v.isoformat()
            vals[c] = v
        batch.append(vals)
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
