#!/usr/bin/env python3
"""Sync new games from local to OCI."""

import json
import logging
import os
import sys
from datetime import date, time, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

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

local_inspector = inspect(local_engine)
local_cols = [c["name"] for c in local_inspector.get_columns("game")]
local_meta_cols = [c["name"] for c in local_inspector.get_columns("game_metadata")]

BOOL_COLUMNS = {"is_primary", "is_active", "is_foreign_player", "is_starter", "is_dome"}


def convert_val(v, col_name=None):
    if isinstance(v, (list, dict)):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, datetime) and v.tzinfo:
        return v.replace(tzinfo=None)
    if isinstance(v, (time, date)):
        return v.isoformat()
    if col_name in BOOL_COLUMNS and isinstance(v, int):
        return bool(v)
    return v


def sync_table(table_name, cols, year_prefix):
    local_conn = local_engine.connect()
    oci_engine.dispose()
    oci_engine_new = create_engine(oci_url)

    with oci_engine_new.begin() as oci_conn:
        # Get existing OCI IDs
        oci_ids = oci_conn.execute(
            text(
                f"SELECT game_id FROM {table_name} WHERE game_id LIKE '{year_prefix}%'",
            ),
        ).fetchall()
        oci_set = {g[0] for g in oci_ids}

        # Get local rows
        local_rows = local_conn.execute(
            text(
                f"SELECT * FROM {table_name} WHERE game_id LIKE '{year_prefix}%'",
            ),
        ).fetchall()

        # Remove 'id' from columns (let OCI generate it)
        insert_cols = [c for c in cols if c != "id"]
        col_str = ",".join(insert_cols)
        val_str = ",".join([f":{c}" for c in insert_cols])

        inserted = 0
        skipped = 0
        for row in local_rows:
            game_id = row[0] if cols[0] == "game_id" else row[1]
            if game_id in oci_set:
                skipped += 1
                continue

            vals = {c: convert_val(row[cols.index(c)], c) for c in insert_cols}
            try:
                oci_conn.execute(text(f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str})"), vals)
                inserted += 1
            except Exception as e:
                logger.warning("Skipping row: %s", e)
                skipped += 1

    local_conn.close()
    print(f"{table_name} {year_prefix}: inserted={inserted}, skipped={skipped}")


# Sync games 2021-2023
for year in ["2021", "2022", "2023"]:
    sync_table("game", local_cols, year)

# Sync game_metadata 2021-2023
for year in ["2021", "2022", "2023"]:
    sync_table("game_metadata", local_meta_cols, year)

print("\nDone!")
