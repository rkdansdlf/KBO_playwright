#!/usr/bin/env python3
"""Sync game_metadata 2021-2023 to OCI."""

import json
import logging
import os
import sys
from datetime import date, time, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

os.chdir(Path(__file__).resolve().parent.parent)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from sqlalchemy import create_engine, inspect, text

oci_url = os.environ.get("TARGET_DATABASE_URL") or os.environ.get("OCI_DB_URL")
local_url = "sqlite:////Users/mac/project/KBO_playwright/data/kbo_dev.db"

oci_engine = create_engine(oci_url)
local_engine = create_engine(local_url)
local_inspector = inspect(local_engine)
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


local_conn = local_engine.connect()
oci_trans = oci_engine.begin()
oci_conn = oci_trans.__enter__()

try:
    inserted = 0
    skipped = 0
    for year in ["2021", "2022", "2023"]:
        rows = local_conn.execute(text(f"SELECT * FROM game_metadata WHERE game_id LIKE '{year}%'")).fetchall()

        for row in rows:
            game_id = row[0]

            # Check if exists
            exists = oci_conn.execute(
                text("SELECT 1 FROM game_metadata WHERE game_id = :gid"),
                {"gid": game_id},
            ).fetchone()
            if exists:
                skipped += 1
                continue

            insert_cols = [c for c in local_meta_cols if c != "id"]
            col_str = ",".join(insert_cols)
            val_str = ",".join([f":{c}" for c in insert_cols])
            vals = {c: convert_val(row[local_meta_cols.index(c)], c) for c in insert_cols}

            try:
                oci_conn.execute(
                    text(f"INSERT INTO game_metadata ({col_str}) VALUES ({val_str})"),
                    vals,
                )
                inserted += 1
            except Exception as e:
                logger.warning("Skipping row: %s", e)
                skipped += 1

    print(f"game_metadata: inserted={inserted}, skipped={skipped}")
finally:
    oci_trans.__exit__(None, None, None)
    local_conn.close()

print("Done!")
