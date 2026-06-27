"""Verify kbo_seasons sync consistency between local SQLite and OCI.

Usage:
    python3 -m scripts.verification.verify_season_sync
    python3 -m scripts.verification.verify_season_sync --fix
    python3 -m scripts.verification.verify_season_sync --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from src.db.engine import create_engine_for_url, get_oci_url, get_source_db_url

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CORE_TYPES = {
    0: "정규시즌",
    1: "시범경기",
    2: "와일드카드",
    3: "준플레이오프",
    4: "플레이오프",
    5: "한국시리즈",
}


def _get_season_set(conn, label: str) -> set[tuple[int, int]]:
    """Fetch (season_year, league_type_code) set from a connection."""
    try:
        rows = conn.execute(text("SELECT season_year, league_type_code FROM kbo_seasons")).fetchall()
        return {(r[0], r[1]) for r in rows}
    except Exception as e:
        logger.warning("Failed to query kbo_seasons from %s: %s", label, e)
        return set()


def verify_seasons(sqlite_url: str, oci_url: str) -> dict:
    """Compare kbo_seasons between SQLite and OCI."""
    sqlite_engine = create_engine_for_url(sqlite_url, disable_sqlite_wal=True)
    oci_engine = create_engine_for_url(oci_url, disable_sqlite_wal=True)

    result: dict = {"status": "OK", "sqlite_missing_in_oci": [], "oci_missing_in_sqlite": []}

    try:
        with sqlite_engine.connect() as sq_conn, oci_engine.connect() as oci_conn:
            sqlite_set = _get_season_set(sq_conn, "SQLite")
            oci_set = _get_season_set(oci_conn, "OCI")

            missing_in_oci = sqlite_set - oci_set
            missing_in_sqlite = oci_set - sqlite_set

            result["sqlite_count"] = len(sqlite_set)
            result["oci_count"] = len(oci_set)
            result["sqlite_missing_in_oci"] = sorted(missing_in_oci)
            result["oci_missing_in_sqlite"] = sorted(missing_in_sqlite)

            if missing_in_oci or missing_in_sqlite:
                result["status"] = "MISMATCH"

            logger.info("SQLite kbo_seasons: %d rows", len(sqlite_set))
            logger.info("OCI kbo_seasons: %d rows", len(oci_set))
            if missing_in_oci:
                logger.warning("  Missing in OCI (%d): %s", len(missing_in_oci), list(missing_in_oci)[:10])
            if missing_in_sqlite:
                logger.warning("  Missing in SQLite (%d): %s", len(missing_in_sqlite), list(missing_in_sqlite)[:10])
            if not missing_in_oci and not missing_in_sqlite:
                logger.info("  ✅ kbo_seasons perfectly synced")
    finally:
        sqlite_engine.dispose()
        oci_engine.dispose()

    return result


def fix_missing_seasons(sqlite_url: str, oci_url: str) -> int:
    """Push missing kbo_seasons from SQLite to OCI."""
    sqlite_engine = create_engine_for_url(sqlite_url, disable_sqlite_wal=True)
    oci_engine = create_engine_for_url(oci_url, disable_sqlite_wal=True)
    pushed = 0

    try:
        with sqlite_engine.connect() as sq_conn, oci_engine.connect() as oci_conn:
            sqlite_rows = sq_conn.execute(
                text("SELECT season_id, season_year, league_type_code, league_type_name FROM kbo_seasons")
            ).fetchall()
            oci_set = {
                (r[0], r[1])
                for r in oci_conn.execute(text("SELECT season_year, league_type_code FROM kbo_seasons")).fetchall()
            }

            for row in sqlite_rows:
                year, code = row[1], row[2]
                if (year, code) in oci_set:
                    continue
                sid = row[0] if row[0] else year * 100 + code
                name = row[3] if row[3] else CORE_TYPES.get(code, f"Type {code}")
                logger.info("  Pushing missing season: year=%s code=%s name=%s", year, code, name)
                oci_conn.execute(
                    text(
                        """
                        INSERT INTO kbo_seasons
                            (season_id, season_year, league_type_code, league_type_name,
                             created_at, updated_at)
                        VALUES (:sid, :year, :code, :name, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING
                        """
                    ),
                    {"sid": sid, "year": year, "code": code, "name": name},
                )
                pushed += 1
            oci_conn.commit()
            logger.info("  ✅ Pushed %d missing season rows to OCI", pushed)
    finally:
        sqlite_engine.dispose()
        oci_engine.dispose()

    return pushed


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify kbo_seasons sync consistency")
    parser.add_argument("--fix", action="store_true", help="Push missing seasons to OCI")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()

    sqlite_url = get_source_db_url()
    oci_url = get_oci_url()

    if not oci_url:
        logger.error("OCI_DB_URL not configured")
        return 1

    if args.fix:
        pushed = fix_missing_seasons(sqlite_url, oci_url)
        result = {"status": "FIXED", "pushed": pushed}
    else:
        result = verify_seasons(sqlite_url, oci_url)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        status = result.get("status", "UNKNOWN")
        if status == "OK":
            logger.info("✅ kbo_seasons sync OK")
        elif status == "MISMATCH":
            logger.warning("⚠️ kbo_seasons sync mismatch detected")
        elif status == "FIXED":
            logger.info("🔧 Fixed %d missing seasons", result.get("pushed", 0))

    return 0 if result.get("status") == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
