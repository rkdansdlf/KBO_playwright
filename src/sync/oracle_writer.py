"""High-performance Oracle Bulk Writer using MERGE INTO and executemany."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import oracledb
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import create_engine_for_url

_KST = ZoneInfo("Asia/Seoul")
logger = logging.getLogger(__name__)

_SHA256 = hashlib.sha256


def _detail_hash(v: object) -> object:
    if v is None:
        return None
    return _SHA256(str(v).encode("utf-8")).hexdigest()


TABLE_OVERRIDE: dict[str, object] = {
    "detail_text_hash": _detail_hash,
}

LEVEL_NORMALIZE = {"1": "KBO1", "1군": "KBO1"}

TEAM_CODE_MAP = {
    "BE": "HH",
    "HT": "KIA",
    "MBC": "LG",
    "NX": "KH",
    "OB": "DB",
    "SK": "SSG",
    "WO": "KH",
}

TABLE_COL_OVERRIDE: dict[tuple[str, str], object] = {
    ("player_season_batting", "level"): lambda v: LEVEL_NORMALIZE.get(str(v), v),
    ("player_season_pitching", "level"): lambda v: LEVEL_NORMALIZE.get(str(v), v),
    ("player_season_batting", "team_code"): lambda v: TEAM_CODE_MAP.get(str(v), v),
    ("player_season_pitching", "team_code"): lambda v: TEAM_CODE_MAP.get(str(v), v),
}


def _parse_datetime_string(val_str: str, oci_type: str) -> object:
    v = val_str.replace("T", " ").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(v, fmt).replace(tzinfo=_KST)
        except ValueError:
            continue
        else:
            return dt if "WITH TIME ZONE" in oci_type else dt.replace(tzinfo=None)

    try:
        dt = datetime.strptime(v, "%H:%M:%S.%f" if "." in v else "%H:%M:%S").replace(
            year=2000, month=1, day=1, tzinfo=_KST
        )
    except ValueError:
        return val_str
    else:
        return dt if "WITH TIME ZONE" in oci_type else dt.replace(tzinfo=None)


class OracleWriter:
    """Handles high-performance batch writes to Oracle Autonomous DB."""

    def __init__(
        self,
        oci_url: str,
        tns_admin: str,
        wallet_password: str | None = None,
        arraysize: int = 5000,
        prefetchrows: int = 5000,
    ) -> None:
        """Initialize Oracle database connection and engine."""
        self.oci_url = oci_url
        self.tns_admin = tns_admin
        self.wallet_password = wallet_password
        self.arraysize = arraysize
        self.prefetchrows = prefetchrows

        self.engine = create_engine_for_url(
            oci_url,
            tns_admin=tns_admin,
            wallet_password=wallet_password,
        )
        self.conn = self.engine.raw_connection()
        self.conn.autocommit = False

    def close(self) -> None:
        """Close connection and dispose engine."""
        try:
            self.conn.close()
        except (oracledb.Error, OSError) as e:
            logger.debug("Error closing oracledb connection: %s", e)
        try:
            self.engine.dispose()
        except (SQLAlchemyError, OSError) as e:
            logger.debug("Error disposing sqlalchemy engine: %s", e)

    def get_columns(self, table: str) -> dict[str, str]:
        """Return dict of column_name.upper() -> data_type."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT column_name, data_type FROM user_tab_columns WHERE table_name = :t ORDER BY column_id"),
                {"t": table.upper()},
            ).fetchall()
        return {r[0].upper(): r[1] for r in rows}

    def get_char_sizes(self, table: str) -> dict[str, int]:
        """Return dict of column_name.upper() -> char_length for string columns."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT column_name, char_length FROM user_tab_columns "
                    "WHERE table_name = :t AND data_type IN ('VARCHAR2','CHAR')"
                ),
                {"t": table.upper()},
            ).fetchall()
        return {r[0].upper(): r[1] for r in rows}

    def get_pk_columns(self, table: str) -> list[str]:
        """Return list of primary key columns in uppercase."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT cc.column_name FROM user_constraints c "
                    "JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name "
                    "WHERE c.constraint_type = 'P' AND c.table_name = :t "
                    "ORDER BY cc.position"
                ),
                {"t": table.upper()},
            ).fetchall()
        return [r[0].upper() for r in rows]

    def set_table_triggers(self, table: str, *, enable: bool) -> int:
        """Enable or disable triggers for the given table."""
        action = "ENABLE" if enable else "DISABLE"
        with self.engine.begin() as conn:
            rows = conn.execute(
                text("SELECT trigger_name FROM user_triggers WHERE table_name = :t AND status = :s"),
                {"t": table.upper(), "s": "DISABLED" if enable else "ENABLED"},
            ).fetchall()
            for (name,) in rows:
                conn.execute(text(f"ALTER TRIGGER {name} {action}"))
            return len(rows)

    def truncate_table(self, table: str) -> None:
        """Truncate the specified table in Oracle."""
        with self.engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE "{table.upper()}"'))

    def count_table(self, table: str) -> int:
        """Count total rows in the specified table."""
        with self.engine.connect() as conn:
            cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{table.upper()}"')).scalar()  # noqa: S608
            return int(cnt or 0)

    def convert_value(self, value: object, oci_type: str, char_limit: int | None = None) -> object:
        """Convert a SQLite source value to an Oracle column type."""
        if value is None or value == "":
            return None

        if oci_type.startswith(("DATE", "TIMESTAMP")):
            if isinstance(value, datetime):
                return value.replace(tzinfo=_KST) if "WITH TIME ZONE" in oci_type else value
            if isinstance(value, str):
                return _parse_datetime_string(value, oci_type)

        if char_limit is not None and isinstance(value, str) and len(value) > char_limit:
            message = f"Oracle character value exceeds limit {char_limit}: {len(value)}"
            raise ValueError(message)

        return value

    def build_merge_sql(self, table: str, columns: list[str], pk_columns: list[str]) -> str:
        """Build an optimized Oracle MERGE INTO SQL statement for bulk upsert."""
        table_upper = table.upper()
        cols_upper = [c.upper() for c in columns]
        pks_upper = [p.upper() for p in pk_columns]

        using_cols = ", ".join(f':c{i} AS "{c}"' for i, c in enumerate(cols_upper))
        using_clause = f"(SELECT {using_cols} FROM DUAL)"  # noqa: S608

        on_clause = " AND ".join(f't."{pk}" = s."{pk}"' for pk in pks_upper)

        update_cols = [c for c in cols_upper if c not in pks_upper]
        matched_clause = ""
        if update_cols:
            update_set = ", ".join(f't."{c}" = s."{c}"' for c in update_cols)
            matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"

        insert_cols = ", ".join(f'"{c}"' for c in cols_upper)
        insert_vals = ", ".join(f's."{c}"' for c in cols_upper)
        not_matched_clause = f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"  # noqa: S608

        parts = [f'MERGE INTO "{table_upper}" t USING {using_clause} s ON ({on_clause})']
        if matched_clause:
            parts.append(matched_clause)
        parts.append(not_matched_clause)

        return "\n".join(parts)

    def build_insert_sql(self, table: str, columns: list[str]) -> str:
        """Build standard INSERT INTO SQL."""
        table_upper = table.upper()
        cols_upper = [f'"{c.upper()}"' for c in columns]
        binds = [f":c{i}" for i in range(len(columns))]
        return f'INSERT INTO "{table_upper}" ({", ".join(cols_upper)}) VALUES ({", ".join(binds)})'  # noqa: S608

    def execute_batch(
        self,
        sql: str,
        payloads: list[dict[str, object]],
        table: str,
    ) -> tuple[int, int]:
        """Execute executemany with row-by-row fallback. Returns (success_count, error_count)."""
        if not payloads:
            return 0, 0

        cursor = self.conn.cursor()
        cursor.arraysize = self.arraysize
        try:
            cursor.executemany(sql, payloads)
        except (oracledb.Error, RuntimeError) as e:
            logger.warning("[%s] Batch executemany failed (%s), falling back to individual rows", table, e)
            success = 0
            errors = 0
            for p in payloads:
                try:
                    cursor.execute(sql, p)
                    success += 1
                except (oracledb.Error, RuntimeError) as row_err:
                    errors += 1
                    logger.debug("[%s] Row execution failed: %s (payload: %s)", table, row_err, p)
            return success, errors
        else:
            return len(payloads), 0
        finally:
            cursor.close()

    def commit(self) -> None:
        """Commit active transaction to Oracle DB."""
        self.conn.commit()

    def rollback(self) -> None:
        """Rollback active transaction in Oracle DB."""
        self.conn.rollback()
