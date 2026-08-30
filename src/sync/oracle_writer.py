"""High-performance Oracle Bulk Writer using MERGE INTO and executemany."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    import oracledb
except ImportError:
    oracledb = None  # type: ignore[assignment]
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
BULK_MERGE_ROWS = 500

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

    def get_column_names(self, table: str) -> dict[str, str]:
        """Return dict of normalized column names to their Oracle spelling."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT column_name FROM user_tab_columns WHERE table_name = :t ORDER BY column_id"),
                {"t": table.upper()},
            ).fetchall()
        return {row[0].upper(): row[0] for row in rows}

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
        try:
            with self.engine.connect() as conn:
                cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{table.upper()}"')).scalar()  # noqa: S608
                return int(cnt or 0)
        except (SQLAlchemyError, oracledb.Error):
            return 0

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

    def build_merge_sql(  # noqa: PLR0913
        self,
        table: str,
        columns: list[str],
        pk_columns: list[str],
        *,
        row_count: int = 1,
        column_types: dict[str, str] | None = None,
        column_names: dict[str, str] | None = None,
    ) -> str:
        """Build an optimized Oracle MERGE INTO SQL statement for bulk upsert."""
        if row_count < 1:
            message = "row_count must be positive"
            raise ValueError(message)
        table_upper = table.upper()
        cols_upper = [column_names.get(c.upper(), c.upper()) if column_names else c.upper() for c in columns]
        pks_upper = [column_names.get(p.upper(), p.upper()) if column_names else p.upper() for p in pk_columns]

        using_rows = []
        for row_index in range(row_count):
            offset = row_index * len(cols_upper)
            bind_columns = [
                (
                    self._bulk_bind_expression(
                        offset + column_index,
                        column,
                        column_types,
                        row_count,
                    ),
                    column,
                )
                for column_index, column in enumerate(cols_upper)
            ]
            using_cols = ", ".join(f'{expression} AS "{column}"' for expression, column in bind_columns)
            using_rows.append(f"SELECT {using_cols} FROM DUAL")  # noqa: S608
        using_clause = f"({' UNION ALL '.join(using_rows)})"

        on_clause = " AND ".join(
            f'(t."{pk}" = s."{pk}" OR (t."{pk}" IS NULL AND s."{pk}" IS NULL))' for pk in pks_upper
        )

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

    @staticmethod
    def _bulk_bind_expression(
        bind_index: int,
        column: str,
        column_types: dict[str, str] | None,
        row_count: int,
    ) -> str:
        """Return a stable Oracle bind expression for multi-row MERGE inputs."""
        bind = f":c{bind_index}"
        if row_count == 1 or not column_types:
            return bind

        data_type = column_types.get(column.upper(), "").upper()
        cast_types = {
            "CLOB": "VARCHAR2(4000)",
            "VARCHAR2": "VARCHAR2(4000)",
            "CHAR": "VARCHAR2(4000)",
            "NCHAR": "VARCHAR2(4000)",
            "NVARCHAR2": "VARCHAR2(4000)",
            "NUMBER": "NUMBER",
            "FLOAT": "NUMBER",
            "BINARY_FLOAT": "NUMBER",
            "BINARY_DOUBLE": "NUMBER",
            "INTEGER": "NUMBER",
            "DATE": "DATE",
        }
        cast_type = data_type if data_type.startswith("TIMESTAMP") else cast_types.get(data_type)
        return f"CAST({bind} AS {cast_type})" if cast_type else bind

    def build_insert_sql(self, table: str, columns: list[str], column_names: dict[str, str] | None = None) -> str:
        """Build standard INSERT INTO SQL."""
        table_upper = table.upper()
        cols_upper = [f'"{column_names.get(c.upper(), c.upper()) if column_names else c.upper()}"' for c in columns]
        binds = [f":c{i}" for i in range(len(columns))]
        return f'INSERT INTO "{table_upper}" ({", ".join(cols_upper)}) VALUES ({", ".join(binds)})'  # noqa: S608

    def _execute_insert_batch(
        self,
        cursor: oracledb.Cursor,
        sql: str,
        payloads: list[dict[str, object]],
        table: str,
    ) -> tuple[int, int]:
        """Execute an array INSERT and fall back to individual rows on error."""
        try:
            cursor.executemany(sql, payloads, batcherrors=True)
            batch_errors = cursor.getbatcherrors()
            if batch_errors:
                logger.warning(
                    "[%s] Bulk INSERT skipped %d row errors; first error: %s",
                    table,
                    len(batch_errors),
                    batch_errors[0].message.splitlines()[0],
                )
                return len(payloads) - len(batch_errors), len(batch_errors)
        except (oracledb.Error, RuntimeError) as exc:
            logger.warning("[%s] Bulk INSERT failed (%s), falling back to individual rows", table, exc)
            self.conn.rollback()
            synced = 0
            errors = 0
            for payload in payloads:
                try:
                    cursor.execute(sql, payload)
                    synced += 1
                except (oracledb.Error, RuntimeError) as row_err:
                    errors += 1
                    logger.debug("[%s] Row execution failed: %s (payload: %s)", table, row_err, payload)
            return synced, errors
        return len(payloads), 0

    def execute_batch(  # noqa: PLR0913
        self,
        sql: str,
        payloads: list[dict[str, object]],
        table: str,
        columns: list[str],
        pk_columns: list[str],
        column_types: dict[str, str],
        column_names: dict[str, str],
        *,
        insert_only: bool = False,
    ) -> tuple[int, int]:
        """Execute bulk MERGE statements with row-by-row fallback."""
        if not payloads:
            return 0, 0

        cursor = self.conn.cursor()
        cursor.arraysize = self.arraysize
        synced_total = 0
        error_total = 0
        try:
            if insert_only:
                return self._execute_insert_batch(cursor, sql, payloads, table)

            for offset in range(0, len(payloads), BULK_MERGE_ROWS):
                chunk = payloads[offset : offset + BULK_MERGE_ROWS]
                try:
                    if len(chunk) == 1:
                        cursor.execute(sql, chunk[0])
                    else:
                        bulk_sql = self.build_merge_sql(
                            table,
                            columns,
                            pk_columns,
                            row_count=len(chunk),
                            column_types=column_types,
                            column_names=column_names,
                        )
                        flattened = {
                            f"c{row_index * len(columns) + column_index}": payload[f"c{column_index}"]
                            for row_index, payload in enumerate(chunk)
                            for column_index in range(len(columns))
                        }
                        cursor.execute(bulk_sql, flattened)
                    synced_total += len(chunk)
                except (oracledb.Error, RuntimeError) as exc:
                    logger.warning("[%s] Bulk MERGE failed (%s), falling back to individual rows", table, exc)
                    self.conn.rollback()
                    for payload in chunk:
                        try:
                            cursor.execute(sql, payload)
                            synced_total += 1
                        except (oracledb.Error, RuntimeError) as row_err:
                            error_total += 1
                            logger.warning("[%s] Row execution failed: %s (payload: %s)", table, row_err, payload)
            return synced_total, error_total
        finally:
            cursor.close()

    def commit(self) -> None:
        """Commit active transaction to Oracle DB."""
        self.conn.commit()

    def align_id_generator(self, table: str) -> None:
        """Advance the NULL-ID trigger sequence past explicit source IDs."""
        with self.engine.begin() as connection:
            trigger_name = connection.execute(
                text(
                    "SELECT trigger_name FROM user_triggers "
                    "WHERE table_name = :table_name AND trigger_name LIKE 'KBO_AI_TR_%'"
                ),
                {"table_name": table.upper()},
            ).scalar()
            if not trigger_name:
                return

            suffix_match = re.search(r"KBO_AI_TR_(\d+)$", str(trigger_name))
            if suffix_match is None:
                return
            sequence_name = f"KBO_AI_SQ_{suffix_match.group(1)}"
            max_id = connection.execute(
                text(f'SELECT NVL(MAX("ID"), 0) FROM "{table.upper()}"'),  # noqa: S608
            ).scalar()
            if max_id is None:
                return

            current_value = connection.exec_driver_sql(f'SELECT "{sequence_name}".NEXTVAL FROM dual').scalar()  # noqa: S608
            if current_value is None or int(current_value) > int(max_id):
                return
            increment = int(max_id) + 1 - int(current_value)
            connection.exec_driver_sql(f'ALTER SEQUENCE "{sequence_name}" INCREMENT BY {increment}')
            connection.exec_driver_sql(f'SELECT "{sequence_name}".NEXTVAL FROM dual').scalar()  # noqa: S608
            connection.exec_driver_sql(f'ALTER SEQUENCE "{sequence_name}" INCREMENT BY 1')
            logger.debug("Aligned %s past explicit source ID %s", sequence_name, max_id)

    def rollback(self) -> None:
        """Rollback active transaction in Oracle DB."""
        self.conn.rollback()
