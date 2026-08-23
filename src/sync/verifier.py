"""Data consistency and row count verifier between SQLite and Oracle Autonomous Database."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.sync.dto import ConsistencyCheckItem, SyncVerificationReport
from src.sync.table_dag import TABLE_REGISTRY, TableMeta

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")


class SyncConsistencyVerifier:
    """Audits and compares row counts and state between SQLite and Oracle ADB."""

    def __init__(self, sqlite_conn: sqlite3.Connection, oracle_engine: Engine | None = None) -> None:
        """Initialize the consistency verifier with source connection and target engine."""
        self.sqlite_conn = sqlite_conn
        self.oracle_engine = oracle_engine

    def get_sqlite_row_count(self, table_name: str) -> int:
        """Get the total row count for a table in SQLite."""
        try:
            cursor = self.sqlite_conn.execute(f"SELECT COUNT(1) FROM {table_name}")  # noqa: S608
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        except (sqlite3.Error, ValueError, TypeError):
            return 0

    def get_oracle_row_count(self, table_name: str) -> int:
        """Get the total row count for a table in Oracle ADB."""
        if not self.oracle_engine:
            return 0
        try:
            with self.oracle_engine.connect() as conn:
                res = conn.execute(text(f"SELECT COUNT(1) FROM {table_name}"))  # noqa: S608
                row = res.fetchone()
                return int(row[0]) if row else 0
        except (SQLAlchemyError, ValueError, TypeError):
            return 0

    def verify_table(self, table_meta: TableMeta) -> ConsistencyCheckItem:
        """Verify row count consistency for a single table."""
        t_name = table_meta.name
        try:
            src_count = self.get_sqlite_row_count(t_name)
            tgt_count = self.get_oracle_row_count(t_name)
            diff = src_count - tgt_count

            if src_count == tgt_count:
                status = "MATCH"
            elif tgt_count == 0 and src_count > 0:
                status = "OCI_EMPTY"
            elif src_count == 0 and tgt_count > 0:
                status = "SQLITE_EMPTY"
            else:
                status = "MISMATCH"

            return ConsistencyCheckItem(
                table_name=t_name,
                level=table_meta.level,
                sqlite_count=src_count,
                oci_count=tgt_count,
                diff=diff,
                status=status,
            )
        except (sqlite3.Error, SQLAlchemyError, ValueError, TypeError) as exc:
            logger.exception("Error verifying table %s", t_name)
            return ConsistencyCheckItem(
                table_name=t_name,
                level=table_meta.level,
                sqlite_count=0,
                oci_count=0,
                diff=0,
                status="ERROR",
                error_message=str(exc),
            )

    def verify_all(self, tables: list[TableMeta] | None = None) -> SyncVerificationReport:
        """Verify all tables in the DAG and return an aggregated report."""
        target_tables = tables or TABLE_REGISTRY
        items: list[ConsistencyCheckItem] = []
        matching = 0
        mismatched = 0
        errors = 0

        for t in target_tables:
            item = self.verify_table(t)
            items.append(item)
            if item.status == "MATCH":
                matching += 1
            elif item.status == "ERROR":
                errors += 1
            else:
                mismatched += 1

        overall_status = "PASS"
        if errors > 0:
            overall_status = "FAIL"
        elif mismatched > 0:
            overall_status = "WARN"

        now_str = datetime.now(_KST).isoformat()
        return SyncVerificationReport(
            timestamp=now_str,
            overall_status=overall_status,
            tables_checked=len(items),
            matching_tables=matching,
            mismatched_tables=mismatched,
            error_tables=errors,
            details=items,
        )
