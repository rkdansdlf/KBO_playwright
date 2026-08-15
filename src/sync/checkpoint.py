"""Checkpoint manager for tracking SQLite to Oracle ADB load progress."""

from __future__ import annotations

import contextlib
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    import sqlite3

_KST = ZoneInfo("Asia/Seoul")

CHECKPOINT_TABLE = "_sync_checkpoints"


@dataclass
class SyncCheckpoint:
    """Represents a sync checkpoint entry for a specific table."""

    table_name: str
    last_synced_at: datetime | None
    last_rowid: int | None
    last_pk_val: str | None
    rows_synced: int
    last_status: str
    updated_at: datetime


class CheckpointManager:
    """Manage initial-load checkpoints in the SQLite source database."""

    def __init__(self, conn: sqlite3.Connection, *, initialize: bool = True) -> None:
        """Initialize checkpoint access for a SQLite connection.

        Args:
            conn: SQLite connection used for checkpoint storage.
            initialize: Create the checkpoint table when true. Read-only sync
                previews leave a source database unchanged when false.

        """
        self.conn = conn
        self._lock = threading.Lock()
        self._initialized = False
        if initialize:
            self._init_table()
        else:
            self._initialized = self._table_exists()

    def _table_exists(self) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (CHECKPOINT_TABLE,),
        ).fetchone()
        return row is not None

    def _init_table(self) -> None:
        with self._lock, self.conn:
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{CHECKPOINT_TABLE}" (
                    table_name TEXT PRIMARY KEY,
                    last_synced_at TEXT,
                    last_rowid INTEGER,
                    last_pk_val TEXT,
                    rows_synced INTEGER DEFAULT 0,
                    last_status TEXT DEFAULT 'PENDING',
                    updated_at TEXT NOT NULL
                )
            """)
        self._initialized = True

    def get_checkpoint(self, table_name: str) -> SyncCheckpoint | None:
        """Fetch the latest sync checkpoint for a given table."""
        if not self._initialized:
            return None
        with self._lock:
            cur = self.conn.execute(
                f"SELECT table_name, last_synced_at, last_rowid, last_pk_val, rows_synced, last_status, updated_at "  # noqa: S608
                f'FROM "{CHECKPOINT_TABLE}" WHERE table_name = ?',
                (table_name,),
            )
            row = cur.fetchone()
        if not row:
            return None

        last_synced_at = None
        if row[1]:
            with contextlib.suppress(ValueError):
                last_synced_at = datetime.fromisoformat(row[1])

        updated_at = datetime.now(_KST)
        if row[6]:
            with contextlib.suppress(ValueError):
                updated_at = datetime.fromisoformat(row[6])

        return SyncCheckpoint(
            table_name=row[0],
            last_synced_at=last_synced_at,
            last_rowid=row[2],
            last_pk_val=row[3],
            rows_synced=row[4] or 0,
            last_status=row[5] or "UNKNOWN",
            updated_at=updated_at,
        )

    def record_success(
        self,
        table_name: str,
        synced_at: datetime,
        rows_synced: int,
        last_rowid: int | None = None,
        last_pk_val: str | None = None,
    ) -> None:
        """Record a successful sync event into checkpoint table."""
        now_str = datetime.now(_KST).isoformat()
        synced_str = synced_at.isoformat()
        with self._lock, self.conn:
            self.conn.execute(
                f"""
                INSERT INTO "{CHECKPOINT_TABLE}" (
                    table_name, last_synced_at, last_rowid, last_pk_val, rows_synced, last_status, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'SUCCESS', ?)
                ON CONFLICT(table_name) DO UPDATE SET
                    last_synced_at = excluded.last_synced_at,
                    last_rowid = COALESCE(excluded.last_rowid, "{CHECKPOINT_TABLE}".last_rowid),
                    last_pk_val = COALESCE(excluded.last_pk_val, "{CHECKPOINT_TABLE}".last_pk_val),
                    rows_synced = "{CHECKPOINT_TABLE}".rows_synced + excluded.rows_synced,
                    last_status = 'SUCCESS',
                    updated_at = excluded.updated_at
                """,  # noqa: S608
                (table_name, synced_str, last_rowid, last_pk_val, rows_synced, now_str),
            )

    def record_failure(self, table_name: str, error_msg: str | None = None) -> None:
        """Record a failed sync event into checkpoint table."""
        now_str = datetime.now(_KST).isoformat()
        status = f"FAILED: {error_msg[:100]}" if error_msg else "FAILED"
        with self._lock, self.conn:
            self.conn.execute(
                f"""
                INSERT INTO "{CHECKPOINT_TABLE}" (
                    table_name, last_status, updated_at
                ) VALUES (?, ?, ?)
                ON CONFLICT(table_name) DO UPDATE SET
                    last_status = excluded.last_status,
                    updated_at = excluded.updated_at
                """,  # noqa: S608
                (table_name, status, now_str),
            )

    def get_all_checkpoints(self) -> dict[str, SyncCheckpoint]:
        """Fetch all table checkpoints from database."""
        if not self._initialized:
            return {}
        with self._lock:
            cur = self.conn.execute(
                f"SELECT table_name, last_synced_at, last_rowid, last_pk_val, rows_synced, last_status, updated_at "  # noqa: S608
                f'FROM "{CHECKPOINT_TABLE}"'
            )
            rows = cur.fetchall()

        res: dict[str, SyncCheckpoint] = {}
        for row in rows:
            last_synced_at = None
            if row[1]:
                with contextlib.suppress(ValueError):
                    last_synced_at = datetime.fromisoformat(row[1])
            updated_at = datetime.now(_KST)
            if row[6]:
                with contextlib.suppress(ValueError):
                    updated_at = datetime.fromisoformat(row[6])
            res[row[0]] = SyncCheckpoint(
                table_name=row[0],
                last_synced_at=last_synced_at,
                last_rowid=row[2],
                last_pk_val=row[3],
                rows_synced=row[4] or 0,
                last_status=row[5] or "UNKNOWN",
                updated_at=updated_at,
            )
        return res

    def reset_checkpoint(self, table_name: str | None = None) -> None:
        """Reset checkpoint for a specific table or all tables."""
        with self._lock, self.conn:
            if table_name:
                self.conn.execute(f'DELETE FROM "{CHECKPOINT_TABLE}" WHERE table_name = ?', (table_name,))  # noqa: S608
            else:
                self.conn.execute(f'DELETE FROM "{CHECKPOINT_TABLE}"')  # noqa: S608
