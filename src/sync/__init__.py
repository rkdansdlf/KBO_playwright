"""Database synchronization package (SQLite → PostgreSQL table DAG, checkpoints, verification)."""

from __future__ import annotations

from src.sync.checkpoint import CheckpointManager, SyncCheckpoint
from src.sync.dto import (
    ConsistencyCheckItem,
    SyncExecutionMode,
    SyncRunSummary,
    SyncTablePlan,
    SyncVerificationReport,
    TableSyncResult,
)
from src.sync.table_dag import TABLE_META_BY_NAME, TABLE_REGISTRY, SyncStrategy, TableMeta, get_tables_by_level
from src.sync.verifier import SyncConsistencyVerifier

__all__ = [
    "TABLE_META_BY_NAME",
    "TABLE_REGISTRY",
    "CheckpointManager",
    "ConsistencyCheckItem",
    "SyncCheckpoint",
    "SyncConsistencyVerifier",
    "SyncExecutionMode",
    "SyncRunSummary",
    "SyncStrategy",
    "SyncTablePlan",
    "SyncVerificationReport",
    "TableMeta",
    "TableSyncResult",
    "get_tables_by_level",
]
