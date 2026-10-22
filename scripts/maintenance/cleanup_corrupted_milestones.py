r"""One-off maintenance cleanup for corrupted player_milestones rows and associated RAG chunks.

Corrupted-Row Definition:
    Every row present in the `player_milestones` table prior to deploying the crawler and
    recalculation bugfix is treated as corrupted. Prior to this fix, the table was written
    exclusively by an old crawler that set `target_val = current_val + 10` and mutated by an
    old StatRecalculator bug that added each player's all-time career totals into `current_val`
    on every nightly run. In production, all 353 existing rows are untrustworthy (e.g. 280
    flagged as achieved, 187 with `current_val > 3 * target_val`).

Why Rows Are Archived and Deleted:
    The `player_milestones` table has no `is_active` or soft-delete column. Every consumer
    (API router, ContextAggregator, MilestoneRepository, notification summaries,
    RagKnowledgeIndexer.index_milestones, and StatRecalculator) queries the table without an
    activity filter. Adding a column would require a live schema migration on a hot table and
    updating every reader. Deleting the corrupted rows after creating an exact archival backup
    in `player_milestones_cleanup_backup` is the least invasive approach:
    1. Zero schema modifications to the production `player_milestones` table.
    2. Zero code changes required in readers, indexers, or recalculators.
    3. All readers instantly stop seeing corrupted data.
    4. Once a real milestone source exists (crawl_milestones is behind MILESTONE_CRAWL_ENABLED, off by
       default), the crawler inserts fresh rows; re-crawls keep an existing row's target_val.
    5. A complete audit trail is preserved in `player_milestones_cleanup_backup`, allowing a trivial
       rollback via RESTORE_ROWS_SQL (see "Rollback") if ever required.

Vector and Sparse Deactivation:
    bega_AI's PG similarity_search filters on COALESCE(is_active, true), expires_at,
    valid_from, valid_to and ignores index_status, so DELETED chunks with is_active true
    stay searchable. Therefore, already-DELETED rows from an earlier run must also get
    is_active=false on rerun.

Rollback:
    If a rollback is required, execute the following steps:
    1. Restore rows per season from the backup table with RESTORE_ROWS_SQL (bind :season). It is an
       INSERT ... SELECT ... WHERE NOT EXISTS, so a backed-up row whose id or natural key
       (season, player_id, milestone_category) was re-created by a re-crawl is skipped instead of failing
       on uq_player_milestone.
    2. Reactivate chunks on sparse and vector stores using REACTIVATE_CHUNKS_SQL and
       REACTIVATE_VECTOR_CHUNKS_SQL with `restore_row_ids(source_session, season)`:
       ```python
       row_ids = restore_row_ids(source_session, season)
       if row_ids:
           index_session.execute(REACTIVATE_CHUNKS_SQL, {"row_ids": row_ids})
           vector_session.execute(REACTIVATE_VECTOR_CHUNKS_SQL, {"row_ids": row_ids})
       ```
    3. Re-index knowledge per season:
       ```bash
       python -m src.cli.rag.index_rag_knowledge --season 2025
       python -m src.cli.rag.index_rag_knowledge --season 2026
       ```
       (Since the scheduled indexer only processes the current season).

Run Order:
    This cleanup script MUST be executed BEFORE deploying the crawler/recalc fix.
    The daily `recalc_milestones_and_rag_job` runs at 07:15 KST. If the fix were deployed without
    cleaning up first, the recalculator would re-confirm existing inflated values as `is_achieved = 1`
    and re-index them into both sparse and dense RAG stores. Running this cleanup prior to deployment
    purges the corrupted baseline and retires all stale RAG chunks.

Usage:
    # Dry-run inspection (default, no writes performed):
    python3 -m scripts.maintenance.cleanup_corrupted_milestones

    # Dry-run for a specific season:
    python3 -m scripts.maintenance.cleanup_corrupted_milestones --season 2026

    # Execute cleanup. --expect-count must equal the dry-run's would_back_up whenever there are new rows
    # to back up (353 in production on the first run); --confirm-target must name every host printed
    # for the source / sparse / vector targets (repeat the flag for several hosts):
    python3 -m scripts.maintenance.cleanup_corrupted_milestones --apply --expect-count 353 \
        --confirm-target <source-db-host> --confirm-target <pgvector-host>

    # With RAG_TARGET_ENV=production the existing RAG write guard also applies:
    RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 python3 -m scripts.maintenance.cleanup_corrupted_milestones \
        --apply --expect-count 353 --confirm-target <source-db-host> --confirm-target <pgvector-host>

    Production guard: RAG_TARGET_ENV defaults to "local" when unset (env.example sets "staging"), and only
    RAG_TARGET_ENV=production enforces RAG_INDEX_ALLOW_PRODUCTION_WRITE=1. Do not rely on it; the guards that
    always apply on --apply are --confirm-target and --expect-count. PGVECTOR_TEST_URL overrides PGVECTOR_URL in
    src.db.vector_engine, so --apply refuses while it is set to a different URL.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TextIO

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    bindparam,
    delete,
    inspect,
    insert,
    select,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError, SQLAlchemyError

from src.models.player_milestone import PlayerMilestone
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import stable_milestone_source_row_id
from src.services.rag_index_lifecycle import IndexLifecycleStatus, transition_status
from src.services.rag_primary_tombstone import inspect_primary_tombstones, tombstone_primary_rows
from src.services.rag_vector_tombstone import split_source_key

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date

    from sqlalchemy.engine import RowMapping
    from sqlalchemy.orm import Session

#: Per-key store failures that are reported and counted (exit 1) instead of aborting the run.
_STORE_ERRORS = (SQLAlchemyError, ValueError)

BACKUP_TABLE_NAME = "player_milestones_cleanup_backup"
SELECTION_REASON = "corrupted_pre_deploy_batch"
GUARD_ENV = "RAG_INDEX_ALLOW_PRODUCTION_WRITE"

RESTORE_ROWS_SQL = text(
    "INSERT INTO player_milestones ("
    "id, season, player_id, player_name, team_code, milestone_category, "
    "current_val, target_val, remaining_val, is_achieved, achieved_date, created_at, updated_at"
    ") "
    "SELECT b.id, b.season, b.player_id, b.player_name, b.team_code, b.milestone_category, "
    "b.current_val, b.target_val, b.remaining_val, b.is_achieved, b.achieved_date, b.created_at, b.updated_at "
    "FROM player_milestones_cleanup_backup b "
    "WHERE b.season = :season AND NOT EXISTS ("
    "SELECT 1 FROM player_milestones p "
    "WHERE p.id = b.id OR (p.season = b.season AND p.player_id = b.player_id AND p.milestone_category = b.milestone_category)"
    ")"
)

REACTIVATE_CHUNKS_SQL = text(
    "UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE source_table = 'milestone' AND source_row_id IN :row_ids"
).bindparams(bindparam("row_ids", expanding=True))

REACTIVATE_VECTOR_CHUNKS_SQL = text(
    "UPDATE rag_chunks SET index_status = 'ACTIVE', is_active = true, valid_to = NULL "
    "WHERE source_table = 'milestone' AND source_row_id IN :row_ids"
).bindparams(bindparam("row_ids", expanding=True))

# Dedicated MetaData instance so Base.metadata.create_all never touches this table.
backup_metadata = MetaData()

backup_table = Table(
    BACKUP_TABLE_NAME,
    backup_metadata,
    Column("id", Integer, primary_key=True, autoincrement=False),
    Column("season", Integer, nullable=False),
    Column("player_id", String(50), nullable=False),
    Column("player_name", String(50), nullable=False),
    Column("team_code", String(20), nullable=True),
    Column("milestone_category", String(50), nullable=False),
    Column("current_val", Integer, nullable=False),
    Column("target_val", Integer, nullable=False),
    Column("remaining_val", Integer, nullable=False),
    Column("is_achieved", Integer, nullable=False),
    Column("achieved_date", Date, nullable=True),
    Column("created_at", DateTime, nullable=False),
    Column("updated_at", DateTime, nullable=False),
    Column("rag_source_key", String(255), nullable=False),
    Column("selection_reason", String(100), nullable=False),
    Column("backed_up_at", DateTime, nullable=False),
)


@dataclass(frozen=True)
class CorruptedMilestoneItem:
    """Normalized milestone record for cleanup tracking."""

    id: int
    season: int
    player_id: str
    player_name: str
    team_code: str | None
    milestone_category: str
    current_val: int
    target_val: int
    remaining_val: int
    is_achieved: int
    achieved_date: date | None
    created_at: datetime
    updated_at: datetime
    rag_source_key: str
    selection_reason: str

    @property
    def natural_key(self) -> tuple[int, str, str]:
        """Return the natural uniqueness key (season, player_id, milestone_category)."""
        pid = str(self.player_id).strip() if self.player_id is not None else "UNKNOWN"
        return (self.season, pid, self.milestone_category.strip())


@dataclass
class CleanupReport:
    """Report summarizing corrupted milestone cleanup operations."""

    mode: str
    selected: int
    already_backed_up: int
    would_back_up: int
    backed_up_new: int
    would_delete: int
    deleted: int
    sparse_active: int
    sparse_tombstoned: int
    sparse_already: int
    sparse_missing: int
    vector_active: int
    vector_tombstoned: int
    vector_already: int
    vector_missing: int
    vector_skipped: bool = False
    status_code: int = 0
    error_message: str | None = None
    sparse_deactivated_only: int = 0
    vector_deactivated_only: int = 0
    delete_skipped_mismatch: int = 0
    legacy_sparse_tombstoned: int = 0
    legacy_vector_tombstoned: int = 0

    @property
    def exit_code(self) -> int:
        """Alias for status_code."""
        return self.status_code

    @property
    def candidates(self) -> int:
        """Alias for selected candidate count."""
        return self.selected


def restore_row_ids(source_session: Session, season: int) -> list[str]:
    """Return source_row_ids of restored rows for a season.

    Identifies backup rows of that season whose natural key exists exactly once in
    player_milestones with the same id as in the backup table.

    Args:
        source_session: Operational database session.
        season: Target season.

    Returns:
        List of row ID strings (the portion of rag_source_key after 'milestone:').

    """
    bind = source_session.connection()
    if not inspect(bind).has_table(BACKUP_TABLE_NAME):
        return []

    backup_rows = source_session.execute(
        select(
            backup_table.c.id,
            backup_table.c.season,
            backup_table.c.player_id,
            backup_table.c.milestone_category,
            backup_table.c.rag_source_key,
        ).where(backup_table.c.season == season)
    ).all()

    live_rows = source_session.execute(
        select(
            PlayerMilestone.id,
            PlayerMilestone.season,
            PlayerMilestone.player_id,
            PlayerMilestone.milestone_category,
        ).where(PlayerMilestone.season == season)
    ).all()

    live_by_key: dict[tuple[int, str, str], list[int]] = {}
    for r in live_rows:
        pid = str(r.player_id).strip() if r.player_id is not None else "UNKNOWN"
        k = (r.season, pid, r.milestone_category.strip())
        live_by_key.setdefault(k, []).append(r.id)

    restored_keys: list[str] = []
    for b in backup_rows:
        pid = str(b.player_id).strip() if b.player_id is not None else "UNKNOWN"
        k = (b.season, pid, b.milestone_category.strip())
        live_ids = live_by_key.get(k, [])
        if len(live_ids) == 1 and live_ids[0] == b.id:
            key_part = b.rag_source_key.partition(":")[2]
            if not key_part:
                key_part = stable_milestone_source_row_id(b.season, b.player_id, b.milestone_category)
            restored_keys.append(key_part)

    return restored_keys


def check_confirm_target(urls: dict[str, str], confirmed: list[str] | None) -> str | None:
    """Validate that all target hostnames among configured write-target URLs are confirmed.

    Args:
        urls: Mapping of target names to connection URL strings.
        confirmed: List of confirmed hostnames provided via --confirm-target.

    Returns:
        Error message string listing expected hosts if confirmation fails, None if valid.

    """
    expected_hosts: set[str] = set()
    for url in urls.values():
        if not url:
            continue
        with contextlib.suppress(ArgumentError, ValueError):
            parsed = make_url(url)
            if parsed.host:
                expected_hosts.add(parsed.host)

    if not expected_hosts:
        return None

    confirmed_set = set(confirmed or [])
    missing = expected_hosts - confirmed_set
    if missing:
        expected_str = ", ".join(sorted(expected_hosts))
        missing_str = ", ".join(sorted(missing))
        return f"--apply requires target confirmation; unconfirmed host(s): {missing_str}; expected: {expected_str}"
    return None


def select_corrupted_rows(session: Session, season: int | None = None) -> list[PlayerMilestone]:
    """Select all player_milestones rows present before fix deployment.

    Args:
        session: Active database session.
        season: Optional season filter.

    Returns:
        List of matching PlayerMilestone ORM instances.

    """
    stmt = select(PlayerMilestone)
    if season is not None:
        stmt = stmt.where(PlayerMilestone.season == season)
    stmt = stmt.order_by(PlayerMilestone.id)
    return list(session.scalars(stmt).all())


def compute_breakdown(items: Sequence[CorruptedMilestoneItem]) -> dict[str, int]:
    """Compute statistical breakdown of corrupted rows.

    Args:
        items: Sequence of normalized corrupted items.

    Returns:
        Dictionary with total, is_achieved_1, inflated_3x, and crawler_signature counts.

    """
    total = len(items)
    achieved = sum(1 for item in items if item.is_achieved == 1)
    inflated = sum(1 for item in items if item.current_val > 3 * item.target_val)
    crawler_sig = sum(1 for item in items if (item.target_val - item.current_val) == 10)
    return {
        "total": total,
        "is_achieved_1": achieved,
        "inflated_3x": inflated,
        "crawler_signature": crawler_sig,
    }


def check_production_guard(*, apply: bool) -> str | None:
    """Validate production environment write guard.

    Note:
        When RAG_TARGET_ENV is unset it defaults to "local" and does not block
        (env.example sets staging), which is why --confirm-target is required
        on every apply.

    Args:
        apply: Whether write operations are enabled.

    Returns:
        Error message string if write is refused, None otherwise.

    """
    if not apply:
        return None
    if os.getenv("RAG_TARGET_ENV", "local").strip().lower() == "production" and os.getenv(GUARD_ENV) != "1":
        return f"production --apply requires {GUARD_ENV}=1"
    return None


def _check_backup_schema(session: Session) -> str | None:
    """Check that player_milestones_cleanup_backup has expected columns if it exists.

    Args:
        session: Active database session.

    Returns:
        Error message string if schema is incompatible, None otherwise.

    """
    bind = session.connection()
    if not inspect(bind).has_table(BACKUP_TABLE_NAME):
        return None
    existing_cols = {col["name"] for col in inspect(bind).get_columns(BACKUP_TABLE_NAME)}
    expected_cols = [col.name for col in backup_table.c]
    missing_cols = [col for col in expected_cols if col not in existing_cols]
    if missing_cols:
        return (
            f"existing {BACKUP_TABLE_NAME} has an incompatible schema; "
            f"missing columns: {', '.join(missing_cols)}; rename or drop it after checking its contents"
        )
    return None


def _resolve_target_set(
    source_session: Session,
    season: int | None,
    out: TextIO,
) -> tuple[list[CorruptedMilestoneItem], set[int], int, int, set[int]]:
    """Resolve the per-season target set of corrupted milestones to process.

    Args:
        source_session: Operational database session.
        season: Optional season filter.
        out: Output stream for logging progress.

    Returns:
        Tuple of (target_items, existing_backup_ids, already_backed_up, would_back_up, processed_seasons).

    """
    bind = source_session.connection()
    has_backup = inspect(bind).has_table(BACKUP_TABLE_NAME)

    processed_seasons: set[int] = set()
    if has_backup:
        processed_seasons = set(source_session.scalars(select(backup_table.c.season).distinct()).all())

    if season is not None:
        seasons_in_scope = [season]
    else:
        live_seasons = set(source_session.scalars(select(PlayerMilestone.season).distinct()).all())
        all_seasons = sorted(live_seasons | processed_seasons)
        seasons_in_scope = all_seasons

    target_items: list[CorruptedMilestoneItem] = []
    already_backed_up = 0
    would_back_up = 0
    existing_backup_ids: set[int] = set()

    for s in seasons_in_scope:
        if s in processed_seasons:
            query = select(backup_table).where(backup_table.c.season == s).order_by(backup_table.c.id)
            records = list(source_session.execute(query).mappings().all())
            out.write(f"season={s} mode=frozen rows={len(records)}\n")
            season_items = [
                CorruptedMilestoneItem(
                    id=r["id"],
                    season=r["season"],
                    player_id=r["player_id"],
                    player_name=r["player_name"],
                    team_code=r["team_code"],
                    milestone_category=r["milestone_category"],
                    current_val=r["current_val"],
                    target_val=r["target_val"],
                    remaining_val=r["remaining_val"],
                    is_achieved=int(r["is_achieved"]),
                    achieved_date=r["achieved_date"],
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                    rag_source_key=r["rag_source_key"],
                    selection_reason=r["selection_reason"],
                )
                for r in records
            ]
            already_backed_up += len(season_items)
            existing_backup_ids.update(r["id"] for r in records)
            target_items.extend(season_items)
        else:
            raw_rows = select_corrupted_rows(source_session, s)
            out.write(f"season={s} mode=new rows={len(raw_rows)}\n")
            season_items = [
                CorruptedMilestoneItem(
                    id=r.id,
                    season=r.season,
                    player_id=r.player_id,
                    player_name=r.player_name,
                    team_code=r.team_code,
                    milestone_category=r.milestone_category,
                    current_val=r.current_val,
                    target_val=r.target_val,
                    remaining_val=r.remaining_val,
                    is_achieved=int(r.is_achieved),
                    achieved_date=r.achieved_date,
                    created_at=r.created_at or datetime.now(UTC),
                    updated_at=r.updated_at or datetime.now(UTC),
                    rag_source_key=f"milestone:{stable_milestone_source_row_id(r.season, r.player_id, r.milestone_category)}",
                    selection_reason=SELECTION_REASON,
                )
                for r in raw_rows
            ]
            would_back_up += len(season_items)
            target_items.extend(season_items)

    return target_items, existing_backup_ids, already_backed_up, would_back_up, processed_seasons


def _datetimes_equal(dt1: datetime | None, dt2: datetime | None) -> bool:
    """Compare two datetimes handling naive/aware equivalence."""
    if dt1 is None or dt2 is None:
        return dt1 == dt2
    if dt1.tzinfo is not None and dt2.tzinfo is None:
        dt2 = dt2.replace(tzinfo=UTC)
    elif dt1.tzinfo is None and dt2.tzinfo is not None:
        dt1 = dt1.replace(tzinfo=UTC)
    return dt1 == dt2


def _backup_and_delete(
    source_session: Session,
    target_items: Sequence[CorruptedMilestoneItem],
    *,
    processed_seasons: set[int],
    existing_ids: set[int],
    out: TextIO,
) -> tuple[int, int, int, set[int]]:
    """Execute backup table creation, new row insertion, and true-match deletion.

    Args:
        source_session: Operational database session.
        target_items: Target items to back up and delete.
        processed_seasons: Seasons that were already processed in backup table.
        existing_ids: IDs already present in backup table.
        out: Output stream for logging progress.

    Returns:
        Tuple of (backed_up_new, deleted, delete_skipped_mismatch, deleted_ids).

    """
    backup_metadata.create_all(source_session.connection(), checkfirst=True)
    backed_up_new = 0

    items_to_backup = [
        item for item in target_items if item.season not in processed_seasons and item.id not in existing_ids
    ]
    if items_to_backup:
        now_dt = datetime.now(UTC)
        backup_dicts = [
            {
                "id": item.id,
                "season": item.season,
                "player_id": item.player_id,
                "player_name": item.player_name,
                "team_code": item.team_code,
                "milestone_category": item.milestone_category,
                "current_val": item.current_val,
                "target_val": item.target_val,
                "remaining_val": item.remaining_val,
                "is_achieved": item.is_achieved,
                "achieved_date": item.achieved_date,
                "created_at": item.created_at,
                "updated_at": item.updated_at,
                "rag_source_key": item.rag_source_key,
                "selection_reason": item.selection_reason,
                "backed_up_at": now_dt,
            }
            for item in items_to_backup
        ]
        source_session.execute(insert(backup_table), backup_dicts)
        source_session.commit()
        backed_up_new = len(items_to_backup)

    target_ids = [item.id for item in target_items]
    live_rows = (
        source_session.scalars(select(PlayerMilestone).where(PlayerMilestone.id.in_(target_ids))).all()
        if target_ids
        else []
    )
    live_by_id = {r.id: r for r in live_rows}

    true_match_ids: list[int] = []
    delete_skipped_mismatch = 0

    for item in target_items:
        live = live_by_id.get(item.id)
        if live is None:
            continue
        pid_match = str(live.player_id).strip() == str(item.player_id).strip()
        cat_match = str(live.milestone_category).strip() == str(item.milestone_category).strip()
        season_match = int(live.season) == int(item.season)
        created_match = _datetimes_equal(live.created_at, item.created_at)

        if pid_match and cat_match and season_match and created_match:
            true_match_ids.append(item.id)
        else:
            delete_skipped_mismatch += 1
            out.write(
                f"delete_skipped_mismatch on id={item.id}: live ({live.season}, {live.player_id}, "
                f"{live.milestone_category}, {live.created_at}) != backup ({item.season}, "
                f"{item.player_id}, {item.milestone_category}, {item.created_at})\n"
            )

    deleted = 0
    if true_match_ids:
        del_stmt = delete(PlayerMilestone).where(PlayerMilestone.id.in_(true_match_ids))
        source_session.execute(del_stmt)
        source_session.commit()
        deleted = len(true_match_ids)

    return backed_up_new, deleted, delete_skipped_mismatch, set(true_match_ids)


def _resolve_tombstone_keys(
    source_session: Session,
    target_items: Sequence[CorruptedMilestoneItem],
    deleted_live_ids: set[int],
    *,
    apply: bool,
) -> tuple[str, ...]:
    """Resolve RAG keys to tombstone by subtracting surviving live rows.

    Args:
        source_session: Operational database session.
        target_items: Target items candidate list.
        deleted_live_ids: IDs deleted or to be deleted from live table.
        apply: Whether write operations are enabled.

    Returns:
        Deduplicated tuple of RAG keys to tombstone or inspect.

    """
    surviving_stmt = select(PlayerMilestone.season, PlayerMilestone.player_id, PlayerMilestone.milestone_category)
    if not apply and deleted_live_ids:
        surviving_stmt = surviving_stmt.where(~PlayerMilestone.id.in_(deleted_live_ids))
    surviving_tuples = source_session.execute(surviving_stmt).all()
    surviving_natural_keys = {
        (s, str(pid).strip() if pid is not None else "UNKNOWN", cat.strip()) for s, pid, cat in surviving_tuples
    }

    keys: list[str] = []
    for item in target_items:
        if item.natural_key in surviving_natural_keys:
            continue
        keys.append(item.rag_source_key)
    return tuple(dict.fromkeys(keys))


def _deactivate_sparse_key(
    index_session: Session,
    key: str,
    *,
    has_is_active: bool,
    has_valid_to: bool,
    out: TextIO,
) -> tuple[int, bool]:
    """Execute honest is_active/valid_to update on a single sparse key."""
    try:
        src_table, src_row_id = split_source_key(key)
    except ValueError as err:
        out.write(f"Sparse store error on {key}: {err}\n")
        return 0, True

    set_c = []
    if has_is_active:
        set_c.append("is_active = false")
    if has_valid_to:
        set_c.append("valid_to = COALESCE(valid_to, CURRENT_TIMESTAMP)")
    where_c = ["source_table = :table", "source_row_id = :row_id"]
    if has_is_active:
        where_c.append("(is_active IS NULL OR is_active = :true_val)")

    stmt = text(f"UPDATE rag_chunks SET {', '.join(set_c)} WHERE {' AND '.join(where_c)}")
    params: dict[str, Any] = {"table": src_table, "row_id": src_row_id}
    if has_is_active:
        params["true_val"] = True

    try:
        res = index_session.execute(stmt, params)
        index_session.commit()
        return (1 if (res.rowcount and res.rowcount > 0) else 0), False
    except _STORE_ERRORS as err:
        index_session.rollback()
        out.write(f"Sparse store error on {key}: {err}\n")
        return 0, True


def _process_sparse_store(
    index_session: Session,
    keys: tuple[str, ...],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, int, bool]:
    """Inspect or tombstone sparse RAG index chunks.

    Args:
        index_session: Sparse RAG index database session.
        keys: RAG keys to inspect or tombstone.
        apply: Whether write operations are enabled.
        out: Output stream for logging progress.

    Returns:
        Tuple of (sparse_active, sparse_tombstoned, sparse_already, sparse_missing, sparse_deactivated_only, has_error).

    """
    bind = index_session.connection()
    sparse_cols = {col["name"] for col in inspect(bind).get_columns("rag_chunks")}
    has_is_active = "is_active" in sparse_cols
    has_valid_to = "valid_to" in sparse_cols

    if not has_is_active and not has_valid_to:
        out.write("sparse store has no is_active/valid_to columns: index_status only\n")

    has_error = False
    deactivated_only = 0

    if apply:
        results = tombstone_primary_rows(index_session, keys)
        tombstoned = sum(
            1
            for r in results
            if r.found and r.final_status == "DELETED" and r.previous_status not in ("DELETED", "TOMBSTONED")
        )
        already = sum(1 for r in results if r.found and r.previous_status in ("DELETED", "TOMBSTONED"))
        missing = sum(1 for r in results if not r.found)
        active = 0

        if has_is_active or has_valid_to:
            for key in keys:
                deact_count, err = _deactivate_sparse_key(
                    index_session, key, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
                )
                deactivated_only += deact_count
                if err:
                    has_error = True
    else:
        results = inspect_primary_tombstones(index_session, keys)
        active = sum(1 for r in results if r.found and r.final_status not in ("DELETED", "TOMBSTONED"))
        already = sum(1 for r in results if r.found and r.final_status in ("DELETED", "TOMBSTONED"))
        missing = sum(1 for r in results if not r.found)
        tombstoned = 0

    for r in results:
        if r.error:
            has_error = True
            out.write(f"Sparse store error on {r.source_key}: {r.error}\n")

    return active, tombstoned, already, missing, deactivated_only, has_error


def _build_vector_update_sql(
    *,
    has_is_active: bool,
    has_valid_to: bool,
) -> str:
    """Build update SQL statement for vector tombstone."""
    set_clauses = ["index_status = 'DELETED'"]
    if has_is_active:
        set_clauses.append("is_active = false")
    if has_valid_to:
        set_clauses.append("valid_to = COALESCE(valid_to, CURRENT_TIMESTAMP)")

    where_clauses = ["source_table = :table", "source_row_id = :row_id"]
    if has_is_active:
        where_clauses.append("(index_status <> 'DELETED' OR is_active IS NULL OR is_active = :true_val)")
    else:
        where_clauses.append("index_status <> 'DELETED'")

    return f"UPDATE rag_chunks SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"


def _tombstone_one_vector_key(
    session: Session,
    key: str,
    *,
    has_is_active: bool,
    has_valid_to: bool,
    out: TextIO,
) -> tuple[int, int, int, int, bool]:
    """Execute honest tombstone/deactivation on a single vector key."""
    try:
        source_table, source_row_id = split_source_key(key)
    except ValueError as err:
        out.write(f"Vector store error on {key}: {err}\n")
        return 0, 0, 0, 0, True

    select_cols = ["index_status"]
    if has_is_active:
        select_cols.append("is_active")
    if has_valid_to:
        select_cols.append("valid_to")

    stmt = text(
        f"SELECT {', '.join(select_cols)} FROM rag_chunks WHERE source_table = :table AND source_row_id = :row_id"
    )
    try:
        row = session.execute(stmt, {"table": source_table, "row_id": source_row_id}).mappings().first()
    except _STORE_ERRORS as err:
        session.rollback()
        out.write(f"Vector store error on {key}: {err}\n")
        return 0, 0, 0, 0, True

    if row is None:
        return 0, 0, 0, 1, False

    curr_status = str(row["index_status"] or "")
    is_act = row.get("is_active") if has_is_active else None
    is_active_true = is_act is None or bool(is_act) is True

    if curr_status == "DELETED" and (not has_is_active or not is_active_true):
        return 0, 0, 1, 0, False

    update_sql = _build_vector_update_sql(has_is_active=has_is_active, has_valid_to=has_valid_to)
    params: dict[str, Any] = {"table": source_table, "row_id": source_row_id}
    if has_is_active:
        params["true_val"] = True

    try:
        session.execute(text(update_sql), params)
        session.commit()
        if curr_status != "DELETED":
            return 1, 0, 0, 0, False
        return 0, 1, 0, 0, False
    except _STORE_ERRORS as err:
        session.rollback()
        out.write(f"Vector store error on {key}: {err}\n")
        return 0, 0, 0, 0, True


def _tombstone_vector_keys(
    session: Session,
    keys: Sequence[str],
    *,
    out: TextIO = sys.stdout,
) -> tuple[int, int, int, int, bool]:
    """Tombstone chunks in the vector store with honest per-key commits.

    Reason:
        bega_AI's PG similarity_search filters on COALESCE(is_active, true), expires_at,
        valid_from, valid_to and ignores index_status. DELETED chunks with is_active=true
        stay searchable. Therefore, already-DELETED rows from an earlier run must also get
        is_active=false on rerun.

    Args:
        session: Active vector database session.
        keys: RAG source keys to tombstone.
        out: Output stream for logging progress and errors.

    Returns:
        Tuple of (tombstoned, deactivated_only, already, missing, has_error).

    """
    bind = session.connection()
    columns = {col["name"] for col in inspect(bind).get_columns("rag_chunks")}
    has_is_active = "is_active" in columns
    has_valid_to = "valid_to" in columns

    tombstoned = 0
    deactivated_only = 0
    already = 0
    missing = 0
    has_error = False

    for key in keys:
        t_delta, d_delta, a_delta, m_delta, err = _tombstone_one_vector_key(
            session, key, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
        )
        tombstoned += t_delta
        deactivated_only += d_delta
        already += a_delta
        missing += m_delta
        if err:
            has_error = True

    return tombstoned, deactivated_only, already, missing, has_error


def _inspect_vector_keys(
    session: Session,
    keys: Sequence[str],
    *,
    out: TextIO = sys.stdout,
) -> tuple[int, int, int, int, bool]:
    """Inspect the state of vector store chunks without writing.

    Reports per key: missing / active / deleted-but-still-is_active / fully retired.

    Args:
        session: Active vector database session.
        keys: RAG source keys to inspect.
        out: Output stream for logging progress and errors.

    Returns:
        Tuple of (active, deleted_still_active, already, missing, has_error).

    """
    bind = session.connection()
    columns = {col["name"] for col in inspect(bind).get_columns("rag_chunks")}
    has_is_active = "is_active" in columns

    active = 0
    deleted_still_active = 0
    already = 0
    missing = 0
    has_error = False

    for key in keys:
        try:
            source_table, source_row_id = split_source_key(key)
        except ValueError as err:
            has_error = True
            out.write(f"Vector store error on {key}: {err}\n")
            continue

        select_cols = ["index_status"]
        if has_is_active:
            select_cols.append("is_active")

        stmt = text(
            f"SELECT {', '.join(select_cols)} FROM rag_chunks WHERE source_table = :table AND source_row_id = :row_id"
        )
        try:
            row = session.execute(stmt, {"table": source_table, "row_id": source_row_id}).mappings().first()
        except _STORE_ERRORS as err:
            has_error = True
            out.write(f"Vector store error on {key}: {err}\n")
            continue

        if row is None:
            missing += 1
            out.write(f"vector chunk {key}: missing\n")
            continue

        curr_status = str(row["index_status"] or "")
        is_act = row.get("is_active") if has_is_active else None
        is_active_true = is_act is None or bool(is_act) is True

        if curr_status != "DELETED":
            active += 1
            out.write(f"vector chunk {key}: active\n")
        elif has_is_active and is_active_true:
            deleted_still_active += 1
            out.write(f"vector chunk {key}: deleted-but-still-is_active\n")
        else:
            already += 1
            out.write(f"vector chunk {key}: fully retired\n")

    return active, deleted_still_active, already, missing, has_error


def _process_vector_store(
    vector_session: Session | None,
    keys: tuple[str, ...],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, int, bool, bool]:
    """Inspect or tombstone dense vector store chunks.

    Args:
        vector_session: Dense vector database session or None.
        keys: RAG keys to inspect or tombstone.
        apply: Whether write operations are enabled.
        out: Output stream for logging progress.

    Returns:
        Tuple of (vector_active, vector_tombstoned, vector_already, vector_missing, vector_deactivated_only, vector_skipped, has_error).

    """
    if vector_session is None:
        out.write("vector store not configured (PGVECTOR_URL unset): skipped\n")
        return 0, 0, 0, 0, 0, True, False

    if apply:
        tombstoned, deactivated_only, already, missing, has_error = _tombstone_vector_keys(
            vector_session, keys, out=out
        )
        return 0, tombstoned, already, missing, deactivated_only, False, has_error

    active, deleted_still_active, already, missing, has_error = _inspect_vector_keys(vector_session, keys, out=out)
    return active, 0, already, missing, deleted_still_active, False, has_error


def _find_and_filter_legacy_chunks(
    source_session: Session,
    index_session: Session,
    target_items: Sequence[CorruptedMilestoneItem],
    deleted_live_ids: set[int],
    out: TextIO,
) -> tuple[list[RagChunk], int, int]:
    """Find and evaluate legacy empty-key chunks in sparse store."""
    sparse_chunks = list(
        index_session.scalars(
            select(RagChunk).where(
                RagChunk.source_table == "milestone",
                (RagChunk.source_row_id == "") | (RagChunk.source_row_id.is_(None)),
            )
        ).all()
    )
    surviving_query = select(PlayerMilestone.player_name, PlayerMilestone.milestone_category)
    if deleted_live_ids:
        surviving_query = surviving_query.where(~PlayerMilestone.id.in_(deleted_live_ids))
    surviving_tuples = source_session.execute(surviving_query).all()
    surviving_pairs = {(str(pname).strip(), str(mcat).strip()) for pname, mcat in surviving_tuples}
    target_pairs = {(str(item.player_name).strip(), str(item.milestone_category).strip()) for item in target_items}

    matched_chunks: list[RagChunk] = []
    active_matched = 0
    for chunk in sparse_chunks:
        meta = chunk.meta or {}
        pname = str(meta.get("player_name", "")).strip()
        mcat = str(meta.get("milestone_category", "")).strip()
        pair = (pname, mcat)

        if pair not in target_pairs:
            out.write(
                f"legacy empty-key chunk unmatched: player_name={pname!r}, "
                f"milestone_category={mcat!r} not in target set\n"
            )
            continue
        if pair in surviving_pairs:
            out.write(
                f"legacy empty-key chunk unmatched: live player_milestones row exists for "
                f"player_name={pname!r}, milestone_category={mcat!r}\n"
            )
            continue
        matched_chunks.append(chunk)
        if chunk.index_status != "DELETED":
            active_matched += 1

    return matched_chunks, len(sparse_chunks), active_matched


def _retire_legacy_sparse_chunks(
    index_session: Session,
    matched_chunks: Sequence[RagChunk],
    out: TextIO,
) -> tuple[int, bool]:
    """Retire matched legacy chunks in sparse store."""
    legacy_sparse_tombstoned = 0
    has_error = False
    for chunk in matched_chunks:
        if chunk.index_status != "DELETED":
            try:
                chunk.index_status = transition_status(chunk.index_status, IndexLifecycleStatus.DELETE_PENDING.value)
                index_session.commit()
                chunk.index_status = transition_status(chunk.index_status, IndexLifecycleStatus.DELETED.value)
                index_session.commit()
                legacy_sparse_tombstoned += 1
            except _STORE_ERRORS as err:
                index_session.rollback()
                has_error = True
                out.write(f"Error transitioning legacy sparse chunk: {err}\n")

    if not matched_chunks:
        return legacy_sparse_tombstoned, has_error
    matched_ids = [chunk.id for chunk in matched_chunks]
    sbind = index_session.connection()
    scols = {col["name"] for col in inspect(sbind).get_columns("rag_chunks")}
    if "is_active" in scols or "valid_to" in scols:
        set_c = []
        if "is_active" in scols:
            set_c.append("is_active = false")
        if "valid_to" in scols:
            set_c.append("valid_to = COALESCE(valid_to, CURRENT_TIMESTAMP)")
        try:
            index_session.execute(
                text(f"UPDATE rag_chunks SET {', '.join(set_c)} WHERE id IN :ids").bindparams(
                    bindparam("ids", expanding=True)
                ),
                {"ids": matched_ids},
            )
            index_session.commit()
        except _STORE_ERRORS as err:
            index_session.rollback()
            has_error = True
            out.write(f"Error deactivating legacy sparse chunk: {err}\n")

    return legacy_sparse_tombstoned, has_error


def _retire_legacy_vector_chunks(
    vector_session: Session,
    vector_rows: Sequence[RowMapping],
    out: TextIO,
) -> tuple[int, bool]:
    """Retire legacy empty-key chunks in vector store."""
    vbind = vector_session.connection()
    vcols = {col["name"] for col in inspect(vbind).get_columns("rag_chunks")}
    has_is_act = "is_active" in vcols
    has_val_to = "valid_to" in vcols

    set_clauses = ["index_status = 'DELETED'"]
    if has_is_act:
        set_clauses.append("is_active = false")
    if has_val_to:
        set_clauses.append("valid_to = COALESCE(valid_to, CURRENT_TIMESTAMP)")

    where_clauses = [
        "source_table = 'milestone'",
        "(source_row_id = '' OR source_row_id IS NULL)",
    ]
    if has_is_act:
        where_clauses.append("(index_status <> 'DELETED' OR is_active IS NULL OR is_active = :true_val)")
    else:
        where_clauses.append("index_status <> 'DELETED'")

    vec_update = f"UPDATE rag_chunks SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"
    params: dict[str, Any] = {}
    if has_is_act:
        params["true_val"] = True

    def _needs_update(vrow: RowMapping) -> bool:
        is_active_val = vrow.get("is_active") if has_is_act else None
        return str(vrow["index_status"] or "") != "DELETED" or (
            has_is_act and (is_active_val is None or bool(is_active_val) is True)
        )

    if not any(_needs_update(vrow) for vrow in vector_rows):
        return 0, False
    # One statement, one commit: count only after the commit succeeded.
    try:
        vector_session.execute(text(vec_update), params)
        vector_session.commit()
    except _STORE_ERRORS as err:
        vector_session.rollback()
        out.write(f"Error tombstoning legacy vector chunk: {err}\n")
        return 0, True
    return sum(1 for vrow in vector_rows if str(vrow["index_status"] or "") != "DELETED"), False


def _process_legacy_empty_chunks(
    source_session: Session,
    index_session: Session,
    vector_session: Session | None,
    target_items: Sequence[CorruptedMilestoneItem],
    deleted_live_ids: set[int],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, bool]:
    """Inspect or tombstone legacy empty-key milestone chunks (source_row_id='').

    Args:
        source_session: Operational database session.
        index_session: Sparse RAG index database session.
        vector_session: Dense vector database session or None.
        target_items: Target items candidate list.
        deleted_live_ids: Live IDs that have been or will be deleted.
        apply: Whether write operations are enabled.
        out: Output stream for logging progress.

    Returns:
        Tuple of (legacy_sparse_tombstoned, legacy_vector_tombstoned, has_error).

    """
    matched_chunks, sparse_found, active_matched = _find_and_filter_legacy_chunks(
        source_session, index_session, target_items, deleted_live_ids, out
    )
    vector_rows: list[RowMapping] = []
    has_error = False

    if vector_session is not None:
        vbind = vector_session.connection()
        vcols = {col["name"] for col in inspect(vbind).get_columns("rag_chunks")}
        vselect = ["index_status"]
        if "is_active" in vcols:
            vselect.append("is_active")
        if "valid_to" in vcols:
            vselect.append("valid_to")
        vstmt = text(
            f"SELECT {', '.join(vselect)} FROM rag_chunks "
            f"WHERE source_table = 'milestone' AND (source_row_id = '' OR source_row_id IS NULL)"
        )
        try:
            vector_rows = list(vector_session.execute(vstmt).mappings().all())
        except _STORE_ERRORS as err:
            has_error = True
            out.write(f"Vector store error inspecting legacy chunks: {err}\n")

    vector_found = len(vector_rows)
    matched = len(matched_chunks)

    if not apply:
        out.write(
            f"legacy_empty_key_chunks: sparse_found={sparse_found} matched={matched} "
            f"active={active_matched} vector_found={vector_found}\n"
        )
        return 0, 0, has_error

    legacy_sparse_tombstoned, s_err = _retire_legacy_sparse_chunks(index_session, matched_chunks, out)
    legacy_vector_tombstoned = 0
    v_err = False
    if vector_session is not None and matched > 0:
        legacy_vector_tombstoned, v_err = _retire_legacy_vector_chunks(vector_session, vector_rows, out)

    return legacy_sparse_tombstoned, legacy_vector_tombstoned, (has_error or s_err or v_err)


def _validate_expect_count(
    *,
    apply: bool,
    would_back_up: int,
    expect_count: int | None,
    total_items: int,
) -> str | None:
    """Validate --expect-count constraints."""
    # --expect-count always refers to would_back_up (new rows to back up). With no new rows (a frozen
    # rerun) 0 or the frozen total are accepted. Dry-run checks it too, so the operator sees a mismatch early.
    if would_back_up > 0:
        if expect_count is None:
            if not apply:
                return None
            return (
                f"--apply with new rows to back up requires --expect-count {would_back_up} "
                f"(run the dry-run first and pass would_back_up)"
            )
        if expect_count != would_back_up:
            return f"count mismatch: expected {expect_count}, found {would_back_up} new rows to back up"
    elif expect_count is not None and expect_count not in (0, total_items):
        return f"count mismatch: expected {expect_count}, found {total_items}"
    return None


def _inspect_dry_run_matches(
    source_session: Session,
    target_items: Sequence[CorruptedMilestoneItem],
    out: TextIO,
) -> tuple[int, int, set[int]]:
    """Inspect true match rows in player_milestones for dry-run."""
    target_ids = [item.id for item in target_items]
    live_rows = (
        source_session.scalars(select(PlayerMilestone).where(PlayerMilestone.id.in_(target_ids))).all()
        if target_ids
        else []
    )
    live_by_id = {r.id: r for r in live_rows}
    true_match_ids = []
    delete_skipped_mismatch = 0
    for item in target_items:
        live = live_by_id.get(item.id)
        if live is None:
            continue
        pid_match = str(live.player_id).strip() == str(item.player_id).strip()
        cat_match = str(live.milestone_category).strip() == str(item.milestone_category).strip()
        season_match = int(live.season) == int(item.season)
        created_match = _datetimes_equal(live.created_at, item.created_at)
        if pid_match and cat_match and season_match and created_match:
            true_match_ids.append(item.id)
        else:
            delete_skipped_mismatch += 1
            out.write(
                f"delete_skipped_mismatch on id={item.id}: live ({live.season}, {live.player_id}, "
                f"{live.milestone_category}, {live.created_at}) != backup ({item.season}, "
                f"{item.player_id}, {item.milestone_category}, {item.created_at})\n"
            )
    return len(true_match_ids), delete_skipped_mismatch, set(true_match_ids)


def run_cleanup(
    source_session: Session,
    index_session: Session,
    vector_session: Session | None = None,
    *,
    apply: bool = False,
    season: int | None = None,
    expect_count: int | None = None,
    out: TextIO = sys.stdout,
) -> CleanupReport:
    """Run corrupted milestone cleanup across primary database and RAG indexes.

    Args:
        source_session: Operational database session.
        index_session: Sparse RAG index database session.
        vector_session: Dense vector database session, or None if PGVECTOR_URL is unset.
        apply: True to execute writes (backup, delete, tombstone); False for dry-run.
        season: Optional season filter.
        expect_count: Optional expected row count to guard against drift.
        out: Output stream for logging progress.

    Returns:
        CleanupReport summarizing counts and operation status.

    """
    schema_err = _check_backup_schema(source_session)
    if schema_err is not None:
        out.write(f"Refusing write: {schema_err}\n")
        return CleanupReport(
            mode="apply" if apply else "dry-run",
            selected=0,
            already_backed_up=0,
            would_back_up=0,
            backed_up_new=0,
            would_delete=0,
            deleted=0,
            sparse_active=0,
            sparse_tombstoned=0,
            sparse_already=0,
            sparse_missing=0,
            vector_active=0,
            vector_tombstoned=0,
            vector_already=0,
            vector_missing=0,
            vector_skipped=vector_session is None,
            status_code=3,
            error_message=schema_err,
        )

    (
        target_items,
        existing_backup_ids,
        already_backed_up,
        would_back_up,
        processed_seasons,
    ) = _resolve_target_set(source_session, season, out=out)

    breakdown = compute_breakdown(target_items)
    out.write(
        f"Breakdown: total={breakdown['total']}, is_achieved=1: {breakdown['is_achieved_1']}, "
        f"current_val > 3*target_val: {breakdown['inflated_3x']}, "
        f"target_val - current_val == 10: {breakdown['crawler_signature']}\n"
    )

    guard_error = check_production_guard(apply=apply)
    if guard_error is not None:
        out.write(f"Refusing write: {guard_error}\n")
        return CleanupReport(
            mode="apply" if apply else "dry-run",
            selected=len(target_items),
            already_backed_up=already_backed_up,
            would_back_up=would_back_up,
            backed_up_new=0,
            would_delete=0,
            deleted=0,
            sparse_active=0,
            sparse_tombstoned=0,
            sparse_already=0,
            sparse_missing=0,
            vector_active=0,
            vector_tombstoned=0,
            vector_already=0,
            vector_missing=0,
            vector_skipped=vector_session is None,
            status_code=3,
            error_message=guard_error,
        )

    count_err = _validate_expect_count(
        apply=apply, would_back_up=would_back_up, expect_count=expect_count, total_items=len(target_items)
    )
    if count_err is not None:
        out.write(f"Refusing write: {count_err}\n")
        return CleanupReport(
            mode="apply" if apply else "dry-run",
            selected=len(target_items),
            already_backed_up=already_backed_up,
            would_back_up=would_back_up,
            backed_up_new=0,
            would_delete=0,
            deleted=0,
            sparse_active=0,
            sparse_tombstoned=0,
            sparse_already=0,
            sparse_missing=0,
            vector_active=0,
            vector_tombstoned=0,
            vector_already=0,
            vector_missing=0,
            vector_skipped=vector_session is None,
            status_code=3,
            error_message=count_err,
        )

    if apply:
        (
            backed_up_new,
            deleted,
            delete_skipped_mismatch,
            deleted_live_ids,
        ) = _backup_and_delete(
            source_session,
            target_items,
            processed_seasons=processed_seasons,
            existing_ids=existing_backup_ids,
            out=out,
        )
        would_delete = deleted
    else:
        backed_up_new = 0
        deleted = 0
        would_delete, delete_skipped_mismatch, deleted_live_ids = _inspect_dry_run_matches(
            source_session, target_items, out
        )

    keys_tuple = _resolve_tombstone_keys(
        source_session,
        target_items,
        deleted_live_ids,
        apply=apply,
    )

    (
        sparse_active,
        sparse_tombstoned,
        sparse_already,
        sparse_missing,
        sparse_deactivated_only,
        sparse_error,
    ) = _process_sparse_store(
        index_session,
        keys_tuple,
        apply=apply,
        out=out,
    )

    (
        vector_active,
        vector_tombstoned,
        vector_already,
        vector_missing,
        vector_deactivated_only,
        vector_skipped,
        vector_error,
    ) = _process_vector_store(
        vector_session,
        keys_tuple,
        apply=apply,
        out=out,
    )

    (
        legacy_sparse_tombstoned,
        legacy_vector_tombstoned,
        legacy_error,
    ) = _process_legacy_empty_chunks(
        source_session,
        index_session,
        vector_session,
        target_items,
        deleted_live_ids,
        apply=apply,
        out=out,
    )

    if not apply:
        out.write(
            f"Counts: candidates={len(target_items)}, already_backed_up={already_backed_up}, "
            f"would_back_up={would_back_up}, would_delete={would_delete}\n"
        )
        out.write(f"Sparse store: active={sparse_active}, already_deleted={sparse_already}, missing={sparse_missing}\n")
        if not vector_skipped:
            out.write(
                f"Vector store: active={vector_active}, already_deleted={vector_already}, missing={vector_missing}\n"
            )

    mode_str = "apply" if apply else "dry-run"
    summary_line = (
        f"mode={mode_str} selected={len(target_items)} backed_up_new={backed_up_new} "
        f"deleted={deleted} sparse_tombstoned={sparse_tombstoned} "
        f"sparse_already={sparse_already} sparse_missing={sparse_missing} "
        f"vector_tombstoned={vector_tombstoned} vector_already={vector_already} "
        f"vector_missing={vector_missing} "
        f"legacy_sparse_tombstoned={legacy_sparse_tombstoned} "
        f"legacy_vector_tombstoned={legacy_vector_tombstoned}\n"
    )
    out.write(summary_line)

    status_code = 1 if (sparse_error or vector_error or legacy_error) else 0

    return CleanupReport(
        mode=mode_str,
        selected=len(target_items),
        already_backed_up=already_backed_up,
        would_back_up=would_back_up,
        backed_up_new=backed_up_new,
        would_delete=would_delete,
        deleted=deleted,
        sparse_active=sparse_active,
        sparse_tombstoned=sparse_tombstoned,
        sparse_already=sparse_already,
        sparse_missing=sparse_missing,
        vector_active=vector_active,
        vector_tombstoned=vector_tombstoned,
        vector_already=vector_already,
        vector_missing=vector_missing,
        vector_skipped=vector_skipped,
        status_code=status_code,
        sparse_deactivated_only=sparse_deactivated_only,
        vector_deactivated_only=vector_deactivated_only,
        delete_skipped_mismatch=delete_skipped_mismatch,
        legacy_sparse_tombstoned=legacy_sparse_tombstoned,
        legacy_vector_tombstoned=legacy_vector_tombstoned,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build command-line argument parser."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the cleanup (backup, delete, and tombstone). Default is dry-run.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Optional season filter (e.g. 2026).",
    )
    parser.add_argument(
        "--expect-count",
        type=int,
        default=None,
        help="Expected number of corrupted rows to back up. Required on --apply when new rows exist.",
    )
    parser.add_argument(
        "--confirm-target",
        action="append",
        dest="confirm_target",
        default=None,
        help="Confirmed target database hostname (repeatable, required on --apply).",
    )
    return parser


def main(argv: Sequence[str] | None = None, out: TextIO = sys.stdout) -> int:
    """Execute the milestone cleanup CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    from src.db.engine import DATABASE_URL as SOURCE_DB_URL

    # The URL the session factory actually connects to, not a possibly different raw env value.
    source_url = SOURCE_DB_URL or ""
    rag_index_url = os.getenv("RAG_INDEX_DB_URL", "")
    pgvector_url = os.getenv("PGVECTOR_URL", "")
    pgvector_test_url = os.getenv("PGVECTOR_TEST_URL", "")

    redacted_source = make_url(source_url).render_as_string(hide_password=True) if source_url else "(not configured)"
    redacted_sparse = (
        make_url(rag_index_url).render_as_string(hide_password=True) if rag_index_url else "(same as source)"
    )
    redacted_vector = (
        make_url(pgvector_url).render_as_string(hide_password=True) if pgvector_url else "(not configured)"
    )

    out.write(f"source DB = {redacted_source}\n")
    out.write(f"sparse index = {redacted_sparse}\n")
    out.write(f"vector = {redacted_vector}\n")

    if pgvector_test_url and pgvector_test_url != (pgvector_url or ""):
        if args.apply:
            out.write(
                "Refusing write: PGVECTOR_TEST_URL is set and would override PGVECTOR_URL in "
                "src.db.vector_engine; unset it\n"
            )
            return 3
        out.write("WARNING: PGVECTOR_TEST_URL is set and would override PGVECTOR_URL in src.db.vector_engine\n")

    if args.apply:
        target_urls = {
            "source": source_url,
            "sparse": rag_index_url,
            "vector": pgvector_url,
        }
        confirm_err = check_confirm_target(target_urls, args.confirm_target)
        if confirm_err is not None:
            out.write(f"Refusing write: {confirm_err}\n")
            return 3

    from src.db.engine import get_db_session, get_rag_index_session

    effective_vector_url = pgvector_url or pgvector_test_url

    with get_db_session() as source_session, get_rag_index_session() as index_session:
        if effective_vector_url:
            from src.db.vector_engine import get_vector_session

            with get_vector_session() as vector_session:
                report = run_cleanup(
                    source_session,
                    index_session,
                    vector_session,
                    apply=args.apply,
                    season=args.season,
                    expect_count=args.expect_count,
                    out=out,
                )
        else:
            report = run_cleanup(
                source_session,
                index_session,
                None,
                apply=args.apply,
                season=args.season,
                expect_count=args.expect_count,
                out=out,
            )

    return report.status_code


if __name__ == "__main__":
    sys.exit(main())
