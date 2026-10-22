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

New Seasons After Cleanup:
    Once ANY backup row exists, a season that is not in the backup table is NOT selected by default.
    Instead, it is reported as skipped (`mode=skipped-new-season`). Rows created after the cleanup
    are not treated as corrupted baseline data; cleaning a new season requires explicitly passing
    both `--season <YEAR>` and `--allow-new-season`.
    Why: This is the safest approach because it avoids any time-zone-sensitive `created_at` cutoff
    heuristics across different database servers and environments. An operator must explicitly name
    the season to target it for cleanup.

Rollback:
    If a rollback is required, execute the following steps:
    1. Restore rows per season from the backup table with RESTORE_ROWS_SQL (bind :season). It is an
       INSERT ... SELECT ... WHERE NOT EXISTS, so a backed-up row whose id or natural key
       (season, player_id, milestone_category) was re-created by a re-crawl is skipped instead of failing
       on uq_player_milestone.
    2. Reactivate chunks on sparse and vector stores using `reactivate_chunks`:
       ```python
       reactivate_chunks(source_session, index_session, season, "sparse")
       reactivate_chunks(source_session, vector_session, season, "vector")
       ```
       `reactivate_chunks` restores each chunk to its exact recorded prior index_status, is_active,
       and valid_to state from `player_milestones_cleanup_chunk_state` (including chunks whose prior
       status was already DELETED/TOMBSTONED). In the vector store, it also strips the
       `kbo_cleanup_tombstoned` marker from `meta`. Upon successful chunk restoration, its state record
       is deleted from `player_milestones_cleanup_chunk_state` so subsequent cleanup/restore cycles record fresh state.
    3. Re-index knowledge per season:
       ```bash
       python -m src.cli.rag.index_rag_knowledge --season 2025
       python -m src.cli.rag.index_rag_knowledge --season 2026
       ```
       (Since the scheduled indexer only processes the current season, 2025 included).

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
    # to back up (353 in production on the first run); --confirm-target must name every target identity
    # printed for the source / sparse / vector targets (repeat the flag for several targets):
    python3 -m scripts.maintenance.cleanup_corrupted_milestones --apply --expect-count 353 \
        --confirm-target <source-db-host>:5432/<db> --confirm-target <pgvector-host>:5432/<db>

    # With RAG_TARGET_ENV=production the existing RAG write guard also applies:
    RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 python3 -m scripts.maintenance.cleanup_corrupted_milestones \
        --apply --expect-count 353 --confirm-target <source-db-host>:5432/<db> --confirm-target <pgvector-host>:5432/<db>

    Production guard: RAG_TARGET_ENV defaults to "local" when unset (env.example sets "staging"), and only
    RAG_TARGET_ENV=production enforces RAG_INDEX_ALLOW_PRODUCTION_WRITE=1. Do not rely on it; the guards that
    always apply on --apply are --confirm-target and --expect-count. PGVECTOR_TEST_URL overrides PGVECTOR_URL in
    src.db.vector_engine, so --apply refuses while it is set to a different URL.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
from src.services.rag_vector_reactivation import CLEANUP_TOMBSTONE_META_KEY
from src.services.rag_vector_tombstone import split_source_key

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine import RowMapping
    from sqlalchemy.orm import Session

#: Per-key store failures that are reported and counted (exit 1) instead of aborting the run.
_STORE_ERRORS = (SQLAlchemyError, ValueError)


def _vector_meta_set_marker_sql(dialect_name: str) -> str:
    """Return dialect-aware SQL fragment to set the cleanup tombstone marker in meta."""
    if dialect_name == "postgresql":
        return f"meta = COALESCE(meta, '{{}}'::jsonb) || jsonb_build_object('{CLEANUP_TOMBSTONE_META_KEY}', true)"
    return f"meta = json_set(COALESCE(meta, '{{}}'), '$.{CLEANUP_TOMBSTONE_META_KEY}', json('true'))"


def _vector_meta_remove_marker_sql(dialect_name: str) -> str:
    """Return dialect-aware SQL fragment to remove the cleanup tombstone marker from meta."""
    if dialect_name == "postgresql":
        return f"meta = meta - '{CLEANUP_TOMBSTONE_META_KEY}'"
    return f"meta = json_remove(meta, '$.{CLEANUP_TOMBSTONE_META_KEY}')"


BACKUP_TABLE_NAME = "player_milestones_cleanup_backup"
CHUNK_STATE_TABLE_NAME = "player_milestones_cleanup_chunk_state"
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

chunk_state_table = Table(
    CHUNK_STATE_TABLE_NAME,
    backup_metadata,
    Column("store", String(10), primary_key=True),
    Column("chunk_ref", String(255), primary_key=True),
    Column("source_row_id", String(255), nullable=True),
    Column("prior_index_status", String(24), nullable=True),
    Column("prior_is_active", Integer, nullable=True),
    Column("prior_valid_to", DateTime, nullable=True),
    Column("recorded_at", DateTime, nullable=False),
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
    legacy_vector_deactivated_only: int = 0

    @property
    def exit_code(self) -> int:
        """Alias for status_code."""
        return self.status_code

    @property
    def candidates(self) -> int:
        """Alias for selected candidate count."""
        return self.selected


def _find_restored_row_ids(source_session: Session, season: int) -> list[str]:
    """Identify restored milestone row IDs for a given season."""
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

    restored_row_ids: list[str] = []
    for b in backup_rows:
        pid = str(b.player_id).strip() if b.player_id is not None else "UNKNOWN"
        k = (b.season, pid, b.milestone_category.strip())
        live_ids = live_by_key.get(k, [])
        if len(live_ids) == 1 and live_ids[0] == b.id:
            key_part = b.rag_source_key.partition(":")[2]
            if not key_part:
                key_part = stable_milestone_source_row_id(b.season, b.player_id, b.milestone_category)
            restored_row_ids.append(key_part)

    return restored_row_ids


def restore_chunk_targets(
    source_session: Session,
    season: int,
    store: str,
    out: TextIO | None = None,
) -> list[tuple[str, str | None, int | None, datetime | None]]:
    """Return target tuples for chunk reactivation for restored rows of a season.

    Identifies backup rows of that season whose natural key exists exactly once in
    player_milestones with the same id as in the backup table. For each restored row,
    looks up the recorded prior state in `player_milestones_cleanup_chunk_state` for
    the given store. Chunks whose prior status was already DELETED or TOMBSTONED are
    skipped, and chunks with no recorded prior state are skipped and listed.

    Args:
        source_session: Operational database session.
        season: Target season.
        store: Store identifier ('sparse' or 'vector').
        out: Optional output stream to list skipped chunks.

    Returns:
        List of tuples: (source_row_id, prior_index_status, prior_is_active, prior_valid_to).

    """
    bind = source_session.connection()
    insp = inspect(bind)
    if not insp.has_table(BACKUP_TABLE_NAME) or not insp.has_table(CHUNK_STATE_TABLE_NAME):
        return []

    restored_row_ids = _find_restored_row_ids(source_session, season)
    if not restored_row_ids:
        return []

    state_rows = (
        source_session.execute(
            select(
                chunk_state_table.c.chunk_ref,
                chunk_state_table.c.source_row_id,
                chunk_state_table.c.prior_index_status,
                chunk_state_table.c.prior_is_active,
                chunk_state_table.c.prior_valid_to,
            ).where(
                chunk_state_table.c.store == store,
                chunk_state_table.c.chunk_ref.in_(restored_row_ids),
            )
        )
        .mappings()
        .all()
    )

    state_by_ref = {r["chunk_ref"]: r for r in state_rows}

    targets: list[tuple[str, str | None, int | None, datetime | None]] = []
    for row_id in restored_row_ids:
        state = state_by_ref.get(row_id)
        if state is None:
            if out is not None:
                out.write(f"restore_chunk_targets: skipping {row_id} (no recorded state in {store})\n")
            continue

        prior_status = state["prior_index_status"]
        targets.append(
            (
                row_id,
                prior_status,
                state["prior_is_active"],
                state["prior_valid_to"],
            )
        )

    return targets


def reactivate_chunks(
    source_session: Session,
    store_session: Session,
    season: int,
    store: str,
    out: TextIO = sys.stdout,
) -> int:
    """Reactivate chunks for restored rows of a season in the specified store.

    Restores index_status to the prior status and, when the columns exist in that store,
    is_active and valid_to to the recorded prior values (NULL if prior value was NULL).
    In the vector store, also strips the cleanup tombstone marker from meta.
    Commits per row, and upon success deletes the recorded chunk_state row.

    Args:
        source_session: Operational database session holding the backup and chunk state tables.
        store_session: Database session for the store to reactivate (sparse or vector).
        season: Target season.
        store: Target store ('sparse' or 'vector').
        out: Output stream for logging progress.

    Returns:
        Number of reactivated chunks.

    """
    targets = restore_chunk_targets(source_session, season, store, out=out)
    if not targets:
        return 0

    bind = store_session.connection()
    cols = {col["name"] for col in inspect(bind).get_columns("rag_chunks")}
    has_is_active = "is_active" in cols
    has_valid_to = "valid_to" in cols

    count = 0
    for source_row_id, prior_index_status, prior_is_active, prior_valid_to in targets:
        set_clauses = ["index_status = :index_status"]
        params: dict[str, Any] = {
            "table": "milestone",
            "row_id": source_row_id,
            "index_status": prior_index_status or "ACTIVE",
        }
        if has_is_active:
            set_clauses.append("is_active = :is_active")
            # Bind a real boolean: PostgreSQL has no implicit integer -> boolean assignment cast.
            params["is_active"] = None if prior_is_active is None else bool(prior_is_active)
        if has_valid_to:
            set_clauses.append("valid_to = :valid_to")
            params["valid_to"] = prior_valid_to
        if store == "vector":
            dialect_name = str(getattr(getattr(store_session.get_bind(), "dialect", None), "name", "postgresql"))
            set_clauses.append(_vector_meta_remove_marker_sql(dialect_name))

        stmt = text(
            f"UPDATE rag_chunks SET {', '.join(set_clauses)} WHERE source_table = :table AND source_row_id = :row_id"
        )
        try:
            res = store_session.execute(stmt, params)
            store_session.commit()
            if res.rowcount and res.rowcount > 0:
                count += 1
                try:
                    source_session.execute(
                        delete(chunk_state_table).where(
                            chunk_state_table.c.store == store,
                            chunk_state_table.c.chunk_ref == source_row_id,
                        )
                    )
                    source_session.commit()
                except _STORE_ERRORS as err:
                    source_session.rollback()
                    out.write(f"Error removing chunk state for {source_row_id} in {store}: {err}\n")
            else:
                out.write(f"reactivate_chunks: chunk {source_row_id} not found in {store} store\n")
        except _STORE_ERRORS as err:
            store_session.rollback()
            out.write(f"Error reactivating chunk {source_row_id} in {store}: {err}\n")

    return count


def url_identity(url: str) -> str | None:
    """Derive the target identity string for a connection URL.

    Identity string is `host:port/database` (with default port 5432 for postgresql*
    when port is omitted; otherwise ':port' is omitted if no port is specified),
    or `<drivername>:<database>` for host-less URLs such as SQLite.

    Args:
        url: Connection URL string.

    Returns:
        Identity string, or None if URL cannot be parsed.

    """
    if not url:
        return None
    try:
        parsed = make_url(url)
    except ArgumentError, ValueError:
        return None

    if parsed.host:
        if parsed.port is not None:
            port_part = f":{parsed.port}"
        elif parsed.drivername.startswith("postgresql"):
            port_part = ":5432"
        else:
            port_part = ""
        database_part = parsed.database or ""
        return f"{parsed.host}{port_part}/{database_part}"

    database_part = parsed.database or ""
    return f"{parsed.drivername}:{database_part}"


def check_confirm_target(urls: dict[str, str], confirmed: list[str] | None) -> str | None:
    """Validate that all target identities among configured write-target URLs are confirmed.

    Args:
        urls: Mapping of target names to connection URL strings.
        confirmed: List of confirmed target identities provided via --confirm-target.

    Returns:
        Error message string listing expected identities if confirmation fails, None if valid.

    """
    expected_identities: set[str] = set()
    for url in urls.values():
        if not url:
            continue
        ident = url_identity(url)
        if ident:
            expected_identities.add(ident)

    if not expected_identities:
        return None

    confirmed_set = set(confirmed or [])
    missing = expected_identities - confirmed_set
    if missing:
        expected_str = ", ".join(sorted(expected_identities))
        missing_str = ", ".join(sorted(missing))
        return f"--apply requires target confirmation; unconfirmed target(s): {missing_str}; expected: {expected_str}"
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
    """Check that backup and chunk state tables have expected columns if they exist.

    Args:
        session: Active database session.

    Returns:
        Error message string if schema is incompatible, None otherwise.

    """
    bind = session.connection()
    insp = inspect(bind)
    if insp.has_table(BACKUP_TABLE_NAME):
        existing_cols = {col["name"] for col in insp.get_columns(BACKUP_TABLE_NAME)}
        expected_cols = [col.name for col in backup_table.c]
        missing_cols = [col for col in expected_cols if col not in existing_cols]
        if missing_cols:
            return (
                f"existing {BACKUP_TABLE_NAME} has an incompatible schema; "
                f"missing columns: {', '.join(missing_cols)}; rename or drop it after checking its contents"
            )
    if insp.has_table(CHUNK_STATE_TABLE_NAME):
        existing_cols = {col["name"] for col in insp.get_columns(CHUNK_STATE_TABLE_NAME)}
        expected_cols = [col.name for col in chunk_state_table.c]
        missing_cols = [col for col in expected_cols if col not in existing_cols]
        if missing_cols:
            return (
                f"existing {CHUNK_STATE_TABLE_NAME} has an incompatible schema; "
                f"missing columns: {', '.join(missing_cols)}; rename or drop it after checking its contents"
            )
    return None


def _record_chunk_prior_state(
    source_session: Session,
    *,
    store: str,
    chunk_ref: str,
    source_row_id: str | None,
    prior_index_status: str | None,
    prior_is_active: int | None,
    prior_valid_to: datetime | str | None,
) -> None:
    """Insert current state of a chunk into player_milestones_cleanup_chunk_state if not present.

    Never overwrites an existing row to preserve the pre-cleanup state across reruns.
    Commits the state row immediately before any chunk write.

    Args:
        source_session: Operational database session.
        store: Store identifier ('sparse' or 'vector').
        chunk_ref: Chunk reference string (source_row_id or legacy:id=<id>).
        source_row_id: Source row ID string, or None for legacy empty-key chunks.
        prior_index_status: Pre-cleanup index_status value.
        prior_is_active: Pre-cleanup is_active value (as integer 1/0, or None).
        prior_valid_to: Pre-cleanup valid_to datetime, string, or None.

    """
    existing = source_session.scalar(
        select(chunk_state_table.c.store).where(
            chunk_state_table.c.store == store,
            chunk_state_table.c.chunk_ref == chunk_ref,
        )
    )
    if existing is not None:
        return

    parsed_valid_to: datetime | None = None
    if isinstance(prior_valid_to, datetime):
        parsed_valid_to = prior_valid_to
    elif isinstance(prior_valid_to, date):
        parsed_valid_to = datetime.combine(prior_valid_to, datetime.min.time(), tzinfo=UTC)
    elif isinstance(prior_valid_to, str):
        try:
            parsed_valid_to = datetime.fromisoformat(prior_valid_to.replace(" ", "T"))
        except ValueError:
            parsed_valid_to = None

    source_session.execute(
        insert(chunk_state_table).values(
            store=store,
            chunk_ref=chunk_ref,
            source_row_id=source_row_id,
            prior_index_status=prior_index_status,
            prior_is_active=prior_is_active,
            prior_valid_to=parsed_valid_to,
            recorded_at=datetime.now(UTC),
        )
    )
    source_session.commit()


def _resolve_target_set(
    source_session: Session,
    season: int | None,
    out: TextIO,
    *,
    allow_new_season: bool = False,
) -> tuple[list[CorruptedMilestoneItem], set[int], int, int, set[int]]:
    """Resolve the per-season target set of corrupted milestones to process.

    Args:
        source_session: Operational database session.
        season: Optional season filter.
        out: Output stream for logging progress.
        allow_new_season: True to permit cleaning a season not present in the backup.

    Returns:
        Tuple of (target_items, existing_backup_ids, already_backed_up, would_back_up, processed_seasons).

    """
    bind = source_session.connection()
    has_backup = inspect(bind).has_table(BACKUP_TABLE_NAME)

    processed_seasons: set[int] = set()
    if has_backup:
        processed_seasons = set(source_session.scalars(select(backup_table.c.season).distinct()).all())

    has_any_backup = len(processed_seasons) > 0

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
            if has_any_backup and not (allow_new_season and season == s):
                out.write(
                    f"season={s} mode=skipped-new-season rows={len(raw_rows)} "
                    f"(rows created after the cleanup are not treated as corrupted; "
                    f"pass --allow-new-season with --season {s} to clean it explicitly)\n"
                )
                continue

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
) -> tuple[int, int, int, set[int], str | None]:
    """Execute backup table creation, new row insertion, and true-match deletion.

    Args:
        source_session: Operational database session.
        target_items: Target items to back up and delete.
        processed_seasons: Seasons that were already processed in backup table.
        existing_ids: IDs already present in backup table.
        out: Output stream for logging progress.

    Returns:
        Tuple of (backed_up_new, deleted, delete_skipped_mismatch, deleted_ids, failed_step).

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
        try:
            source_session.execute(insert(backup_table), backup_dicts)
            source_session.commit()
            backed_up_new = len(items_to_backup)
        except _STORE_ERRORS as err:
            source_session.rollback()
            out.write(f"ERROR: backup step failed: {err}; no RAG chunks were changed; rerun to finish\n")
            return 0, 0, 0, set(), "backup"

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
        try:
            source_session.execute(del_stmt)
            source_session.commit()
            deleted = len(true_match_ids)
        except _STORE_ERRORS as err:
            source_session.rollback()
            out.write(f"ERROR: delete step failed: {err}; no RAG chunks were changed; rerun to finish\n")
            return backed_up_new, 0, delete_skipped_mismatch, set(), "delete"

    return backed_up_new, deleted, delete_skipped_mismatch, set(true_match_ids), None


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


def _record_sparse_chunk_states(
    source_session: Session,
    index_session: Session,
    keys: tuple[str, ...],
    *,
    has_is_active: bool,
    has_valid_to: bool,
    out: TextIO,
) -> bool:
    """Record pre-cleanup state for sparse chunks before modifying them."""
    has_error = False
    for key in keys:
        try:
            src_table, src_row_id = split_source_key(key)
        except ValueError:
            continue

        select_cols = ["index_status"]
        if has_is_active:
            select_cols.append("is_active")
        if has_valid_to:
            select_cols.append("valid_to")
        stmt = text(
            f"SELECT {', '.join(select_cols)} FROM rag_chunks WHERE source_table = :table AND source_row_id = :row_id"
        )
        try:
            row = index_session.execute(stmt, {"table": src_table, "row_id": src_row_id}).mappings().first()
            if row is not None:
                prior_st = str(row["index_status"]) if row["index_status"] is not None else None
                prior_act = int(row["is_active"]) if (has_is_active and row["is_active"] is not None) else None
                prior_val = row["valid_to"] if has_valid_to else None
                _record_chunk_prior_state(
                    source_session,
                    store="sparse",
                    chunk_ref=src_row_id,
                    source_row_id=src_row_id,
                    prior_index_status=prior_st,
                    prior_is_active=prior_act,
                    prior_valid_to=prior_val,
                )
        except _STORE_ERRORS as err:
            index_session.rollback()
            source_session.rollback()
            has_error = True
            out.write(f"Sparse store error reading prior state for {key}: {err}\n")
    return has_error


def _execute_sparse_tombstones(
    index_session: Session,
    keys: tuple[str, ...],
    *,
    has_is_active: bool,
    has_valid_to: bool,
    out: TextIO,
) -> tuple[int, int, int, int, bool]:
    """Execute tombstone and deactivation operations on sparse chunks."""
    results = tombstone_primary_rows(index_session, keys)
    tombstoned = sum(
        1
        for r in results
        if r.found and r.final_status == "DELETED" and r.previous_status not in ("DELETED", "TOMBSTONED")
    )
    already = sum(1 for r in results if r.found and r.previous_status in ("DELETED", "TOMBSTONED"))
    missing = sum(1 for r in results if not r.found)

    has_error = False
    for r in results:
        if r.error:
            has_error = True
            out.write(f"Sparse store error on {r.source_key}: {r.error}\n")

    deactivated_only = 0
    if has_is_active or has_valid_to:
        for key in keys:
            deact_count, err = _deactivate_sparse_key(
                index_session, key, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
            )
            deactivated_only += deact_count
            if err:
                has_error = True

    return tombstoned, already, missing, deactivated_only, has_error


def _process_sparse_store(
    source_session: Session,
    index_session: Session,
    keys: tuple[str, ...],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, int, bool]:
    """Inspect or tombstone sparse RAG index chunks.

    Args:
        source_session: Operational database session for chunk state ledger.
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
    tombstoned = 0
    already = 0
    missing = 0
    active = 0

    if apply:
        if _record_sparse_chunk_states(
            source_session, index_session, keys, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
        ):
            has_error = True

        try:
            tombstoned, already, missing, deactivated_only, tombstone_err = _execute_sparse_tombstones(
                index_session, keys, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
            )
            if tombstone_err:
                has_error = True
        except _STORE_ERRORS as err:
            index_session.rollback()
            out.write(f"ERROR: sparse store step failed: {err}; vector step still attempted; rerun to finish\n")
            has_error = True
    else:
        results = inspect_primary_tombstones(index_session, keys)
        active = sum(1 for r in results if r.found and r.final_status not in ("DELETED", "TOMBSTONED"))
        already = sum(1 for r in results if r.found and r.final_status in ("DELETED", "TOMBSTONED"))
        missing = sum(1 for r in results if not r.found)
        for r in results:
            if r.error:
                has_error = True
                out.write(f"Sparse store error on {r.source_key}: {r.error}\n")

    return active, tombstoned, already, missing, deactivated_only, has_error


def _build_vector_update_sql(
    *,
    has_is_active: bool,
    has_valid_to: bool,
    dialect_name: str = "postgresql",
) -> str:
    """Build update SQL statement for vector tombstone."""
    set_clauses = ["index_status = 'DELETED'"]
    if has_is_active:
        set_clauses.append("is_active = false")
    if has_valid_to:
        set_clauses.append("valid_to = COALESCE(valid_to, CURRENT_TIMESTAMP)")
    if has_is_active or has_valid_to:
        set_clauses.append(_vector_meta_set_marker_sql(dialect_name))

    where_clauses = ["source_table = :table", "source_row_id = :row_id"]
    if has_is_active:
        where_clauses.append("(index_status <> 'DELETED' OR is_active IS NULL OR is_active = :true_val)")
    else:
        where_clauses.append("index_status <> 'DELETED'")

    return f"UPDATE rag_chunks SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}"


def _fetch_vector_chunk_row(
    session: Session,
    source_table: str,
    source_row_id: str,
    *,
    has_is_active: bool,
    has_valid_to: bool,
    out: TextIO,
) -> tuple[RowMapping | None, bool]:
    """Fetch a single vector chunk row mapping."""
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
        return row, False
    except _STORE_ERRORS as err:
        session.rollback()
        out.write(f"Vector store error on {source_table}:{source_row_id}: {err}\n")
        return None, True


def _tombstone_one_vector_key(
    source_session: Session,
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

    row, fetch_err = _fetch_vector_chunk_row(
        session, source_table, source_row_id, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
    )
    if fetch_err:
        return 0, 0, 0, 0, True
    if row is None:
        return 0, 0, 0, 1, False

    curr_status = str(row["index_status"] or "")
    is_act = row.get("is_active") if has_is_active else None
    is_active_true = is_act is None or bool(is_act) is True
    prior_is_act = int(is_act) if is_act is not None else None
    prior_val_to = row.get("valid_to") if has_valid_to else None

    try:
        _record_chunk_prior_state(
            source_session,
            store="vector",
            chunk_ref=source_row_id,
            source_row_id=source_row_id,
            prior_index_status=curr_status,
            prior_is_active=prior_is_act,
            prior_valid_to=prior_val_to,
        )
    except _STORE_ERRORS as err:
        source_session.rollback()
        out.write(f"Error recording vector chunk prior state on {key}: {err}\n")
        return 0, 0, 0, 0, True

    if curr_status == "DELETED" and (not has_is_active or not is_active_true):
        return 0, 0, 1, 0, False

    dialect_name = str(getattr(getattr(session.get_bind(), "dialect", None), "name", "postgresql"))
    update_sql = _build_vector_update_sql(
        has_is_active=has_is_active, has_valid_to=has_valid_to, dialect_name=dialect_name
    )
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
    source_session: Session,
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
        source_session: Operational database session for chunk state ledger.
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
            source_session, session, key, has_is_active=has_is_active, has_valid_to=has_valid_to, out=out
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
    source_session: Session,
    vector_session: Session | None,
    keys: tuple[str, ...],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, int, bool, bool]:
    """Inspect or tombstone dense vector store chunks.

    Args:
        source_session: Operational database session for chunk state ledger.
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
            source_session, vector_session, keys, out=out
        )
        return 0, tombstoned, already, missing, deactivated_only, False, has_error

    active, deleted_still_active, already, missing, has_error = _inspect_vector_keys(vector_session, keys, out=out)
    return active, 0, already, missing, deleted_still_active, False, has_error


def _legacy_pair_decision(
    meta: object,
    target_pairs: set[tuple[str, str]],
    surviving_pairs: set[tuple[str, str]],
) -> tuple[bool, str]:
    """Evaluate whether a chunk's meta matches corrupted targets without surviving rows.

    Args:
        meta: Metadata dict, JSON string, or None/other.
        target_pairs: Set of (player_name, milestone_category) pairs from target candidates.
        surviving_pairs: Set of (player_name, milestone_category) pairs from surviving live rows.

    Returns:
        Tuple of (matched, reason).

    """
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError, ValueError:
            return False, "no usable meta"
    if not isinstance(meta, dict):
        return False, "no usable meta"

    pname = str(meta.get("player_name", "")).strip()
    mcat = str(meta.get("milestone_category", "")).strip()
    pair = (pname, mcat)

    if pair not in target_pairs:
        return False, f"player_name={pname!r}, milestone_category={mcat!r} not in target set"
    if pair in surviving_pairs:
        return False, f"live player_milestones row exists for player_name={pname!r}, milestone_category={mcat!r}"
    return True, "matched"


def _find_and_filter_legacy_chunks(
    index_session: Session,
    target_pairs: set[tuple[str, str]],
    surviving_pairs: set[tuple[str, str]],
    out: TextIO,
) -> tuple[list[RagChunk], int, int]:
    """Find and evaluate legacy empty-key chunks in sparse store.

    Args:
        index_session: Sparse RAG index database session.
        target_pairs: Set of (player_name, milestone_category) target pairs.
        surviving_pairs: Set of (player_name, milestone_category) surviving live pairs.
        out: Output stream for logging progress.

    Returns:
        Tuple of (matched_chunks, sparse_found, active_matched).

    """
    sparse_chunks = list(
        index_session.scalars(
            select(RagChunk).where(
                RagChunk.source_table == "milestone",
                (RagChunk.source_row_id == "") | (RagChunk.source_row_id.is_(None)),
            )
        ).all()
    )

    matched_chunks: list[RagChunk] = []
    active_matched = 0
    for chunk in sparse_chunks:
        matched, reason = _legacy_pair_decision(chunk.meta, target_pairs, surviving_pairs)
        if not matched:
            out.write(f"legacy empty-key chunk unmatched: {reason}\n")
            continue
        matched_chunks.append(chunk)
        if chunk.index_status != "DELETED":
            active_matched += 1

    return matched_chunks, len(sparse_chunks), active_matched


def _record_legacy_sparse_prior_state(
    source_session: Session,
    index_session: Session,
    matched_chunks: Sequence[RagChunk],
    scols: set[str],
    out: TextIO,
) -> bool:
    """Record pre-cleanup state for matched legacy sparse chunks."""
    has_is_act = "is_active" in scols
    has_val_to = "valid_to" in scols
    has_error = False

    for chunk in matched_chunks:
        try:
            select_cols = ["index_status"]
            if has_is_act:
                select_cols.append("is_active")
            if has_val_to:
                select_cols.append("valid_to")
            stmt = text(f"SELECT {', '.join(select_cols)} FROM rag_chunks WHERE id = :id")
            row = index_session.execute(stmt, {"id": chunk.id}).mappings().first()
            if row is not None:
                p_status = str(row["index_status"] or "")
                p_act = int(row["is_active"]) if (has_is_act and row["is_active"] is not None) else None
                p_val = row["valid_to"] if has_val_to else None
                _record_chunk_prior_state(
                    source_session,
                    store="sparse",
                    chunk_ref=f"legacy:id={chunk.id}",
                    source_row_id=None,
                    prior_index_status=p_status,
                    prior_is_active=p_act,
                    prior_valid_to=p_val,
                )
        except _STORE_ERRORS as err:
            index_session.rollback()
            source_session.rollback()
            has_error = True
            out.write(f"Error recording legacy sparse chunk prior state: {err}\n")
    return has_error


def _retire_legacy_sparse_chunks(
    source_session: Session,
    index_session: Session,
    matched_chunks: Sequence[RagChunk],
    out: TextIO,
) -> tuple[int, bool]:
    """Retire matched legacy chunks in sparse store."""
    sbind = index_session.connection()
    scols = {col["name"] for col in inspect(sbind).get_columns("rag_chunks")}
    has_is_act = "is_active" in scols
    has_val_to = "valid_to" in scols

    has_error = _record_legacy_sparse_prior_state(source_session, index_session, matched_chunks, scols, out)
    legacy_sparse_tombstoned = 0

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
                out.write(f"ERROR: sparse store step failed: {err}; vector step still attempted; rerun to finish\n")

    if not matched_chunks:
        return legacy_sparse_tombstoned, has_error
    matched_ids = [chunk.id for chunk in matched_chunks]
    if has_is_act or has_val_to:
        set_c = []
        if has_is_act:
            set_c.append("is_active = false")
        if has_val_to:
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
            out.write(f"ERROR: sparse store step failed: {err}; vector step still attempted; rerun to finish\n")

    return legacy_sparse_tombstoned, has_error


def _inspect_legacy_vector_chunks(
    vector_session: Session,
    vcols: set[str],
    target_pairs: set[tuple[str, str]],
    surviving_pairs: set[tuple[str, str]],
    out: TextIO,
) -> tuple[int, int, list[RowMapping], bool]:
    """Inspect and match legacy empty-key vector chunks.

    Args:
        vector_session: Dense vector database session.
        vcols: Set of existing rag_chunks column names.
        target_pairs: Set of (player_name, milestone_category) target pairs.
        surviving_pairs: Set of (player_name, milestone_category) surviving live pairs.
        out: Output stream for logging progress.

    Returns:
        Tuple of (vector_found, vector_matched, rows_needing_work, has_error).

    """
    has_is_act = "is_active" in vcols
    vselect = ["id", "index_status", "meta"]
    if has_is_act:
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
        vector_session.rollback()
        out.write(f"Vector store error inspecting legacy chunks: {err}\n")
        return 0, 0, [], True

    vector_found = len(vector_rows)
    matched_rows: list[RowMapping] = []
    for row in vector_rows:
        row_id = row["id"]
        meta_val = row["meta"]
        matched, reason = _legacy_pair_decision(meta_val, target_pairs, surviving_pairs)
        if not matched:
            out.write(f"legacy empty-key vector chunk id={row_id} unmatched: {reason}\n")
            continue
        matched_rows.append(row)

    rows_needing_work: list[RowMapping] = []
    for r in matched_rows:
        curr_status = str(r["index_status"] or "")
        is_act = r.get("is_active") if has_is_act else None
        if curr_status != "DELETED" or (has_is_act and (is_act is None or bool(is_act) is True)):
            rows_needing_work.append(r)

    return vector_found, len(matched_rows), rows_needing_work, False


def _retire_legacy_vector_chunks(
    source_session: Session,
    vector_session: Session,
    rows: Sequence[RowMapping],
    vcols: set[str],
    out: TextIO,
) -> tuple[int, int, bool]:
    """Retire matched legacy empty-key vector rows that need work.

    Args:
        source_session: Operational database session for chunk state ledger.
        vector_session: Dense vector database session.
        rows: Sequence of matched vector rows needing work.
        vcols: Set of existing rag_chunks column names.
        out: Output stream for logging progress.

    Returns:
        Tuple of (legacy_vector_tombstoned, legacy_vector_deactivated_only, has_error).

    """
    has_is_act = "is_active" in vcols
    has_val_to = "valid_to" in vcols

    dialect_name = str(getattr(getattr(vector_session.get_bind(), "dialect", None), "name", "postgresql"))
    set_clauses = ["index_status = 'DELETED'"]
    if has_is_act:
        set_clauses.append("is_active = false")
    if has_val_to:
        set_clauses.append("valid_to = COALESCE(valid_to, CURRENT_TIMESTAMP)")
    if has_is_act or has_val_to:
        set_clauses.append(_vector_meta_set_marker_sql(dialect_name))
    update_sql = text(f"UPDATE rag_chunks SET {', '.join(set_clauses)} WHERE id = :id")

    legacy_vector_tombstoned = 0
    legacy_vector_deactivated_only = 0
    has_error = False

    for row in rows:
        row_id = row["id"]
        curr_status = str(row["index_status"] or "")
        p_act = int(row["is_active"]) if (has_is_act and row.get("is_active") is not None) else None
        p_val = row.get("valid_to") if has_val_to else None

        try:
            _record_chunk_prior_state(
                source_session,
                store="vector",
                chunk_ref=f"legacy:id={row_id}",
                source_row_id=None,
                prior_index_status=curr_status,
                prior_is_active=p_act,
                prior_valid_to=p_val,
            )
        except _STORE_ERRORS as err:
            source_session.rollback()
            has_error = True
            out.write(f"Error recording legacy vector chunk prior state for id={row_id}: {err}\n")
            continue

        try:
            vector_session.execute(update_sql, {"id": row_id})
            vector_session.commit()
            if curr_status != "DELETED":
                legacy_vector_tombstoned += 1
            else:
                legacy_vector_deactivated_only += 1
        except _STORE_ERRORS as err:
            vector_session.rollback()
            has_error = True
            out.write(f"Vector store error tombstoning legacy chunk id={row_id}: {err}\n")

    return legacy_vector_tombstoned, legacy_vector_deactivated_only, has_error


def _process_legacy_vector_chunks(
    source_session: Session,
    vector_session: Session | None,
    target_pairs: set[tuple[str, str]],
    surviving_pairs: set[tuple[str, str]],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, int, bool]:
    """Inspect and tombstone legacy empty-key chunks in dense vector store.

    Args:
        source_session: Operational database session for chunk state ledger.
        vector_session: Dense vector database session or None.
        target_pairs: Set of (player_name, milestone_category) target pairs.
        surviving_pairs: Set of (player_name, milestone_category) surviving live pairs.
        apply: Whether write operations are enabled.
        out: Output stream for logging progress.

    Returns:
        Tuple of (vector_found, vector_matched, vector_active, legacy_vector_tombstoned,
        legacy_vector_deactivated_only, has_error).

    """
    if vector_session is None:
        return 0, 0, 0, 0, 0, False

    vbind = vector_session.connection()
    vcols = {col["name"] for col in inspect(vbind).get_columns("rag_chunks")}

    if "id" not in vcols or "meta" not in vcols:
        count_stmt = text(
            "SELECT COUNT(*) FROM rag_chunks "
            "WHERE source_table = 'milestone' AND (source_row_id = '' OR source_row_id IS NULL)"
        )
        try:
            count_val = int(vector_session.scalar(count_stmt) or 0)
        except _STORE_ERRORS as err:
            vector_session.rollback()
            out.write(f"Vector store error counting legacy chunks: {err}\n")
            return 0, 0, 0, 0, 0, True
        out.write(
            f"legacy vector chunks: found={count_val}, skipped "
            f"(vector rag_chunks has no meta/id column; cannot identify precisely)\n"
        )
        return count_val, 0, 0, 0, 0, False

    found, matched, rows_needing_work, inspect_err = _inspect_legacy_vector_chunks(
        vector_session, vcols, target_pairs, surviving_pairs, out
    )
    if inspect_err:
        return 0, 0, 0, 0, 0, True

    active = len(rows_needing_work)
    if not apply:
        return found, matched, active, 0, 0, False

    tombstoned, deact_only, tombstone_err = _retire_legacy_vector_chunks(
        source_session, vector_session, rows_needing_work, vcols, out
    )
    return found, matched, active, tombstoned, deact_only, tombstone_err


def _process_legacy_empty_chunks(
    source_session: Session,
    index_session: Session,
    vector_session: Session | None,
    target_items: Sequence[CorruptedMilestoneItem],
    deleted_live_ids: set[int],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, bool]:
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
        Tuple of (legacy_sparse_tombstoned, legacy_vector_tombstoned,
        legacy_vector_deactivated_only, has_error).

    """
    surviving_query = select(PlayerMilestone.player_name, PlayerMilestone.milestone_category)
    if deleted_live_ids:
        surviving_query = surviving_query.where(~PlayerMilestone.id.in_(deleted_live_ids))
    surviving_tuples = source_session.execute(surviving_query).all()
    surviving_pairs = {(str(pname).strip(), str(mcat).strip()) for pname, mcat in surviving_tuples}
    target_pairs = {(str(item.player_name).strip(), str(item.milestone_category).strip()) for item in target_items}

    legacy_sparse_tombstoned = 0
    sparse_error = False
    try:
        matched_chunks, sparse_found, sparse_active = _find_and_filter_legacy_chunks(
            index_session, target_pairs, surviving_pairs, out
        )
    except _STORE_ERRORS as err:
        index_session.rollback()
        out.write(f"ERROR: sparse store step failed: {err}; vector step still attempted; rerun to finish\n")
        matched_chunks, sparse_found, sparse_active = [], 0, 0
        sparse_error = True
    sparse_matched = len(matched_chunks)

    if not apply:
        (
            vector_found,
            vector_matched,
            vector_active,
            _,
            _,
            vector_error,
        ) = _process_legacy_vector_chunks(
            source_session,
            vector_session,
            target_pairs,
            surviving_pairs,
            apply=False,
            out=out,
        )
        out.write(
            f"legacy_empty_key_chunks: sparse_found={sparse_found} sparse_matched={sparse_matched} "
            f"sparse_active={sparse_active} vector_found={vector_found} vector_matched={vector_matched} "
            f"vector_active={vector_active}\n"
        )
        return 0, 0, 0, (sparse_error or vector_error)

    try:
        legacy_sparse_tombstoned, sparse_error = _retire_legacy_sparse_chunks(
            source_session, index_session, matched_chunks, out
        )
    except _STORE_ERRORS as err:
        index_session.rollback()
        out.write(f"ERROR: sparse store step failed: {err}; vector step still attempted; rerun to finish\n")
        sparse_error = True

    (
        vector_found,
        vector_matched,
        vector_active,
        legacy_vector_tombstoned,
        legacy_vector_deact,
        vector_error,
    ) = _process_legacy_vector_chunks(
        source_session,
        vector_session,
        target_pairs,
        surviving_pairs,
        apply=True,
        out=out,
    )

    return (
        legacy_sparse_tombstoned,
        legacy_vector_tombstoned,
        legacy_vector_deact,
        (sparse_error or vector_error),
    )


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
    allow_new_season: bool = False,
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
        allow_new_season: True to permit cleaning a season not in existing backup (requires season).
        out: Output stream for logging progress.

    Returns:
        CleanupReport summarizing counts and operation status.

    """
    if allow_new_season and season is None:
        err = "--allow-new-season requires --season"
        out.write(f"{err}\n")
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
            error_message=err,
        )

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
    ) = _resolve_target_set(source_session, season, out=out, allow_new_season=allow_new_season)

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
            failed_step,
        ) = _backup_and_delete(
            source_session,
            target_items,
            processed_seasons=processed_seasons,
            existing_ids=existing_backup_ids,
            out=out,
        )
        would_delete = deleted
        if failed_step is not None:
            mode_str = "apply"
            summary_line = (
                f"mode={mode_str} selected={len(target_items)} backed_up_new={backed_up_new} "
                f"deleted={deleted} sparse_tombstoned=0 "
                f"sparse_deactivated_only=0 "
                f"sparse_already=0 sparse_missing=0 "
                f"vector_tombstoned=0 "
                f"vector_deactivated_only=0 "
                f"vector_already=0 vector_missing=0 "
                f"legacy_sparse_tombstoned=0 "
                f"legacy_vector_tombstoned=0 "
                f"legacy_vector_deactivated_only=0\n"
            )
            out.write(summary_line)
            return CleanupReport(
                mode=mode_str,
                selected=len(target_items),
                already_backed_up=already_backed_up,
                would_back_up=would_back_up,
                backed_up_new=backed_up_new,
                would_delete=would_delete,
                deleted=deleted,
                sparse_active=0,
                sparse_tombstoned=0,
                sparse_already=0,
                sparse_missing=0,
                vector_active=0,
                vector_tombstoned=0,
                vector_already=0,
                vector_missing=0,
                vector_skipped=vector_session is None,
                status_code=1,
                error_message=f"{failed_step} step failed",
                sparse_deactivated_only=0,
                vector_deactivated_only=0,
                delete_skipped_mismatch=delete_skipped_mismatch,
                legacy_sparse_tombstoned=0,
                legacy_vector_tombstoned=0,
                legacy_vector_deactivated_only=0,
            )
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

    try:
        (
            sparse_active,
            sparse_tombstoned,
            sparse_already,
            sparse_missing,
            sparse_deactivated_only,
            sparse_error,
        ) = _process_sparse_store(
            source_session,
            index_session,
            keys_tuple,
            apply=apply,
            out=out,
        )
    except _STORE_ERRORS as err:
        # Any sparse failure (connection, column inspection, ledger) must not skip the vector step.
        index_session.rollback()
        source_session.rollback()
        out.write(f"ERROR: sparse store step failed: {err}; vector step still attempted; rerun to finish\n")
        sparse_active = sparse_tombstoned = sparse_already = sparse_missing = sparse_deactivated_only = 0
        sparse_error = True

    (
        vector_active,
        vector_tombstoned,
        vector_already,
        vector_missing,
        vector_deactivated_only,
        vector_skipped,
        vector_error,
    ) = _process_vector_store(
        source_session,
        vector_session,
        keys_tuple,
        apply=apply,
        out=out,
    )

    (
        legacy_sparse_tombstoned,
        legacy_vector_tombstoned,
        legacy_vector_deact,
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
        f"sparse_deactivated_only={sparse_deactivated_only} "
        f"sparse_already={sparse_already} sparse_missing={sparse_missing} "
        f"vector_tombstoned={vector_tombstoned} "
        f"vector_deactivated_only={vector_deactivated_only} "
        f"vector_already={vector_already} vector_missing={vector_missing} "
        f"legacy_sparse_tombstoned={legacy_sparse_tombstoned} "
        f"legacy_vector_tombstoned={legacy_vector_tombstoned} "
        f"legacy_vector_deactivated_only={legacy_vector_deact}\n"
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
        legacy_vector_deactivated_only=legacy_vector_deact,
    )


def _safe_render_url(url: str, *, fallback_empty: str) -> tuple[str, bool]:
    """Safely redact a connection URL without exposing secrets or raising on invalid input.

    Args:
        url: Connection URL string.
        fallback_empty: String to return when url is empty.

    Returns:
        Tuple of (rendered_string, is_parseable). If unparseable, rendered_string is
        "(unparseable URL hidden)" and is_parseable is False.

    """
    if not url:
        return fallback_empty, True
    try:
        parsed = make_url(url)
        return parsed.render_as_string(hide_password=True), True
    except ArgumentError, ValueError:
        return "(unparseable URL hidden)", False


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
        "--allow-new-season",
        action="store_true",
        default=False,
        help="Permit cleaning a season not present in existing backup (requires --season).",
    )
    parser.add_argument(
        "--confirm-target",
        action="append",
        dest="confirm_target",
        default=None,
        help="Confirmed target database identity (repeatable, required on --apply).",
    )
    return parser


def main(argv: Sequence[str] | None = None, out: TextIO = sys.stdout) -> int:
    """Execute the milestone cleanup CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.allow_new_season and args.season is None:
        out.write("--allow-new-season requires --season\n")
        return 3

    from src.db.engine import DATABASE_URL as SOURCE_DB_URL

    # The URL the session factory actually connects to, not a possibly different raw env value.
    source_url = SOURCE_DB_URL or ""
    rag_index_url = os.getenv("RAG_INDEX_DB_URL", "")
    pgvector_url = os.getenv("PGVECTOR_URL", "")
    pgvector_test_url = os.getenv("PGVECTOR_TEST_URL", "")

    redacted_source, source_parsed = _safe_render_url(source_url, fallback_empty="(not configured)")
    redacted_sparse, sparse_parsed = _safe_render_url(rag_index_url, fallback_empty="(same as source)")
    redacted_vector, vector_parsed = _safe_render_url(pgvector_url, fallback_empty="(not configured)")

    source_ident = url_identity(source_url) if source_url and source_parsed else None
    sparse_ident = url_identity(rag_index_url) if rag_index_url and sparse_parsed else None
    vector_ident = url_identity(pgvector_url) if pgvector_url and vector_parsed else None

    source_target_str = f" [target: {source_ident}]" if source_ident else ""
    sparse_target_str = f" [target: {sparse_ident}]" if sparse_ident else ""
    vector_target_str = f" [target: {vector_ident}]" if vector_ident else ""

    out.write(f"source DB = {redacted_source}{source_target_str}\n")
    out.write(f"sparse index = {redacted_sparse}{sparse_target_str}\n")
    out.write(f"vector = {redacted_vector}{vector_target_str}\n")

    if pgvector_test_url and pgvector_test_url != (pgvector_url or ""):
        if args.apply:
            out.write(
                "Refusing write: PGVECTOR_TEST_URL is set and would override PGVECTOR_URL in "
                "src.db.vector_engine; unset it\n"
            )
            return 3
        out.write("WARNING: PGVECTOR_TEST_URL is set and would override PGVECTOR_URL in src.db.vector_engine\n")

    if args.apply:
        for name, is_valid in (("source", source_parsed), ("sparse", sparse_parsed), ("vector", vector_parsed)):
            if not is_valid:
                out.write(f"Refusing write: cannot parse {name} URL; refusing to write\n")
                return 3

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
                    allow_new_season=args.allow_new_season,
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
                allow_new_season=args.allow_new_season,
                out=out,
            )

    return report.status_code


if __name__ == "__main__":
    sys.exit(main())
