"""One-off maintenance cleanup for corrupted player_milestones rows and associated RAG chunks.

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
    4. The fixed crawler will safely re-crawl and insert fresh, valid milestones.
    5. A complete audit trail is preserved in `player_milestones_cleanup_backup`, allowing a trivial
       rollback via `INSERT INTO player_milestones SELECT ... FROM player_milestones_cleanup_backup`
       if ever required.

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

    # Execute cleanup with explicit count check:
    python3 -m scripts.maintenance.cleanup_corrupted_milestones --apply --expect-count 353

    # In production environments (RAG_TARGET_ENV=production):
    RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 python3 -m scripts.maintenance.cleanup_corrupted_milestones --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TextIO

from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    inspect,
    insert,
    select,
)

from src.models.player_milestone import PlayerMilestone
from src.services.rag_index_identity import stable_milestone_source_row_id
from src.services.rag_primary_tombstone import inspect_primary_tombstones, tombstone_primary_rows
from src.services.rag_vector_tombstone import inspect_vector_tombstones, tombstone_vector_rows

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date

    from sqlalchemy.engine import RowMapping
    from sqlalchemy.orm import Session

BACKUP_TABLE_NAME = "player_milestones_cleanup_backup"
SELECTION_REASON = "corrupted_pre_deploy_batch"
GUARD_ENV = "RAG_INDEX_ALLOW_PRODUCTION_WRITE"

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

    @property
    def exit_code(self) -> int:
        """Alias for status_code."""
        return self.status_code

    @property
    def candidates(self) -> int:
        """Alias for selected candidate count."""
        return self.selected


def select_corrupted_rows(session: Session, season: int | None = None) -> list[PlayerMilestone]:
    """Select all player_milestones rows present before fix deployment.

    Rationale:
        Before the fix is deployed, the player_milestones table has only been written
        by the old crawler and mutated by the old StatRecalculator. Every row is untrustworthy:
        current_val was incremented by all-time stats on each nightly run with no audit trail,
        and target_val was written by the old crawler as current_val + 10.
        Therefore, every row present prior to deployment is deemed corrupted.

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


def _resolve_target_set(
    source_session: Session,
    season: int | None,
) -> tuple[bool, str, list[CorruptedMilestoneItem], set[int], int, int]:
    """Resolve the target set of corrupted milestones to process.

    Args:
        source_session: Operational database session.
        season: Optional season filter.

    Returns:
        Tuple of (is_frozen, selection_mode, target_items, existing_ids, already_backed_up, would_back_up).

    """
    bind = source_session.connection()
    has_backup = inspect(bind).has_table(BACKUP_TABLE_NAME)

    backup_records: list[RowMapping] = []
    frozen = False
    if has_backup:
        # Any earlier --apply freezes the target set for every later run, season filter or not, so a
        # rerun after the fix is deployed can never select the freshly crawled (correct) rows.
        frozen = source_session.execute(select(backup_table.c.id).limit(1)).first() is not None
    if frozen:
        query = select(backup_table)
        if season is not None:
            query = query.where(backup_table.c.season == season)
        query = query.order_by(backup_table.c.id)
        backup_records = list(source_session.execute(query).mappings().all())

    if frozen:
        mode = f"frozen backup set ({len(backup_records)} rows from {BACKUP_TABLE_NAME})"
        items = [
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
            for r in backup_records
        ]
        existing_ids = {r["id"] for r in backup_records}
        return True, mode, items, existing_ids, len(backup_records), 0

    raw_rows = select_corrupted_rows(source_session, season)
    mode = f"initial selection ({len(raw_rows)} rows from player_milestones)"
    items = [
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
    if has_backup and items:
        item_ids = [item.id for item in items]
        existing_ids = set(
            source_session.scalars(select(backup_table.c.id).where(backup_table.c.id.in_(item_ids))).all()
        )
    else:
        existing_ids = set()

    already_backed_up = len(existing_ids)
    would_back_up = len(items) - already_backed_up
    return False, mode, items, existing_ids, already_backed_up, would_back_up


def _backup_and_delete(
    source_session: Session,
    target_items: Sequence[CorruptedMilestoneItem],
    *,
    is_frozen: bool,
    existing_ids: set[int],
    present_in_live: set[int],
) -> tuple[int, int]:
    """Execute backup table creation, row insertion, and row deletion.

    Args:
        source_session: Operational database session.
        target_items: Target items to back up.
        is_frozen: True if target set comes from existing backup.
        existing_ids: IDs already present in backup table.
        present_in_live: IDs currently present in player_milestones.

    Returns:
        Tuple of (backed_up_new, deleted).

    """
    backup_metadata.create_all(source_session.connection(), checkfirst=True)
    backed_up_new = 0
    if not is_frozen:
        items_to_backup = [item for item in target_items if item.id not in existing_ids]
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

    deleted = 0
    if present_in_live:
        del_stmt = delete(PlayerMilestone).where(PlayerMilestone.id.in_(present_in_live))
        source_session.execute(del_stmt)
        source_session.commit()
        deleted = len(present_in_live)

    return backed_up_new, deleted


def _resolve_tombstone_keys(
    source_session: Session,
    target_items: Sequence[CorruptedMilestoneItem],
    present_in_live: set[int],
    *,
    apply: bool,
) -> tuple[str, ...]:
    """Resolve RAG keys to tombstone by subtracting surviving live rows.

    Args:
        source_session: Operational database session.
        target_items: Target items candidate list.
        present_in_live: IDs present in live table before deletion.
        apply: Whether write operations are enabled.

    Returns:
        Deduplicated tuple of RAG keys to tombstone or inspect.

    """
    surviving_stmt = select(PlayerMilestone.season, PlayerMilestone.player_id, PlayerMilestone.milestone_category)
    if not apply and present_in_live:
        surviving_stmt = surviving_stmt.where(~PlayerMilestone.id.in_(present_in_live))
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


def _process_sparse_store(
    index_session: Session,
    keys: tuple[str, ...],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, bool]:
    """Inspect or tombstone sparse RAG index chunks.

    Args:
        index_session: Sparse RAG index database session.
        keys: RAG keys to inspect or tombstone.
        apply: Whether write operations are enabled.
        out: Output stream for logging progress.

    Returns:
        Tuple of (sparse_active, sparse_tombstoned, sparse_already, sparse_missing, has_error).

    """
    has_error = False
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

    return active, tombstoned, already, missing, has_error


def _process_vector_store(
    vector_session: Session | None,
    keys: tuple[str, ...],
    *,
    apply: bool,
    out: TextIO,
) -> tuple[int, int, int, int, bool, bool]:
    """Inspect or tombstone dense vector store chunks.

    Args:
        vector_session: Dense vector database session or None.
        keys: RAG keys to inspect or tombstone.
        apply: Whether write operations are enabled.
        out: Output stream for logging progress.

    Returns:
        Tuple of (vector_active, vector_tombstoned, vector_already, vector_missing, vector_skipped, has_error).

    """
    if vector_session is None:
        out.write("vector store not configured (PGVECTOR_URL unset): skipped\n")
        return 0, 0, 0, 0, True, False

    has_error = False
    if apply:
        results = tombstone_vector_rows(vector_session, keys)
        tombstoned = sum(
            1 for r in results if r.found and r.final_status == "DELETED" and r.previous_status != "DELETED"
        )
        already = sum(1 for r in results if r.found and r.previous_status == "DELETED")
        missing = sum(1 for r in results if not r.found)
        active = 0
    else:
        results = inspect_vector_tombstones(vector_session, keys)
        active = sum(1 for r in results if r.found and r.previous_status != "DELETED")
        already = sum(1 for r in results if r.found and r.previous_status == "DELETED")
        missing = sum(1 for r in results if not r.found)
        tombstoned = 0

    for r in results:
        if r.error:
            has_error = True
            out.write(f"Vector store error on {r.source_key}: {r.error}\n")

    return active, tombstoned, already, missing, False, has_error


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
    (
        is_frozen,
        selection_mode,
        target_items,
        existing_ids,
        already_backed_up,
        would_back_up,
    ) = _resolve_target_set(source_session, season)

    target_ids = [item.id for item in target_items]
    present_in_live = (
        set(source_session.scalars(select(PlayerMilestone.id).where(PlayerMilestone.id.in_(target_ids))).all())
        if target_ids
        else set()
    )
    would_delete = len(present_in_live)

    breakdown = compute_breakdown(target_items)
    out.write(f"Selection mode: {selection_mode}\n")
    out.write(
        f"Breakdown: total={breakdown['total']}, is_achieved=1: {breakdown['is_achieved_1']}, "
        f"current_val > 3*target_val: {breakdown['inflated_3x']}, "
        f"target_val - current_val == 10: {breakdown['crawler_signature']}\n"
    )

    # Step (a): Guard checks
    guard_error = check_production_guard(apply=apply)
    if guard_error is not None:
        out.write(f"Refusing write: {guard_error}\n")
        return CleanupReport(
            mode="apply" if apply else "dry-run",
            selected=len(target_items),
            already_backed_up=already_backed_up,
            would_back_up=would_back_up,
            backed_up_new=0,
            would_delete=would_delete,
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

    if expect_count is not None and len(target_items) != expect_count:
        count_err = f"count mismatch: expected {expect_count}, found {len(target_items)}"
        out.write(f"Refusing write: {count_err}\n")
        return CleanupReport(
            mode="apply" if apply else "dry-run",
            selected=len(target_items),
            already_backed_up=already_backed_up,
            would_back_up=would_back_up,
            backed_up_new=0,
            would_delete=would_delete,
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
        backed_up_new, deleted = _backup_and_delete(
            source_session,
            target_items,
            is_frozen=is_frozen,
            existing_ids=existing_ids,
            present_in_live=present_in_live,
        )
    else:
        backed_up_new, deleted = 0, 0

    keys_tuple = _resolve_tombstone_keys(
        source_session,
        target_items,
        present_in_live,
        apply=apply,
    )

    sparse_active, sparse_tombstoned, sparse_already, sparse_missing, sparse_error = _process_sparse_store(
        index_session,
        keys_tuple,
        apply=apply,
        out=out,
    )

    vector_active, vector_tombstoned, vector_already, vector_missing, vector_skipped, vector_error = (
        _process_vector_store(
            vector_session,
            keys_tuple,
            apply=apply,
            out=out,
        )
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
        f"vector_missing={vector_missing}\n"
    )
    out.write(summary_line)

    status_code = 1 if (sparse_error or vector_error) else 0

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
        help="Expected number of corrupted rows. Fails early if mismatch.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute the milestone cleanup CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    vector_url = os.getenv("PGVECTOR_TEST_URL") or os.getenv("PGVECTOR_URL", "")

    from src.db.engine import get_db_session, get_rag_index_session

    with get_db_session() as source_session, get_rag_index_session() as index_session:
        if vector_url:
            from src.db.vector_engine import get_vector_session

            with get_vector_session() as vector_session:
                report = run_cleanup(
                    source_session,
                    index_session,
                    vector_session,
                    apply=args.apply,
                    season=args.season,
                    expect_count=args.expect_count,
                    out=sys.stdout,
                )
        else:
            report = run_cleanup(
                source_session,
                index_session,
                None,
                apply=args.apply,
                season=args.season,
                expect_count=args.expect_count,
                out=sys.stdout,
            )

    return report.status_code


if __name__ == "__main__":
    sys.exit(main())
