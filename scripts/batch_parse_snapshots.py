"""Batch parser middleware: processes pending RawSourceSnapshot records.

For each pending snapshot:
  1. Re-parses the stored content-addressed artifact (no network fetch)
  2. Dispatches to the appropriate parser via registry
  3. Saves parsed data to the correct repository
  4. Marks parse_status as 'done' or 'failed'

The offline replay itself is owned by ``src.services.snapshot_replay``.
"""

from __future__ import annotations

import argparse
import logging

from sqlalchemy import select

from src.db.engine import SessionLocal
from src.models.source_registry import DataSource as DSModel
from src.repositories.source_registry_repository import RawSourceSnapshotRepository
from src.services.snapshot_persist import save_parsed
from src.services.snapshot_replay import SnapshotReplayError, parse_snapshot

logger = logging.getLogger(__name__)

PARSER_VERSION = "1.0"


def _process_snapshot(session, snap_repo, snapshot, dry_run: bool, session_factory) -> str:
    stmt = select(DSModel).where(DSModel.id == snapshot.data_source_id)
    ds = session.execute(stmt).scalar_one_or_none()
    if not ds:
        snap_repo.update_parse_status(snapshot.id, "failed", error_message="DataSource not found")
        return "failed"

    try:
        parsed = parse_snapshot(snapshot.id, session_factory=session_factory)
    except SnapshotReplayError as exc:
        snap_repo.update_parse_status(snapshot.id, "failed", error_message=str(exc))
        session.commit()
        logger.warning("Snapshot %s cannot be replayed: %s", snapshot.id, exc)
        return "failed"

    if not parsed.success:
        snap_repo.update_parse_status(snapshot.id, "failed", error_message=parsed.error or "parse failed")
        session.commit()
        return "failed"

    if dry_run:
        logger.info("[DRY-RUN] %s: %d items would be saved", ds.source_key, parsed.parsed_count)
        snap_repo.update_parse_status(snapshot.id, "done", parser_version=parsed.parser_version or PARSER_VERSION)
        session.commit()
        return "done"

    saved = save_parsed(session, ds.target_domain, parsed.records)
    snap_repo.update_parse_status(snapshot.id, "done", parser_version=parsed.parser_version or PARSER_VERSION)
    session.commit()
    logger.info("[PARSE] %s: %d items saved", ds.source_key, saved)
    return "done"


def run_batch_parse(
    limit: int = 50,
    dry_run: bool = False,
    retry_failed: bool = True,
    retry_after_hours: int = 1,
    session_factory=None,
) -> dict[str, int]:
    stats: dict[str, int] = {"processed": 0, "done": 0, "failed": 0, "skipped": 0}
    factory = session_factory or SessionLocal
    with factory() as session:
        snap_repo = RawSourceSnapshotRepository(session)
        pending = snap_repo.get_unparsed(limit=limit)
        if retry_failed:
            failed = snap_repo.get_failed_for_retry(retry_after_hours=retry_after_hours, limit=limit - len(pending))
            pending.extend(failed)
        if not pending:
            logger.info("[PARSE] No pending snapshots found.")
            return stats
        logger.info("[PARSE] Processing %d snapshots (dry_run=%s)...", len(pending), dry_run)
        for snapshot in pending:
            stats["processed"] += 1
            result = _process_snapshot(session, snap_repo, snapshot, dry_run, factory)
            stats[result] += 1
    logger.info("[PARSE] Done: %d, Failed: %d, Skipped: %d", stats["done"], stats["failed"], stats["skipped"])
    return stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch-parse pending raw snapshots")
    parser.add_argument("--limit", type=int, default=50, help="Max snapshots to process")
    parser.add_argument("--dry-run", action="store_true", help="Parse stored snapshots but do not save to repos")
    parser.add_argument("--no-retry", action="store_true", help="Skip retry of failed snapshots")
    parser.add_argument(
        "--retry-after-hours",
        type=int,
        default=1,
        help="Retry failed snapshots older than N hours (default: 1)",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    run_batch_parse(
        limit=args.limit,
        dry_run=args.dry_run,
        retry_failed=not args.no_retry,
        retry_after_hours=args.retry_after_hours,
    )


if __name__ == "__main__":
    main()
