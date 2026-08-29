"""Run a read-only R2 RAG identity census and write an apply manifest."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

from src.models.rag_chunk import RagChunk
from src.services.rag_identity_census import (
    R2_SOURCE_TABLES,
    ExistingIdentityRow,
    IdentityCensusReport,
    build_identity_census,
    iter_source_identity_records,
    validate_source_tables,
)


def fetch_existing_identity_rows(session: Session, source_tables: Sequence[str]) -> list[ExistingIdentityRow]:
    """Load only persisted identity fields needed by the census."""
    statement = select(
        RagChunk.id,
        RagChunk.source_table,
        RagChunk.source_row_id,
        RagChunk.content_hash,
        RagChunk.index_status,
    ).where(RagChunk.source_table.in_(tuple(source_tables)))
    return [
        ExistingIdentityRow(
            chunk_id=int(row[0]),
            source_table=str(row[1]),
            source_row_id=str(row[2]),
            content_hash=str(row[3]) if row[3] is not None else None,
            index_status=str(row[4] or ""),
        )
        for row in session.execute(statement).yield_per(5_000)
    ]


def collect_identity_census(
    source_session: Session,
    index_session: Session,
    source_tables: Sequence[str],
    *,
    season: int | None = None,
) -> IdentityCensusReport:
    """Collect source projections and persisted rows into one census report."""
    source_records = (
        record
        for source_table in source_tables
        for record in iter_source_identity_records(source_session, source_table, season)
    )
    existing_rows = fetch_existing_identity_rows(index_session, source_tables)
    return build_identity_census(existing_rows, source_records, source_tables=source_tables)


def write_manifest(report: IdentityCensusReport, path: Path) -> None:
    """Write a complete JSON manifest using an atomic same-directory replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report.to_manifest_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary:
        temporary.write(payload)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse read-only R2 census options."""
    parser = argparse.ArgumentParser(description="Census legacy and natural RAG identities (read-only)")
    parser.add_argument(
        "--source",
        action="append",
        choices=R2_SOURCE_TABLES,
        help="Limit the census to one source table; repeat for multiple tables",
    )
    parser.add_argument("--season", type=int, help="Limit source records to one season/year")
    parser.add_argument("--sample", type=int, default=20, help="Number of unsafe entries to print in JSON output")
    parser.add_argument("--output", type=Path, help="Write the complete apply-gated JSON manifest to this path")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicitly confirm read-only mode; the census never writes database rows",
    )
    parser.add_argument(
        "--fail-on-unsafe",
        action="store_true",
        help="Return exit code 1 when orphan, collision, or target-conflict rows exist",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render the summary as JSON")
    return parser.parse_args(argv)


def _render_summary(
    report: IdentityCensusReport,
    *,
    as_json: bool,
    sample_limit: int,
    manifest_path: Path | None,
) -> None:
    """Render a compact report while keeping the full manifest on disk."""
    payload = report.to_summary_dict(sample_limit=sample_limit)
    if manifest_path is not None:
        payload["manifest"] = str(manifest_path)
    if as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return
    totals = payload["totals"]
    sys.stdout.write(
        "read_only=true sources={sources} source_rows={source_rows} legacy_numeric={legacy_numeric_rows} "
        "mapped={safe_source_matches} safe_rekey={safe_rekey_candidates} orphan={orphan_rows} "
        "collisions={collision_rows} target_conflicts={existing_natural_target} unsafe={unsafe_entry_count}\n".format(
            sources=",".join(report.source_tables),
            **totals,
            unsafe_entry_count=report.unsafe_entry_count,
        )
    )
    if manifest_path is not None:
        sys.stdout.write(f"manifest={manifest_path}\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the R2 identity census without modifying database state."""
    from src.db.engine import get_rag_index_session, get_rag_source_session

    args = _parse_args(argv)
    if args.sample < 0:
        sys.stderr.write("census_error: --sample must not be negative\n")
        return 2
    try:
        source_tables = validate_source_tables(args.source or R2_SOURCE_TABLES)
        with get_rag_source_session() as source_session, get_rag_index_session() as index_session:
            report = collect_identity_census(
                source_session,
                index_session,
                source_tables,
                season=args.season,
            )
        if args.output is not None:
            write_manifest(report, args.output)
    except (SQLAlchemyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        sys.stderr.write(f"census_error: {exc}\n")
        return 2

    _render_summary(
        report,
        as_json=args.as_json,
        sample_limit=args.sample,
        manifest_path=args.output,
    )
    return 1 if args.fail_on_unsafe and report.unsafe_entry_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
