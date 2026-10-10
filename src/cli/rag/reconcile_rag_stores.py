"""Export and compare point-in-time RAG identity manifests across stores (read-only)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.constants import KST
from src.services.rag_reconciliation import (
    ROLE_DENSE,
    ROLE_SPARSE,
    ManifestEntry,
    entry_from_manifest_row,
    parse_updated_at,
    read_manifest,
    reconcile_manifests,
    write_manifest,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sqlalchemy.orm import Session

_DEFAULT_OUTPUT_ROOT = Path("reports") / "rag_reconciliation"
_SIDES = ("primary", "staging")

_IDENTITY_SELECT_PREFIX = "SELECT source_table, source_row_id, content_hash, index_version, index_status, "
_WITH_TIMESTAMPS_SUFFIX = ", created_at, updated_at FROM rag_chunks"
_PLAIN_SUFFIX = " FROM rag_chunks"


def _default_stamp() -> str:
    """Return a filesystem-safe KST timestamp for output naming."""
    return datetime.now(KST).strftime("%Y%m%d_%H%M%S")


def _entry_from_db_row(row: Mapping[Any, Any]) -> ManifestEntry:
    """Convert one DB identity row mapping into a manifest entry."""
    mapping = dict(row)
    raw_ts = mapping.get("updated_at")
    if raw_ts is not None and hasattr(raw_ts, "isoformat"):
        isoformat = getattr(raw_ts, "isoformat", None)
        if callable(isoformat):
            mapping["updated_at"] = isoformat()
    return entry_from_manifest_row(mapping)


def _embedding_column_names(bind: object) -> set[str]:
    """Return the embedding-named columns of ``rag_chunks`` on this store."""
    from sqlalchemy import inspect

    return {column["name"] for column in inspect(bind).get_columns("rag_chunks")}


def _resolve_embedding_column(session: Session) -> str:
    """Return the column that holds the vector on this store.

    Asked of the table rather than inferred from the dialect: the operational
    database and the pgvector store are both PostgreSQL and name the column
    differently (``embedding_vector`` and ``embedding``), so a dialect switch
    picked the wrong one for exactly one of the two stores it serves.
    """
    names = _embedding_column_names(session.get_bind())
    for candidate in ("embedding_vector", "embedding"):
        if candidate in names:
            return candidate
    message = "rag_chunks has no embedding or embedding_vector column on this store"
    raise RuntimeError(message)


def _identity_select_prefix(session: Session) -> str:
    """Build the identity projection using the active backend's vector column.

    The presence test is deliberately stricter than ``IS NULL`` on stores whose
    column is textual: the local database records "no vector" as the literal
    string ``null``, which an ``IS NULL`` test counts as embedded. Oracle keeps
    the plain test, because its ``VECTOR`` type rejects the text cast this uses
    (ORA-51810) and never carries that placeholder.
    """
    bind = session.get_bind()
    dialect = getattr(getattr(bind, "dialect", None), "name", None)
    column = _resolve_embedding_column(session)
    if dialect == "oracle":
        presence = f"CASE WHEN {column} IS NULL THEN 0 ELSE 1 END"
    else:
        presence = f"CASE WHEN {column} IS NULL OR CAST({column} AS TEXT) IN ('null', '[]', '') THEN 0 ELSE 1 END"
    return f"{_IDENTITY_SELECT_PREFIX}{presence} AS embedding_present"


def fetch_identity_entries(session: Session) -> list[ManifestEntry]:
    """Load identity projections, preferring timestamp columns when present."""
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError

    prefix = _identity_select_prefix(session)
    for suffix in (_WITH_TIMESTAMPS_SUFFIX, _PLAIN_SUFFIX):
        try:
            rows = session.execute(text(prefix + suffix)).mappings().all()
        except SQLAlchemyError:
            continue
        return [_entry_from_db_row(row) for row in rows]
    message = "rag_chunks identity query failed on both timestamped and plain variants"
    raise RuntimeError(message)


def _side_role(side: str) -> str:
    """Return the storage role the named side actually serves.

    Derived from the same resolution dense search uses, not from the side name.
    ``get_vector_session()`` is what ``vector_search_repository`` reads, and it
    answers with the Oracle index session when there is no ``PGVECTOR_URL`` and
    the operational database is Oracle, and with the pgvector store otherwise.
    So:

    * Oracle single-store -- ``primary`` **is** the dense store, and naming it
      ``sparse`` would exempt the one side that must be checked.
    * Separate pgvector -- ``primary`` is the sparse store and ``staging`` holds
      the vectors.

    Reading this off the configuration is the point. Hard-coding it by side name
    is how the previous version came to demand vectors from a store that no
    dense reader opens.
    """
    if side == "primary":
        from src.db.vector_engine import is_oracle_vector_backend

        return ROLE_DENSE if is_oracle_vector_backend() else ROLE_SPARSE
    return ROLE_DENSE


def _write_key_lines(path: Path, keys: Sequence[str]) -> None:
    """Write one identity key per line, ending with a newline when non-empty."""
    body = "\n".join(keys) + "\n" if keys else ""
    path.write_text(body, encoding="utf-8")


def cmd_export(args: argparse.Namespace) -> int:
    """Export one store's identity manifest as NDJSON."""
    out_path = Path(args.out) if args.out else _DEFAULT_OUTPUT_ROOT / f"manifest_{args.side}_{_default_stamp()}.ndjson"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        entries = _export_side(args.side)
    except RuntimeError as error:
        sys.stderr.write(f"export failed: {error}\n")
        return 1

    written = write_manifest(entries, out_path)
    sys.stdout.write(json.dumps({"side": args.side, "rows": written, "manifest": str(out_path)}) + "\n")
    return 0


def _export_side(side: str) -> list[ManifestEntry]:
    """Open the configured session for a side and export its manifest.

    The role is stamped onto every entry here, at export time, because this is
    the only place that knows which stores the configuration actually resolved
    to. A manifest read back later cannot recover that -- and a comparison that
    guesses the role from a file name is the defect this stamping removes.
    """
    if side not in _SIDES:
        message = f"unknown side: {side}"
        raise RuntimeError(message)
    role = _side_role(side)
    if side == "primary":
        from src.db.engine import get_rag_index_session

        with get_rag_index_session() as session:
            return _with_role(fetch_identity_entries(session), role)
    from src.db.vector_engine import get_vector_session

    with get_vector_session() as session:
        return _with_role(fetch_identity_entries(session), role)


def _with_role(entries: list[ManifestEntry], role: str) -> list[ManifestEntry]:
    """Return the entries stamped with the role of the store they came from."""
    return [
        ManifestEntry(
            source_table=entry.source_table,
            source_row_id=entry.source_row_id,
            content_hash=entry.content_hash,
            index_version=entry.index_version,
            index_status=entry.index_status,
            embedding_present=entry.embedding_present,
            updated_at=entry.updated_at,
            role=role,
        )
        for entry in entries
    ]


def cmd_compare(args: argparse.Namespace) -> int:
    """Compare two exported manifests and persist a classification report."""
    left_entries = list(read_manifest(Path(args.left)))
    right_entries = list(read_manifest(Path(args.right)))
    report = reconcile_manifests(
        left_entries,
        right_entries,
        left_label=Path(args.left).stem,
        right_label=Path(args.right).stem,
        as_of=parse_updated_at(args.as_of) if args.as_of else None,
    )

    output_dir = Path(args.output_dir) if args.output_dir else _DEFAULT_OUTPUT_ROOT / f"compare_{_default_stamp()}"
    output_dir.mkdir(parents=True, exist_ok=True)

    left_map = {entry.key: entry for entry in left_entries}
    right_map = {entry.key: entry for entry in right_entries}
    summary_path = output_dir / "comparison_summary.json"
    payload = json.dumps(report.to_summary_dict(left_map, right_map), indent=2) + "\n"
    summary_path.write_text(payload, encoding="utf-8")

    unexplained_keys = sorted({key for keys in report.unexplained_issues.values() for key in keys})
    _write_key_lines(output_dir / "unexplained_keys.txt", unexplained_keys)
    _write_key_lines(output_dir / "left_only_keys.txt", report.unexplained_issues.get("MISSING_IN_RIGHT", ()))
    _write_key_lines(output_dir / "right_only_keys.txt", report.unexplained_issues.get("MISSING_IN_LEFT", ()))
    _write_key_lines(output_dir / "time_explainable_keys.txt", report.time_explainable_keys)

    result = {"summary": str(summary_path), "unexplained": report.unexplained_count, "clean": report.is_clean}
    sys.stdout.write(json.dumps(result) + "\n")
    if args.fail_on_unexplained and not report.is_clean:
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the reconciliation CLI."""
    parser = argparse.ArgumentParser(description="Point-in-time RAG store reconciliation (read-only)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export one store's identity manifest")
    export_parser.add_argument("--side", choices=_SIDES, required=True, help="Which configured store to export")
    export_parser.add_argument("--out", help="Output NDJSON path (default under reports/rag_reconciliation/)")
    export_parser.set_defaults(handler=cmd_export)

    compare_parser = subparsers.add_parser("compare", help="Compare two manifests with optional as-of cutoff")
    compare_parser.add_argument("--left", required=True, help="Left manifest NDJSON")
    compare_parser.add_argument("--right", required=True, help="Right manifest NDJSON")
    compare_parser.add_argument("--as-of", help="ISO cutoff; changes after it are time-explainable")
    compare_parser.add_argument("--output-dir", help="Report directory (default under reports/rag_reconciliation/)")
    compare_parser.add_argument(
        "--fail-on-unexplained",
        action="store_true",
        help="Exit 1 when unexplained drift remains",
    )
    compare_parser.set_defaults(handler=cmd_compare)

    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
