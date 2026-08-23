"""Export and compare point-in-time RAG identity manifests across stores (read-only)."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST
from src.services.rag_reconciliation import (
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

_IDENTITY_SELECT_PREFIX = (
    "SELECT source_table, source_row_id, content_hash, index_version, index_status, "
    "CASE WHEN embedding IS NULL THEN 0 ELSE 1 END AS embedding_present"
)
_WITH_TIMESTAMPS_SUFFIX = ", created_at, updated_at FROM rag_chunks"
_PLAIN_SUFFIX = " FROM rag_chunks"


def _default_stamp() -> str:
    """Return a filesystem-safe KST timestamp for output naming."""
    return datetime.now(KST).strftime("%Y%m%d_%H%M%S")


def _entry_from_db_row(row: Mapping[str, object]) -> ManifestEntry:
    """Convert one DB identity row mapping into a manifest entry."""
    mapping = dict(row)
    raw_ts = mapping.get("updated_at")
    if hasattr(raw_ts, "isoformat"):
        mapping["updated_at"] = raw_ts.isoformat()
    return entry_from_manifest_row(mapping)


def fetch_identity_entries(session: Session) -> list[ManifestEntry]:
    """Load identity projections, preferring timestamp columns when present."""
    from sqlalchemy import text
    from sqlalchemy.exc import SQLAlchemyError

    for suffix in (_WITH_TIMESTAMPS_SUFFIX, _PLAIN_SUFFIX):
        try:
            rows = session.execute(text(_IDENTITY_SELECT_PREFIX + suffix)).mappings().all()
        except SQLAlchemyError:
            continue
        return [_entry_from_db_row(row) for row in rows]
    message = "rag_chunks identity query failed on both timestamped and plain variants"
    raise RuntimeError(message)


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
    """Open the configured session for a side and export its manifest."""
    if side not in _SIDES:
        message = f"unknown side: {side}"
        raise RuntimeError(message)
    if side == "primary":
        from src.db.engine import get_rag_index_session

        with get_rag_index_session() as session:
            return fetch_identity_entries(session)
    from src.db.vector_engine import get_vector_session

    with get_vector_session() as session:
        return fetch_identity_entries(session)


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
