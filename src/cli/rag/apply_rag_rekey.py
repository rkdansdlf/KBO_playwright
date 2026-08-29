"""Apply R2 RAG identity rekey manifest under maintenance lock."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db import engine as db_engine
from src.models.rag_chunk import RagChunk
from src.scheduler import locks as scheduler_locks

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session


DISPOSITION_REKEY = "SAFE_REKEY"
DISPOSITION_TOMBSTONE = "TARGET_EXISTS_SAME_CONTENT"

_MANIFEST_SCHEMA_VERSION = "r2-rekey-manifest-v1"
_IDENTITY_SCHEMA_VERSION = "r2"
_REQUIRED_HEADER_FIELDS = (
    "manifest_schema_version",
    "identity_schema_version",
    "generated_at",
    "database_fingerprint",
    "git_commit_sha",
    "manifest_sha256",
    "expected_entry_count",
    "expected_disposition_counts",
)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply R2 RAG identity rekey manifest")
    parser.add_argument("--manifest", type=Path, required=True, help="Path to manifest JSON file")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist changes (default is dry-run)",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> str | None:
    if not args.manifest.exists():
        return f"manifest not found: {args.manifest}"
    if args.apply:
        if os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
            return "--apply requires RAG_INDEX_ALLOW_WRITE=1"
        if os.getenv("RAG_TARGET_ENV") == "production" and os.getenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE") != "1":
            return "production --apply requires RAG_INDEX_ALLOW_PRODUCTION_WRITE=1"
    return None


def _load_manifest(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        msg = f"invalid JSON in manifest: {exc}"
        raise ValueError(msg) from exc


def _get_current_git_sha() -> str:
    """Return current git commit SHA or 'unknown' if not available."""
    try:
        result = subprocess.run(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path.cwd(),
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return "unknown"


def _compute_manifest_sha(manifest: dict) -> str:
    """Compute SHA256 of manifest excluding header."""
    content = json.dumps(
        {k: v for k, v in manifest.items() if k != "manifest_header"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(content).hexdigest()


def _get_current_database_fingerprint() -> str:
    """Generate a fingerprint of the current database schema and data state."""
    db_url = os.getenv("DATABASE_URL", "")
    safe_url = re.sub(r"://[^:]+:[^@]+@", "://***:***@", db_url)
    fp_content = f"{safe_url}|{os.getenv('RAG_INDEX_VERSION', 'rag-v1')}".encode()
    return hashlib.sha256(fp_content).hexdigest()[:16]


def _validate_manifest_header(manifest: dict, *, apply: bool) -> str | None:
    """Validate manifest header metadata against current environment."""
    header = manifest.get("manifest_header", {})
    errors: list[str] = []

    # Check required fields
    missing_fields = [f for f in _REQUIRED_HEADER_FIELDS if f not in header]
    if missing_fields:
        errors.extend(f"manifest header missing required field: {f}" for f in missing_fields)

    # Check schema versions
    if header.get("manifest_schema_version") != _MANIFEST_SCHEMA_VERSION:
        errors.append(f"unsupported manifest schema version: {header.get('manifest_schema_version')}")
    if header.get("identity_schema_version") != _IDENTITY_SCHEMA_VERSION:
        errors.append(f"unsupported identity schema version: {header.get('identity_schema_version')}")

    # Check git commit matches current code
    current_sha = _get_current_git_sha()
    if header.get("git_commit_sha") != current_sha:
        errors.append(f"git commit mismatch: manifest={header.get('git_commit_sha')}, current={current_sha}")

    # Verify manifest SHA
    actual_sha = _compute_manifest_sha(manifest)
    if header.get("manifest_sha256") != actual_sha:
        errors.append(f"manifest SHA mismatch: expected={header.get('manifest_sha256')}, actual={actual_sha}")

    # Verify expected entry count matches
    entries = manifest.get("entries", [])
    if header.get("expected_entry_count") != len(entries):
        errors.append(f"entry count mismatch: expected={header.get('expected_entry_count')}, actual={len(entries)}")

    # Verify disposition counts
    actual_counts = Counter(e.get("disposition") for e in entries)
    expected_counts = header.get("expected_disposition_counts", {})
    if dict(actual_counts) != expected_counts:
        errors.append(f"disposition count mismatch: expected={expected_counts}, actual={dict(actual_counts)}")

    # If apply mode, verify database fingerprint
    if apply and header.get("database_fingerprint") != _get_current_database_fingerprint():
        errors.append(
            f"database fingerprint mismatch: "
            f"manifest={header.get('database_fingerprint')}, "
            f"current={_get_current_database_fingerprint()}"
        )

    if errors:
        return "; ".join(errors)
    return None


def _write_preimage_and_rollback(manifest: dict, *, do_write: bool) -> tuple[Path | None, Path | None]:
    """Write preimage and inverse rollback manifest files."""
    timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    base_name = f"r2_rekey_{timestamp}"

    preimage_path: Path | None = None
    rollback_path: Path | None = None

    if do_write:
        output_dir = Path(os.getenv("R2_REKEY_OUTPUT_DIR", "data/r2_rekey"))
        output_dir.mkdir(parents=True, exist_ok=True)

        preimage_path = output_dir / f"r2_rekey_{timestamp}_preimage.jsonl"
        rollback_path = output_dir / f"{base_name}_rollback.json"

        # Write preimage as JSONL (one entry per line for streaming)
        with preimage_path.open("w", encoding="utf-8") as f:
            for entry in manifest.get("entries", []):
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        # Write rollback manifest (inverse operations)
        rollback_entries = []
        for entry in manifest.get("entries", []):
            disposition = entry.get("disposition")
            if disposition == DISPOSITION_REKEY:
                rollback_entries.append(
                    {
                        "chunk_id": entry["chunk_id"],
                        "disposition": DISPOSITION_REKEY,
                        "legacy_source_row_id": entry["legacy_source_row_id"],
                        "natural_source_row_id": entry["natural_source_row_id"],
                        "reverse": True,
                    }
                )
            elif disposition == DISPOSITION_TOMBSTONE:
                rollback_entries.append(
                    {
                        "chunk_id": entry["chunk_id"],
                        "disposition": DISPOSITION_TOMBSTONE,
                        "legacy_source_row_id": entry["legacy_source_row_id"],
                        "natural_source_row_id": entry["natural_source_row_id"],
                        "reverse": True,
                    }
                )

        rollback_manifest = {
            "manifest_header": {
                "manifest_schema_version": _MANIFEST_SCHEMA_VERSION,
                "identity_schema_version": _IDENTITY_SCHEMA_VERSION,
                "generated_at": datetime.now(KST).isoformat(),
                "purpose": "rollback",
                "original_manifest_sha256": manifest.get("manifest_header", {}).get("manifest_sha256"),
            },
            "entries": rollback_entries,
        }

        with rollback_path.open("w", encoding="utf-8") as f:
            json.dump(rollback_manifest, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")

    return preimage_path, rollback_path


def _build_where_clause(entry: dict) -> list:
    """Build WHERE clause for optimistic concurrency check."""
    where_clause = [RagChunk.id == entry.get("chunk_id")]
    legacy_id = entry.get("legacy_source_row_id")
    if legacy_id is not None:
        where_clause.append(RagChunk.source_row_id == legacy_id)
    if entry.get("legacy_content_hash") is not None:
        where_clause.append(RagChunk.content_hash == entry["legacy_content_hash"])
    if entry.get("index_status") is not None:
        where_clause.append(RagChunk.index_status == entry["index_status"])
    if entry.get("index_version") is not None:
        where_clause.append(RagChunk.index_version == entry["index_version"])
    return where_clause


def _apply_rekey(
    session: Session,
    entries: list[dict],
    *,
    dry_run: bool,
) -> tuple[int, int, list[dict]]:
    """Apply rekey and tombstone operations with optimistic concurrency."""
    rekey_count = 0
    tombstone_count = 0
    skipped: list[dict] = []

    for entry in entries:
        disposition = entry.get("disposition")
        chunk_id = entry.get("chunk_id")
        natural_id = entry.get("natural_source_row_id")
        legacy_id = entry.get("legacy_source_row_id")

        where_clause = _build_where_clause(entry)

        if disposition == DISPOSITION_REKEY and natural_id:
            if not dry_run:
                result = session.execute(
                    update(RagChunk).where(*where_clause).values(source_row_id=natural_id, updated_at=datetime.now(KST))
                )
                if result.rowcount != 1:
                    skipped.append(
                        {
                            "chunk_id": chunk_id,
                            "legacy_source_row_id": legacy_id,
                            "disposition": disposition,
                            "reason": f"optimistic concurrency check failed: expected 1 row, matched {result.rowcount}",
                        }
                    )
                    continue
            rekey_count += 1
        elif disposition == DISPOSITION_TOMBSTONE:
            if not dry_run:
                result = session.execute(
                    update(RagChunk).where(*where_clause).values(index_status="DELETED", updated_at=datetime.now(KST))
                )
                if result.rowcount != 1:
                    skipped.append(
                        {
                            "chunk_id": chunk_id,
                            "legacy_source_row_id": legacy_id,
                            "disposition": disposition,
                            "reason": f"optimistic concurrency check failed: expected 1 row, matched {result.rowcount}",
                        }
                    )
                    continue
            tombstone_count += 1
        else:
            skipped.append(
                {
                    "chunk_id": chunk_id,
                    "legacy_source_row_id": legacy_id,
                    "disposition": disposition,
                    "reason": "manual review required",
                }
            )

    if not dry_run:
        session.commit()

    return rekey_count, tombstone_count, skipped


def _render_report(  # noqa: PLR0913
    rekey_count: int,
    tombstone_count: int,
    skipped: list[dict],
    *,
    as_json: bool,
    dry_run: bool,
    preimage_path: Path | None = None,
    rollback_path: Path | None = None,
) -> None:
    payload: dict[str, Any] = {
        "dry_run": dry_run,
        "rekeyed": rekey_count,
        "tombstoned": tombstone_count,
        "skipped": len(skipped),
        "skipped_entries": skipped,
    }
    if preimage_path:
        payload["preimage_path"] = str(preimage_path)
    if rollback_path:
        payload["rollback_path"] = str(rollback_path)

    if as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        if preimage_path:
            sys.stdout.write(f"preimage written to: {preimage_path}\n")
        if rollback_path:
            sys.stdout.write(f"rollback manifest written to: {rollback_path}\n")
        mode_str = "dry-run" if dry_run else "apply"
        sys.stdout.write(f"mode={mode_str} rekeyed={rekey_count} tombstoned={tombstone_count} skipped={len(skipped)}\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Apply R2 RAG identity rekey manifest under maintenance lock."""
    args = _parse_args(argv)
    error = _validate_args(args)
    if error:
        sys.stderr.write(f"rekey_apply_error: {error}\n")
        return 2

    try:
        manifest = _load_manifest(args.manifest)
    except ValueError as exc:
        sys.stderr.write(f"rekey_apply_error: {exc}\n")
        return 2

    # Validate manifest header
    header_error = _validate_manifest_header(manifest, apply=args.apply)
    if header_error:
        sys.stderr.write(f"rekey_apply_error: manifest validation failed: {header_error}\n")
        return 2

    entries = manifest.get("entries", [])

    # Write preimage and rollback manifest (only on apply)
    preimage_path, rollback_path = _write_preimage_and_rollback(manifest, do_write=args.apply)

    lock_acquired = False
    try:
        if args.apply:
            lock_acquired = scheduler_locks.MAINTENANCE_LOCK.acquire(blocking=True, timeout=300)
            if not lock_acquired:
                sys.stderr.write("rekey_apply_error: could not acquire maintenance lock\n")
                return 1

        with db_engine.get_rag_index_session() as session:
            rekey_count, tombstone_count, skipped = _apply_rekey(session, entries, dry_run=not args.apply)

        _render_report(
            rekey_count,
            tombstone_count,
            skipped,
            as_json=args.as_json,
            dry_run=not args.apply,
            preimage_path=preimage_path,
            rollback_path=rollback_path,
        )
    except (SQLAlchemyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        sys.stderr.write(f"rekey_apply_error: {exc}\n")
        return 1
    else:
        return 0
    finally:
        if lock_acquired:
            scheduler_locks.MAINTENANCE_LOCK.release()


if __name__ == "__main__":
    raise SystemExit(main())
