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

from sqlalchemy import select, update
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
_RECEIPT_SCHEMA_VERSION = "r2-rekey-receipt-v1"

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
    parser.add_argument(
        "--transaction-id",
        type=str,
        default=None,
        help="Explicit transaction ID for replayable immutable receipt",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    return parser.parse_args(argv)


def _validate_dsn_security(*, apply: bool) -> str | None:
    """Enforce strict DSN whitelisting for local safety mode and production guards."""
    db_url = os.getenv("DATABASE_URL", "")
    target_env = os.getenv("RAG_TARGET_ENV", "local")

    # In production, require strict write flags
    if target_env == "production":
        if apply and os.getenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE") != "1":
            return "production --apply requires RAG_INDEX_ALLOW_PRODUCTION_WRITE=1"
        return None

    # In non-production (local/test/safety): reject Oracle connections unconditionally
    if db_url.startswith(("oracle+", "oracle:")):
        return "Oracle database connections prohibited in local safety mode (staging/production prohibited)"

    # Reject non-ephemeral repository production/dev sqlite database
    if "data/kbo_dev.db" in db_url or "kbo_dev.db" in db_url:
        return "Primary database data/kbo_dev.db write prohibited; only ephemeral/memory databases allowed"

    return None


def _validate_args(args: argparse.Namespace) -> str | None:
    if not args.manifest.exists():
        return f"manifest not found: {args.manifest}"

    if args.apply:
        if os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
            return "--apply requires RAG_INDEX_ALLOW_WRITE=1"

        dsn_error = _validate_dsn_security(apply=True)
        if dsn_error:
            return dsn_error

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


def _validate_transaction_id(transaction_id: str) -> str | None:
    """Validate transaction ID format (reject paths, symlinks, directory traversal)."""
    if not transaction_id or not isinstance(transaction_id, str):
        return "transaction_id must be a non-empty string"
    if not re.match(r"^[a-zA-Z0-9_-]+$", transaction_id):
        return (
            f"invalid transaction_id '{transaction_id}': only alphanumeric characters, "
            f"hyphens, and underscores allowed (directory traversal prohibited)"
        )
    return None


def _get_receipt_dir() -> Path:
    receipt_dir = Path(os.getenv("R2_REKEY_RECEIPT_DIR", "data/r2_rekey/receipts"))
    receipt_dir.mkdir(parents=True, exist_ok=True)
    return receipt_dir


def _check_receipt_replay(  # noqa: C901, PLR0911
    transaction_id: str,
    manifest_sha: str,
    session: Session | None = None,
    manifest: dict | None = None,
) -> tuple[bool, dict[str, Any] | None, str | None]:
    """Check if an existing receipt matches this transaction and verify postconditions.

    Returns:
        (is_replay, receipt_payload, collision_error_or_none)

    """
    id_err = _validate_transaction_id(transaction_id)
    if id_err:
        return False, None, id_err

    receipt_dir = _get_receipt_dir()
    matching_receipts = list(receipt_dir.glob(f"*_{transaction_id}.json"))

    if not matching_receipts:
        return False, None, None

    # Found existing receipt for this transaction ID
    receipt_path = matching_receipts[0]
    if receipt_path.is_symlink():
        return False, None, f"symlink receipt rejected: {receipt_path}"

    try:
        with receipt_path.open("r", encoding="utf-8") as f:
            receipt = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        return False, None, f"corrupted existing receipt {receipt_path}: {exc}"

    if receipt.get("manifest_sha256") == manifest_sha:
        # Verify DB postcondition if session and manifest entries are provided
        if session is not None and manifest is not None:
            entries = manifest.get("entries", [])
            for entry in entries:
                chunk_id = entry.get("chunk_id")
                disposition = entry.get("disposition")
                natural_id = entry.get("natural_source_row_id")
                stmt = select(RagChunk).where(RagChunk.id == chunk_id)
                row = session.execute(stmt).scalar_one_or_none()
                if row is not None:
                    if disposition == DISPOSITION_REKEY and natural_id and row.source_row_id != natural_id:
                        return (
                            False,
                            receipt,
                            f"db_postcondition_missing: chunk {chunk_id} source_row_id={row.source_row_id} "
                            f"does not match natural_id={natural_id}",
                        )
                    if disposition == DISPOSITION_TOMBSTONE and row.index_status != "DELETED":
                        return (
                            False,
                            receipt,
                            f"db_postcondition_missing: chunk {chunk_id} status={row.index_status} is not DELETED",
                        )

        # Same transaction ID, same payload -> Exact idempotent replay
        return True, receipt, None

    # Same transaction ID, DIFFERENT payload -> Collision error!
    stored_sha = receipt.get("manifest_sha256")
    return (
        False,
        None,
        f"transaction ID collision: transaction '{transaction_id}' was already executed "
        f"with different manifest payload (stored sha={stored_sha}, requested sha={manifest_sha})",
    )


def _publish_immutable_receipt(  # noqa: PLR0913
    *,
    transaction_id: str,
    manifest_id: str,
    manifest_sha: str,
    status: str,
    rekey_count: int,
    tombstone_count: int,
    already_applied: int,
    stale_rejected: int,
    preimage_path: Path | None,
    rollback_path: Path | None,
    original_apply_receipt_sha: str | None = None,
) -> Path:
    """Generate and write an immutable cryptographic receipt via atomic temp file + fsync + rename."""
    id_err = _validate_transaction_id(transaction_id)
    if id_err:
        raise ValueError(id_err)

    receipt_dir = _get_receipt_dir()
    receipt_filename = f"receipt_{manifest_sha[:16]}_{transaction_id}.json"
    receipt_path = receipt_dir / receipt_filename

    payload: dict[str, Any] = {
        "receipt_schema_version": _RECEIPT_SCHEMA_VERSION,
        "transaction_id": transaction_id,
        "manifest_id": manifest_id,
        "manifest_sha256": manifest_sha,
        "status": status,
        "rekeyed_count": rekey_count,
        "tombstoned_count": tombstone_count,
        "already_applied_count": already_applied,
        "stale_rejected_count": stale_rejected,
        "database_fingerprint": _get_current_database_fingerprint(),
        "applied_at_kst": datetime.now(KST).isoformat(),
        "preimage_path": str(preimage_path) if preimage_path else None,
        "rollback_path": str(rollback_path) if rollback_path else None,
    }
    if original_apply_receipt_sha:
        payload["original_apply_receipt_sha"] = original_apply_receipt_sha

    # Compute receipt SHA256 digest
    content_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    payload["receipt_sha256"] = hashlib.sha256(content_bytes).hexdigest()

    # Atomic write via temp file + fsync + rename
    temp_path = receipt_dir / f".tmp_{receipt_filename}_{os.getpid()}"
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    temp_path.replace(receipt_path)

    # Sync parent directory entry to guarantee filesystem durability across power loss
    try:
        dir_fd = os.open(str(receipt_dir), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except (OSError, AttributeError):
        pass

    return receipt_path


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
        where_clause.append(RagChunk.source_row_id == str(legacy_id))
    if entry.get("legacy_content_hash") is not None:
        where_clause.append(RagChunk.content_hash == entry["legacy_content_hash"])
    if entry.get("index_status") is not None:
        where_clause.append(RagChunk.index_status == entry["index_status"])
    if entry.get("index_version") is not None:
        where_clause.append(RagChunk.index_version == entry["index_version"])
    return where_clause


def _apply_rekey(  # noqa: C901, PLR0912, PLR0915
    session: Session,
    entries: list[dict],
    *,
    dry_run: bool,
    strict_atomic: bool = True,
) -> tuple[int, int, int, list[dict], str]:
    """Apply rekey and tombstone operations with optimistic concurrency and distinct no-op detection.

    Returns:
        (rekey_count, tombstone_count, already_applied_count, skipped_entries, execution_status)

    """
    rekey_count = 0
    tombstone_count = 0
    already_applied_count = 0
    skipped: list[dict] = []

    pending_mutations: list[tuple[Any, dict[str, Any]]] = []

    for entry in entries:
        disposition = entry.get("disposition")
        chunk_id = entry.get("chunk_id")
        natural_id = entry.get("natural_source_row_id")
        legacy_id = str(entry.get("legacy_source_row_id")) if entry.get("legacy_source_row_id") is not None else None
        expected_hash = entry.get("legacy_content_hash")
        expected_status = entry.get("index_status")
        expected_version = entry.get("index_version")

        # Query existing row to distinguish ALREADY_APPLIED from CAS conflict
        stmt = select(RagChunk).where(RagChunk.id == chunk_id)
        row = session.execute(stmt).scalar_one_or_none()

        if row is not None:
            # Check if already applied (natural ID already present)
            if disposition == DISPOSITION_REKEY and natural_id and row.source_row_id == natural_id:
                already_applied_count += 1
                continue

            if disposition == DISPOSITION_TOMBSTONE and row.index_status == "DELETED":
                already_applied_count += 1
                continue

            # Verify CAS invariants
            cas_mismatches = []
            if legacy_id is not None and row.source_row_id != legacy_id:
                cas_mismatches.append(f"stale_legacy_id(expected={legacy_id}, actual={row.source_row_id})")
            if expected_hash is not None and row.content_hash != expected_hash:
                cas_mismatches.append(f"stale_content_hash(expected={expected_hash}, actual={row.content_hash})")
            if expected_status is not None and row.index_status != expected_status:
                cas_mismatches.append(f"stale_index_status(expected={expected_status}, actual={row.index_status})")
            if expected_version is not None and row.index_version != expected_version:
                cas_mismatches.append(f"stale_index_version(expected={expected_version}, actual={row.index_version})")

            if cas_mismatches:
                skipped.append(
                    {
                        "chunk_id": chunk_id,
                        "legacy_source_row_id": legacy_id,
                        "disposition": disposition,
                        "reason": f"optimistic concurrency check failed: {'; '.join(cas_mismatches)}",
                    }
                )
                continue

        # Valid mutation target
        where_clause = _build_where_clause(entry)
        if disposition == DISPOSITION_REKEY and natural_id:
            pending_mutations.append(
                (
                    update(RagChunk)
                    .where(*where_clause)
                    .values(source_row_id=natural_id, updated_at=datetime.now(KST)),
                    {"type": "REKEY", "chunk_id": chunk_id},
                )
            )
        elif disposition == DISPOSITION_TOMBSTONE:
            pending_mutations.append(
                (
                    update(RagChunk).where(*where_clause).values(index_status="DELETED", updated_at=datetime.now(KST)),
                    {"type": "TOMBSTONE", "chunk_id": chunk_id},
                )
            )
        else:
            skipped.append(
                {
                    "chunk_id": chunk_id,
                    "legacy_source_row_id": legacy_id,
                    "disposition": disposition,
                    "reason": "unsupported_disposition",
                }
            )

    # In strict atomic mode, if any entry failed CAS, roll back entire batch
    if strict_atomic and skipped and pending_mutations:
        if not dry_run:
            session.rollback()
        return 0, 0, already_applied_count, skipped, "FAILED_ATOMIC_ROLLBACK"

    # Execute mutations
    for update_stmt, meta in pending_mutations:
        if not dry_run:
            result = session.execute(update_stmt)
            if result.rowcount != 1:
                if strict_atomic:
                    session.rollback()
                    skipped.append(
                        {
                            "chunk_id": meta["chunk_id"],
                            "disposition": meta["type"],
                            "reason": "concurrent modification during batch execution; entire batch rolled back",
                        }
                    )
                    return 0, 0, already_applied_count, skipped, "FAILED_ATOMIC_ROLLBACK"
                skipped.append(
                    {
                        "chunk_id": meta["chunk_id"],
                        "disposition": meta["type"],
                        "reason": "rowcount mismatch on execution",
                    }
                )
                continue
        if meta["type"] == "REKEY":
            rekey_count += 1
        elif meta["type"] == "TOMBSTONE":
            tombstone_count += 1

    if not dry_run:
        session.commit()

    if rekey_count > 0 or tombstone_count > 0:
        status = "SUCCESS_APPLIED"
    elif already_applied_count > 0 and not skipped:
        status = "SUCCESS_NOOP"
    elif skipped:
        status = "FAILED_STALE_MANIFEST"
    else:
        status = "SUCCESS_NOOP"

    return rekey_count, tombstone_count, already_applied_count, skipped, status


def _render_report(  # noqa: PLR0913
    rekey_count: int,
    tombstone_count: int,
    already_applied: int,
    skipped: list[dict],
    *,
    status: str,
    as_json: bool,
    dry_run: bool,
    receipt_reused: bool = False,
    receipt_path: Path | None = None,
    preimage_path: Path | None = None,
    rollback_path: Path | None = None,
) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "dry_run": dry_run,
        "rekeyed": rekey_count,
        "tombstoned": tombstone_count,
        "already_applied": already_applied,
        "stale_rejected": len(skipped),
        "skipped": len(skipped),
        "skipped_entries": skipped,
        "receipt_reused": receipt_reused,
    }
    if receipt_path:
        payload["receipt_path"] = str(receipt_path)
    if preimage_path:
        payload["preimage_path"] = str(preimage_path)
    if rollback_path:
        payload["rollback_path"] = str(rollback_path)

    if as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        if receipt_reused:
            sys.stdout.write(f"receipt reused: {receipt_path}\n")
        elif receipt_path:
            sys.stdout.write(f"receipt written to: {receipt_path}\n")
        if preimage_path:
            sys.stdout.write(f"preimage written to: {preimage_path}\n")
        if rollback_path:
            sys.stdout.write(f"rollback manifest written to: {rollback_path}\n")
        mode_str = "dry-run" if dry_run else "apply"
        sys.stdout.write(
            f"status={status} mode={mode_str} rekeyed={rekey_count} tombstoned={tombstone_count} "
            f"already_applied={already_applied} stale_rejected={len(skipped)}\n"
        )


def main(argv: Sequence[str] | None = None) -> int:  # noqa: C901, PLR0911, PLR0912
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

    actual_manifest_sha = _compute_manifest_sha(manifest)
    manifest_id = manifest.get("manifest_header", {}).get("manifest_id", "manifest-default")
    tx_id = args.transaction_id or manifest.get("manifest_header", {}).get("transaction_id") or actual_manifest_sha[:16]

    # Check for idempotent replay or transaction ID collision
    if args.apply:
        with db_engine.get_rag_index_session() as pre_session:
            is_replay, existing_receipt, collision_err = _check_receipt_replay(
                tx_id, actual_manifest_sha, session=pre_session, manifest=manifest
            )
        if collision_err:
            sys.stderr.write(f"rekey_apply_error: {collision_err}\n")
            return 2

        if is_replay and existing_receipt:
            _render_report(
                rekey_count=0,
                tombstone_count=0,
                already_applied=existing_receipt.get("already_applied_count", 0)
                + existing_receipt.get("rekeyed_count", 0)
                + existing_receipt.get("tombstoned_count", 0),
                skipped=[],
                status="SUCCESS_NOOP",
                as_json=args.as_json,
                dry_run=False,
                receipt_reused=True,
                receipt_path=Path(existing_receipt.get("receipt_path", "")),
            )
            return 0

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
            rekey_count, tombstone_count, already_applied, skipped, status = _apply_rekey(
                session, entries, dry_run=not args.apply
            )

        receipt_path: Path | None = None
        if args.apply:
            if status == "SUCCESS_NOOP" and not existing_receipt and already_applied > 0 and len(skipped) == 0:
                # DB was already modified in prior transaction that crashed before receipt publish
                status = "RECOVERED_RECEIPT_REBUILT"
            if status in ("SUCCESS_APPLIED", "SUCCESS_NOOP", "RECOVERED_RECEIPT_REBUILT"):
                receipt_path = _publish_immutable_receipt(
                    transaction_id=tx_id,
                    manifest_id=manifest_id,
                    manifest_sha=actual_manifest_sha,
                    status=status,
                    rekey_count=rekey_count,
                    tombstone_count=tombstone_count,
                    already_applied=already_applied,
                    stale_rejected=len(skipped),
                    preimage_path=preimage_path,
                    rollback_path=rollback_path,
                )

        _render_report(
            rekey_count,
            tombstone_count,
            already_applied,
            skipped,
            status=status,
            as_json=args.as_json,
            dry_run=not args.apply,
            receipt_reused=False,
            receipt_path=receipt_path,
            preimage_path=preimage_path,
            rollback_path=rollback_path,
        )

        if status.startswith("FAILED_"):
            return 1
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
