"""Tests for RAG index lifecycle and identity backfill contracts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.services.rag_index_backfill import backfill_identity_rows
from src.services.rag_index_lifecycle import can_transition, transition_status


def test_lifecycle_accepts_reindex_and_delete_paths() -> None:
    """Validate the supported update and tombstone transitions."""
    assert can_transition("ACTIVE", "STALE")
    assert transition_status("STALE", "REINDEX_REQUIRED") == "REINDEX_REQUIRED"
    assert transition_status("DELETE_PENDING", "TOMBSTONED") == "TOMBSTONED"
    assert transition_status("TOMBSTONED", "PURGED") == "PURGED"


def test_lifecycle_rejects_resurrection_from_purged() -> None:
    """Reject invalid lifecycle resurrection."""
    with pytest.raises(ValueError, match="Invalid RAG index lifecycle transition"):
        transition_status("PURGED", "ACTIVE")


def test_identity_backfill_is_dry_run_by_default() -> None:
    """Report missing identity fields without retaining dry-run mutations."""
    row = SimpleNamespace(source_table="game", source_row_id="g1", title="Game", content="Content")
    report = backfill_identity_rows([row])

    assert report.scanned == 1
    assert report.changed == 1
    assert row.content_hash is None
    assert not hasattr(row, "index_version") or row.index_version is None


def test_identity_backfill_apply_populates_shared_fields() -> None:
    """Populate the canonical hash, version, status, and timestamp on apply."""
    row = SimpleNamespace(source_table="game", source_row_id="g1", title="Game", content="Content")
    report = backfill_identity_rows([row], apply=True, index_version="rag-v2")

    assert report.changed == 1
    assert len(row.content_hash) == 64
    assert row.index_version == "rag-v2"
    assert row.index_status == "ACTIVE"
    assert row.indexed_at is not None
