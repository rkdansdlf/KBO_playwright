"""Tests for read-only RAG source inventory calculations."""

from __future__ import annotations

from types import SimpleNamespace

from src.services.rag_corpus_inventory import inventory_source_chunks
from src.services.rag_index_identity import chunk_content_hash


def test_inventory_counts_new_updated_unchanged_and_deleted_rows() -> None:
    """Compare generated chunks with the canonical sparse index by content hash."""
    generated = [
        {"source_table": "game", "source_row_id": "g1", "title": "same", "content": "same", "document_type": "game"},
        {"source_table": "game", "source_row_id": "g2", "title": "new", "content": "new", "document_type": "game"},
        {"source_table": "game", "source_row_id": "g3", "title": "changed", "content": "new", "document_type": "game"},
    ]
    existing = [
        SimpleNamespace(
            source_table="game",
            source_row_id="g1",
            content_hash=chunk_content_hash("same", "same"),
        ),
        SimpleNamespace(source_table="game", source_row_id="g3", content_hash="old"),
        SimpleNamespace(source_table="game", source_row_id="g4", content_hash="deleted"),
    ]

    report = inventory_source_chunks("games", generated, existing)

    assert report.new == 1
    assert report.updated == 1
    assert report.unchanged == 1
    assert report.deleted == 1
    assert report.deleted_identities == ("game:g4",)
    assert report.elapsed_ms >= 0
    assert report.to_dict()["source_rows"] == 3


def test_inventory_keeps_other_source_rows_out_of_delete_census() -> None:
    """Scope complete-scan deletions to tables emitted by the selected source."""
    generated = [{"source_table": "game", "source_row_id": "g1", "document_type": "game"}]
    existing = [
        SimpleNamespace(source_table="game", source_row_id="g2", content_hash="old"),
        SimpleNamespace(source_table="player_basic", source_row_id="p1", content_hash="other"),
    ]

    report = inventory_source_chunks("games", (chunk for chunk in generated), existing)

    assert report.deleted == 1
    assert report.deleted_identities == ("game:g2",)


def test_inventory_flags_duplicate_identity_and_missing_metadata() -> None:
    """Reject duplicate source keys and chunks without document type metadata."""
    generated = [
        {"source_table": "game", "source_row_id": "g1", "title": "A", "content": "A"},
        {"source_table": "game", "source_row_id": "g1", "title": "B", "content": "B"},
        {"source_table": "game", "source_row_id": "", "title": "C", "content": "C"},
    ]

    report = inventory_source_chunks("games", generated, [])

    assert report.duplicate_identities == 1
    assert report.invalid_identities == 1
    assert report.missing_metadata == 1
    assert report.deleted == 0
