"""Tests for sparse/vector RAG index consistency checks."""

from __future__ import annotations

from src.services.rag_index_consistency import compare_index_rows


def test_compare_index_rows_reports_missing_and_stale_rows() -> None:
    """Report missing vector rows and mismatched canonical content hashes."""
    report = compare_index_rows(
        [
            {
                "source_table": "game",
                "source_row_id": "g1",
                "content_hash": "hash-a",
                "index_version": "v1",
                "index_status": "ACTIVE",
            },
            {
                "source_table": "game",
                "source_row_id": "g2",
                "content_hash": "hash-b",
                "index_version": "v1",
            },
        ],
        [
            {
                "source_table": "game",
                "source_row_id": "g1",
                "content_hash": "hash-new",
                "index_version": "v2",
                "index_status": "ACTIVE",
            },
            {
                "source_table": "game",
                "source_row_id": "g3",
                "content_hash": "hash-c",
                "index_version": "v1",
            },
        ],
    )

    assert report.primary_count == 2
    assert report.vector_count == 2
    assert not report.is_consistent
    assert {finding.issue for finding in report.findings} == {
        "CONTENT_HASH_MISMATCH",
        "INDEX_VERSION_MISMATCH",
        "MISSING_IN_PRIMARY",
        "MISSING_IN_VECTOR",
    }


def test_compare_index_rows_treats_missing_status_as_active() -> None:
    """Avoid false lifecycle mismatches for rows created before the migration."""
    report = compare_index_rows(
        [{"source_table": "rule", "source_row_id": "1", "content_hash": "h", "index_version": "v1"}],
        [{"source_table": "rule", "source_row_id": "1", "content_hash": "h", "index_version": "v1"}],
    )
    assert report.is_consistent


def test_consistency_report_exposes_acceptance_counts() -> None:
    """Expose sparse/vector/orphan/hash/version counts for the audit CLI."""
    report = compare_index_rows(
        [
            {
                "source_table": "game",
                "source_row_id": "g1",
                "content_hash": "h1",
                "index_version": "v1",
                "index_status": "STALE",
            }
        ],
        [
            {
                "source_table": "game",
                "source_row_id": "g2",
                "content_hash": "h2",
                "index_version": "v1",
                "index_status": "ACTIVE",
            }
        ],
    )
    payload = report.to_dict()
    assert payload["total"] == 2
    assert payload["sparse_only"] == 1
    assert payload["orphan"] == 1
    assert payload["stale"] == 1
    assert not report.is_consistent


def test_compare_index_rows_rejects_vector_rows_without_embeddings() -> None:
    """Treat a vector identity row without an embedding as an index defect."""
    report = compare_index_rows(
        [
            {
                "source_table": "game",
                "source_row_id": "g1",
                "content_hash": "h1",
                "index_version": "v1",
            }
        ],
        [
            {
                "source_table": "game",
                "source_row_id": "g1",
                "content_hash": "h1",
                "index_version": "v1",
                "embedding": None,
            }
        ],
    )

    assert not report.is_consistent
    assert report.to_dict()["embedding_missing"] == 1


def test_compare_index_rows_allows_deleted_rows_without_embeddings() -> None:
    """Do not require vectors for rows intentionally removed from retrieval."""
    row = {
        "source_table": "game",
        "source_row_id": "deleted-1",
        "content_hash": "h1",
        "index_version": "v1",
        "index_status": "DELETED",
        "embedding": None,
    }

    report = compare_index_rows([row], [row])

    assert report.is_consistent
    assert report.to_dict()["embedding_missing"] == 0
    assert report.to_dict()["deleted"] == 1
