"""Tests for the reproducible RAG evaluation corpus contract."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.rag_chunk import RagChunk
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.services.rag_eval_corpus import (
    DeterministicEmbeddingService,
    build_eval_chunks,
    load_eval_documents,
    validate_eval_corpus_files,
)
from src.services.rag_search_engine import RagSearchEngine
from src.services.retrieval_evaluation import evaluate_dataset, load_golden_queries


ROOT = Path(__file__).resolve().parents[2]
DOCUMENTS = ROOT / "tests" / "fixtures" / "rag_corpus" / "documents.json"
GOLDEN = ROOT / "tests" / "fixtures" / "rag_corpus" / "golden_queries.json"


def test_fixture_corpus_produces_100_valid_golden_cases() -> None:
    """Validate every annotated retrieval ID against transformed fixture chunks."""
    report = validate_eval_corpus_files(DOCUMENTS, GOLDEN)

    assert report.document_count == 12
    assert report.chunk_count == 12
    assert report.query_count == 100
    assert report.is_valid
    assert report.missing_chunk_ids == ()


def test_fixture_chunks_use_stable_source_identity() -> None:
    """Use source-table and source-row identity rather than database-generated IDs."""
    chunks = build_eval_chunks(load_eval_documents(DOCUMENTS))

    assert {chunk["source_table"] for chunk in chunks} == {
        "eval_rulebook",
        "eval_player",
        "eval_batting",
        "eval_pitching",
        "eval_game",
        "eval_standings",
        "eval_stadium",
        "eval_award",
        "eval_history",
        "eval_team",
    }
    assert all(chunk["source_row_id"].endswith(":1") for chunk in chunks)
    assert all(chunk["meta"]["source_row_id"] == chunk["source_row_id"] for chunk in chunks)


def test_deterministic_embedding_is_reproducible_and_normalized() -> None:
    """Generate the same non-zero normalized vector without an external API."""
    service = DeterministicEmbeddingService()
    first = service.get_embedding("김도영 2025 타격기록")
    second = service.get_embedding("김도영 2025 타격기록")

    assert first == second
    assert len(first) == 1536
    assert sum(value * value for value in first) == pytest.approx(1.0)


def test_bm25_fixture_baseline_uses_the_same_indexed_chunk_ids() -> None:
    """Evaluate the lexical baseline against rows indexed through RagChunkRepository."""
    engine = create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(engine)
    session = sessionmaker(bind=engine)()
    chunks = build_eval_chunks(load_eval_documents(DOCUMENTS))
    RagChunkRepository().upsert_chunks(session, chunks)
    queries = load_golden_queries(GOLDEN)
    search = RagSearchEngine(session)

    report = evaluate_dataset(queries, lambda query, top_k: search.search(query.query, top_k, filters=query.filters))

    assert report["query_count"] == 100
    assert report["hit_rate"] >= 0.9
    assert report["recall_at_k"] >= 0.9
    session.close()
