"""Unit tests for dense, sparse, and hybrid retrievers in src.rag.retrievers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.rag.dto import RetrievalCandidate, RetrievalQuery
from src.rag.retrievers.hybrid import UnifiedHybridRetriever
from src.rag.retrievers.oracle_dense import OracleDenseRetriever
from src.rag.retrievers.sparse_bm25 import SparseBM25Retriever


def test_oracle_dense_retriever_mocked() -> None:
    mock_embed = MagicMock()
    mock_embed.get_embedding.return_value = [0.1, 0.2, 0.3]

    mock_repo = MagicMock()
    mock_repo.search_similar.return_value = [
        {
            "chunk_id": "chunk_1",
            "title": "김도영 기록",
            "content": "30홈런 30도루 달성",
            "similarity": 0.92,
            "category": "milestone",
            "source_url": "https://kbo.com/1",
        }
    ]

    with patch("src.rag.retrievers.oracle_dense.is_oracle_vector_backend", return_value=True):
        retriever = OracleDenseRetriever(embedding_service=mock_embed, vector_repo=mock_repo)
        assert retriever.is_available() is True

        res = retriever.retrieve("김도영 30-30", top_k=5)
        assert res.retrieval_mode == "dense_vector"
        assert len(res.candidates) == 1
        assert res.candidates[0].chunk_id == "chunk_1"
        assert res.candidates[0].score == 0.92
        assert res.candidates[0].vector_rank == 1
        assert res.candidates[0].bm25_rank is None


def test_sparse_bm25_retriever_mocked() -> None:
    mock_session = MagicMock()
    mock_search_engine = MagicMock()
    mock_search_engine.search.return_value = [
        {
            "id": 101,
            "title": "2025 경기 일정",
            "content": "개막전 일정 공시",
            "score": 4.5,
            "category": "schedule",
        }
    ]

    retriever = SparseBM25Retriever(mock_session, search_engine=mock_search_engine)
    res = retriever.retrieve("2025 개막전", top_k=3)

    assert res.retrieval_mode == "sparse_bm25"
    assert len(res.candidates) == 1
    assert res.candidates[0].chunk_id == "101"
    assert res.candidates[0].score == 4.5
    assert res.candidates[0].bm25_rank == 1
    assert res.candidates[0].vector_rank is None


def test_unified_hybrid_retriever_rrf_fusion() -> None:
    mock_session = MagicMock()

    mock_dense = MagicMock()
    dense_cand1 = RetrievalCandidate(
        chunk_id="chk_A",
        title="Doc A",
        content="Dense match 1",
        score=0.9,
        vector_rank=1,
    )
    dense_cand2 = RetrievalCandidate(
        chunk_id="chk_B",
        title="Doc B",
        content="Dense match 2",
        score=0.8,
        vector_rank=2,
    )
    mock_dense.retrieve.return_value.candidates = [dense_cand1, dense_cand2]

    mock_sparse = MagicMock()
    sparse_cand1 = RetrievalCandidate(
        chunk_id="chk_B",
        title="Doc B",
        content="Sparse match 1",
        score=5.0,
        bm25_rank=1,
    )
    sparse_cand2 = RetrievalCandidate(
        chunk_id="chk_C",
        title="Doc C",
        content="Sparse match 2",
        score=3.0,
        bm25_rank=2,
    )
    mock_sparse.retrieve.return_value.candidates = [sparse_cand1, sparse_cand2]

    retriever = UnifiedHybridRetriever(
        mock_session,
        dense_retriever=mock_dense,
        sparse_retriever=mock_sparse,
    )

    query = RetrievalQuery(query_text="테스트 쿼리", top_k=3, rrf_k=2, resolve_entities=False)
    res = retriever.retrieve(query)

    assert res.retrieval_mode == "hybrid_rrf"
    assert len(res.candidates) == 3

    # RRF Score calculations (k=2, dense_w=2.0, sparse_w=1.0):
    # chk_A: dense_rank 1 -> 2.0 / (2 + 1) = 2/3 = 0.6667
    # chk_B: dense_rank 2 (2.0 / (2 + 2) = 0.5) + sparse_rank 1 (1.0 / (2 + 1) = 0.3333) = 0.8333
    # chk_C: sparse_rank 2 -> 1.0 / (2 + 2) = 0.25
    # Expected order: chk_B (0.8333) > chk_A (0.6667) > chk_C (0.25)
    assert res.candidates[0].chunk_id == "chk_B"
    assert res.candidates[0].vector_rank == 2
    assert res.candidates[0].bm25_rank == 1

    assert res.candidates[1].chunk_id == "chk_A"
    assert res.candidates[1].vector_rank == 1
    assert res.candidates[1].bm25_rank is None

    assert res.candidates[2].chunk_id == "chk_C"
    assert res.candidates[2].vector_rank is None
    assert res.candidates[2].bm25_rank == 2
