"""Unit tests for src.rag.base_retriever."""

from __future__ import annotations

import pytest

from src.rag.base_retriever import BaseRetriever
from src.rag.dto import RetrievalCandidate, RetrievalQuery, RetrievalResult


class DummyRetriever(BaseRetriever):
    """Concrete dummy retriever for testing abstract retriever functionality."""

    def __init__(self) -> None:
        super().__init__(name="DummyRetriever")

    def retrieve(self, query: RetrievalQuery | str, **kwargs) -> RetrievalResult:
        t0 = 0.0
        norm_query = self._normalize_query(query, **kwargs)
        candidates = [
            RetrievalCandidate(
                chunk_id="test_1",
                title="Test Title",
                content=f"Result for: {norm_query.query_text}",
                score=0.95,
            )
        ]
        return self._create_result(norm_query, candidates, t0)


def test_normalize_query_string() -> None:
    retriever = DummyRetriever()
    norm = retriever._normalize_query("LG 트윈스", top_k=8, category="team")
    assert isinstance(norm, RetrievalQuery)
    assert norm.query_text == "LG 트윈스"
    assert norm.top_k == 8
    assert norm.category == "team"


def test_normalize_query_object_with_overrides() -> None:
    retriever = DummyRetriever()
    base_query = RetrievalQuery(query_text="삼성 라이온즈", top_k=3)
    norm = retriever._normalize_query(base_query, top_k=10, category="roster")
    assert norm.query_text == "삼성 라이온즈"
    assert norm.top_k == 10
    assert norm.category == "roster"


def test_sync_retrieve() -> None:
    retriever = DummyRetriever()
    res = retriever.retrieve("한화 이글스", top_k=3)
    assert isinstance(res, RetrievalResult)
    assert res.query.query_text == "한화 이글스"
    assert len(res.candidates) == 1
    assert res.candidates[0].chunk_id == "test_1"
    assert res.retrieval_mode == "DummyRetriever"


@pytest.mark.asyncio
async def test_async_retrieve() -> None:
    retriever = DummyRetriever()
    res = await retriever.retrieve_async("KIA 타이거즈", top_k=5)
    assert isinstance(res, RetrievalResult)
    assert res.query.query_text == "KIA 타이거즈"
    assert len(res.candidates) == 1
    assert res.candidates[0].content == "Result for: KIA 타이거즈"
