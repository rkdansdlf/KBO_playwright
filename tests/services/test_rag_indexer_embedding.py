"""Embedding attachment tests for RagKnowledgeIndexer."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.models.base import Base
from src.models.kbo_press_release import KboPressRelease
from src.models.rag_chunk import RagChunk
from src.services.rag_indexer import RagKnowledgeIndexer


class FakeEmbeddingService:
    """Deterministic offline provider returning one fixed-size vector per text."""

    def __init__(self, *, vector: list[float] | None = None, count_override: int | None = None) -> None:
        self._vector = vector or [0.1, 0.2, 0.3]
        self._count_override = count_override

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        if self._count_override is not None:
            return [self._vector] * self._count_override
        return [list(self._vector) for _ in texts]


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    yield session
    session.close()
    engine.dispose()


def _add_release(session: Session, *, notice_id: str = "101") -> None:
    from datetime import date

    session.add(
        KboPressRelease(
            notice_id=notice_id,
            category="공시",
            title="로스터트 변경 공시",
            published_date=date(2026, 8, 23),
            source_url=f"https://example.com/{notice_id}",
        )
    )
    session.commit()


def test_index_press_releases_persists_embeddings(db_session: Session) -> None:
    _add_release(db_session)
    indexer = RagKnowledgeIndexer(db_session, embedding_service=FakeEmbeddingService())

    count = indexer.index_press_releases()

    assert count == 1
    row = db_session.execute(select(RagChunk)).scalars().one()
    assert row.embedding == [0.1, 0.2, 0.3]


def test_zero_vector_batch_is_rejected_without_upsert(db_session: Session) -> None:
    _add_release(db_session)
    indexer = RagKnowledgeIndexer(
        db_session,
        embedding_service=FakeEmbeddingService(vector=[0.0, 0.0]),
    )

    with pytest.raises(RuntimeError, match="zero vector"):
        indexer.index_press_releases()

    assert db_session.execute(select(RagChunk)).scalars().all() == []


def test_embedding_count_mismatch_is_rejected(db_session: Session) -> None:
    _add_release(db_session)
    indexer = RagKnowledgeIndexer(
        db_session,
        embedding_service=FakeEmbeddingService(count_override=0),
    )

    with pytest.raises(RuntimeError, match="returned 0 vectors"):
        indexer.index_press_releases()

    assert db_session.execute(select(RagChunk)).scalars().all() == []
