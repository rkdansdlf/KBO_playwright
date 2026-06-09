from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.models.rag_chunk import RagChunk
from src.repositories.rag_chunk_repository import RagChunkRepository


class TestRagChunkRepository:
    def _engine(self):
        return create_engine("sqlite:///:memory:")

    def _session(self, engine):
        return sessionmaker(bind=engine)()

    def _init_tables(self, engine):
        RagChunk.__table__.create(engine)

    def test_upsert_chunks_inserts_new(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository()

        chunks = [
            {
                "title": "Rule 1",
                "content": "Ground rule double...",
                "meta": {"category": "rulebook", "source_row_id": "rule_001"},
            }
        ]
        count = repo.upsert_chunks(session, chunks)

        assert count == 1
        stmt = select(RagChunk).where(RagChunk.source_table == "rulebook")
        row = session.execute(stmt).scalars().one()
        assert row.title == "Rule 1"
        assert row.content == "Ground rule double..."

    def test_upsert_chunks_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository()

        chunks1 = [
            {
                "title": "Rule 1",
                "content": "v1",
                "meta": {"category": "rulebook", "source_row_id": "rule_001"},
            }
        ]
        repo.upsert_chunks(session, chunks1)

        chunks2 = [
            {
                "title": "Rule 1",
                "content": "v2",
                "meta": {"category": "rulebook", "source_row_id": "rule_001"},
            }
        ]
        count = repo.upsert_chunks(session, chunks2)

        assert count == 1
        rows = list(session.execute(select(RagChunk)).scalars().all())
        assert len(rows) == 1
        assert rows[0].content == "v2"

    def test_upsert_chunks_multiple(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository()

        chunks = [
            {"title": "A", "content": "AAA", "meta": {"category": "news", "source_row_id": "n1"}},
            {"title": "B", "content": "BBB", "meta": {"category": "news", "source_row_id": "n2"}},
        ]
        count = repo.upsert_chunks(session, chunks)

        assert count == 2
        rows = list(session.execute(select(RagChunk)).scalars().all())
        assert len(rows) == 2

    def test_upsert_chunks_empty_list(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository()

        count = repo.upsert_chunks(session, [])

        assert count == 0
