from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_term import RagChunkTerm
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
        repo = RagChunkRepository(session)

        chunks = [
            {
                "title": "Rule 1",
                "content": "Ground rule double...",
                "meta": {"category": "rulebook", "source_row_id": "rule_001"},
            },
        ]
        count = repo.upsert_chunks(chunks)

        assert count == 1
        stmt = select(RagChunk).where(RagChunk.source_table == "rulebook")
        row = session.execute(stmt).scalars().one()
        assert row.title == "Rule 1"
        assert row.content == "Ground rule double..."
        assert len(row.content_hash) == 64
        assert row.index_version == "rag-v1"
        assert row.index_status == "ACTIVE"

    def test_upsert_chunks_updates_existing(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository(session)

        chunks1 = [
            {
                "title": "Rule 1",
                "content": "v1",
                "meta": {"category": "rulebook", "source_row_id": "rule_001"},
            },
        ]
        repo.upsert_chunks(chunks1)

        chunks2 = [
            {
                "title": "Rule 1",
                "content": "v2",
                "meta": {"category": "rulebook", "source_row_id": "rule_001"},
            },
        ]
        count = repo.upsert_chunks(chunks2)

        assert count == 1
        rows = list(session.execute(select(RagChunk)).scalars().all())
        assert len(rows) == 1
        assert rows[0].content == "v2"

    def test_upsert_chunks_multiple(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository(session)

        chunks = [
            {"title": "A", "content": "AAA", "meta": {"category": "news", "source_row_id": "n1"}},
            {"title": "B", "content": "BBB", "meta": {"category": "news", "source_row_id": "n2"}},
        ]
        count = repo.upsert_chunks(chunks)

        assert count == 2
        rows = list(session.execute(select(RagChunk)).scalars().all())
        assert len(rows) == 2

    def test_upsert_chunks_empty_list(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository(session)

        count = repo.upsert_chunks([])

        assert count == 0

    def test_upsert_chunks_prefers_explicit_source_table(self):
        engine = self._engine()
        self._init_tables(engine)
        session = self._session(engine)
        repo = RagChunkRepository(session)

        repo.upsert_chunks(
            [
                {
                    "title": "Regulation",
                    "content": "content",
                    "meta": {"category": "rulebook", "source_table": "kbo_regulations", "source_row_id": "r1"},
                },
            ],
        )

        row = session.execute(select(RagChunk)).scalars().one()
        assert row.source_table == "kbo_regulations"

    def test_terms_mode_synchronizes_changed_chunk_postings(self, monkeypatch):
        engine = self._engine()
        self._init_tables(engine)
        RagChunkTerm.__table__.create(engine)
        session = self._session(engine)
        repo = RagChunkRepository(session)
        monkeypatch.setattr(repo, "_term_index_enabled", lambda _session: True)

        repo.upsert_chunks(
            [
                {
                    "title": "OPS 기록",
                    "content": "OPS 선수 기록",
                    "meta": {"category": "stats", "source_row_id": "s1"},
                },
            ],
        )
        repo.upsert_chunks(
            [
                {
                    "title": "타율 기록",
                    "content": "타율 선수 기록",
                    "meta": {"category": "stats", "source_row_id": "s1"},
                },
            ],
        )

        terms = set(session.execute(select(RagChunkTerm.token)).scalars())
        assert "ops" not in terms
        assert "타율" in terms
