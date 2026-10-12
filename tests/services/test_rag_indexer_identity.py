"""Tests for the identities the service indexer writes.

The indexer hands chunks to the repository, which takes ``meta.source_row_id`` as
the chunk identity whenever a top-level ``source_row_id`` is absent. That fallback
is what makes these tests worth writing: a wrong identity is not a crash, it is a
second copy of the same source rows under a key nothing else recognises, appearing
once per run and quietly inflating the store.

press_releases is the one that got this wrong -- the table's own ``id``, which the
census knows is re-assigned on every re-crawl -- while milestones and Futures next
to it already key on the natural identity.
"""

from __future__ import annotations

from src.services.rag_indexer import RagKnowledgeIndexer


class _RecordingRepo:
    """Capture the chunks the indexer would upsert."""

    def __init__(self) -> None:
        """Start with nothing captured."""
        self.chunks: list[dict] = []

    def upsert_chunks(self, chunks: list[dict]) -> int:
        """Record the batch instead of writing it."""
        self.chunks.extend(chunks)
        return len(chunks)


class _Result:
    """Stand in for a SQLAlchemy result supporting the scalars().all() chain."""

    def __init__(self, rows: list[object]) -> None:
        """Store the rows to hand out."""
        self._rows = rows

    def scalars(self) -> _Scalars:
        """Return the scalar accessor."""
        return _Scalars(self._rows)


class _Scalars:
    """Scalar accessor that can also be iterated directly."""

    def __init__(self, rows: list[object]) -> None:
        """Store the rows to hand out."""
        self._rows = rows

    def all(self) -> list[object]:
        """Return every row."""
        return list(self._rows)

    def __iter__(self) -> object:
        """Iterate rows, for call sites that skip ``all``."""
        return iter(self._rows)


class _Session:
    """Serve one table's rows and refuse anything else."""

    def __init__(self, rows: list[object]) -> None:
        """Store the rows to hand out."""
        self._rows = rows

    def execute(self, statement: object) -> _Result:
        """Return the canned rows for any query."""
        return _Result(self._rows)


class _Release:
    """A press release row with the fields the indexer reads."""

    def __init__(self, pk: int, notice_id: str) -> None:
        """Set every column the chunk body interpolates."""
        self.id = pk
        self.notice_id = notice_id
        self.published_date = "2026-08-17"
        self.title = "백인천 전 감독 별세"
        self.category = "공시/공지"
        self.source_url = "https://www.koreabaseball.com/Notice/View.aspx?notice=9646"


def _index_press_releases(rows: list[object]) -> _RecordingRepo:
    """Run the press-release indexer against canned rows, return what it wrote."""
    indexer = RagKnowledgeIndexer(session=_Session(rows))
    repo = _RecordingRepo()
    indexer.rag_repo = repo
    indexer.embedding_service = None
    indexer._embed_chunks = lambda chunks: None
    indexer.index_press_releases()
    return repo


class TestPressReleaseIdentity:
    """Pin the identity press-release chunks are written under."""

    def test_uses_the_notice_number(self) -> None:
        """Key on KBO's notice number, which survives a re-crawl."""
        repo = _index_press_releases([_Release(3, "9646")])
        assert repo.chunks[0]["meta"]["source_row_id"] == "9646"

    def test_the_table_id_never_reaches_the_identity(self) -> None:
        """The legacy id stays out of the identity field entirely."""
        repo = _index_press_releases([_Release(3, "9646")])
        meta = repo.chunks[0]["meta"]
        assert meta["source_row_id"] != "3"
        assert meta["notice_id"] == "9646"
