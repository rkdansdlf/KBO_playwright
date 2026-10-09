"""Tests for copying Oracle dense vectors into a pgvector store.

The three things these pin are the ones that would corrupt a destination
quietly rather than loudly:

* a vector of the wrong length is refused at the row that carries it, instead of
  becoming a NULL embedding that still counts as "indexed";
* ``DELETED`` keeps its status, because retrieval already excludes it and
  rewriting it as ``ACTIVE`` resurrects a tombstone;
* META is promoted into the columns retrieval filters on, and junk in META
  degrades to "no promoted fields" instead of failing the row.
"""

from __future__ import annotations

from array import array
from datetime import date, datetime
from typing import Any

import pytest
from sqlalchemy.exc import SQLAlchemyError
from src.services.rag_vector_migration import (
    _SOURCE_SQL,
    EMBEDDING_DIMENSION,
    SOURCE_TABLE,
    TargetVerification,
    apply_batch,
    chunk_from_row,
    iter_source_chunks,
    vector_literal,
)


def _row(**overrides: Any) -> dict[str, Any]:
    """Return a source row shaped like the Oracle projection."""
    row: dict[str, Any] = {
        "id": 1,
        "season_year": 2026,
        "season_id": None,
        "league_type_code": 0,
        "team_id": "OB",
        "player_id": "52204",
        "source_table": "game_play_by_play",
        "source_row_id": "20260823LTOB0:1",
        "title": "1회초 타자 정보",
        "content": "본문",
        "content_hash": "a" * 64,
        "index_version": "rag-v1",
        "index_status": "ACTIVE",
        "indexed_at": datetime(2026, 8, 29, 12, 13),
        "created_at": datetime(2026, 8, 23, 10, 0),
        "updated_at": datetime(2026, 8, 29, 12, 13),
        "meta": '{"document_type": "game_play_by_play", "language": "ko", "game_date": "2026-08-23"}',
        "embedding_vector": array("f", [0.5] * EMBEDDING_DIMENSION),
    }
    row.update(overrides)
    return row


class _StubResult:
    """Return canned partitions so streaming can be tested without a database.

    Deliberately exposes only what ``CursorResult`` exposes to this code path:
    ``mappings()`` then ``partitions()``. Two earlier stubs were more generous
    than the driver and hid two real bugs -- an ``execution_options()`` method
    that belongs to the call rather than the result, and rows that answered
    ``row["id"]`` when ``partitions()`` yields positional ``Row`` tuples unless
    ``mappings()`` is asked for first.
    """

    def __init__(self, partitions: list[list[dict[str, Any]]]) -> None:
        """Store the partitions this result will hand out."""
        self._partitions = partitions

    def mappings(self) -> _StubResult:
        """Return mapping rows, as the driver does when asked for them."""
        return self

    def partitions(self, size: int) -> list[list[dict[str, Any]]]:
        """Return the canned partitions."""
        assert size > 0, "batch size must be positive"
        return self._partitions


class _StubSession:
    """Record executed statements and fail on demand."""

    def __init__(self, *, fail_batches: bool = False, fail_ids: set[int] | None = None) -> None:
        """Configure which writes should fail."""
        self.fail_batches = fail_batches
        self.fail_ids = fail_ids or set()
        self.executed: list[Any] = []
        self.commits = 0
        self.rollbacks = 0
        self.rows = [[_row()]]

    def execute(self, statement: Any, params: Any = None, **options: Any) -> _StubResult:
        """Record the call and raise for the rows configured to fail."""
        if self.fail_batches and isinstance(params, list):
            raise SQLAlchemyError("batch rejected")
        if isinstance(params, dict) and params.get("source_row_id") in self.fail_ids:
            raise SQLAlchemyError("row rejected")
        self.executed.append(params)
        return _StubResult(self.rows)

    def commit(self) -> None:
        """Count a commit."""
        self.commits += 1

    def rollback(self) -> None:
        """Count a rollback."""
        self.rollbacks += 1


class TestVectorLiteral:
    """Pin the length check that keeps bad vectors out of the destination."""

    def test_the_source_query_reads_the_declared_source_table(self) -> None:
        """Keep the query and the declared source table from drifting apart."""
        assert f"FROM {SOURCE_TABLE} " in _SOURCE_SQL.text

    def test_round_trips_the_exact_dimension(self) -> None:
        """Return the pgvector text form for a full-length vector."""
        literal = vector_literal([0.5] * EMBEDDING_DIMENSION)
        assert literal.startswith("[0.5,")
        assert literal.endswith("]")
        assert literal.count(",") == EMBEDDING_DIMENSION - 1

    def test_refuses_a_short_vector_and_names_the_length(self) -> None:
        """Reject a short vector instead of letting the database fail on it."""
        with pytest.raises(ValueError, match="expected 1536 dimensions, got 3"):
            vector_literal([0.1, 0.2, 0.3])


class TestMetaPromotion:
    """Pin the META-to-column promotion that retrieval filters depend on."""

    def test_promotes_the_filterable_fields(self) -> None:
        """Lift document type, language and game date out of META."""
        chunk = chunk_from_row(_row())
        assert chunk.document_type == "game_play_by_play"
        assert chunk.language == "ko"
        assert chunk.game_date == date(2026, 8, 23)
        assert chunk.source_url is None
        assert chunk.published_at is None

    def test_unparsable_meta_degrades_to_no_promoted_fields(self) -> None:
        """Keep the row rather than failing it when META is junk."""
        chunk = chunk_from_row(_row(meta="{not json"))
        assert chunk.document_type is None
        assert chunk.meta == {}

    def test_non_mapping_meta_is_ignored(self) -> None:
        """Treat a JSON array in META as carrying nothing promotable."""
        chunk = chunk_from_row(_row(meta="[1, 2, 3]"))
        assert chunk.meta == {}

    def test_blank_promoted_values_read_as_absent(self) -> None:
        """Not store empty strings where the column means "unknown"."""
        chunk = chunk_from_row(_row(meta='{"document_type": "  ", "language": "ko"}'))
        assert chunk.document_type is None
        assert chunk.language == "ko"


class TestStatusIsPreserved:
    """Pin that tombstones survive the copy instead of being resurrected."""

    def test_deleted_rows_keep_their_status(self) -> None:
        """Carry DELETED through rather than normalizing it to ACTIVE."""
        chunk = chunk_from_row(_row(index_status="DELETED"))
        assert chunk.as_bind_params()["index_status"] == "DELETED"


class TestBindParameters:
    """Pin the destination row shape."""

    def test_carries_identity_content_meta_and_vector(self) -> None:
        """Bind every destination column the upsert names."""
        params = chunk_from_row(_row()).as_bind_params()
        assert params["source_table"] == "game_play_by_play"
        assert params["source_row_id"] == "20260823LTOB0:1"
        assert params["content"] == "본문"
        assert params["document_type"] == "game_play_by_play"
        assert params["meta"] == '{"document_type": "game_play_by_play", "language": "ko", "game_date": "2026-08-23"}'
        assert params["embedding"].startswith("[0.5,")

    def test_rejects_a_vector_of_the_wrong_length(self) -> None:
        """Refuse the row rather than writing a null embedding."""
        chunk = chunk_from_row(_row(embedding_vector=array("f", [0.5, 0.5])))
        with pytest.raises(ValueError, match="expected 1536 dimensions"):
            chunk.as_bind_params()

    def test_accepts_the_vector_text_form(self) -> None:
        """Read a vector delivered as text as well as one delivered as an array."""
        chunk = chunk_from_row(_row(embedding_vector="[0.25,0.5]"))
        assert chunk.embedding == (0.25, 0.5)


class TestStreaming:
    """Pin that source rows arrive as bounded batches in id order."""

    def test_streams_rows_as_batches(self) -> None:
        """Convert every partition row into a chunk."""
        session = _StubSession()
        session.rows = [[_row(id=1), _row(id=2)], [_row(id=3)]]
        batches = list(iter_source_chunks(session, after_id=0, batch_size=2))
        assert [chunk.source_id for batch in batches for chunk in batch] == [1, 2, 3]


class TestBatchIsolation:
    """Pin that one bad row cannot roll back the rows beside it."""

    def test_a_healthy_batch_commits_once(self) -> None:
        """Take the fast path when the whole batch is accepted."""
        session = _StubSession()
        copied, failed = apply_batch(session, [chunk_from_row(_row())])
        assert (copied, failed) == (1, 0)
        assert session.commits == 1
        assert session.rollbacks == 0

    def test_a_rejected_batch_is_retried_row_by_row(self) -> None:
        """Isolate the offending row and still copy the rest."""
        session = _StubSession(fail_batches=True, fail_ids={"20260823LTOB0:1"})
        copied, failed = apply_batch(
            session,
            [chunk_from_row(_row()), chunk_from_row(_row(id=2, source_row_id="20260823LTOB0:2"))],
        )
        assert (copied, failed) == (1, 1)
        assert session.rollbacks == 2  # the batch attempt, then the offending row

    def test_an_empty_batch_is_a_no_op(self) -> None:
        """Do not touch the database for an empty batch."""
        session = _StubSession()
        assert apply_batch(session, []) == (0, 0)
        assert session.executed == []


class TestTargetVerification:
    """Pin when a finished copy counts as clean."""

    def test_clean_requires_every_embedding(self) -> None:
        """Report clean only with all rows embedded, right-sized and non-zero."""
        assert TargetVerification(rows=10, with_embedding=10, wrong_dimension=0, zero_vectors=0).is_clean

    def test_missing_embeddings_are_not_clean(self) -> None:
        """Catch a partial copy."""
        assert not TargetVerification(rows=10, with_embedding=9, wrong_dimension=0, zero_vectors=0).is_clean

    def test_zero_vectors_are_not_clean(self) -> None:
        """Catch the silent fallback that writes plausible-looking empties."""
        assert not TargetVerification(rows=10, with_embedding=10, wrong_dimension=0, zero_vectors=1).is_clean

    def test_wrong_dimension_is_not_clean(self) -> None:
        """Catch a dimension mismatch that would break the vector index."""
        assert not TargetVerification(rows=10, with_embedding=10, wrong_dimension=2, zero_vectors=0).is_clean
