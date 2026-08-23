"""Tests for Oracle sparse term postings queries."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy.dialects import oracle

from src.repositories.oracle_sparse_search_repository import OracleSparseSearchRepository


def test_search_candidates_uses_term_and_date_indexes_without_vectors() -> None:
    """Compile a bounded term slice instead of scanning RAG CLOB columns."""
    session = MagicMock()
    empty_rows = MagicMock()
    empty_rows.all.return_value = []
    session.execute.side_effect = [empty_rows, empty_rows]

    OracleSparseSearchRepository().search_candidates(
        session,
        ["OPS", "선수"],
        top_k=5,
        filters={"source_table": "player_basic", "game_date": "2026-08-21"},
    )

    statement = session.execute.call_args.args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "rag_chunk_terms" in compiled.lower()
    assert "FETCH FIRST" in compiled
    assert "embedding_vector" not in compiled


def test_search_candidates_accepts_oracle_like_bind_dialect() -> None:
    """Keep the repository independent from a concrete Oracle engine in callers."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
    empty_rows = MagicMock()
    empty_rows.all.return_value = []
    session.execute.side_effect = [empty_rows]

    results = OracleSparseSearchRepository().search_candidates(session, ["OPS"], top_k=5)

    assert results == []


def test_search_candidates_merges_per_token_postings_before_fetching_chunks() -> None:
    """Aggregate indexed token scores in Python after bounded index lookups."""
    session = MagicMock()
    slice_one = MagicMock()
    slice_one.all.return_value = [(1,)]
    detail_one = MagicMock()
    detail_one.all.return_value = [SimpleNamespace(rag_chunk_id=1, term_count=3, title_count=1)]
    slice_two = MagicMock()
    slice_two.all.return_value = [(2,)]
    detail_two = MagicMock()
    detail_two.all.return_value = [SimpleNamespace(rag_chunk_id=2, term_count=4, title_count=0)]
    chunk_one = SimpleNamespace(id=1)
    chunk_two = SimpleNamespace(id=2)
    chunk_result = MagicMock()
    chunk_result.scalars.return_value.all.return_value = [chunk_one, chunk_two]
    session.execute.side_effect = [slice_one, detail_one, slice_two, detail_two, chunk_result]

    results = OracleSparseSearchRepository().search_candidates(session, ["OPS", "선수"], top_k=1)

    assert results == [chunk_one, chunk_two]
    assert session.execute.call_count == 5


def test_unfiltered_postings_queries_avoid_chunk_join_and_status_scan() -> None:
    """Keep unfiltered token lookups index-only until the final ID fetch."""
    session = MagicMock()
    empty_rows = MagicMock()
    empty_rows.all.return_value = []
    session.execute.side_effect = [empty_rows]

    OracleSparseSearchRepository().search_candidates(session, ["OPS"], top_k=5)

    statement = session.execute.call_args.args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "rag_chunk_terms" in compiled.lower()
    assert "rag_chunks" not in compiled.lower()
    assert "index_status" not in compiled.lower()


def test_source_filter_slices_postings_within_the_scope() -> None:
    """Slice high-frequency tokens inside the source scope via the composite index."""
    session = MagicMock()
    empty_rows = MagicMock()
    empty_rows.all.return_value = []
    session.execute.side_effect = [empty_rows]

    OracleSparseSearchRepository().search_candidates(
        session,
        ["규정"],
        top_k=5,
        filters={"source_table": "kbo_regulations", "document_type": "markdown_doc"},
    )

    statement = session.execute.call_args.args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "rag_chunk_terms" in compiled.lower()
    assert "source_table" in compiled.lower()
    assert "FETCH FIRST" in compiled


def test_chunk_column_filters_keep_the_joined_scored_path() -> None:
    """Apply team/season filters through rag_chunks during per-token lookups."""
    session = MagicMock()
    empty_rows = MagicMock()
    empty_rows.all.return_value = []
    session.execute.side_effect = [empty_rows]

    OracleSparseSearchRepository().search_candidates(session, ["OPS"], top_k=5, filters={"team_id": "HT"})

    statement = session.execute.call_args.args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "rag_chunks" in compiled.lower()
    assert "index_status" in compiled.lower()


def test_full_row_fetch_is_limited_to_top_scored_buffer() -> None:
    """Fetch complete rows only for the highest-scored candidate buffer."""
    session = MagicMock()
    slice_rows = MagicMock()
    slice_rows.all.return_value = [(i,) for i in range(1, 31)]
    detail_rows = MagicMock()
    detail_rows.all.return_value = [SimpleNamespace(rag_chunk_id=i, term_count=i, title_count=0) for i in range(1, 31)]
    chunk_result = MagicMock()
    chunk_result.scalars.return_value.all.return_value = []
    session.execute.side_effect = [slice_rows, detail_rows, chunk_result]

    results = OracleSparseSearchRepository().search_candidates(session, ["선수"], top_k=5)

    assert results == []
    fetch_statement = session.execute.call_args_list[2].args[0]
    expanded = str(fetch_statement.compile(dialect=oracle.dialect(), compile_kwargs={"literal_binds": True}))
    in_clause = expanded.split("IN (", maxsplit=1)[1].split(")", maxsplit=1)[0]
    fetched_ids = {int(value.strip()) for value in in_clause.split(",")}
    assert fetched_ids == set(range(1, 31))
