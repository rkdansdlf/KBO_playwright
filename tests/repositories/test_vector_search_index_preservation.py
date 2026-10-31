"""A filtered vector search narrows by ids instead of disabling the indexes.

The repository used to answer a filtered search with
``SET LOCAL enable_indexscan = off`` so the filter's rows were scanned exactly
rather than approximately. Measured on the production table (2026-10-30):

* the same predicate through ``idx_rag_chunks_game_date`` costs **0.049 ms** with
  index scans on and **91.7 ms** with them off -- ``enable_indexscan`` does not stop
  a bitmap scan, so the date still came through its B-tree, just slowly;
* with no ``game_date`` in the filter nothing could be bitmapped and the plan became
  a sequential read of the whole 3.1 GB heap: **300 s timeout**.

The replacement reads the ids the scalar predicate selects (through the same
B-tree) and hands them to the distance query as an ``IN`` list. That keeps every
index in play, computes distance only for rows the filter admits, and involves no
approximation -- which is why the exactness fallback retry was removed with it.

What is pinned here:

* the repository never disables an index again (source assertion, because that is
  the shape of the regression);
* the ids come from the scalar filter and are bounded;
* a filter that is not selective enough leaves the statement alone rather than
  building a huge ``IN`` list.
"""

from __future__ import annotations

import ast
import inspect
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.models.rag_chunk_vector import RagChunkVector
from src.repositories import vector_search_repository as module
from src.repositories.vector_search_repository import (
    _MAX_EXACT_ID_FILTER,
    _filtered_ids,
    _scalar_filter_conditions,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    RagChunkVector.__table__.create(engine)
    return sessionmaker(bind=engine)()


#: A 1536-dimension vector, because that is the column's declared width. SQLite
#: stores the pgvector column as JSON text, so any list of the right length will do.
_EMBEDDING = [0.1] * 1536


def _row(session, *, row_id: int, source_table: str, game_date: date | None, with_embedding: bool = True) -> None:
    session.add(
        RagChunkVector(
            id=row_id,
            source_table=source_table,
            source_row_id=f"row-{row_id}",
            content="내용",
            game_date=game_date,
            index_status="ACTIVE",
            embedding=_EMBEDDING if with_embedding else None,
        ),
    )
    session.commit()


def _sql_text_literals(node: ast.AST) -> list[str]:
    """Return every string handed to ``text(...)`` in a module.

    The docstring records the old ``SET LOCAL enable_indexscan = off`` in prose, so
    a plain substring search over the source cannot tell an explanation from the
    statement. Only the literals that are arguments of a ``text()`` call can reach
    the database, and those are what the assertion has to look at.

    Args:
        node: Parsed module.

    Returns:
        The string arguments of every ``text(...)`` call.

    """
    literals: list[str] = []
    for call in ast.walk(node):
        if not isinstance(call, ast.Call):
            continue
        if getattr(call.func, "id", None) != "text" and getattr(call.func, "attr", None) != "text":
            continue
        for arg in call.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                literals.append(arg.value)
    return literals


class TestTheIndexIsNeverDisabled:
    def test_the_repository_does_not_turn_index_scans_off(self) -> None:
        """The regression this file exists for is one ``SET LOCAL`` away."""
        literals = _sql_text_literals(ast.parse(inspect.getsource(module)))

        assert not [sql for sql in literals if "enable_indexscan" in sql]

    def test_the_docstring_still_records_why(self) -> None:
        """Removing the explanation invites reinstating the optimisation."""
        source = inspect.getsource(module)
        assert "91.7 ms" in source
        assert "300 s timeout" in source


class TestTheConditions:
    def test_no_source_table_means_nothing_to_narrow_by(self) -> None:
        assert _scalar_filter_conditions(None, None) is None

    def test_a_source_table_narrows_on_its_own(self) -> None:
        conditions = _scalar_filter_conditions("game", None)

        assert conditions is not None
        assert len(conditions) == 2

    def test_a_game_date_narrows_further(self) -> None:
        conditions = _scalar_filter_conditions("game", "2026-10-26")

        assert conditions is not None
        assert len(conditions) == 3

    def test_an_unusable_date_drops_only_that_condition(self) -> None:
        """A mistyped date narrows less; it does not fail the search."""
        conditions = _scalar_filter_conditions("game", "어제")

        assert conditions is not None
        assert len(conditions) == 2


class TestTheIds:
    def test_it_returns_the_ids_the_filter_selects(self, session) -> None:
        _row(session, row_id=1, source_table="game", game_date=date(2026, 10, 26))
        _row(session, row_id=2, source_table="game", game_date=date(2026, 10, 26))
        _row(session, row_id=3, source_table="game", game_date=date(2026, 10, 25))

        conditions = _scalar_filter_conditions("game", "2026-10-26")
        assert conditions is not None

        assert sorted(_filtered_ids(session, conditions) or []) == [1, 2]

    def test_rows_without_an_embedding_are_not_candidates(self, session) -> None:
        """Distance against NULL selects nothing, so it should not be listed."""
        _row(session, row_id=1, source_table="game", game_date=date(2026, 10, 26))
        _row(session, row_id=2, source_table="game", game_date=date(2026, 10, 26), with_embedding=False)

        conditions = _scalar_filter_conditions("game", "2026-10-26")
        assert conditions is not None

        assert _filtered_ids(session, conditions) == [1]

    def test_an_empty_filter_returns_none_rather_than_an_empty_list(self, session) -> None:
        """None means "run the statement unchanged"; ``[]`` would mean "no candidates"."""
        _row(session, row_id=1, source_table="game", game_date=date(2026, 10, 25))

        conditions = _scalar_filter_conditions("game", "2026-10-26")
        assert conditions is not None

        assert _filtered_ids(session, conditions) is None

    def test_a_filter_that_is_not_selective_enough_returns_none(self, session) -> None:
        """An ``IN`` list of everything is a sequential scan spelled differently."""
        session.add_all(
            [
                RagChunkVector(
                    id=i,
                    source_table="game_play_by_play",
                    source_row_id=f"row-{i}",
                    content="내용",
                    game_date=date(2026, 10, 26),
                    index_status="ACTIVE",
                    embedding=_EMBEDDING,
                )
                for i in range(1, _MAX_EXACT_ID_FILTER + 2)
            ],
        )
        session.commit()

        conditions = _scalar_filter_conditions("game_play_by_play", "2026-10-26")
        assert conditions is not None

        assert _filtered_ids(session, conditions) is None

    def test_the_bound_is_inclusive(self, session) -> None:
        """A filter at the limit is still worth an ``IN`` list."""
        session.add_all(
            [
                RagChunkVector(
                    id=i,
                    source_table="game",
                    source_row_id=f"row-{i}",
                    content="내용",
                    game_date=date(2026, 10, 26),
                    index_status="ACTIVE",
                    embedding=_EMBEDDING,
                )
                for i in range(1, _MAX_EXACT_ID_FILTER + 1)
            ],
        )
        session.commit()

        conditions = _scalar_filter_conditions("game", "2026-10-26")
        assert conditions is not None

        ids = _filtered_ids(session, conditions)
        assert ids is not None
        assert len(ids) == _MAX_EXACT_ID_FILTER


class TestTheStatementItRuns:
    def _capture(self, monkeypatch) -> dict:
        captured: dict = {}

        def fake_load(session, stmt):
            captured["sql"] = str(stmt.compile(compile_kwargs={"literal_binds": False}))
            captured["params"] = dict(stmt.compile().params)
            return []

        monkeypatch.setattr(module.VectorSearchRepository, "_load_search_results", staticmethod(fake_load))
        return captured

    def test_a_selective_filter_becomes_an_in_list(self, session, monkeypatch) -> None:
        from sqlalchemy import select

        _row(session, row_id=1, source_table="game", game_date=date(2026, 10, 26))
        captured = self._capture(monkeypatch)
        stmt = select(RagChunkVector)

        module.VectorSearchRepository._load_filtered_search_results(session, stmt, "game", "2026-10-26")

        assert " IN (" in captured["sql"].upper()

    def test_an_unselective_filter_leaves_the_statement_alone(self, session, monkeypatch) -> None:
        from sqlalchemy import select

        captured = self._capture(monkeypatch)
        stmt = select(RagChunkVector)

        # No rows at all: the narrowing has nothing to add and must not turn the
        # search into an empty result.
        module.VectorSearchRepository._load_filtered_search_results(session, stmt, "game", "2026-10-26")

        assert " IN (" not in captured["sql"].upper()

    def test_no_source_table_means_no_narrowing(self, session, monkeypatch) -> None:
        from sqlalchemy import select

        captured = self._capture(monkeypatch)
        stmt = select(RagChunkVector)

        module.VectorSearchRepository._load_filtered_search_results(session, stmt, None, None)

        assert " IN (" not in captured["sql"].upper()
