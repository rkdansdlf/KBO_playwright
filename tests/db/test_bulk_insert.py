"""Multi-row INSERTs are bounded by the dialect's bind-parameter ceiling.

PostgreSQL refuses a statement with more than 65,535 parameters and SQLite 32,766,
and the refusal is an error for the whole batch rather than a skipped row. A single
``insert(T).values(rows)`` statement costs ``rows x columns`` parameters, so the
rows have to be chunked before the statement is built. Measured 2026-11-02: a
nightly ``daily_sync`` DAG run failed on exactly this limit; the statement could
not be re-identified afterwards because container recreation had taken the logs,
which is why the fix bounds the whole class instead of one statement.

What is pinned here:

* the arithmetic — every chunk stays under the limit, every row survives, the
  order is preserved;
* the limit is read at call time, so a test can lower it without touching callers
  (the repository-level proof in ``test_player_basic_repository`` relies on this);
* every repository that builds one of these statements consults the helper — a
  future edit that reintroduces an unbounded ``.values(rows)`` fails here.
"""

from __future__ import annotations

import inspect

from src.db import bulk_insert
from src.db.bulk_insert import MAX_BIND_PARAMS_PER_STATEMENT, chunk_for_bind


def _rows(n: int, columns: int = 5) -> list[dict[str, int]]:
    return [{f"c{i}": i for i in range(columns)} for _ in range(n)]


class TestTheArithmetic:
    def test_a_small_batch_is_one_chunk(self) -> None:
        rows = _rows(10)

        assert chunk_for_bind(rows) == [rows]

    def test_every_chunk_stays_under_the_limit(self) -> None:
        rows = _rows(1_000, columns=35)

        chunks = chunk_for_bind(rows, limit=1_000)

        assert all(len(chunk) * 35 <= 1_000 for chunk in chunks)

    def test_every_row_survives_in_order(self) -> None:
        rows = _rows(1_000)

        chunks = chunk_for_bind(rows, limit=100)

        assert [row for chunk in chunks for row in chunk] == rows

    def test_chunks_are_not_empty(self) -> None:
        chunks = chunk_for_bind(_rows(100), limit=7)

        assert chunks and all(chunks)

    def test_an_empty_batch_yields_no_chunks(self) -> None:
        """The caller decides what an empty batch means; there is no chunk for it."""
        assert chunk_for_bind([]) == []

    def test_a_batch_at_the_boundary_is_one_chunk(self) -> None:
        rows = _rows(20, columns=5)

        assert chunk_for_bind(rows, limit=100) == [rows]

    def test_one_row_more_splits(self) -> None:
        rows = _rows(21, columns=5)

        chunks = chunk_for_bind(rows, limit=100)

        assert len(chunks) == 2


class TestTheLimitIsReadAtCallTime:
    def test_the_module_constant_decides_by_default(self) -> None:
        rows = _rows(MAX_BIND_PARAMS_PER_STATEMENT, columns=1)

        chunks = chunk_for_bind(rows)

        assert len(chunks) == 1

    def test_a_monkeypatched_constant_is_honoured(self, monkeypatch) -> None:
        monkeypatch.setattr(bulk_insert, "MAX_BIND_PARAMS_PER_STATEMENT", 10)

        chunks = bulk_insert.chunk_for_bind(_rows(10, columns=5))

        assert len(chunks) == 5

    def test_the_default_covers_sqlites_lower_ceiling(self) -> None:
        """One bound serves every dialect: below SQLite's 32,766, far below PG's."""
        assert MAX_BIND_PARAMS_PER_STATEMENT < 32_766


class TestEachDialectWrapperChunks:
    """One wrapper bypassing the helper is the whole defect back in one dialect.

    The module-level check above cannot see this: two wrappers still mention
    ``chunk_for_bind`` after the third stops using it. Measured by mutation:
    bypassing ``_save_sqlite_rows`` alone passed every test until this class existed.
    """

    def test_every_safe_batting_wrapper_consults_the_helper(self) -> None:
        from src.repositories import safe_batting_repository as module

        for name in ("_save_sqlite_rows", "_save_mysql_rows", "_save_postgresql_rows"):
            source = inspect.getsource(getattr(module, name))
            assert "chunk_for_bind(" in source, f"{name} builds one unbounded statement"

    def test_every_team_roster_dialect_consults_the_helper(self) -> None:
        """The three dialect branches live in one method; each has its own loop."""
        from src.repositories.team_repository import TeamRepository

        source = inspect.getsource(TeamRepository._execute_roster_upserts)

        assert source.count("for chunk in chunk_for_bind(values):") == 3


class TestEveryUnboundedSiteConsultsTheHelper:
    def test_the_three_batch_upsert_repositories_use_it(self) -> None:
        from src.repositories import player_basic_repository, safe_batting_repository, team_repository

        for module in (player_basic_repository, safe_batting_repository, team_repository):
            assert "chunk_for_bind(" in inspect.getsource(module), module.__name__

    def test_no_multi_row_values_call_remains_unbounded(self) -> None:
        """``.values(rows)`` with a list is the shape that has to be chunked."""
        from src.repositories import player_basic_repository, safe_batting_repository, team_repository

        for module in (player_basic_repository, safe_batting_repository, team_repository):
            source = inspect.getsource(module)
            for line in source.splitlines():
                if ".values(values)" in line or ".values(rows)" in line:
                    # The chunk functions take ``rows`` but are only ever called with
                    # a chunk; the unbounded call site is what must not come back.
                    assert "chunk" in source, f"unbounded multi-row insert in {module.__name__}"
