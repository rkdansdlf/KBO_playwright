"""VECTOR bind handling in the Oracle writer.

The RAG path writes embeddings to Oracle. Oracle does not infer a bind type for
a VECTOR column from a Python list, so a NULL embedding has to be declared as
one, and a merged chunk has to declare one bind per row. These tests pin both
halves of that: what a source value becomes, and which binds the writer
declares as VECTOR.

The last class pins a removal. Rows written by earlier chunks are still
uncommitted on the same transaction, so the rollback that used to run after a
chunk failure discarded them -- silently, because the caller was told the batch
had partial failures and the earlier chunk had already been counted as synced.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.sync.oracle_writer as oracle_writer_module
from src.sync.oracle_writer import BULK_MERGE_ROWS, OracleWriter, _normalize_vector_value


def _writer() -> OracleWriter:
    """Return an OracleWriter with a mocked connection -- no real Oracle."""
    writer = OracleWriter.__new__(OracleWriter)
    writer.engine = MagicMock()
    writer.conn = MagicMock()
    writer.arraysize = 5000
    return writer


class TestTheSourceValueBecomesAFloatSequence:
    @pytest.mark.parametrize("raw", ["NULL", "null", "NONE", " None ", "nOnE"])
    def test_a_placeholder_means_no_embedding(self, raw: str) -> None:
        """A placeholder has to bind as SQL NULL. Binding the literal string
        would be rejected by the VECTOR column instead of storing "no vector".
        """
        assert _normalize_vector_value(raw) is None

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("[1.0,2.0,3.0]", [1.0, 2.0, 3.0]),
            ("[1.0, 2.0]", [1.0, 2.0]),
            ("array([1.0,2.0])", [1.0, 2.0]),
            ("ARRAY([0.5])", [0.5]),
            ([1, 2, 3], [1.0, 2.0, 3.0]),
            ((1.5, 2.5), [1.5, 2.5]),
        ],
    )
    def test_a_vector_is_coerced_to_floats(self, raw: object, expected: list[float]) -> None:
        assert _normalize_vector_value(raw) == expected

    def test_an_empty_bracket_list_is_an_empty_vector(self) -> None:
        assert _normalize_vector_value("[]") == []

    @pytest.mark.parametrize(
        "raw",
        ["not a vector", "[1.0,", "array()", "[not-a-number]", 4.2, None, {"a": 1}, b"\x01"],
    )
    def test_anything_unparseable_is_returned_unchanged(self, raw: object) -> None:
        """The failure has to surface at the database. Returning the value
        unchanged keeps it visible, where turning it into NULL would look like
        an embedding that had never been computed.
        """
        assert _normalize_vector_value(raw) == raw


class TestTheBindsDeclaredAsVector:
    def test_vector_columns_are_flattened_for_every_row(self) -> None:
        names = OracleWriter._vector_bind_names(
            ["id", "embedding", "note"],
            {"ID": "NUMBER", "EMBEDDING": "VECTOR(1536)", "NOTE": "VARCHAR2"},
            2,
        )

        assert names == ["c1", "c4"]

    def test_a_table_without_vector_columns_declares_nothing(self) -> None:
        assert OracleWriter._vector_bind_names(["id"], {"ID": "NUMBER"}, 3) == []

    def test_the_type_mapping_is_keyed_by_the_catalog_case(self) -> None:
        """Oracle reports column names in upper case while the writer's own
        column list is lower case, so the lookup upper-cases the column before
        consulting the mapping. That convention predates the vector work -- it
        is how ``build_merge_sql`` already read the same mapping.
        """
        assert OracleWriter._vector_bind_names(["embedding"], {"EMBEDDING": "vector"}, 1) == ["c0"]

        assert OracleWriter._vector_bind_names(["embedding"], {"embedding": "vector"}, 1) == []

    def test_each_bind_is_declared_as_vector_on_the_cursor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        sentinel = object()
        monkeypatch.setattr(oracle_writer_module, "oracledb", SimpleNamespace(DB_TYPE_VECTOR=sentinel))
        cursor = MagicMock()

        OracleWriter._pin_vector_bind_types(cursor, ["c0", "c4"])

        cursor.setinputsizes.assert_called_once_with(c0=sentinel, c4=sentinel)

    def test_no_binds_is_a_no_op(self) -> None:
        cursor = MagicMock()

        OracleWriter._pin_vector_bind_types(cursor, [])

        cursor.setinputsizes.assert_not_called()

    def test_a_driver_without_vector_support_is_a_no_op(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An older driver has no DB_TYPE_VECTOR. The writer must not crash on
        the way to a table that may not even have a vector column.
        """
        monkeypatch.setattr(oracle_writer_module, "oracledb", SimpleNamespace())
        cursor = MagicMock()

        OracleWriter._pin_vector_bind_types(cursor, ["c0"])

        cursor.setinputsizes.assert_not_called()


class TestAChunkFailureKeepsEarlierChunks:
    def test_a_failed_chunk_does_not_roll_back_the_transaction(self, monkeypatch: pytest.MonkeyPatch) -> None:
        writer = _writer()
        monkeypatch.setattr(writer, "build_merge_sql", MagicMock(return_value="MERGE INTO t ..."))
        cursor = writer.conn.cursor.return_value
        calls = {"n": 0}

        def execute(_sql: str, _payload: object = None, **_kwargs: object) -> None:
            calls["n"] += 1
            # The first chunk merges as one bulk statement and succeeds. The
            # second chunk is a single row: that execute fails, and the writer
            # is expected to fall back to per-row execution instead of undoing
            # the first chunk.
            if calls["n"] == 2:
                raise RuntimeError("bulk failed")

        cursor.execute.side_effect = execute
        payloads = [{"c0": 1, "c1": "x"} for _ in range(BULK_MERGE_ROWS + 1)]

        synced, errors = writer.execute_batch(
            "MERGE INTO t ...",
            payloads,
            "rag_chunks",
            ["id", "embedding"],
            ["id"],
            {"ID": "NUMBER", "EMBEDDING": "VECTOR(1536)"},
            {"ID": "id", "EMBEDDING": "embedding"},
        )

        writer.conn.rollback.assert_not_called()
        assert (synced, errors) == (len(payloads), 0)

    def test_insert_only_delegates_with_the_single_row_vector_binds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The INSERT path has to receive the bind names too -- Oracle needs the
        declaration on that statement as much as on the MERGE one.
        """
        writer = _writer()
        monkeypatch.setattr(writer, "_execute_insert_batch", MagicMock(return_value=(4, 0)))

        synced, errors = writer.execute_batch(
            "INSERT INTO t ...",
            [{"c0": 1, "c1": None}],
            "rag_chunks",
            ["id", "embedding"],
            ["id"],
            {"ID": "NUMBER", "EMBEDDING": "VECTOR(1536)"},
            {"ID": "id", "EMBEDDING": "embedding"},
            insert_only=True,
        )

        assert (synced, errors) == (4, 0)
        writer._execute_insert_batch.assert_called_once()
        vector_binds = writer._execute_insert_batch.call_args.args[4]
        assert vector_binds == ["c1"]
