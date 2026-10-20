"""Unit tests for Oracle VECTOR bind handling in the bulk writer."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

import src.sync.oracle_writer as writer_module
from src.sync.oracle_writer import OracleWriter


@pytest.fixture
def writer() -> OracleWriter:
    """Build an OracleWriter without opening a database connection."""
    instance = OracleWriter.__new__(OracleWriter)
    instance.arraysize = 100
    return instance


def test_normalize_vector_none_and_placeholders(writer: OracleWriter) -> None:
    """NULL and placeholder strings become SQL NULL for VECTOR columns."""
    assert writer.convert_value(None, "VECTOR") is None
    assert writer.convert_value("", "VECTOR") is None
    assert writer.convert_value("null", "VECTOR") is None
    assert writer.convert_value("  NULL  ", "VECTOR") is None


def test_normalize_vector_parses_bracket_strings(writer: OracleWriter) -> None:
    """Bracket strings become float sequences the driver binds as VECTOR."""
    assert writer.convert_value("[1.0, 2.5, -3]", "VECTOR") == [1.0, 2.5, -3.0]


def test_normalize_vector_passes_through_sequences(writer: OracleWriter) -> None:
    """Float sequences pass through with float coercion."""
    assert writer.convert_value([1, 2], "VECTOR") == [1.0, 2.0]
    assert writer.convert_value((1, 2), "VECTOR") == [1.0, 2.0]


def test_normalize_vector_keeps_unparseable_strings(writer: OracleWriter) -> None:
    """Unparseable strings surface at the database instead of vanishing."""
    assert writer.convert_value("not-a-vector", "VECTOR") == "not-a-vector"


def test_normalize_vector_parses_array_repr(writer: OracleWriter) -> None:
    """Python array reprs stored by the source parse to float sequences."""
    assert writer.convert_value("array('f', [1.5, -2.0])", "VECTOR") == [1.5, -2.0]


def test_vector_bind_names_single_row() -> None:
    """Single-row binds reuse the per-column bind names."""
    names = OracleWriter._vector_bind_names(["ID", "EMBEDDING"], {"EMBEDDING": "VECTOR"}, 1)
    assert names == ["c1"]


def test_vector_bind_names_bulk_rows() -> None:
    """Bulk MERGE binds flatten across every row in the chunk."""
    names = OracleWriter._vector_bind_names(["ID", "EMBEDDING"], {"EMBEDDING": "VECTOR"}, 2)
    assert names == ["c1", "c3"]


def test_vector_bind_names_ignores_other_types() -> None:
    """Non-VECTOR columns never receive explicit bind typing."""
    names = OracleWriter._vector_bind_names(["ID", "V"], {"ID": "NUMBER", "V": "VARCHAR2"}, 3)
    assert names == []


class _FakeCursor:
    def __init__(self) -> None:
        self.sizes: dict[str, object] = {}
        self.executed: list[tuple[str, object]] = []

    def setinputsizes(self, **kwargs: object) -> None:
        self.sizes.update(kwargs)

    def execute(self, sql: str, payload: object = None) -> None:
        self.executed.append((sql, payload))

    def executemany(self, sql: str, payloads: object, batcherrors: bool = False) -> None:
        self.executed.append((sql, payloads))

    def getbatcherrors(self) -> list[object]:
        return []

    def close(self) -> None:
        return None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_insert_batch_pins_vector_bind_types(writer: OracleWriter, monkeypatch: pytest.MonkeyPatch) -> None:
    """Array INSERT pins DB_TYPE_VECTOR so NULL embeddings bind cleanly."""
    stub = SimpleNamespace(DB_TYPE_VECTOR="VECTOR_TYPE", Error=Exception)
    monkeypatch.setattr(writer_module, "oracledb", stub)
    cursor = _FakeCursor()
    writer.conn = _FakeConn(cursor)

    synced, errors = writer.execute_batch(
        "INSERT INTO T (ID, EMBEDDING) VALUES (:c0, :c1)",
        [{"c0": 1, "c1": None}, {"c0": 2, "c1": None}],
        "rag_chunks",
        ["id", "embedding"],
        ["ID"],
        {"ID": "NUMBER", "EMBEDDING": "VECTOR"},
        {"ID": "ID", "EMBEDDING": "EMBEDDING"},
        insert_only=True,
    )

    assert (synced, errors) == (2, 0)
    assert cursor.sizes == {"c1": "VECTOR_TYPE"}


def test_insert_batch_skips_typing_without_vector_columns(
    writer: OracleWriter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tables without VECTOR columns leave bind typing untouched."""
    stub = SimpleNamespace(DB_TYPE_VECTOR="VECTOR_TYPE", Error=Exception)
    monkeypatch.setattr(writer_module, "oracledb", stub)
    cursor = _FakeCursor()
    writer.conn = _FakeConn(cursor)

    writer.execute_batch(
        "INSERT INTO T (ID) VALUES (:c0)",
        [{"c0": 1}],
        "teams",
        ["id"],
        ["ID"],
        {"ID": "NUMBER"},
        {"ID": "ID"},
        insert_only=True,
    )

    assert cursor.sizes == {}


@pytest.mark.parametrize("insert_only", [False, True])
def test_batch_fallback_preserves_uncommitted_rows(writer, monkeypatch, insert_only):
    monkeypatch.setattr(writer_module, "oracledb", SimpleNamespace(Error=RuntimeError))
    monkeypatch.setattr(writer_module, "BULK_MERGE_ROWS", 2)
    database = sqlite3.connect(":memory:")
    database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    database.execute("INSERT INTO t VALUES (0)")

    class Cursor:
        failed = False

        def execute(self, sql, binds=None):
            if binds is None:
                database.execute(sql)
                return
            if len(binds) > 1 and binds["c0"] == 3 and not self.failed:
                self.failed = True
                raise RuntimeError("injected bulk failure")
            for value in binds.values():
                database.execute("INSERT INTO t VALUES (?) ON CONFLICT(id) DO NOTHING", (value,))

        def executemany(self, sql, payloads, *, batcherrors):
            database.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("injected array failure")

        def close(self):
            pass

    cursor = Cursor()
    writer.conn = SimpleNamespace(cursor=lambda: cursor, rollback=database.rollback)
    payloads = [{"c0": value} for value in range(1, 5)]
    try:
        result = writer.execute_batch(
            "MERGE",
            payloads,
            "t",
            ["id"],
            ["ID"],
            {"ID": "NUMBER"},
            {"ID": "ID"},
            insert_only=insert_only,
        )
        assert result == (4, 0)
        assert database.execute("SELECT id FROM t ORDER BY id").fetchall() == [(0,), (1,), (2,), (3,), (4,)]
    finally:
        database.close()
