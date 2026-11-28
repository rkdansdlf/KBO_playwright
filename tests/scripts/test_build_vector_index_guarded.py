"""Contracts for the guarded dense-index builder's two index kinds."""

from __future__ import annotations

import contextlib

import pytest

from scripts.maintenance import build_hnsw_index_guarded as builder


class FakeConnection:
    """Record executed statements and answer like a connection with no backend."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object, params: object = None) -> FakeResult:
        self.statements.append(str(statement))
        return FakeResult()


class FakeResult:
    """Minimal result object for the scalar() calls under test."""

    def scalar(self) -> None:
        return None


class FakeEngine:
    """Engine double whose connect() yields a usable context manager."""

    def connect(self) -> object:
        return contextlib.nullcontext(FakeConnection())


def test_index_names_are_derived_from_the_kind() -> None:
    assert builder._index_name("hnsw") == "idx_rag_chunks_embedding_hnsw"
    assert builder._index_name("ivfflat") == "idx_rag_chunks_embedding_ivfflat"


def test_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="unsupported index kind"):
        builder._index_name("lsh")


def test_hnsw_sql_keeps_its_measured_parameters() -> None:
    sql = builder._create_sql("hnsw", m=8, ef_construction=32, lists=100)
    assert "CREATE INDEX idx_rag_chunks_embedding_hnsw ON rag_chunks USING hnsw" in sql
    assert "m = 8, ef_construction = 32" in sql
    assert "CONCURRENTLY" not in sql
    assert "lists" not in sql


def test_ivfflat_sql_uses_lists_and_not_hnsw_parameters() -> None:
    sql = builder._create_sql("ivfflat", m=8, ef_construction=32, lists=100)
    assert "CREATE INDEX idx_rag_chunks_embedding_ivfflat ON rag_chunks USING ivfflat" in sql
    assert "WITH (lists = 100)" in sql
    assert "ef_construction" not in sql
    assert "CONCURRENTLY" not in sql


def test_the_lists_value_is_not_hardcoded() -> None:
    sql = builder._create_sql("ivfflat", m=8, ef_construction=32, lists=50)
    assert "WITH (lists = 50)" in sql


def test_concurrently_is_a_ddl_keyword_not_a_suffix() -> None:
    sql = builder._create_sql("ivfflat", m=8, ef_construction=32, lists=100, concurrently=True)
    assert "CREATE INDEX CONCURRENTLY idx_rag_chunks_embedding_ivfflat" in sql


def test_the_cancel_query_matches_both_index_kinds() -> None:
    conn = FakeConnection()
    builder._build_backend_pid(conn)
    query = conn.statements[0]
    assert "CREATE INDEX%embedding%" in query
    assert "hnsw" not in query
    assert "ivfflat" not in query


def test_report_uses_a_cast_that_sqlalchemy_can_bind() -> None:
    conn = FakeConnection()
    builder._report_index(conn, "idx_rag_chunks_embedding_ivfflat")
    assert "CAST(:n AS regclass)" in conn.statements[0]
    assert ":n::" not in conn.statements[0]


def test_an_existing_index_aborts_before_the_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(builder, "_connection_url", lambda: "postgresql://unused")
    monkeypatch.setattr(builder, "create_engine", lambda *_a, **_k: FakeEngine())
    monkeypatch.setattr(builder, "_index_exists", lambda *_a, **_k: True)

    assert builder.main(["--kind", "ivfflat"]) == 2
