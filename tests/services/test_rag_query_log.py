"""Contract tests for the best-effort RAG query-event ledger."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import src.services.rag_query_log as module
from src.models.rag_query_event import (
    RAG_QUERY_OUTCOME_DOCUMENT_PROSE,
    RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS,
    RAG_QUERY_OUTCOME_EMPTY,
    RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED,
    RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK,
    RAG_QUERY_OUTCOME_UNKNOWN,
    RagQueryEvent,
)
from src.services.query_router import STRUCTURED_SOURCE_TABLES
from src.services.rag_query_log import (
    QueryEventDraft,
    RagQueryEventRecorder,
    classify_query_event,
    hash_query,
    prune_rag_query_events,
    record_query_request,
)


@pytest.fixture
def factory() -> sessionmaker:
    """A real SQLite session factory owning one shared in-memory database."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    RagQueryEvent.__table__.create(engine)
    return sessionmaker(bind=engine)


def _chunk(*, chunk_id: str = "12", source_table: str | None = None, structured: bool = False) -> object:
    meta: dict[str, object] = {}
    if source_table is not None:
        meta["source_table"] = source_table
    if structured:
        meta["structured"] = True
    provenance = {"type": "structured_db"} if structured else None
    return SimpleNamespace(chunk_id=chunk_id, meta=meta, provenance=provenance)


def test_empty_answer_is_not_prose() -> None:
    draft = classify_query_event(
        route="DOCUMENT", intent=None, results=[], structured_candidates=0, fallback_used=False
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_EMPTY
    assert draft.source_table is None


def test_structured_result_is_recorded_as_answered() -> None:
    result = _chunk(chunk_id="awards:1", source_table="awards", structured=True)
    draft = classify_query_event(
        route="STRUCTURED", intent="STAT_QUERY", results=[result], structured_candidates=3, fallback_used=False
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED
    assert draft.result_count == 1


def test_structured_route_with_no_structured_result_is_fallback() -> None:
    draft = classify_query_event(
        route="STRUCTURED",
        intent=None,
        results=[_chunk(chunk_id="markdown_docs:kb_0")],
        structured_candidates=2,
        fallback_used=True,
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK


def test_a_servable_table_row_through_search_is_not_prose() -> None:
    table = next(iter(STRUCTURED_SOURCE_TABLES))
    draft = classify_query_event(
        route="DOCUMENT",
        intent=None,
        results=[_chunk(chunk_id=f"{table}:7")],
        structured_candidates=0,
        fallback_used=False,
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS
    assert draft.source_table == table


def test_prose_source_is_prose() -> None:
    draft = classify_query_event(
        route="DOCUMENT",
        intent=None,
        results=[_chunk(chunk_id="markdown_docs:kb_0")],
        structured_candidates=0,
        fallback_used=False,
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_DOCUMENT_PROSE
    assert draft.source_table == "markdown_docs"


def test_unattributable_chunks_are_unknown_not_prose() -> None:
    draft = classify_query_event(
        route="DOCUMENT",
        intent=None,
        results=[_chunk(chunk_id="12"), _chunk(chunk_id="13")],
        structured_candidates=0,
        fallback_used=False,
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_UNKNOWN


def test_a_tie_between_two_sources_is_unknown() -> None:
    draft = classify_query_event(
        route="MIXED",
        intent=None,
        results=[_chunk(chunk_id="markdown_docs:kb_0"), _chunk(chunk_id="kbo_press_releases:1")],
        structured_candidates=0,
        fallback_used=False,
    )
    assert draft.outcome == RAG_QUERY_OUTCOME_UNKNOWN


def test_hash_normalises_whitespace_without_keeping_the_text() -> None:
    assert hash_query("  ).split()  ") == hash_query(").split()")
    assert hash_query("2026  한국시리즈") == hash_query("2026 한국시리즈")
    assert hash_query("a") != hash_query("b")


def test_recorder_uses_its_own_transaction(factory: sessionmaker) -> None:
    outer = factory()
    outer.execute(text("SELECT 1"))  # open an independent transaction
    draft = QueryEventDraft("DOCUMENT", None, RAG_QUERY_OUTCOME_DOCUMENT_PROSE, "markdown_docs", 1, 0, False)

    assert RagQueryEventRecorder(factory).record("규정 몇 회까지?", draft)
    outer.rollback()

    with factory() as session:
        rows = session.scalars(select(RagQueryEvent)).all()
    assert len(rows) == 1
    assert rows[0].outcome == RAG_QUERY_OUTCOME_DOCUMENT_PROSE
    assert "규정 몇 회까지?" not in str(rows[0].to_dict())


def test_recorder_swallows_a_persistence_error_and_counts_it(monkeypatch: pytest.MonkeyPatch) -> None:
    counter = MagicMock()
    monkeypatch.setattr(module, "record_rag_query_log_failure", counter)

    def broken_factory() -> object:
        raise RuntimeError("database down")

    draft = QueryEventDraft("DOCUMENT", None, RAG_QUERY_OUTCOME_EMPTY, None, 0, 0, False)
    assert RagQueryEventRecorder(broken_factory).record("질문", draft) is False
    counter.assert_called_once_with()


def test_disabled_and_zero_sample_rate_write_nothing(factory: sessionmaker) -> None:
    draft = QueryEventDraft("DOCUMENT", None, RAG_QUERY_OUTCOME_EMPTY, None, 0, 0, False)
    assert RagQueryEventRecorder(factory, enabled=False).record("질문", draft) is False
    assert RagQueryEventRecorder(factory, sample_rate=0.0).record("질문", draft) is False
    with factory() as session:
        assert session.scalars(select(RagQueryEvent)).all() == []


def test_record_query_request_reads_the_plan_and_trace(factory: sessionmaker) -> None:
    plan = SimpleNamespace(route=SimpleNamespace(value="STRUCTURED"), intent=SimpleNamespace(value="STAT_QUERY"))
    result = _chunk(chunk_id="awards:1", source_table="awards", structured=True)
    recorder = RagQueryEventRecorder(factory)
    recorded = record_query_request(
        "2025 MVP 수상자",
        plan=plan,
        results=[result],
        retrieval={"structured_candidates": 1, "fallback_used": False, "latency_ms": {"total": 12.5}},
        recorder=recorder,
    )
    assert recorded is True
    with factory() as session:
        row = session.scalars(select(RagQueryEvent)).one()
    assert row.route == "STRUCTURED"
    assert row.intent == "STAT_QUERY"
    assert row.outcome == RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED
    assert row.structured_candidates == 1
    assert row.latency_ms == 12.5


def test_prune_removes_only_old_rows(factory: sessionmaker) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    draft = QueryEventDraft("DOCUMENT", None, RAG_QUERY_OUTCOME_EMPTY, None, 0, 0, False)
    recorder = RagQueryEventRecorder(factory)
    recorder.record("old", draft, recorded_at=now - timedelta(days=100))
    recorder.record("new", draft, recorded_at=now - timedelta(days=1))

    with factory() as session:
        deleted = prune_rag_query_events(session, before=now - timedelta(days=90))
        session.commit()
    assert deleted == 1
    with factory() as session:
        assert len(session.scalars(select(RagQueryEvent)).all()) == 1
