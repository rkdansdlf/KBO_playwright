"""The traffic CLI must read real rows and keep unknown out of the prose bucket."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.cli.rag.traffic as module
from src.models.rag_query_event import (
    RAG_QUERY_OUTCOME_DOCUMENT_PROSE,
    RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS,
    RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED,
    RAG_QUERY_OUTCOME_UNKNOWN,
    RagQueryEvent,
)
from src.cli.rag.traffic import main


@pytest.fixture
def factory() -> sessionmaker[Session]:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    RagQueryEvent.__table__.create(engine)
    return sessionmaker(bind=engine)


@pytest.fixture
def patch_session(monkeypatch: pytest.MonkeyPatch, factory: sessionmaker[Session]) -> sessionmaker[Session]:
    @contextmanager
    def fake_session() -> Iterator[Session]:
        with factory() as session:
            yield session

    monkeypatch.setattr(module, "get_db_session", fake_session)
    return factory


def _seed(factory: sessionmaker[Session], outcomes: list[str]) -> None:
    now = datetime.now(UTC).replace(tzinfo=None)
    with factory() as session:
        for index, outcome in enumerate(outcomes):
            session.add(
                RagQueryEvent(
                    created_at=now,
                    query_hash=f"{outcome}-{index}",
                    query_chars=10,
                    route="DOCUMENT",
                    outcome=outcome,
                    result_count=1,
                )
            )
        session.commit()


def test_json_report_keeps_unknown_separate(
    patch_session: sessionmaker[Session], capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(
        patch_session,
        [RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED] * 9 + [RAG_QUERY_OUTCOME_DOCUMENT_PROSE, RAG_QUERY_OUTCOME_UNKNOWN],
    )
    assert main(["--json", "--min-events", "1"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["basis"] == "served_chunks"
    assert report["total"] == 11
    assert report["unknown"] == 1
    assert report["classified"] == 10
    assert report["shares"]["unknown_share"] == round(1 / 11, 4)
    assert report["shares"]["prose_share_of_classified"] == 0.1
    assert report["shares"]["sql_answerable_share"] == 0.9
    assert report["verdict"] == "sql_expansion_warranted"


def test_too_few_events_is_undetermined(
    patch_session: sessionmaker[Session], capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(patch_session, [RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED])
    assert main(["--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["verdict"] == "undetermined"
    assert "min-events" in report["undetermined_reason"]


def test_high_unknown_share_overrides_the_verdict(
    patch_session: sessionmaker[Session], capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(patch_session, [RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED, RAG_QUERY_OUTCOME_UNKNOWN] * 2)
    assert main(["--json", "--min-events", "1"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["shares"]["unknown_share"] == 0.5
    assert report["verdict"] == "undetermined"


def test_table_rows_count_as_sql_answerable(
    patch_session: sessionmaker[Session], capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(
        patch_session,
        [
            RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED,
            RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS,
            RAG_QUERY_OUTCOME_DOCUMENT_PROSE,
        ],
    )
    assert main(["--json", "--min-events", "1"]) == 0
    report = json.loads(capsys.readouterr().out)
    share = report["shares"]
    assert share["structured_answered_share"] == round(1 / 3, 4)
    assert share["document_table_rows_share"] == round(1 / 3, 4)
    assert share["sql_answerable_share"] == round(2 / 3, 4)


def test_text_report_states_the_measurement_limits(
    patch_session: sessionmaker[Session], capsys: pytest.CaptureFixture[str]
) -> None:
    _seed(patch_session, [RAG_QUERY_OUTCOME_DOCUMENT_PROSE])
    assert main(["--min-events", "1"]) == 0
    text = capsys.readouterr().out
    assert "basis=served_chunks" in text
    assert "never counted as prose" in text
    assert "proxy for SQL-answerability" in text
