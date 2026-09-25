"""Tests for the crawl execution run ledger service."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_run_service import CrawlRunService, track_crawl_run


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:")
    CrawlExecutionRun.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def session(session_factory: sessionmaker) -> Iterator[Session]:
    active = session_factory()
    try:
        yield active
    finally:
        active.close()


def _spec(**overrides: object) -> CrawlRunSpec:
    data: dict[str, object] = {"crawler": "award", "target_type": "award_history"}
    data.update(overrides)
    return CrawlRunSpec(**data)  # type: ignore[arg-type]


class TestTrackCrawlRun:
    def test_success_commits_self_managed_session(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
        with track_crawl_run(_spec()) as run:
            run.records_read = 10
            run.records_written = 9

        with session_factory() as check:
            stored = check.query(CrawlExecutionRun).one()
        assert stored.status == "success"
        assert stored.records_read == 10
        assert stored.records_written == 9
        assert stored.finished_at is not None

    def test_failure_is_recorded_and_reraised(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
        with pytest.raises(RuntimeError, match="boom"):
            with track_crawl_run(_spec()):
                raise RuntimeError("boom")

        with session_factory() as check:
            stored = check.query(CrawlExecutionRun).one()
        assert stored.status == "failed"
        assert stored.error_code == "RuntimeError"
        assert "boom" in (stored.error_message or "")
        assert stored.finished_at is not None

    def test_partial_status_preserved(
        self,
        session_factory: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
        with track_crawl_run(_spec()) as run:
            run.status = "partial"
            run.error_code = "SOURCE_PARTIAL"

        with session_factory() as check:
            stored = check.query(CrawlExecutionRun).one()
        assert stored.status == "partial"
        assert stored.error_code == "SOURCE_PARTIAL"

    def test_caller_session_is_only_flushed(self, session: Session) -> None:
        with track_crawl_run(_spec(), session=session) as run:
            run.records_read = 3

        assert session.query(CrawlExecutionRun).count() == 1
        session.rollback()
        assert session.query(CrawlExecutionRun).count() == 0


class TestCrawlRunService:
    def test_record_counts_and_success(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())
        service.record_counts(run, records_read=5, records_written=4)
        service.success(run)
        assert run.status == "success"
        assert (run.records_read, run.records_written) == (5, 4)

    def test_failed_sets_error_fields(self, session: Session) -> None:
        service = CrawlRunService(session)
        run = service.start(_spec())
        service.failed(run, error_code="PARSE_ERROR", error_message="bad html")
        assert run.status == "failed"
        assert run.error_code == "PARSE_ERROR"
