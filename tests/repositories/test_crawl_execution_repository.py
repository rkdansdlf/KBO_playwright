"""Tests for the generic crawl execution run ledger repository."""

from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    active = factory()
    try:
        yield active
    finally:
        active.close()


def _spec(**overrides: object) -> CrawlRunSpec:
    data: dict[str, object] = {
        "crawler": "boxscore",
        "target_type": "game",
        "game_id": "20260926LGKIA",
        "season": 2026,
    }
    data.update(overrides)
    return CrawlRunSpec(**data)  # type: ignore[arg-type]


class TestStartRun:
    def test_start_run_persists_running_row(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        assert run.id is not None
        assert run.run_id
        assert run.crawler == "boxscore"
        assert run.game_id == "20260926LGKIA"
        assert run.season == 2026
        assert run.status == "running"
        assert run.attempt == 1
        assert run.records_read == 0
        assert run.records_written == 0
        assert run.records_failed == 0
        assert run.finished_at is None
        assert isinstance(run.started_at, datetime)

    def test_start_run_with_explicit_run_id(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec(run_id="fixed-run-id"))
        assert run.run_id == "fixed-run-id"

    def test_start_run_records_lineage_references(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(
            _spec(
                parent_run_id="parent-1",
                replay_of_run_id="original-1",
                snapshot_id=42,
                evidence_id=7,
                source_url="https://example.com/games",
                parser_version="boxscore-v3",
            ),
        )
        assert run.parent_run_id == "parent-1"
        assert run.replay_of_run_id == "original-1"
        assert run.snapshot_id == 42
        assert run.evidence_id == 7
        assert run.source_url == "https://example.com/games"
        assert run.parser_version == "boxscore-v3"


class TestFinalize:
    def test_mark_success_applies_counts_and_timestamp(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        repo.mark_success(run, records_read=10, records_written=9, checkpoint={"page": 2})
        assert run.status == "success"
        assert run.records_read == 10
        assert run.records_written == 9
        assert run.checkpoint == {"page": 2}
        assert run.finished_at is not None

    def test_mark_partial_keeps_partial_status(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        repo.mark_partial(run, records_read=5, records_written=3, records_failed=2)
        assert run.status == "partial"
        assert run.records_failed == 2

    def test_mark_failed_records_error_classification(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        repo.mark_failed(run, error_code="SELECTOR_NOT_FOUND", error_message="no rows")
        assert run.status == "failed"
        assert run.error_code == "SELECTOR_NOT_FOUND"
        assert run.error_message == "no rows"
        assert run.finished_at is not None

    def test_mark_success_leaves_unspecified_counts_unchanged(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        repo.increment_counts(run, records_read=4)
        repo.mark_success(run)
        assert run.records_read == 4
        assert run.records_written == 0


class TestCountsAndQueries:
    def test_increment_counts_accumulates(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        repo.increment_counts(run, records_read=3, records_written=2, records_failed=1)
        repo.increment_counts(run, records_read=2, records_written=1, records_failed=0)
        assert (run.records_read, run.records_written, run.records_failed) == (5, 3, 1)

    def test_get_by_run_id(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec(run_id="lookup-me"))
        assert repo.get_by_run_id("lookup-me") is run
        assert repo.get_by_run_id("missing") is None

    def test_list_recent_filters(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        first = repo.start_run(_spec(crawler="boxscore"))
        repo.mark_success(first)
        second = repo.start_run(_spec(crawler="lineup"))
        repo.mark_failed(second, error_code="E", error_message="m")

        assert {run.crawler for run in repo.list_recent()} == {"boxscore", "lineup"}
        assert [run.crawler for run in repo.list_recent(crawler="lineup")] == ["lineup"]
        assert [run.crawler for run in repo.list_recent(status="failed")] == ["lineup"]


class TestTransactionContract:
    def test_repository_does_not_commit(self, session: Session) -> None:
        repo = CrawlExecutionRepository(session)
        run = repo.start_run(_spec())
        repo.mark_success(run, records_read=1)
        assert session.query(CrawlExecutionRun).count() == 1
        session.rollback()
        assert session.query(CrawlExecutionRun).count() == 0
