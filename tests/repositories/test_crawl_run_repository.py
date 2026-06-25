from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.crawl import CrawlRun
from src.repositories.crawl_run_repository import CrawlRunRepository, RunStats


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    CrawlRun.__table__.create(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


@pytest.fixture(autouse=True)
def patch_session_local(session):
    with patch("src.repositories.crawl_run_repository.SessionLocal", return_value=session):
        yield


class TestCrawlRunRepository:
    def test_create_run(self):
        repo = CrawlRunRepository()
        now = datetime.now(UTC).replace(tzinfo=None)
        run = repo.create_run(
            stats=RunStats(
                label="test-label",
                started_at=now,
                finished_at=now,
                active_count=10,
                retired_count=5,
                staff_count=2,
                confirmed_profiles=8,
                heuristic_only=1,
            ),
        )
        assert run.id is not None
        assert run.label == "test-label"
        assert run.active_count == 10
        assert run.retired_count == 5
        assert run.staff_count == 2
        assert run.confirmed_profiles == 8
        assert run.heuristic_only == 1

    def test_create_run_minimal(self):
        repo = CrawlRunRepository()
        now = datetime.now(UTC).replace(tzinfo=None)
        run = repo.create_run(
            stats=RunStats(
                label=None,
                started_at=now,
                finished_at=now,
                active_count=0,
                retired_count=0,
                staff_count=0,
                confirmed_profiles=0,
                heuristic_only=0,
            ),
        )
        assert run.id is not None
        assert run.label is None
