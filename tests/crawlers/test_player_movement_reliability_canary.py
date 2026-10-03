"""The player movement reliability canary.

Second port of the ledger + dead letter chain, and the first one whose crawler
already swallows its own failures. ``_crawl_year`` keeps going after a year
fails, so before this port a broken year left nothing behind but a log line: the
daily pipeline saw an empty contribution and no one could replay it.

The contract under test:

    a failed year is captured, surfaced as a ``partial`` run, and enqueued as its
    own dead letter (a year is the replay unit);
    a whole-range failure marks the run ``failed``;
    a compliance skip is not a failure.

The crawler, the run ledger, the dead letter queue, and the taxonomy run for
real. Only the browser boundary is replaced.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode, FailureStage, stage_for_code
from src.crawlers.player_movement_crawler import (
    PLAYER_MOVEMENT_CRAWLER_NAME,
    PLAYER_MOVEMENT_TARGET_TYPE,
    PlayerMovementCrawler,
)
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm

ROW: dict = {"date": "2024-03-15", "section": "Trade", "team_code": "LG", "player_name": "Kim", "remarks": ""}


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _fresh_metric_state() -> Iterator[None]:
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


@pytest.fixture(autouse=True)
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.crawlers.player_movement_crawler.SessionLocal", session_factory)


def _crawl_years_returning(*, data: list[dict], failure_reason: str | None = None, year_failures=None):
    async def _crawl_years(
        crawler: PlayerMovementCrawler,
        _start_year: int,
        _end_year: int,
        *,
        save_snapshots: bool = False,
    ) -> list[dict]:
        crawler._last_failure_reason = failure_reason
        crawler._year_failures = list(year_failures or [])
        return data

    return _crawl_years


def _crawl_years_raising(exc: BaseException):
    async def _crawl_years(
        _crawler: PlayerMovementCrawler,
        _start_year: int,
        _end_year: int,
        *,
        save_snapshots: bool = False,
    ) -> list[dict]:
        raise exc

    return _crawl_years


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestHealthyRuns:
    @pytest.mark.asyncio
    async def test_a_year_range_is_one_successful_run(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(PlayerMovementCrawler, "crawl_years", _crawl_years_returning(data=[ROW]))

        data = await PlayerMovementCrawler().run(2023, 2024, save_snapshots=False)

        assert data == [ROW]
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert run.records_read == 1
        assert run.crawler == PLAYER_MOVEMENT_CRAWLER_NAME
        assert run.target_type == PLAYER_MOVEMENT_TARGET_TYPE
        assert run.target_id == "2023-2024"
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_single_year_targets_the_year(self, session_factory: sessionmaker, monkeypatch) -> None:
        """The daily pipeline crawls one year, so the ledger row must identify it."""
        monkeypatch.setattr(PlayerMovementCrawler, "crawl_years", _crawl_years_returning(data=[ROW]))

        await PlayerMovementCrawler().run(2026, 2026, save_snapshots=False)

        assert _only_run(session_factory).target_id == "2026"


class TestYearFailures:
    @pytest.mark.asyncio
    async def test_a_failed_year_marks_the_run_partial_and_enqueues(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            PlayerMovementCrawler,
            "crawl_years",
            _crawl_years_returning(data=[ROW], year_failures=[(2024, RuntimeError("table gone"))]),
        )

        data = await PlayerMovementCrawler().run(2023, 2024, save_snapshots=False)

        assert data == [ROW]
        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.records_read == 1
        assert "2024" in (run.error_message or "")

        letter = _letters(session_factory)[0]
        assert letter.target_id == "2024"
        assert letter.season == 2024
        assert letter.original_run_id == run.run_id
        # The stage is always derived from the code, never supplied beside it.
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_each_failed_year_gets_its_own_dead_letter(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            PlayerMovementCrawler,
            "crawl_years",
            _crawl_years_returning(
                data=[],
                year_failures=[(2023, RuntimeError("a")), (2024, TimeoutError("b"))],
            ),
        )

        await PlayerMovementCrawler().run(2023, 2024, save_snapshots=False)

        letters = _letters(session_factory)
        assert sorted(letter.target_id for letter in letters) == ["2023", "2024"]
        assert {letter.error_code for letter in letters} == {
            FailureCode.UNKNOWN.value,
            FailureCode.FETCH_TIMEOUT.value,
        }

    @pytest.mark.asyncio
    async def test_no_dead_letter_is_enqueued_when_recording_is_disabled(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(
            PlayerMovementCrawler,
            "crawl_years",
            _crawl_years_returning(data=[], year_failures=[(2024, RuntimeError("table gone"))]),
        )

        await PlayerMovementCrawler().run(2024, 2024, save_snapshots=False, record_dead_letters=False)

        assert _only_run(session_factory).status == "partial"
        assert _letters(session_factory) == []


class TestRangeFailures:
    @pytest.mark.asyncio
    async def test_a_range_failure_marks_the_run_failed_and_enqueues(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(PlayerMovementCrawler, "crawl_years", _crawl_years_raising(TimeoutError("nav timeout")))

        data = await PlayerMovementCrawler().run(2026, 2026, save_snapshots=False)

        assert data == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value

        letter = _letters(session_factory)[0]
        assert letter.target_id == "2026"
        assert letter.season == 2026
        assert letter.failure_stage == FailureStage.FETCH.value


class TestComplianceSkips:
    @pytest.mark.asyncio
    async def test_a_compliance_skip_is_not_a_failure(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(
            PlayerMovementCrawler,
            "crawl_years",
            _crawl_years_returning(data=[], failure_reason="compliance: source not allowed"),
        )

        data = await PlayerMovementCrawler().run(2026, 2026, save_snapshots=False)

        assert data == []
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert run.checkpoint is not None
        assert run.checkpoint["outcome"] == "source_limited"
        assert _letters(session_factory) == []
