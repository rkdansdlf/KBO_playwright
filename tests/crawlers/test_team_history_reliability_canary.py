"""The team history reliability canary.

First port of the ledger + dead letter chain onto a snapshot-writing crawler.
The contract under test is the failure/observability shape, not persistence
internals:

    a crawl failure is classified, recorded in the run ledger, and enqueued in
    the dead letter queue instead of vanishing into a blanket ``except``;
    a compliance skip is not a failure;
    a persistence failure is classified as ``persist`` and is never mistaken for
    a fetch problem.

The crawler, the run ledger, the dead letter queue, and the taxonomy all run for
real. Only the browser boundary is replaced.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import CrawlPersistError, FailureCode, FailureStage
from src.crawlers.team_history_crawler import (
    TEAM_HISTORY_CRAWLER_NAME,
    TEAM_HISTORY_TARGET_ID,
    TEAM_HISTORY_TARGET_TYPE,
    TeamHistoryCrawler,
)
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm

ENTRY: dict = {"season": 2024, "team_name": "LG Twins", "logo_url": None, "ranking": 1}


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
    monkeypatch.setattr("src.crawlers.team_history_crawler.SessionLocal", session_factory)


def _crawl_returning(*, data: list[dict], failure_reason: str | None = None):
    async def _crawl(crawler: TeamHistoryCrawler) -> list[dict]:
        crawler._last_failure_reason = failure_reason
        return data

    return _crawl


def _crawl_raising(exc: BaseException):
    async def _crawl(_crawler: TeamHistoryCrawler) -> list[dict]:
        raise exc

    return _crawl


def _save_returning(saved: int, failed: int = 0):
    async def _save(
        _crawler: TeamHistoryCrawler, _data: list[dict], *, raise_on_error: bool = False
    ) -> tuple[int, int]:
        assert raise_on_error is True
        return saved, failed

    return _save


def _save_raising(exc: BaseException):
    async def _save(
        _crawler: TeamHistoryCrawler, _data: list[dict], *, raise_on_error: bool = False
    ) -> tuple[int, int]:
        raise exc

    return _save


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestHealthyRuns:
    @pytest.mark.asyncio
    async def test_a_healthy_page_is_one_successful_run(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_returning(data=[ENTRY]))
        monkeypatch.setattr(TeamHistoryCrawler, "save", _save_returning(1))

        data = await TeamHistoryCrawler().run(save=True)

        assert data == [ENTRY]
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert run.records_read == 1
        assert run.records_written == 1
        assert run.records_failed == 0
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_the_page_is_the_replay_unit(self, session_factory: sessionmaker, monkeypatch) -> None:
        """One page carries every season, so the ledger row must identify the page."""
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_returning(data=[ENTRY]))
        monkeypatch.setattr(TeamHistoryCrawler, "save", _save_returning(1))

        await TeamHistoryCrawler().run(save=True)

        run = _only_run(session_factory)
        assert run.crawler == TEAM_HISTORY_CRAWLER_NAME
        assert run.target_type == TEAM_HISTORY_TARGET_TYPE
        assert run.target_id == TEAM_HISTORY_TARGET_ID

    @pytest.mark.asyncio
    async def test_dropped_rows_are_counted_not_silent(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_returning(data=[ENTRY]))
        monkeypatch.setattr(TeamHistoryCrawler, "save", _save_returning(1, failed=1))

        await TeamHistoryCrawler().run(save=True)

        run = _only_run(session_factory)
        assert run.records_written == 1
        assert run.records_failed == 1


class TestCrawlFailures:
    @pytest.mark.asyncio
    async def test_a_fetch_failure_marks_the_run_and_enqueues(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_raising(TimeoutError("gateway slow")))

        data = await TeamHistoryCrawler().run(save=True)

        assert data == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_TIMEOUT.value

        letter = _letters(session_factory)[0]
        assert letter.crawler == TEAM_HISTORY_CRAWLER_NAME
        assert letter.target_id == TEAM_HISTORY_TARGET_ID
        assert letter.error_code == FailureCode.FETCH_TIMEOUT.value
        assert letter.failure_stage == FailureStage.FETCH.value
        assert letter.original_run_id == run.run_id

    @pytest.mark.asyncio
    async def test_a_parse_failure_is_staged_as_parse_not_fetch(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_raising(ValueError("no table")))

        await TeamHistoryCrawler().run(save=True)

        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.PARSE_INVALID_FORMAT.value
        assert _letters(session_factory)[0].failure_stage == FailureStage.PARSE.value

    @pytest.mark.asyncio
    async def test_the_failure_is_not_raised_at_the_caller(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        """The ledger is the failure record; the weekly job must keep running."""
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_raising(RuntimeError("Page not initialized")))

        assert await TeamHistoryCrawler().run(save=True) == []
        assert len(_letters(session_factory)) == 1


class TestComplianceSkips:
    @pytest.mark.asyncio
    async def test_a_compliance_skip_is_not_a_failure(self, session_factory: sessionmaker, monkeypatch) -> None:
        monkeypatch.setattr(
            TeamHistoryCrawler,
            "crawl",
            _crawl_returning(data=[], failure_reason="compliance: source not allowed"),
        )

        data = await TeamHistoryCrawler().run(save=True)

        assert data == []
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert run.checkpoint is not None
        assert run.checkpoint["outcome"] == "source_limited"
        assert _letters(session_factory) == []


class TestPersistFailures:
    @pytest.mark.asyncio
    async def test_a_persist_failure_is_classified_as_persist(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_returning(data=[ENTRY]))
        monkeypatch.setattr(
            TeamHistoryCrawler,
            "save",
            _save_raising(CrawlPersistError("db down", error_code=FailureCode.PERSIST_CONNECTION)),
        )

        data = await TeamHistoryCrawler().run(save=True)

        assert data == [ENTRY]
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.PERSIST_CONNECTION.value

        letter = _letters(session_factory)[0]
        assert letter.failure_stage == FailureStage.PERSIST.value
        assert letter.error_code == FailureCode.PERSIST_CONNECTION.value

    @pytest.mark.asyncio
    async def test_raise_on_persist_error_keeps_the_taxonomy(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_returning(data=[ENTRY]))
        monkeypatch.setattr(
            TeamHistoryCrawler,
            "save",
            _save_raising(CrawlPersistError("db slow", error_code=FailureCode.PERSIST_TIMEOUT)),
        )

        with pytest.raises(CrawlPersistError):
            await TeamHistoryCrawler().run(save=True, raise_on_persist_error=True)

        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.PERSIST_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_no_dead_letter_is_enqueued_when_recording_is_disabled(
        self,
        session_factory: sessionmaker,
        monkeypatch,
    ) -> None:
        monkeypatch.setattr(TeamHistoryCrawler, "crawl", _crawl_raising(TimeoutError("slow")))

        await TeamHistoryCrawler().run(save=True, record_dead_letters=False)

        run = _only_run(session_factory)
        assert run.status == "failed"
        assert _letters(session_factory) == []


class TestAnUnreadablePageReachesTheQueue:
    """A page whose selectors stopped matching used to be a silent success.

    ``crawl`` returns a list, so an unreadable table and a genuinely blank one
    arrived as the same ``[]`` -- recorded as success, with nothing queued. The
    run then said the table was current while the data in it had not moved. These
    tests put the two apart by driving the read verdict the parser now produces.
    """

    @staticmethod
    def _crawler_returning(read):
        async def _crawl(crawler: TeamHistoryCrawler) -> list[dict]:
            crawler._last_read = read
            return []

        return _crawl

    @pytest.mark.asyncio
    async def test_a_page_with_no_table_is_a_failed_run(self, session_factory, monkeypatch) -> None:
        from src.crawlers.team_history_outcome import read_team_history

        monkeypatch.setattr(
            TeamHistoryCrawler, "crawl", self._crawler_returning(read_team_history(rows_found=0, years_parsed=0))
        )

        data = await TeamHistoryCrawler().run(save=False)

        assert data == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.PARSE_SELECTOR_MISSING.value

    @pytest.mark.asyncio
    async def test_it_is_queued_as_that_page(self, session_factory, monkeypatch) -> None:
        """One page covers every season, so the queue names the page."""
        from src.crawlers.team_history_outcome import read_team_history

        monkeypatch.setattr(
            TeamHistoryCrawler, "crawl", self._crawler_returning(read_team_history(rows_found=0, years_parsed=0))
        )

        await TeamHistoryCrawler().run(save=False)

        letters = _letters(session_factory)
        assert len(letters) == 1
        assert letters[0].target_id == TEAM_HISTORY_TARGET_ID
        assert letters[0].error_code == FailureCode.PARSE_SELECTOR_MISSING.value

    @pytest.mark.asyncio
    async def test_the_message_says_what_was_found_and_what_was_read(self, session_factory, monkeypatch) -> None:
        """``rows_found=0`` and ``rows_found=43`` need different responses."""
        from src.crawlers.team_history_outcome import read_team_history

        monkeypatch.setattr(
            TeamHistoryCrawler, "crawl", self._crawler_returning(read_team_history(rows_found=43, years_parsed=0))
        )

        await TeamHistoryCrawler().run(save=False)

        message = _only_run(session_factory).error_message or ""
        assert "rows_found=43" in message
        assert "years_parsed=0" in message

    @pytest.mark.asyncio
    async def test_nothing_is_stored_when_the_page_cannot_be_read(self, session_factory, monkeypatch) -> None:
        """Storing zero rows over a table nobody could read is the failure."""
        from src.crawlers.team_history_outcome import read_team_history

        saved: list[list[dict]] = []

        async def _save(crawler: TeamHistoryCrawler, data, *, raise_on_error: bool = False):
            saved.append(data)
            return 0, 0

        monkeypatch.setattr(
            TeamHistoryCrawler, "crawl", self._crawler_returning(read_team_history(rows_found=0, years_parsed=0))
        )
        monkeypatch.setattr(TeamHistoryCrawler, "save", _save)

        await TeamHistoryCrawler().run(save=True)

        assert saved == []

    @pytest.mark.asyncio
    async def test_the_recording_switch_covers_drift(self, session_factory, monkeypatch) -> None:
        from src.crawlers.team_history_outcome import read_team_history

        monkeypatch.setattr(
            TeamHistoryCrawler, "crawl", self._crawler_returning(read_team_history(rows_found=0, years_parsed=0))
        )

        await TeamHistoryCrawler().run(save=False, record_dead_letters=False)

        assert _letters(session_factory) == []
        assert _only_run(session_factory).status == "failed"

    @pytest.mark.asyncio
    async def test_a_readable_page_still_succeeds(self, session_factory, monkeypatch) -> None:
        """Sparse seasons are not drift; a page that parsed is a success."""
        from src.crawlers.team_history_outcome import read_team_history

        monkeypatch.setattr(
            TeamHistoryCrawler, "crawl", self._crawler_returning(read_team_history(rows_found=43, years_parsed=43))
        )
        monkeypatch.setattr(TeamHistoryCrawler, "save", _save_returning(3))

        await TeamHistoryCrawler().run(save=True)

        run = _only_run(session_factory)
        assert run.status == "success"
        assert _letters(session_factory) == []
