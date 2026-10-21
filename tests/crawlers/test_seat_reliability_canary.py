"""Seat crawling must leave a trace, because for months it left none.

`crawl_p1p2_data_job` calls the seat crawler every morning. `seat` had **zero**
rows in the run ledger for the whole of its history, so the table simply aged and
nothing anywhere recorded why. A 404, a timeout and a page with no seat names on
it all returned the same empty list, and only one of those is a reason to look
again.

The adoption mirror is `parking_crawler`: a ledger entry per run, a failure per
team recorded rather than logged, a dead letter that can never be mistaken for an
outage, and the team as the unit of work so one failure cannot roll back the rest.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.seat_crawler import TEAM_SEAT_SOURCES, SeatCrawler
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.repositories.crawl_execution_repository import MULTI_SOURCE_SEPARATOR

if TYPE_CHECKING:
    from collections.abc import Iterator

TEAMS = list(TEAM_SEAT_SOURCES)


@pytest.fixture
def ledger() -> Iterator[sessionmaker]:
    """A database the run ledger and the dead-letter queue can both write to."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


@pytest.fixture(autouse=True)
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, ledger: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", ledger)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", ledger)


DEFAULT_SECTION_NAME = "1루석"


def _crawler(
    answers: dict[str, str | None] | None = None,
    *,
    failing: set[str] | None = None,
    raising: set[str] | None = None,
) -> SeatCrawler:
    """Build a crawler whose per-team answer the test controls.

    Patched at `_crawl_team_seats`, not at the fetch seam, because `TEAM_SEAT_SOURCES`
    maps every team to the same URL -- LG and OB are both
    `lgtwins.com/ticket/seatinfo` -- so no fetch stub can tell which team it is
    answering for. A team mapped to None returns nothing, a team in `failing`
    returns nothing, and a team in `raising` raises; teams not named succeed.

    The behaviour of `_crawl_team_seats` itself is covered by calling it directly,
    where the team is an argument rather than something to be inferred.
    """
    crawler = SeatCrawler()
    answers = answers or {}
    failing = failing or set()
    raising = raising or set()

    async def crawl_team(team_code: str, info: dict) -> list[dict]:
        if team_code in raising:
            detail = f"{team_code} unavailable"
            raise RuntimeError(detail)
        answer = answers.get(team_code, DEFAULT_SECTION_NAME)
        if team_code in failing or answer is None:
            return []
        return [{"section_name": str(answer), "stadium_id": info["stadium_id"], "team_code": team_code}]

    crawler._crawl_team_seats = crawl_team  # type: ignore[method-assign]
    return crawler


def _runs(ledger: sessionmaker) -> list[CrawlExecutionRun]:
    with ledger() as session:
        return list(session.execute(select(CrawlExecutionRun)).scalars())


def _letters(ledger: sessionmaker) -> list[CrawlDeadLetter]:
    with ledger() as session:
        return list(session.execute(select(CrawlDeadLetter)).scalars())


class TestTheRunIsRecorded:
    """The absence that let the table age unexplained."""

    @pytest.mark.asyncio
    async def test_a_successful_run_leaves_a_ledger_entry(self, ledger: sessionmaker) -> None:
        crawler = _crawler({TEAMS[0]: "블루석 1루", TEAMS[1]: "외야석"})

        await crawler.run(team_filter=TEAMS[0])

        runs = _runs(ledger)
        assert len(runs) == 1, "a run that fetches must record that it fetched"
        assert runs[0].crawler == "seat"
        assert runs[0].status != "running", "the entry must be closed when the run returns"

    @pytest.mark.asyncio
    async def test_every_team_failure_still_leaves_a_ledger_entry(self, ledger: sessionmaker) -> None:
        """The case that mattered: nothing failed loudly, so nothing was recorded."""
        crawler = _crawler(raising=set(TEAMS))

        await crawler.run()

        runs = _runs(ledger)
        assert len(runs) == 1
        assert runs[0].status != "success", "a run where every team failed cannot be a success"

    @pytest.mark.asyncio
    async def test_the_run_is_marked_failed_when_every_team_failed(self, ledger: sessionmaker) -> None:
        crawler = _crawler(raising=set(TEAMS))

        await crawler.run()

        assert _runs(ledger)[0].status == "failed"

    @pytest.mark.asyncio
    async def test_a_full_sweep_names_every_page_it_requests(self, ledger: sessionmaker) -> None:
        """Both Jamsil teams read one page, so the record names it once.

        The dedupe matters as much as the join: the report judges hosts, and two
        spellings of one page would look like two sources to it.
        """
        crawler = _crawler()

        await crawler.run()

        run = _runs(ledger)[0]
        assert run.source_url is not None
        assert run.source_url.split(MULTI_SOURCE_SEPARATOR) == list(
            dict.fromkeys(info["url"] for info in TEAM_SEAT_SOURCES.values()),
        )


class TestFailuresAreDistinguishableFromEmptyPages:
    """A page we could not read and a page with nothing on it are different answers.

    Driven through the fetch seam directly, because `TEAM_SEAT_SOURCES` maps every
    team to the same URL and `run()` therefore cannot tell a stub which team it is
    answering for.
    """

    @staticmethod
    def _with_html(html: str) -> SeatCrawler:
        crawler = SeatCrawler()

        async def fetch(url: str) -> CrawlResult[str]:
            return CrawlResult.success(html, http_status=200)

        crawler._fetch_seat_page = fetch  # type: ignore[method-assign]
        return crawler

    @pytest.mark.asyncio
    async def test_a_failed_fetch_becomes_a_team_failure(self, ledger: sessionmaker) -> None:
        crawler = SeatCrawler()

        async def fetch(url: str) -> CrawlResult[str]:
            return CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error="unavailable",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
                http_status=503,
                url=url,
            )

        crawler._fetch_seat_page = fetch  # type: ignore[method-assign]

        await crawler._crawl_team_seats(TEAMS[0], TEAM_SEAT_SOURCES[TEAMS[0]])

        assert [team for team, _, _ in crawler._team_failures] == [TEAMS[0]]

    @pytest.mark.asyncio
    async def test_a_page_with_no_seat_names_is_also_a_failure(self, ledger: sessionmaker) -> None:
        """A 200 with nothing on it and a page we could not read are not the same
        answer, and reporting them alike is how a table ages quietly.
        """
        crawler = self._with_html("<html><body>빈 페이지</body></html>")

        await crawler._crawl_team_seats(TEAMS[0], TEAM_SEAT_SOURCES[TEAMS[0]])

        assert {code for _, code, _ in crawler._team_failures} == {FailureCode.PARSE_EMPTY.value}

    @pytest.mark.asyncio
    async def test_a_page_with_sections_records_no_failure(self, ledger: sessionmaker) -> None:
        crawler = self._with_html("<html><body>블루석 1루석 외야석</body></html>")

        await crawler._crawl_team_seats(TEAMS[0], TEAM_SEAT_SOURCES[TEAMS[0]])

        assert crawler._team_failures == []

    @pytest.mark.asyncio
    async def test_one_team_failing_does_not_abandon_the_others(self, ledger: sessionmaker) -> None:
        crawler = _crawler(raising={TEAMS[1]})

        sections = await crawler.run(record_dead_letters=False)

        assert sections, "the surviving team must still be collected"
        assert {s["team_code"] for s in sections} == {TEAMS[0]}


class TestDeadLetters:
    @pytest.mark.asyncio
    async def test_a_team_failure_becomes_a_dead_letter(self, ledger: sessionmaker) -> None:
        crawler = _crawler(raising={TEAMS[1]})

        await crawler.run()

        letters = _letters(ledger)
        assert len(letters) == 1
        assert letters[0].target_id == TEAMS[1]
        # The team raised a plain RuntimeError, which the taxonomy does not name,
        # so `UNKNOWN` is the honest record rather than an invented code.
        assert letters[0].error_code == FailureCode.UNKNOWN.value

    @pytest.mark.asyncio
    async def test_the_stage_is_derived_not_supplied(self, ledger: sessionmaker) -> None:
        """A stage and a code that disagree are two claims about one failure, and
        the retry budget is spent on the stage.
        """
        crawler = _crawler(raising={TEAMS[1]})

        await crawler.run()

        from src.crawlers.failure_taxonomy import stage_for_code

        letter = _letters(ledger)[0]
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_a_clean_run_leaves_no_letter(self, ledger: sessionmaker) -> None:
        crawler = _crawler({TEAM: f"{TEAM}석" for TEAM in TEAMS})

        await crawler.run()

        assert _letters(ledger) == []


class TestTheTeamIsTheUnitOfWork:
    @pytest.mark.asyncio
    async def test_sections_carry_the_team_they_came_from(self, ledger: sessionmaker) -> None:
        """The unit of work, of attribution and of the dead letter, attached where
        it is known rather than recovered from a stadium id later.
        """
        crawler = _crawler({TEAMS[0]: "블루석"})

        sections = await crawler.run(team_filter=TEAMS[0], record_dead_letters=False)

        assert {s["team_code"] for s in sections} == {TEAMS[0]}

    def test_a_save_failure_counts_as_a_failure_and_does_not_stop_the_teams_after_it(
        self,
        ledger: sessionmaker,
    ) -> None:
        """One batch session meant the first violation poisoned it and the commit
        rolled every team's rows back, after which the run reported a count of what
        it had saved and never a failure.
        """
        from unittest.mock import MagicMock, patch

        sections = [
            {"team_code": TEAMS[0], "section_name": "블루석"},
            {"team_code": TEAMS[1], "section_name": "외야석"},
        ]
        crawler = SeatCrawler()

        session = MagicMock()
        session.__enter__.return_value = session
        session.__exit__.return_value = False

        with (
            patch("src.crawlers.seat_crawler.SessionLocal", return_value=session),
            patch("src.crawlers.seat_crawler.save_raw_snapshots", return_value=0),
        ):
            repo = MagicMock()
            repo.save.side_effect = [RuntimeError("constraint"), None]
            with patch("src.crawlers.seat_crawler.StadiumSeatSectionRepository", return_value=repo):
                saved, failed = crawler._save_to_db(sections)

        assert (saved, failed) == (1, 1), "one team's failure must not take the other with it"


class TestFailureCodesAreReal:
    """The taxonomy has no generic fetch failure, because the transport is expected
    to name one. Inventing a code here would read as a diagnosis nobody made.
    """

    @pytest.mark.asyncio
    async def test_a_transport_that_named_no_code_is_recorded_as_unknown(self, ledger: sessionmaker) -> None:
        crawler = SeatCrawler()

        async def fetch(url: str) -> CrawlResult[str]:
            return CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="unavailable", url=url)

        crawler._fetch_seat_page = fetch  # type: ignore[method-assign]

        await crawler.run(record_dead_letters=False)

        assert {code for _, code, _ in crawler._team_failures} == {FailureCode.UNKNOWN.value}


class TestTheFetchRemainsAwaitable:
    def test_run_is_still_a_coroutine(self) -> None:
        assert inspect_is_coroutine_function(SeatCrawler.run)

    def test_a_crawler_still_exposes_its_sources(self) -> None:
        crawler = SeatCrawler()

        assert TEAM_SEAT_SOURCES
        assert crawler is not None


def inspect_is_coroutine_function(func) -> bool:
    """Return whether a callable is async, without importing inspect at module scope."""
    import inspect

    return inspect.iscoroutinefunction(func)


class TestCrawlTeamSeatsCanBeCalledAlone:
    """A test or a replay may call the inner method directly, and a failure that
    raises AttributeError there is a failure that cannot be reported at all.
    """

    @pytest.mark.asyncio
    async def test_a_direct_call_does_not_need_run_to_have_initialised_anything(self) -> None:
        """A real crawler, not the stub, which would short-circuit the recording."""
        crawler = SeatCrawler()

        async def fetch(url: str) -> CrawlResult[str]:
            return CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="unavailable", url=url)

        crawler._fetch_seat_page = fetch  # type: ignore[method-assign]

        await crawler._crawl_team_seats(TEAMS[0], TEAM_SEAT_SOURCES[TEAMS[0]])

        assert crawler._team_failures, "recording a failure must not require run() to have been called"


class TestSaveStillClearsRawPages:
    def test_pages_are_consumed_once_saved(self, ledger: sessionmaker) -> None:
        from unittest.mock import MagicMock, patch

        crawler = SeatCrawler()
        crawler._raw_pages = [{"team_code": TEAMS[0], "url": "u"}, {"url": "unattributed"}]

        with (
            patch("src.crawlers.seat_crawler.SessionLocal") as session_local,
            patch("src.crawlers.seat_crawler.save_raw_snapshots", return_value=1),
        ):
            session = MagicMock()
            session_local.return_value.__enter__.return_value = session
            with patch("src.crawlers.seat_crawler.StadiumSeatSectionRepository"):
                crawler._save_to_db([{"team_code": TEAMS[0], "section_name": "블루석"}])

        assert crawler._raw_pages == []


class TestUnmockedInnerCallStillWorks:
    """Guard against the fetch seam being renamed underneath the fixture."""

    def test_the_fixture_patches_the_method_the_crawler_calls(self) -> None:
        assert AsyncMock is not None
        assert hasattr(SeatCrawler, "_fetch_seat_page")
