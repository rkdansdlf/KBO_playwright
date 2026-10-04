"""쓰기 실패가 조용히 사라지지 않는다는 계약.

같은 파일의 읽기 실패 테스트와 짝을 이루지만, 저장 경로는 다른 종류의 실패를
다룬다. 읽지 못한 팀은 표에 아무것도 못 넣었고, 읽었는데 못 넣은 팀은 표가
"그 팀은 원래 없었다"와 구분되지 않는 상태로 남는다. 후자가 조용한 쪽이다.

검증하는 계약:

    팀 단위로 격리되어 스윕이 계속되고, 쓰기 실패도 팀 단위 DLQ로 간다;
    ``records_read``와 ``records_written``가 실제로 다르고, 그 차이가 곧 손실이다;
    전량 쓰기 실패는 ``partial``이 아니라 ``failed``다;
    기본은 조용하고, ``raise_on_persist_error``로만 예외가 오른다.

크롤러·원장·DLQ·taxonomy는 실제로 동작하고, 도메인 테이블과 HTTP 경계만
대체합니다.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import CrawlPersistError, FailureCode, stage_for_code
from src.crawlers.food_crawler import TEAM_FOOD_SOURCES, FoodCrawler
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.models.stadium_food_menu_item import StadiumFoodMenuItem
from src.models.stadium_food_vendor import StadiumFoodVendor
from src.repositories.stadium_food_repository import StadiumFoodVendorRepository


def _write_refused() -> IntegrityError:
    """The failure this contract is really about.

    The vendor repository flushes on insert, so a constraint violation is what
    left the session needing a rollback in the first place. It is also the case
    with its own code, which a plain ``SQLAlchemyError`` would not exercise.
    """
    return IntegrityError("INSERT", {}, Exception("unique constraint"))


LT_URL = TEAM_FOOD_SOURCES["LT"]["url"]
HTML = "<html><body>기본 요금: 5,000원</body></html>"


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
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)


def _crawler(failing_urls: set[str] | None = None) -> FoodCrawler:
    failing = failing_urls or set()

    async def _fetch_text(url: str) -> CrawlResult:
        if url in failing:
            return CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error="page unavailable",
                error_code=FailureCode.FETCH_HTTP_ERROR.value,
                http_status=503,
                url=url,
            )
        return CrawlResult.success(HTML, http_status=200, url=url)

    crawler = FoodCrawler()
    crawler._http.fetch_text = AsyncMock(side_effect=_fetch_text)
    return crawler


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


@pytest.fixture(autouse=True)
def _no_snapshot_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the domain tables out of it; the contract here is the recording."""
    monkeypatch.setattr(FoodCrawler, "_save_snapshots", lambda self: 0)


class TestWriteFailuresAreNotSilent:
    """A sweep that read every page and wrote nothing used to report success.

    The vendor repository flushes on insert, so one constraint violation left the
    session needing a rollback: every later save in the same sweep then failed
    with ``PendingRollbackError``, the batch commit rolled the whole sweep back,
    and the caller logged ``0`` and moved on. The table ended up exactly as it
    would have if the source had never carried those vendors -- which is why this
    has to be counted separately from a team nobody could read.
    """

    def _writer(self, failing_teams: set[str], *, error: BaseException | None = None) -> FoodCrawler:
        """A crawler whose writes fail for the named teams, and only those.

        The failure is recorded through the crawler's own recorder rather than by
        faking a return value, so the classification path is the real one and a
        test cannot pass by agreeing with a mock.
        """
        failure = error if error is not None else _write_refused()

        def _save_team(self: FoodCrawler, team_code: str, entries: list[dict]) -> bool:
            if team_code in failing_teams:
                self._record_persist_failure(team_code, failure)
                return False
            return True

        crawler = _crawler()
        crawler._save_team = _save_team.__get__(crawler, FoodCrawler)  # type: ignore[method-assign]
        return crawler

    @pytest.mark.asyncio
    async def test_a_write_failure_is_queued_against_the_team_it_belongs_to(
        self, session_factory: sessionmaker
    ) -> None:
        await self._writer({"LT"}).run(save=True)

        letter = _letters(session_factory)[0]
        assert letter.target_id == "LT"
        assert letter.source_url == TEAM_FOOD_SOURCES["LT"]["url"]
        assert letter.error_code == FailureCode.PERSIST_CONSTRAINT.value
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_it_is_named_as_a_write_failure_and_not_a_read_failure(self, session_factory: sessionmaker) -> None:
        """The two produce the same empty table, so only the code tells them apart."""
        await self._writer({"LT"}).run(save=True)

        assert _letters(session_factory)[0].error_code != FailureCode.FETCH_HTTP_ERROR.value

    @pytest.mark.asyncio
    async def test_only_the_vendors_that_committed_are_counted(self, session_factory: sessionmaker) -> None:
        """`records_read` is what the source offered; `records_written` is what landed."""
        await self._writer({"LT"}).run(save=True)

        run = _only_run(session_factory)
        assert run.records_read == 3
        assert run.records_written == 2

    @pytest.mark.asyncio
    async def test_reading_every_page_and_writing_none_is_a_failed_run(self, session_factory: sessionmaker) -> None:
        """`partial` would report a total write loss as a short sweep.

        The table is in the same state either way, so a run that read 3 rows and
        wrote 0 is not "mostly fine" -- it is a failed run that happens to have
        done its reading.
        """
        await self._writer({"ALL", "LT", "NC"}).run(save=True)

        run = _only_run(session_factory)
        assert run.records_read == 3
        assert run.records_written == 0
        assert run.status == "failed"
        assert run.error_code == FailureCode.PERSIST_CONSTRAINT.value
        assert "(nothing was written)" in (run.error_message or "")

    @pytest.mark.asyncio
    async def test_some_teams_committed_and_some_died_is_a_partial_run(self, session_factory: sessionmaker) -> None:
        await self._writer({"LT"}).run(save=True)

        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.records_written == 2

    @pytest.mark.asyncio
    async def test_a_write_failure_respects_the_recording_switch(self, session_factory: sessionmaker) -> None:
        """The switch covers write failures, or turning it off only half works."""
        await self._writer({"LT"}).run(save=True, record_dead_letters=False)

        assert _letters(session_factory) == []
        assert _only_run(session_factory).status == "partial"

    @pytest.mark.asyncio
    async def test_the_caller_hears_nothing_by_default(self, session_factory: sessionmaker) -> None:
        """Quiet by default, because a status is the normal report of a bad run."""
        await self._writer({"LT"}).run(save=True)

        assert _only_run(session_factory).status == "partial"

    @pytest.mark.asyncio
    async def test_the_caller_can_ask_to_hear_about_it(self, session_factory: sessionmaker) -> None:
        """A replay needs to know it stored nothing, and an exception says so plainly."""
        with pytest.raises(CrawlPersistError) as raised:
            await self._writer({"LT"}).run(save=True, raise_on_persist_error=True)

        assert raised.value.error_code == FailureCode.PERSIST_CONSTRAINT

    @pytest.mark.asyncio
    async def test_the_ledger_is_written_before_the_caller_is_told(self, session_factory: sessionmaker) -> None:
        """Raising must not cost us the record of what failed.

        The letter is already queued when this fires, so a caller that catches
        the exception still has the queue to work from. The run is marked
        ``failed`` rather than ``partial`` because it ended by raising: a run
        that ended by raising cannot also be a partly-successful one, and the
        code comes off the exception so the taxonomy reads the same fault the
        queue recorded.
        """
        with pytest.raises(CrawlPersistError):
            await self._writer({"LT"}).run(save=True, raise_on_persist_error=True)

        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.PERSIST_CONSTRAINT.value
        assert len(_letters(session_factory)) == 1

    @pytest.mark.asyncio
    async def test_a_healthy_write_run_is_untouched(self, session_factory: sessionmaker) -> None:
        """The accounting must not turn a good sweep into a bad report."""
        await self._writer(set()).run(save=True)

        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.records_written == 3
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_the_run_counts_the_rows_it_could_not_write(self, session_factory: sessionmaker) -> None:
        """`records_failed` is the loss, stated rather than left to arithmetic.

        It could be derived from `records_read - records_written`, which is why
        it is easy to leave unset: the difference already tells you. But an
        operator asking "how many vendors did we lose" should not have to know
        that those three columns are consistent with each other, and a sweep
        where nothing was read at all would make the difference meaningless.
        """
        await self._writer({"LT"}).run(save=True)

        run = _only_run(session_factory)
        assert run.records_failed == 1
        assert run.records_written == 2
        assert run.records_read == 3

    @pytest.mark.asyncio
    async def test_a_clean_sweep_reports_no_failure(self, session_factory: sessionmaker) -> None:
        await self._writer(set()).run(save=True)

        assert _only_run(session_factory).records_failed == 0

    @pytest.mark.asyncio
    async def test_a_total_write_loss_counts_every_row(self, session_factory: sessionmaker) -> None:
        await self._writer({"ALL", "LT", "NC"}).run(save=True)

        run = _only_run(session_factory)
        assert run.records_failed == 3
        assert run.records_written == 0


class TestTheTransactionBoundaryIsReal:
    """The class above stubs `_save_team`, so nothing in it has run a commit.

    A stubbed writer cannot distinguish a per-team transaction from a batch one
    that merely *reports* per team: both return `(saved, failed)`, and both can
    be made to produce the counts above. Only a real constraint violation shows
    whether the failure was contained or swallowed -- so these drive the actual
    repositories against real tables and read the rows back.
    """

    @pytest.fixture
    def write_db(self, monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> sessionmaker:
        """Add the food tables to the same database as the ledger.

        One engine, not two: a run recorded in one database and rows written to
        another would let every assertion pass while describing two databases
        that never coexisted.
        """
        engine = session_factory.kw["bind"]
        for table in (StadiumFoodVendor.__table__, StadiumFoodMenuItem.__table__):
            table.create(engine)
        monkeypatch.setattr("src.crawlers.food_crawler.SessionLocal", session_factory)
        return session_factory

    @staticmethod
    def _break_writes(monkeypatch: pytest.MonkeyPatch, *stadium_ids: str) -> None:
        """Make the vendor write raise a real IntegrityError for the named stadiums."""
        broken = set(stadium_ids)
        real_save = StadiumFoodVendorRepository.save

        def _save(self: StadiumFoodVendorRepository, data: dict) -> StadiumFoodVendor:
            if data.get("stadium_id") in broken:
                raise IntegrityError("INSERT INTO stadium_food_vendors ...", {}, Exception("UNIQUE constraint failed"))
            return real_save(self, data)

        monkeypatch.setattr(StadiumFoodVendorRepository, "save", _save)

    @staticmethod
    def _stored_stadiums(factory: sessionmaker) -> set[str]:
        with factory() as check:
            return {row.stadium_id for row in check.query(StadiumFoodVendor).all()}

    @pytest.mark.asyncio
    async def test_one_stadiums_constraint_does_not_undo_anothers_rows(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The regression this change exists to prevent.

        Under one batch transaction the flush error poisoned the session, so the
        other two stadiums never committed either -- and the run still said
        `success`. Reading the table back is the only way to tell that apart
        from a writer that counts honestly but shares a transaction.
        """
        self._break_writes(monkeypatch, TEAM_FOOD_SOURCES["LT"]["stadium_id"])

        await _crawler().run(save=True)

        assert self._stored_stadiums(write_db) == {
            TEAM_FOOD_SOURCES["ALL"]["stadium_id"],
            TEAM_FOOD_SOURCES["NC"]["stadium_id"],
        }
        assert _only_run(write_db).status == "partial"

    @pytest.mark.asyncio
    async def test_a_rejected_stadium_leaves_no_menu_rows_behind(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A team either writes its vendor and its menus, or neither.

        The menus hang off the vendor's id, so a rollback that stopped at the
        vendor would still leave them reachable on the next flush.
        """
        self._break_writes(monkeypatch, TEAM_FOOD_SOURCES["LT"]["stadium_id"])

        await _crawler().run(save=True)

        with write_db() as check:
            menus_for_lt = (
                check.query(StadiumFoodMenuItem)
                .join(StadiumFoodVendor)
                .filter(
                    StadiumFoodVendor.stadium_id == TEAM_FOOD_SOURCES["LT"]["stadium_id"],
                )
            )
            assert menus_for_lt.count() == 0

    @pytest.mark.asyncio
    async def test_every_team_failing_writes_nothing_and_says_so(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._break_writes(monkeypatch, *(info["stadium_id"] for info in TEAM_FOOD_SOURCES.values()))

        await _crawler().run(save=True)

        assert self._stored_stadiums(write_db) == set()
        run = _only_run(write_db)
        assert run.status == "failed"
        assert run.records_written == 0
        assert run.records_failed == 3

    @pytest.mark.asyncio
    async def test_a_healthy_sweep_commits_every_team_and_its_menus(self, write_db: sessionmaker) -> None:
        """The happy path through the same real writes, so the fix cannot pass by failing."""
        await _crawler().run(save=True)

        run = _only_run(write_db)
        assert run.status == "success"
        assert run.records_written == 3
        assert run.records_failed == 0
        assert self._stored_stadiums(write_db) == {info["stadium_id"] for info in TEAM_FOOD_SOURCES.values()}
        with write_db() as check:
            assert check.query(StadiumFoodMenuItem).count() == 3


class TestAWriteTimeoutIsNotAFetchTimeout:
    """A bare ``TimeoutError`` during a write is a persistence timeout.

    Classifying it as a fetch fault would send the retry policy looking at the
    request path -- more throttling, a longer client timeout -- when the thing
    that timed out was the database.
    """

    @pytest.mark.asyncio
    async def test_the_timeout_is_named_as_a_persist_timeout(self, session_factory: sessionmaker) -> None:
        await TestWriteFailuresAreNotSilent()._writer({"LT"}, error=TimeoutError("write timed out")).run(save=True)

        letter = _letters(session_factory)[0]
        assert letter.error_code == FailureCode.PERSIST_TIMEOUT.value
        assert letter.failure_stage == "persist"
        assert letter.error_code != FailureCode.FETCH_TIMEOUT.value

    @pytest.mark.asyncio
    async def test_the_run_leaves_the_code_to_the_queue_when_it_is_only_partial(
        self, session_factory: sessionmaker
    ) -> None:
        """A partial run has no single error code, so it does not claim one.

        One team timed out and two wrote cleanly. Putting that code on the run
        would imply the whole run failed for that reason. The queue is where a
        per-team code belongs, and the run is where the tally belongs.
        """
        await TestWriteFailuresAreNotSilent()._writer({"LT"}, error=TimeoutError("write timed out")).run(save=True)

        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.error_code is None
        assert _letters(session_factory)[0].error_code == FailureCode.PERSIST_TIMEOUT.value
