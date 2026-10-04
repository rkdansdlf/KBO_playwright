"""주차장 크롤러 신뢰성 canary.

원장+DLQ 체인의 네 번째 이식이며, 처음으로 **실패가 예외가 아니라 빈 결과로**
나타나는 크롤러입니다. 이전에는 읽지 못한 팀 페이지가 로그 한 줄과 빈 리스트로
끝나 아무도 재처리할 수 없었습니다.

검증하는 계약:

    읽지 못한 팀은 격리되어 스윕이 계속되고, 실행은 ``partial``이 되며,
    팀 단위 DLQ 항목이 생긴다(팀이 재처리 단위);
    모든 팀이 실패하면 실행은 ``failed``가 된다;
    정상 스윕은 ``success``이며 DLQ 항목이 없다.

크롤러·원장·DLQ·taxonomy는 실제로 동작하고, HTTP 경계만 대체합니다.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import CrawlPersistError, FailureCode, stage_for_code
from src.crawlers.parking_crawler import (
    PARKING_CRAWLER_NAME,
    PARKING_TARGET_TYPE,
    TEAM_PARKING_SOURCES,
    ParkingCrawler,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.models.parking_fee_rule import ParkingFeeRule
from src.models.parking_lot import ParkingLot
from src.models.source_registry import DataSource, RawSourceSnapshot
from src.monitoring import crawler_metrics as cm
from src.repositories.parking_lot_repository import ParkingLotRepository

SK_URL = TEAM_PARKING_SOURCES["SK"]["url"]
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
def _fresh_metric_state() -> Iterator[None]:
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


@pytest.fixture(autouse=True)
def _wire_sessions(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)


def _crawler(failing_urls: set[str] | None = None) -> ParkingCrawler:
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

    crawler = ParkingCrawler()
    crawler._http.fetch_text = AsyncMock(side_effect=_fetch_text)
    return crawler


def _only_run(session_factory: sessionmaker) -> CrawlExecutionRun:
    with session_factory() as check:
        return check.query(CrawlExecutionRun).one()


def _letters(session_factory: sessionmaker) -> list[CrawlDeadLetter]:
    with session_factory() as check:
        return check.query(CrawlDeadLetter).all()


class TestTeamFailures:
    @pytest.mark.asyncio
    async def test_a_failing_team_is_isolated_and_enqueued(
        self,
        session_factory: sessionmaker,
    ) -> None:
        lots = await _crawler({SK_URL}).run()

        # 나머지 두 팀은 정상적으로 수집된다.
        assert len(lots) == 2
        run = _only_run(session_factory)
        assert run.status == "partial"
        assert run.records_read == 2
        assert run.crawler == PARKING_CRAWLER_NAME
        assert run.target_type == PARKING_TARGET_TYPE

        letter = _letters(session_factory)[0]
        assert letter.target_id == "SK"
        assert letter.source_url == SK_URL
        assert letter.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert letter.original_run_id == run.run_id
        # failure_stage는 code에서만 파생된다.
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    @pytest.mark.asyncio
    async def test_every_failing_team_marks_the_run_failed(self, session_factory: sessionmaker) -> None:
        all_urls = {info["url"] for info in TEAM_PARKING_SOURCES.values()}

        lots = await _crawler(all_urls).run()

        assert lots == []
        run = _only_run(session_factory)
        assert run.status == "failed"
        assert run.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert sorted(letter.target_id for letter in _letters(session_factory)) == ["LG", "SK", "SS"]

    @pytest.mark.asyncio
    async def test_no_dead_letter_is_enqueued_when_recording_is_disabled(
        self,
        session_factory: sessionmaker,
    ) -> None:
        await _crawler({SK_URL}).run(record_dead_letters=False)

        assert _only_run(session_factory).status == "partial"
        assert _letters(session_factory) == []


class TestHealthyAndFilteredRuns:
    @pytest.mark.asyncio
    async def test_a_healthy_sweep_is_a_successful_run(self, session_factory: sessionmaker) -> None:
        lots = await _crawler().run()

        assert len(lots) == 3
        run = _only_run(session_factory)
        assert run.status == "success"
        assert run.error_code is None
        assert _letters(session_factory) == []

    @pytest.mark.asyncio
    async def test_a_team_filter_narrows_the_ledger_target(self, session_factory: sessionmaker) -> None:
        lots = await _crawler().run(team_filter="LG")

        assert len(lots) == 1
        run = _only_run(session_factory)
        assert run.target_id == "LG"
        assert run.source_url == TEAM_PARKING_SOURCES["LG"]["url"]


# ── the write side ──────────────────────────────────────────────────────────
#
# The lot repository flushes on insert, so one constraint violation used to leave
# the session needing a rollback: every later save in the sweep failed with
# PendingRollbackError, the batch commit rolled all three teams back, and the
# run reported `success` with zero rows written. `crawl_p1p2_data_job` then wrote
# its "ok" run marker and the lock-health check reported a healthy lock.


@pytest.fixture
def write_db(monkeypatch: pytest.MonkeyPatch, session_factory: sessionmaker) -> sessionmaker:
    """Add the tables the save path writes to, on the same database as the ledger.

    One engine rather than two: a run recorded in one database and rows written
    to another would let every assertion below pass while describing two
    databases that never coexisted.
    """
    engine = session_factory.kw["bind"]
    for table in (DataSource.__table__, RawSourceSnapshot.__table__, ParkingLot.__table__, ParkingFeeRule.__table__):
        table.create(engine)
    monkeypatch.setattr("src.crawlers.parking_crawler.SessionLocal", session_factory)
    with session_factory() as session:
        for info in TEAM_PARKING_SOURCES.values():
            session.add(
                DataSource(
                    source_key=info["source_key"],
                    source_type="web",
                    target_domain="parking",
                    is_active=True,
                ),
            )
        session.commit()
    return session_factory


def _break_writes(monkeypatch: pytest.MonkeyPatch, *stadium_ids: str) -> None:
    """Make the lot write raise a real IntegrityError for the named stadiums."""
    broken = set(stadium_ids)
    real_save = ParkingLotRepository.save

    def _save(self, data: dict) -> ParkingLot:
        if data.get("stadium_id") in broken:
            raise IntegrityError("INSERT INTO parking_lots ...", {}, Exception("UNIQUE constraint failed"))
        return real_save(self, data)

    monkeypatch.setattr(ParkingLotRepository, "save", _save)


def _stored_stadiums(factory: sessionmaker) -> set[str]:
    with factory() as check:
        return {row.stadium_id for row in check.query(ParkingLot).all()}


def _snapshot_count(factory: sessionmaker) -> int:
    with factory() as check:
        return check.query(RawSourceSnapshot).count()


class TestPersistenceFailures:
    @pytest.mark.asyncio
    async def test_one_team_failing_to_write_does_not_roll_back_the_others(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _break_writes(monkeypatch, TEAM_PARKING_SOURCES["SK"]["stadium_id"])

        lots = await _crawler().run(save=True)

        assert len(lots) == 3
        # The other two teams kept their rows: the transaction unit is the team.
        assert _stored_stadiums(write_db) == {
            TEAM_PARKING_SOURCES["LG"]["stadium_id"],
            TEAM_PARKING_SOURCES["SS"]["stadium_id"],
        }
        run = _only_run(write_db)
        assert run.status == "partial"
        assert run.records_written == 2
        assert run.records_failed == 1
        assert run.records_read == 3

    @pytest.mark.asyncio
    async def test_only_the_unwritten_team_is_enqueued(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The letter has to name a team, because a team is what a replay re-runs."""
        _break_writes(monkeypatch, TEAM_PARKING_SOURCES["SK"]["stadium_id"])

        await _crawler().run(save=True)

        letters = _letters(write_db)
        assert [letter.target_id for letter in letters] == ["SK"]
        assert letters[0].error_code == FailureCode.PERSIST_CONSTRAINT.value
        assert letters[0].failure_stage == stage_for_code(letters[0].error_code).value
        assert letters[0].source_url == TEAM_PARKING_SOURCES["SK"]["url"]

    @pytest.mark.asyncio
    async def test_a_sweep_that_writes_nothing_is_failed_not_partial(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Nothing in the table looks the same either way; only the status tells them apart."""
        _break_writes(monkeypatch, *(info["stadium_id"] for info in TEAM_PARKING_SOURCES.values()))

        await _crawler().run(save=True)

        run = _only_run(write_db)
        assert run.status == "failed"
        assert run.records_written == 0
        assert run.records_failed == 3
        assert run.error_code == FailureCode.PERSIST_CONSTRAINT.value
        assert _stored_stadiums(write_db) == set()

    @pytest.mark.asyncio
    async def test_a_write_failure_does_not_change_what_was_read(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _break_writes(monkeypatch, TEAM_PARKING_SOURCES["LG"]["stadium_id"])

        await _crawler().run(save=True)

        # The source answered; only the write died. Reporting a short read here
        # would send an operator looking at the page instead of at the database.
        assert _only_run(write_db).records_read == 3

    @pytest.mark.asyncio
    async def test_the_raw_page_survives_a_failed_write(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The snapshot is the evidence a replay re-parses, so the write must not discard it."""
        _break_writes(monkeypatch, *(info["stadium_id"] for info in TEAM_PARKING_SOURCES.values()))

        await _crawler().run(save=True)

        assert _snapshot_count(write_db) == len(TEAM_PARKING_SOURCES)

    @pytest.mark.asyncio
    async def test_a_failed_snapshot_does_not_block_the_domain_rows(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Evidence is worth keeping, but the rows a live sweep read are worth more."""
        monkeypatch.setattr(
            "src.crawlers.parking_crawler.save_raw_snapshots",
            MagicMock(side_effect=SQLAlchemyError("snapshot table is gone")),
        )

        await _crawler().run(save=True)

        assert _stored_stadiums(write_db) == {info["stadium_id"] for info in TEAM_PARKING_SOURCES.values()}
        assert _only_run(write_db).status == "success"

    @pytest.mark.asyncio
    async def test_the_replay_flag_hands_the_caller_the_classification(
        self,
        write_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _break_writes(monkeypatch, TEAM_PARKING_SOURCES["LG"]["stadium_id"])

        with pytest.raises(CrawlPersistError) as raised:
            await _crawler().run(save=True, raise_on_persist_error=True)

        assert raised.value.error_code == FailureCode.PERSIST_CONSTRAINT.value
        # The run is judged from the stored row, so it has to be terminal already.
        assert _only_run(write_db).status == "failed"

    @pytest.mark.asyncio
    async def test_a_healthy_save_writes_every_team(
        self,
        write_db: sessionmaker,
    ) -> None:
        await _crawler().run(save=True)

        run = _only_run(write_db)
        assert run.status == "success"
        assert run.records_written == 3
        assert run.records_failed == 0
        assert _letters(write_db) == []
        assert _stored_stadiums(write_db) == {info["stadium_id"] for info in TEAM_PARKING_SOURCES.values()}

    @pytest.mark.asyncio
    async def test_parsed_fees_reach_the_snapshot_and_not_the_fee_table(
        self,
        write_db: sessionmaker,
    ) -> None:
        """The fee text is real evidence; a vehicle class for it would not be.

        The parser produces a kind (기본/추가/일일/행사) and `parking_fee_rules`
        is keyed by vehicle class with a non-null base duration, so writing the
        kind there raised `KeyError: 'vehicle_type'` and took the lot's own row
        down with it. The kinds belong to the snapshot; asserting that here keeps
        a later schema change from silently reintroducing the mismatch.
        """
        await _crawler().run(save=True)

        with write_db() as check:
            assert check.query(ParkingFeeRule).count() == 0
        snapshots = _snapshot_count(write_db)
        assert snapshots == len(TEAM_PARKING_SOURCES)
        # The page text that produced the fees is still on disk to re-parse.
        assert _crawler()._parse_parking_page(HTML, TEAM_PARKING_SOURCES["LG"])
