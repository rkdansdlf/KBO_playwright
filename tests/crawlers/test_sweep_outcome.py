"""What a child sweep reports, and how three of them collapse into one status.

`crawl_p1p2_data_job` judged its three children by whether they raised, because
all three returned `None`. On 2026-10-19 that produced a success alert for a
sweep in which `seat` lost every team to `FETCH_HTTP_ERROR` and `food` parsed
nothing. These tests pin the report the children now return, and the arithmetic
that turns three of them into one status -- including the case that must never
round up: a child that reports nothing at all.

The most load-bearing assertion here is not about arithmetic. `from_run` reads a
row that is still `running` and calls it a success, which is only honest if the
block's exit does the same thing; the first test runs a real `track_crawl_run`
block so that reading and finalization are compared against each other rather
than each against its own assumption.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.food_crawler import FOOD_CRAWLER_NAME, TEAM_FOOD_SOURCES, FoodCrawler
from src.crawlers.parking_crawler import PARKING_CRAWLER_NAME, TEAM_PARKING_SOURCES, ParkingCrawler
from src.crawlers.seat_crawler import SEAT_CRAWLER_NAME, TEAM_SEAT_SOURCES, SeatCrawler
from src.crawlers.sweep_outcome import CrawlSweepOutcome, summarize_sweep
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import (
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_RUNNING,
    RUN_STATUS_SUCCESS,
    CrawlExecutionRun,
)
from src.monitoring import crawler_metrics as cm
from src.repositories.crawl_execution_repository import CrawlRunSpec
from src.services.crawl_run_service import track_crawl_run


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
    """Point the ledger and the dead letter queue at the in-memory database."""
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.crawl_dead_letter_service.SessionLocal", session_factory)
    for module in ("parking_crawler", "seat_crawler", "food_crawler"):
        monkeypatch.setattr(f"src.crawlers.{module}.SessionLocal", session_factory)


def _run(**overrides: object) -> CrawlExecutionRun:
    values: dict[str, object] = {
        "run_id": "run-0001",
        "crawler": "seat",
        "target_type": "seat_section",
        "started_at": datetime(2026, 10, 19, 6, 45, tzinfo=UTC),
        "status": RUN_STATUS_RUNNING,
        "records_read": 0,
        "records_written": 0,
        "records_failed": 0,
    }
    return CrawlExecutionRun(**{**values, **overrides})


def _outcome(crawler: str, status: str, **overrides: object) -> CrawlSweepOutcome:
    values: dict[str, object] = {"crawler": crawler, "status": status}
    return CrawlSweepOutcome(**{**values, **overrides})


class TestReadingALiveRun:
    def test_a_run_with_no_failures_reads_as_the_status_its_block_exit_writes(self, session_factory):
        """The mapping and the finalization are compared by running both.

        Reading a `running` row as a success is a claim about what
        `track_crawl_run` will record a moment later. If that claim is ever
        wrong, every healthy child would be reported as failed instead, so the
        test does not assert the mapping in the abstract: it captures the
        outcome inside a real block and then reads the terminal row.
        """
        with session_factory() as session:
            spec = CrawlRunSpec(crawler="seat", target_type="seat_section", target_id="all")
            with track_crawl_run(spec, session=session) as run:
                run.records_read = 12
                run.records_written = 12
                captured = CrawlSweepOutcome.from_run(run, crawler="seat")

            assert run.status == RUN_STATUS_SUCCESS

        assert captured.status == RUN_STATUS_SUCCESS
        assert (captured.rows, captured.written, captured.failed) == (12, 12, 0)

    def test_a_run_the_crawler_marked_keeps_the_crawler_s_own_status(self):
        """A child that recorded teams as failed does not read back as healthy."""
        outcome = CrawlSweepOutcome.from_run(
            _run(status=RUN_STATUS_FAILED, records_read=9, records_written=0, records_failed=9),
            crawler="parking",
        )

        assert outcome.status == RUN_STATUS_FAILED
        assert (outcome.rows, outcome.written, outcome.failed) == (9, 0, 9)

    def test_a_partial_run_stays_partial(self):
        outcome = CrawlSweepOutcome.from_run(
            _run(status=RUN_STATUS_PARTIAL, records_read=9, records_written=4, records_failed=5),
            crawler="food",
        )

        assert outcome.status == RUN_STATUS_PARTIAL

    def test_counters_the_database_has_not_defaulted_yet_read_as_zero(self):
        """An unflushed row carries `None`; a report cannot."""
        outcome = CrawlSweepOutcome.from_run(
            _run(records_read=None, records_written=None, records_failed=None),
            crawler="seat",
        )

        assert (outcome.rows, outcome.written, outcome.failed) == (0, 0, 0)

    def test_the_failure_code_travels_with_the_outcome(self):
        outcome = CrawlSweepOutcome.from_run(
            _run(status=RUN_STATUS_FAILED, error_code="FETCH_HTTP_ERROR"),
            crawler="seat",
        )

        assert outcome.error_code == "FETCH_HTTP_ERROR"

    def test_failed_units_keep_run_order_and_lose_repeats(self):
        """A team can fail on read and on write; a warning naming it twice reads as two teams."""
        outcome = CrawlSweepOutcome.from_run(
            _run(status=RUN_STATUS_FAILED),
            crawler="parking",
            failed_units=["SK", "LG", "SK"],
        )

        assert outcome.failed_units == ("SK", "LG")


class TestDescribingAFailure:
    def test_a_child_that_raised_is_failed_and_names_the_exception(self):
        """The exception is what the operator has to act on, so it is in the report."""
        outcome = CrawlSweepOutcome.launch_failure(crawler="parking", error=RuntimeError("boom"))

        assert outcome.status == RUN_STATUS_FAILED
        assert outcome.collected is False
        assert outcome.detail == "RuntimeError: boom"

    def test_a_child_that_reported_nothing_is_failed_rather_than_assumed_healthy(self):
        """The 2026-10-19 defect, restated as a value.

        The old job inferred health from `main()` returning at all. A child that
        reports no outcome must not be able to reinstate that inference, however
        healthy the build it came from.
        """
        outcome = CrawlSweepOutcome.unreported(crawler="food")

        assert outcome.status == RUN_STATUS_FAILED
        assert outcome.detail == "returned no outcome"

    def test_a_child_that_stored_rows_is_reported_as_collecting(self):
        outcome = _outcome("seat", RUN_STATUS_SUCCESS, written=7)

        assert outcome.collected is True

    def test_describe_names_the_units_the_code_and_the_detail(self):
        outcome = _outcome(
            "seat",
            RUN_STATUS_FAILED,
            failed=2,
            failed_units=("SK", "LG"),
            error_code="FETCH_HTTP_ERROR",
        )

        line = outcome.describe()

        assert line.startswith("seat=failed")
        assert "written=0" in line
        assert "units=SK,LG" in line
        assert "code=FETCH_HTTP_ERROR" in line

    def test_describe_omits_the_absent_parts(self):
        """A clean child's line must not read as if it had failures to name."""
        line = _outcome("food", RUN_STATUS_SUCCESS, written=21).describe()

        assert "units=" not in line
        assert "code=" not in line
        assert "detail=" not in line


class TestCollapsingTheSweep:
    def test_all_children_clean_is_success(self):
        summary = summarize_sweep(
            [
                _outcome("seat", RUN_STATUS_SUCCESS, written=30),
                _outcome("parking", RUN_STATUS_SUCCESS, written=3),
                _outcome("food", RUN_STATUS_SUCCESS, written=21),
            ]
        )

        assert summary.status == RUN_STATUS_SUCCESS
        assert summary.written_total == 54

    def test_one_failed_child_makes_the_sweep_partial(self):
        summary = summarize_sweep(
            [
                _outcome("seat", RUN_STATUS_FAILED),
                _outcome("parking", RUN_STATUS_SUCCESS, written=3),
                _outcome("food", RUN_STATUS_SUCCESS, written=21),
            ]
        )

        assert summary.status == RUN_STATUS_PARTIAL
        assert [child.crawler for child in summary.unsuccessful_children] == ["seat"]

    def test_a_partial_child_keeps_the_sweep_out_of_success(self):
        """One team lost is not a clean sweep, even when every other child was perfect."""
        summary = summarize_sweep(
            [
                _outcome("seat", RUN_STATUS_SUCCESS, written=30),
                _outcome("parking", RUN_STATUS_PARTIAL, written=3, failed=1),
                _outcome("food", RUN_STATUS_SUCCESS, written=21),
            ]
        )

        assert summary.status == RUN_STATUS_PARTIAL

    def test_every_child_failed_is_failed(self):
        summary = summarize_sweep(
            [
                _outcome("seat", RUN_STATUS_FAILED, failed=2),
                _outcome("parking", RUN_STATUS_FAILED, failed=2),
                _outcome("food", RUN_STATUS_FAILED, failed=2),
            ]
        )

        assert summary.status == RUN_STATUS_FAILED
        assert summary.failed_total == 6

    def test_no_children_is_failed_because_nothing_ran(self):
        """A sweep that ran nothing collected nothing; that is the reading not to round up."""
        summary = summarize_sweep([])

        assert summary.status == RUN_STATUS_FAILED
        assert summary.written_total == 0

    def test_a_clean_child_that_stored_nothing_does_not_make_the_sweep_partial(self):
        """An empty page is a legitimate result, not a shortfall.

        The statuses come from the children, who know whether a page was empty
        or unreadable. Re-deciding that here from record counts would replace
        their reading with a guess, and the guess would call every quiet day a
        partial failure.
        """
        summary = summarize_sweep(
            [
                _outcome("seat", RUN_STATUS_SUCCESS, written=0),
                _outcome("parking", RUN_STATUS_SUCCESS, written=0),
                _outcome("food", RUN_STATUS_SUCCESS, written=0),
            ]
        )

        assert summary.status == RUN_STATUS_SUCCESS

    def test_the_summary_is_json_able_for_the_marker_and_the_registry(self):
        """`_write_p1p2_run_marker` json-dumps this; tuples do not survive that unaided."""
        summary = summarize_sweep(
            [
                _outcome(
                    "seat", RUN_STATUS_PARTIAL, written=3, failed=1, failed_units=("SK",), error_code="PARSE_EMPTY"
                ),
                CrawlSweepOutcome.launch_failure(crawler="parking", error=RuntimeError("boom")),
            ]
        )

        payload = json.loads(json.dumps(summary.as_dict()))

        assert payload["status"] == RUN_STATUS_PARTIAL
        assert payload["written"] == 3
        assert payload["failed"] == 1
        assert [child["crawler"] for child in payload["children"]] == ["seat", "parking"]
        assert payload["children"][0]["units"] == ["SK"]
        assert payload["children"][1]["detail"] == "RuntimeError: boom"

    def test_describe_lists_every_child_in_run_order(self):
        """One line an incident can carry, ordered the way the sweep ran."""
        summary = summarize_sweep(
            [
                _outcome("seat", RUN_STATUS_FAILED),
                _outcome("parking", RUN_STATUS_SUCCESS, written=3),
                _outcome("food", RUN_STATUS_SUCCESS, written=21),
            ]
        )

        line = summary.describe()

        assert line.index("seat=") < line.index("parking=") < line.index("food=")
        assert line.count(";") == 2


CRAWLER_SEAMS = (
    pytest.param(ParkingCrawler, "_crawl_team_parking", TEAM_PARKING_SOURCES, PARKING_CRAWLER_NAME, id="parking"),
    pytest.param(SeatCrawler, "_crawl_team_seats", TEAM_SEAT_SOURCES, SEAT_CRAWLER_NAME, id="seat"),
    pytest.param(FoodCrawler, "_crawl_team_food", TEAM_FOOD_SOURCES, FOOD_CRAWLER_NAME, id="food"),
)


class TestTheCrawlerLeavesItsOutcomeBehind:
    """The property the CLI reads, driven through each crawler's own failure handling.

    `from_run` is unit-tested above; what these cover is the wiring around it --
    that each crawler captures its sweep, on both sides of the branch, so that a
    failed sweep and a quiet one are distinguishable at the caller. The per-team
    method is the seam rather than the HTTP fetch because one URL can answer for
    more than one team (seat maps LG and OB to the same page), and a fetch stub
    cannot say which team it answered for.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("crawler_cls", "seam", "sources", "name"), CRAWLER_SEAMS)
    async def test_a_sweep_that_lost_every_team_is_reported_as_failed(
        self,
        crawler_cls: type,
        seam: str,
        sources: dict[str, dict],
        name: str,
    ) -> None:
        async def _unavailable(team_code: str, info: dict) -> list[dict]:
            detail = f"{team_code} unavailable"
            raise RuntimeError(detail)

        crawler = crawler_cls()
        setattr(crawler, seam, _unavailable)

        await crawler.run()

        outcome = crawler.last_run_outcome
        assert outcome is not None
        assert outcome.crawler == name
        assert outcome.status == RUN_STATUS_FAILED
        assert sorted(outcome.failed_units) == sorted(sources)
        assert outcome.collected is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("crawler_cls", "seam", "sources", "name"), CRAWLER_SEAMS)
    async def test_a_sweep_that_found_nothing_reports_success_with_nothing_collected(
        self,
        crawler_cls: type,
        seam: str,
        sources: dict[str, dict],
        name: str,
    ) -> None:
        """A quiet page is a success, and the report says it collected nothing.

        Both readings travel: the caller can tell this apart from a failure
        because the status differs, and apart from a productive sweep because
        the count differs. Collapsing either one is how the old job reported
        success for a sweep that stored nothing.
        """

        async def _empty(team_code: str, info: dict) -> list[dict]:
            return []

        crawler = crawler_cls()
        setattr(crawler, seam, _empty)

        await crawler.run()

        outcome = crawler.last_run_outcome
        assert outcome is not None
        assert outcome.status == RUN_STATUS_SUCCESS
        assert outcome.collected is False
        assert outcome.failed_units == ()

    def test_the_outcome_is_empty_before_a_run_has_completed(self):
        """An unrun crawler must not be able to answer for a sweep it never made."""
        assert ParkingCrawler().last_run_outcome is None
