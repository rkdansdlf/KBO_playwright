"""The run ledger must record *who* started a run, not only what it did.

The ledger answers "what ran and did it work", and until `origin` existed it did
not answer "who asked for it". That gap is invisible until something has to be
attributed: a day of `schedule` runs at a two-minute cadence is
indistinguishable from the daily pipeline's single run, and the ledger is the
projection every crawl alert reads, so an unattributable row is an
unobservable one rather than an inconvenient one.

These tests pin the field's contract at the layer where it can be enforced:
the value set is closed, the column is written at INSERT rather than on a
terminal path, and a row left in `running` -- the case most worth attributing,
because a dead process writes nothing else -- still carries its caller.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.run_origin import CRAWL_RUN_ORIGINS, CrawlRunOrigin
from src.models.crawl_execution import RUN_STATUS_RUNNING, CrawlExecutionRun
from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
from src.services.crawl_run_service import CrawlRunService

_STARTED = datetime(2026, 10, 7, 6, 0, 0)


@pytest.fixture
def session():
    """Provide an isolated ledger with only this model's table."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as active:
        yield active
    engine.dispose()


def _spec(**overrides: object) -> CrawlRunSpec:
    fields: dict[str, object] = {
        "crawler": "schedule",
        "target_type": "schedule_month",
        "target_id": "2026-10",
    }
    fields.update(overrides)
    return CrawlRunSpec(**fields)  # type: ignore[arg-type]


class TestTheOriginValueSetIsClosed:
    """An open string here would reintroduce the unbounded-label problem."""

    def test_every_member_has_a_stable_wire_value(self):
        assert {o.value for o in CrawlRunOrigin} == CRAWL_RUN_ORIGINS

    def test_the_set_covers_the_subsystems_that_record_runs(self):
        """The callers that can start a recorded run must all be nameable.

        A caller with no member cannot identify itself, and the fix for that is
        never "use a close-enough string" -- it is adding the member, which this
        assertion forces to be deliberate.
        """
        assert {
            "live_crawler",
            "daily_update",
            "dlq_retry",
            "scheduler",
            "replay",
            "cli",
        } == CRAWL_RUN_ORIGINS

    def test_the_wire_value_is_the_enum_name_in_lower_snake_case(self):
        for origin in CrawlRunOrigin:
            assert origin.value == origin.name.lower(), origin


class TestOriginIsRecordedAtInsert:
    """The column must exist before the run finishes, or it is useless."""

    def test_start_run_persists_the_origin(self, session):
        run = CrawlExecutionRepository(session).start_run(_spec(origin=CrawlRunOrigin.DLQ_RETRY))

        assert run.origin == "dlq_retry"

        stored = session.scalar(select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run.run_id))
        assert stored is not None
        assert stored.origin == "dlq_retry"

    def test_a_run_still_in_running_carries_its_caller(self, session):
        """The dead-process case: no terminal path ever ran.

        This is the whole reason the field is not `checkpoint`. A stranded row is
        the one an operator most needs to attribute, and a value written only on
        termination is absent from exactly that row.
        """
        run = CrawlExecutionRepository(session).start_run(_spec(origin=CrawlRunOrigin.SCHEDULER))
        session.commit()

        # Deliberately no mark_* call: the process died here.
        assert run.status == RUN_STATUS_RUNNING
        assert run.finished_at is None
        assert run.origin == "scheduler"

        # Re-read through a closed session so the assertion sees what a sweeper
        # in another process would see: the committed row, not the identity map.
        session.expunge_all()
        row = session.scalar(select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run.run_id))
        assert row is not None, "the stranded run was never committed, so nothing could attribute it"
        assert row.status == RUN_STATUS_RUNNING
        assert row.origin == "scheduler"

    def test_an_unlabelled_caller_stays_null(self, session):
        """NULL means "not recorded", which is the honest value for old rows.

        It must not be backfilled with a guess: inferring a caller from the run's
        timing would be the same untraceable reasoning the field exists to
        replace.
        """
        run = CrawlExecutionRepository(session).start_run(_spec())

        assert run.origin is None

    def test_the_enum_value_is_accepted_as_well_as_the_member(self, session):
        """Both forms store the same thing, so a caller cannot get it wrong."""
        run = CrawlExecutionRepository(session).start_run(_spec(origin=CrawlRunOrigin.CLI.value))

        assert run.origin == "cli"


class TestTheColumnExistsForRealDeployments:
    """A model attribute alone does not add a column to an existing database."""

    def test_the_column_and_its_index_are_in_the_schema(self):
        engine = create_engine("sqlite:///:memory:")
        CrawlExecutionRun.__table__.create(engine)

        columns = {c["name"] for c in inspect(engine).get_columns("crawl_execution_runs")}
        indexes = {i["name"] for i in inspect(engine).get_indexes("crawl_execution_runs")}

        assert "origin" in columns
        assert "idx_crawl_execution_runs_origin" in indexes
        engine.dispose()

    def test_the_column_is_nullable(self):
        """Existing rows must not need backfilling to load the table."""
        column = CrawlExecutionRun.__table__.columns["origin"]

        assert column.nullable is True


class TestOriginSurvivesTheTerminalTransition:
    """Projection happens on the terminal path, so origin must not be lost there."""

    def test_a_failed_run_keeps_the_origin_it_started_with(self, session):
        service = CrawlRunService(session)
        run = service.start(_spec(origin=CrawlRunOrigin.DAILY_UPDATE))

        service.failed(run, error_code="FETCH_TIMEOUT", error_message="timed out")
        session.commit()

        stored = session.scalar(select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run.run_id))
        assert stored is not None
        assert stored.origin == "daily_update"

    def test_a_succeeded_run_keeps_the_origin_it_started_with(self, session):
        service = CrawlRunService(session)
        run = service.start(_spec(origin=CrawlRunOrigin.LIVE_CRAWLER))

        service.success(run)
        session.commit()

        stored = session.scalar(select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run.run_id))
        assert stored is not None
        assert stored.origin == "live_crawler"


class TestTheFieldSeparatesCallersOfTheSameCrawler:
    """The property the 422-run investigation could not answer without it."""

    def test_two_callers_of_one_crawler_are_distinguishable(self, session):
        """Same crawler, same target, different caller -- and now attributable.

        This is the shape that made the 10/7 `schedule` surge unexplainable: 422
        runs of the same crawler and target, with nothing on the row saying
        whether the daily pipeline or the live loop had asked for them.
        """
        repository = CrawlExecutionRepository(session)
        pipeline = repository.start_run(_spec(origin=CrawlRunOrigin.DAILY_UPDATE))
        live = repository.start_run(_spec(origin=CrawlRunOrigin.LIVE_CRAWLER))
        session.commit()

        rows = session.scalars(
            select(CrawlExecutionRun)
            .where(CrawlExecutionRun.run_id.in_([pipeline.run_id, live.run_id]))
            .order_by(CrawlExecutionRun.started_at),
        ).all()

        assert [r.origin for r in rows] == ["daily_update", "live_crawler"]
        assert len({r.origin for r in rows}) == 2

    def test_stale_running_runs_can_be_grouped_by_caller(self, session):
        """The query a sweeper needs: who left this behind, and how long ago.

        Pinned because it is the reason `started_at` and `origin` are read
        together. Without a caller the sweep can only report *that* runs were
        stranded, not which subsystem to ask about it.
        """
        session.add_all(
            [
                CrawlExecutionRun(
                    run_id="stranded-scheduler",
                    crawler="schedule",
                    target_type="schedule_month",
                    status=RUN_STATUS_RUNNING,
                    started_at=_STARTED,
                    origin=CrawlRunOrigin.SCHEDULER,
                ),
                CrawlExecutionRun(
                    run_id="stranded-live",
                    crawler="schedule",
                    target_type="schedule_month",
                    status=RUN_STATUS_RUNNING,
                    started_at=_STARTED,
                    origin=CrawlRunOrigin.LIVE_CRAWLER,
                ),
            ],
        )
        session.commit()

        cutoff = _STARTED + timedelta(hours=6)
        stranded = session.scalars(
            select(CrawlExecutionRun.origin)
            .where(CrawlExecutionRun.status == RUN_STATUS_RUNNING)
            .where(CrawlExecutionRun.started_at < cutoff)
            .distinct(),
        ).all()

        assert set(stranded) == {"scheduler", "live_crawler"}


class TestOriginAndReplayLineageStayIndependent:
    """`origin` says who called; the run-id columns say what it was for.

    Keeping them separate stops a future refactor from trying to infer one from
    the other -- a replay is not "the caller", it is a lineage relation.
    """

    def test_a_replay_row_carries_both_the_origin_and_the_link(self, session):
        run = CrawlExecutionRepository(session).start_run(
            _spec(
                origin=CrawlRunOrigin.REPLAY,
                parent_run_id="original-run",
                replay_of_run_id="original-run",
            ),
        )

        assert run.origin == "replay"
        assert run.parent_run_id == "original-run"
        assert run.replay_of_run_id == "original-run"

    def test_origin_does_not_imply_a_replay_link(self, session):
        """A live run is not a replay, and must not be marked as one by origin."""
        run = CrawlExecutionRepository(session).start_run(_spec(origin=CrawlRunOrigin.LIVE_CRAWLER))

        assert run.replay_of_run_id is None
        assert run.parent_run_id is None

    def test_the_timestamp_column_stays_naive_utc(self, session):
        """Adding a column must not disturb the schema's timestamp convention.

        `started_at` is naive UTC across the whole ledger; a sweeper comparing it
        against `datetime.now(UTC)` only works while that holds.
        """
        run = CrawlExecutionRepository(session).start_run(_spec(origin=CrawlRunOrigin.CLI))
        stored = session.scalar(select(CrawlExecutionRun).where(CrawlExecutionRun.run_id == run.run_id))

        assert stored is not None
        assert stored.started_at.tzinfo is None
        assert stored.started_at <= datetime.now(UTC).replace(tzinfo=None)
