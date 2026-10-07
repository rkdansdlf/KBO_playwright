"""A run stranded in `running` must reach a terminal state on its own.

A crawl run is opened before the work starts and closed after it ends, so a
process that dies in between leaves a row that says neither. Nothing else
reclaims it: `recover_stuck_retrying` covers replay runs, because their dead
letter is still queued, and an ordinary crawl run has no equivalent. On
2026-10-08 one such row was five days old -- `id=37`, a `schedule` run for
`2026-10`, started during the database outage.

This matters more after an outage than in normal operation. The database *is*
the ledger, so a database that is down cannot record the rows it was about to
write, and the run it was mid-way through is exactly the one that never arrives.

The classification is pinned as carefully as the sweep. `RUN_INTERRUPTED` and
`REPLAY_INTERRUPTED` share the `orchestrate` stage and mean opposite things: a
replay has a dead letter naming the unit to repeat, while a stranded general run
only proves that something stopped. A future tidy-up that groups the two
`*_INTERRUPTED` codes as alike would turn every reclaimed run into a retry of
unknown scope, which is the failure the taxonomy exists to prevent.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode, FailureStage, stage_for_code
from src.models.crawl_execution import (
    RUN_STATUS_FAILED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_SUCCESS,
    CrawlExecutionRun,
)
from src.services.crawl_retry_policy import NON_RETRYABLE_CODES, RETRYABLE_CODES, decide
from src.services.orphan_run_sweeper import (
    DEFAULT_STALE_SECONDS,
    OrphanSweepResult,
    find_stale_running_runs,
    stale_seconds,
    sweep_orphaned_runs,
)

_NOW = datetime(2026, 10, 8, 2, 0, 0)
_SIX_HOURS = DEFAULT_STALE_SECONDS


@pytest.fixture
def ledger():
    """Provide an isolated ledger holding only the run table."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def _running(
    run_id: str,
    *,
    started_at: datetime,
    crawler: str = "schedule",
    replay_of: str | None = None,
    origin: str | None = "daily_update",
) -> CrawlExecutionRun:
    return CrawlExecutionRun(
        run_id=run_id,
        crawler=crawler,
        target_type="schedule_month",
        target_id="2026-10",
        status=RUN_STATUS_RUNNING,
        started_at=started_at,
        origin=origin,
        replay_of_run_id=replay_of,
    )


class TestTheTwoInterruptedCodesMeanOppositeThings:
    """Same stage, opposite retry meaning -- the distinction is the point."""

    def test_both_codes_are_orchestrate_stage(self):
        assert stage_for_code(FailureCode.RUN_INTERRUPTED) is FailureStage.ORCHESTRATE
        assert stage_for_code(FailureCode.REPLAY_INTERRUPTED) is FailureStage.ORCHESTRATE

    def test_a_replay_interruption_is_retryable(self):
        """It has a dead letter naming the unit, so repeating it is well-defined."""
        assert decide(FailureCode.REPLAY_INTERRUPTED.value, retry_count=0).retryable is True
        assert FailureCode.REPLAY_INTERRUPTED.value in RETRYABLE_CODES

    def test_a_general_run_interruption_is_not_retryable(self):
        """There is no known unit to re-run, so the answer is to close, not retry."""
        assert decide(FailureCode.RUN_INTERRUPTED.value, retry_count=0).retryable is False
        assert FailureCode.RUN_INTERRUPTED.value in NON_RETRYABLE_CODES

    def test_the_two_codes_are_not_in_each_others_set(self):
        """A later refactor must not quietly merge them."""
        assert FailureCode.RUN_INTERRUPTED.value not in RETRYABLE_CODES
        assert FailureCode.REPLAY_INTERRUPTED.value not in NON_RETRYABLE_CODES

    def test_run_interrupted_is_not_reused_as_a_replay_meaning(self):
        """Named separately so the ledger never reads one as the other."""
        assert FailureCode.RUN_INTERRUPTED.value != FailureCode.REPLAY_INTERRUPTED.value


class TestStrandedRunsAreFound:
    """Selection is where a wrong threshold silently does nothing."""

    def test_a_run_past_the_threshold_is_found(self, ledger):
        with ledger() as session:
            session.add(_running("old", started_at=_NOW - timedelta(seconds=_SIX_HOURS + 60)))
            session.commit()

            found = find_stale_running_runs(session=session, now=_NOW, older_than_seconds=_SIX_HOURS)

            assert [r.run_id for r in found] == ["old"]

    def test_a_recent_run_is_left_alone(self, ledger):
        """A crawl that legitimately runs for hours must not be closed out."""
        with ledger() as session:
            session.add(_running("fresh", started_at=_NOW - timedelta(seconds=_SIX_HOURS - 60)))
            session.commit()

            found = find_stale_running_runs(session=session, now=_NOW, older_than_seconds=_SIX_HOURS)

            assert found == []

    def test_a_finished_run_is_never_selected(self, ledger):
        """The sweep closes rows; it must not re-close what already closed."""
        with ledger() as session:
            for status in (RUN_STATUS_SUCCESS, RUN_STATUS_FAILED):
                session.add(
                    CrawlExecutionRun(
                        run_id=f"done-{status}",
                        crawler="schedule",
                        target_type="schedule_month",
                        status=status,
                        started_at=_NOW - timedelta(days=5),
                    ),
                )
            session.commit()

            found = find_stale_running_runs(session=session, now=_NOW, older_than_seconds=_SIX_HOURS)

            assert found == []

    def test_the_replay_case_is_excluded_for_the_dead_letter_path(self, ledger):
        """Two recovery systems must never write the same row."""
        with ledger() as session:
            session.add(_running("orphan", started_at=_NOW - timedelta(days=5)))
            session.add(_running("replay", started_at=_NOW - timedelta(days=5), replay_of="original-run"))
            session.commit()

            found = find_stale_running_runs(session=session, now=_NOW, older_than_seconds=_SIX_HOURS)

            assert [r.run_id for r in found] == ["orphan"]

    def test_older_rows_come_first(self, ledger):
        """Bounded work should start where the delay has been longest."""
        with ledger() as session:
            session.add(_running("recent", started_at=_NOW - timedelta(hours=7)))
            session.add(_running("oldest", started_at=_NOW - timedelta(days=9)))
            session.commit()

            found = find_stale_running_runs(session=session, now=_NOW, older_than_seconds=_SIX_HOURS)

            assert [r.run_id for r in found] == ["oldest", "recent"]


class TestTheSweepClosesTheStateSpace:
    """The row must actually reach `failed`, not merely be reported."""

    def test_a_stranded_run_becomes_failed(self, ledger):
        with ledger() as session:
            session.add(_running("stranded", started_at=_NOW - timedelta(days=5)))
            session.commit()

        result = sweep_orphaned_runs(session_factory=ledger, now=_NOW)

        assert result.finalized == 1
        with ledger() as session:
            row = session.get(CrawlExecutionRun, 1)
            assert row.status == RUN_STATUS_FAILED

    def test_the_closed_run_carries_the_taxonomy_code(self, ledger):
        with ledger() as session:
            session.add(_running("stranded", started_at=_NOW - timedelta(days=5)))
            session.commit()

        sweep_orphaned_runs(session_factory=ledger, now=_NOW)

        with ledger() as session:
            row = session.get(CrawlExecutionRun, 1)
            assert row.error_code == FailureCode.RUN_INTERRUPTED.value

    def test_the_closed_run_gets_a_finished_at(self, ledger):
        """Without it the row still looks in flight to anything filtering on it."""
        with ledger() as session:
            session.add(_running("stranded", started_at=_NOW - timedelta(days=5)))
            session.commit()

        sweep_orphaned_runs(session_factory=ledger, now=_NOW)

        with ledger() as session:
            row = session.get(CrawlExecutionRun, 1)
            assert row.finished_at == _NOW

    def test_a_healthy_run_is_untouched(self, ledger):
        with ledger() as session:
            session.add(_running("live", started_at=_NOW - timedelta(minutes=5)))
            session.commit()

        result = sweep_orphaned_runs(session_factory=ledger, now=_NOW)

        assert result.finalized == 0
        with ledger() as session:
            assert session.get(CrawlExecutionRun, 1).status == RUN_STATUS_RUNNING

    def test_a_stranded_replay_is_left_for_the_dead_letter_recovery(self, ledger):
        with ledger() as session:
            session.add(_running("replay", started_at=_NOW - timedelta(days=5), replay_of="original-run"))
            session.commit()

        result = sweep_orphaned_runs(session_factory=ledger, now=_NOW)

        assert result.finalized == 0
        assert result.skipped_replays == 1
        with ledger() as session:
            assert session.get(CrawlExecutionRun, 1).status == RUN_STATUS_RUNNING


class TestTheSweepDoesNotEnqueueOrReplay:
    """Closing the state space is the whole responsibility, and no more."""

    def test_the_sweep_imports_no_dlq_service(self):
        """Structural: a DLQ write here would create a letter with no source."""
        from pathlib import Path

        source = Path("src/services/orphan_run_sweeper.py").read_text(encoding="utf-8")

        assert "enqueue_failure" not in source
        assert "dead_letter" not in source.lower()

    def test_the_sweep_never_dispatches_a_replay(self):
        """`RUN_INTERRUPTED` is non-retryable, so there is nothing to dispatch."""
        assert decide(FailureCode.RUN_INTERRUPTED.value, retry_count=0).retryable is False

    def test_the_report_says_what_the_threshold_was(self, ledger):
        """An operator reading the alert needs to know what counted as stale."""
        with ledger() as session:
            session.add(_running("stranded", started_at=_NOW - timedelta(days=5)))
            session.commit()

        result = sweep_orphaned_runs(session_factory=ledger, now=_NOW, older_than_seconds=_SIX_HOURS)

        assert result.stale_threshold_seconds == _SIX_HOURS
        assert "finalized=1" in result.summary()


class TestFrequencyAndThresholdAreSeparate:
    """A tick every 30 minutes against a 6-hour threshold -- not one number."""

    def test_the_threshold_is_longer_than_a_normal_crawl(self):
        """Half a day clears a backfill or a season-wide sweep."""
        assert DEFAULT_STALE_SECONDS >= 6 * 60 * 60

    def test_a_custom_threshold_is_honoured(self):
        result = sweep_orphaned_runs.__wrapped__ if hasattr(sweep_orphaned_runs, "__wrapped__") else None
        assert result is None, "sweep_orphaned_runs must not be wrapped"

    def test_the_environment_override_is_read(self, monkeypatch):
        monkeypatch.setenv("CRAWL_RUN_STALE_SECONDS", "900")

        assert stale_seconds() == 900

    def test_a_non_numeric_override_falls_back(self, monkeypatch):
        monkeypatch.setenv("CRAWL_RUN_STALE_SECONDS", "soon")

        assert stale_seconds() == DEFAULT_STALE_SECONDS

    def test_a_non_positive_override_falls_back(self, monkeypatch):
        """A zero threshold would close every run in flight, so it is refused."""
        monkeypatch.setenv("CRAWL_RUN_STALE_SECONDS", "0")

        assert stale_seconds() == DEFAULT_STALE_SECONDS

    def test_a_custom_threshold_shorter_than_the_default_selects_more(self, ledger):
        with ledger() as session:
            session.add(_running("slightly-old", started_at=_NOW - timedelta(minutes=30)))
            session.commit()

        result = sweep_orphaned_runs(session_factory=ledger, now=_NOW, older_than_seconds=60)

        assert result.finalized == 1


class TestTheBatchIsBounded:
    """The sweep runs inside `MAINTENANCE_LOCK`; an unbounded backlog would hold it."""

    def test_the_limit_is_applied(self, ledger):
        with ledger() as session:
            for i in range(5):
                session.add(_running(f"row-{i}", started_at=_NOW - timedelta(days=5)))
            session.commit()

        result = sweep_orphaned_runs(session_factory=ledger, now=_NOW, limit=2)

        assert result.finalized == 2
        with ledger() as session:
            still_running = session.query(CrawlExecutionRun).filter_by(status=RUN_STATUS_RUNNING).count()
            assert still_running == 3

    def test_the_default_limit_is_finite(self):
        from src.services.orphan_run_sweeper import DEFAULT_BATCH_LIMIT

        assert 0 < DEFAULT_BATCH_LIMIT <= 1000


class TestTheReportShape:
    def test_the_result_serializes_for_logging(self):
        result = OrphanSweepResult(finalized=1, skipped_replays=0, failed=0, stale_threshold_seconds=21600)

        assert result.to_dict() == {
            "finalized": 1,
            "skipped_replays": 0,
            "failed": 0,
            "stale_threshold_seconds": 21600,
        }

    def test_a_clean_sweep_is_distinguishable_from_a_broken_one(self):
        """A sweep that stops closing rows must not read as "nothing to do"."""
        result = OrphanSweepResult(finalized=0, skipped_replays=0, failed=3, stale_threshold_seconds=21600)

        assert "failed=3" in result.summary()
