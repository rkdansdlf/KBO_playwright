"""Contract for `kbo_scheduler_job_consecutive_skips`.

The two pre-flight gates return quietly -- deliberately, so a job does not hold
`MAINTENANCE_LOCK` while a dead database burns connect timeouts -- and that
decision has a cost that is easy to miss: **a skipped run is reported to
APScheduler as a success.** The heartbeat is stamped at submission, so
`KBOSchedulerJobNotRunning` cannot page either. The job looks healthy by every
signal except the work that did not happen, and this gauge is that signal.

Two failure modes, neither of which the rule fixture can see:

    * the streak is recorded under the function's `__name__` instead of the
      registered job id. The two differ (`crawl_daily_games_job` is registered as
      `crawl_daily_games`), so the skip would land on a label no other scheduler
      metric carries and the rule would name a job an operator cannot join to its
      heartbeat or its failure counter;
    * the flag that carries "this run was skipped" from the gate to the listener
      is set but never consumed, or is consumed by the wrong run -- either way the
      streak stops meaning "the last attempt did no work".

The first is pinned by driving a real gate and asserting on the label the
completion event carries; the second by running the listener over both outcomes,
in the order the scheduler produces them.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_SUBMITTED, JobEvent

import src.scheduler.locks as locks
from src.scheduler.metrics import job_lifecycle_listener
from src.utils.metrics import KBO_SCHEDULER_JOB_CONSECUTIVE_SKIPS


@pytest.fixture(autouse=True)
def _no_incident_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every completion path resolves a failure incident, which opens a session."""
    monkeypatch.setattr("src.scheduler.metrics.apply_incidents", lambda *_a, **_k: None)


def _streak(job_id: str) -> float | None:
    for metric in KBO_SCHEDULER_JOB_CONSECUTIVE_SKIPS.collect():
        for sample in metric.samples:
            if sample.labels.get("job_id") == job_id:
                return sample.value
    return None


def _sample_ids() -> set[str]:
    return {
        sample.labels["job_id"]
        for metric in KBO_SCHEDULER_JOB_CONSECUTIVE_SKIPS.collect()
        for sample in metric.samples
        if "job_id" in sample.labels
    }


def _submitted(job_id: str) -> None:
    """The heartbeat the listener stamps before either gate runs."""
    job_lifecycle_listener(JobEvent(EVENT_JOB_SUBMITTED, job_id, "default"))


def _finished(job_id: str) -> None:
    """The completion APScheduler reports for a job function that returned."""
    job_lifecycle_listener(JobEvent(EVENT_JOB_EXECUTED, job_id, "default"))


@pytest.fixture
def finished_run(monkeypatch: pytest.MonkeyPatch):
    """Return a callable that drives one run to completion through the DB gate.

    ``database_up=False`` is a skip -- the probe body must not run -- and ``True``
    is a run that executed. Both go through the real gate and the real listener,
    because the contract is the pair of them: the gate decides and says so
    through the thread-local flag, the listener reads it and names the series.
    """

    def run(job_id: str, *, database_up: bool) -> list[str]:
        # Start from a clean flag: another test (or the gate's own earlier skip)
        # may have left one set on this thread, and inheriting it would make the
        # assertion below pass for a reason this test did not create.
        locks.consume_gate_skip()
        monkeypatch.setattr(locks, "_db_gate", lambda urls=None: database_up)
        ran: list[str] = []

        @locks._with_db_fail_fast_guard
        def probe() -> None:
            if not database_up:
                raise AssertionError("the gate let the job through with the database unreachable")
            ran.append("ran")

        _submitted(job_id)
        probe()
        _finished(job_id)
        return ran

    return run


class TestASkipLandsOnTheRegisteredJobId:
    def test_the_db_gate_records_under_the_event_id_not_the_function_name(self, finished_run) -> None:
        """The label must be the id every other scheduler series uses.

        Recording under the function name would put the streak on a series that
        `kbo_scheduler_job_last_run_timestamp` and `kbo_scheduler_job_total` never
        carry, so an operator reading the alert could not find the job's
        heartbeat, its failure count, or its cadence.
        """
        job_id = "_test_skip_db_gate"

        assert finished_run(job_id, database_up=False) == []

        assert _streak(job_id) == 1.0
        assert "probe" not in _sample_ids(), "the streak landed on the function name instead of the job id"

    def test_the_lock_gate_records_under_the_event_id(self) -> None:
        @locks._with_lock_skip_guard
        def probe() -> None:
            raise locks._LockSkipped(locks.LOCK_SKIP_TIER)

        job_id = "_test_skip_lock_gate"

        _submitted(job_id)
        assert probe() is None
        _finished(job_id)

        assert _streak(job_id) == 1.0
        assert "probe" not in _sample_ids()


class TestTheStreakFollowsTheLastAttempt:
    def test_two_skips_in_a_row_extend_the_streak(self, finished_run) -> None:
        """The rule reads `> 0`, but the count is what says "still not recovering"."""
        job_id = "_test_skip_twice"

        finished_run(job_id, database_up=False)
        finished_run(job_id, database_up=False)

        assert _streak(job_id) == 2.0

    def test_a_run_that_executes_resets_the_streak(self, finished_run) -> None:
        """One tick of contention must clear itself, or the alert never resolves."""
        job_id = "_test_skip_then_run"

        finished_run(job_id, database_up=False)
        assert _streak(job_id) == 1.0

        assert finished_run(job_id, database_up=True) == ["ran"]
        assert _streak(job_id) == 0.0

    def test_a_run_that_fails_still_resets_the_streak(self, finished_run) -> None:
        """A failure is work attempted, not a skip.

        Leaving the streak up would page "skipped" at a job whose failure
        `KBOSchedulerJobFailed` already reports, and the two alerts would be
        talking about the same run.
        """
        job_id = "_test_skip_then_fail"

        finished_run(job_id, database_up=False)
        assert _streak(job_id) == 1.0

        _submitted(job_id)
        job_lifecycle_listener(SimpleNamespace(code=EVENT_JOB_ERROR, job_id=job_id, exception=ValueError("boom")))

        assert _streak(job_id) == 0.0


class TestTheFlagDoesNotLeakBetweenRuns:
    def test_another_job_is_unaffected(self, finished_run) -> None:
        """Interleaved jobs: only the skipped one carries a streak."""
        skipped_id, other_id = "_test_leak_skipped", "_test_leak_other"

        finished_run(skipped_id, database_up=False)
        finished_run(other_id, database_up=True)

        assert _streak(skipped_id) == 1.0
        assert _streak(other_id) == 0.0

    def test_the_flag_is_thread_local(self) -> None:
        """One run stays on one executor thread from submission to completion.

        A module global would be consumed by whichever job finished first, so a
        job skipped while another job completed would have its streak reset
        without ever running. The thread that reads must be the thread that set.
        """
        locks.note_gate_skip()

        seen: list[bool] = []
        thread = threading.Thread(target=lambda: seen.append(locks.consume_gate_skip()))
        thread.start()
        thread.join()

        assert seen == [False]
        assert locks.consume_gate_skip() is True
