"""Contract for the per-job cadence the scheduler publishes for the staleness rule.

`KBOSchedulerJobNotRunning` thresholds an age against
``1.5 * kbo_scheduler_job_expected_interval_seconds``. That indirection is what
lets one rule serve an every-ten-minute job and a monthly one without an
exemption list, and it has two failure modes that no fixture can see:

    * the cadence is derived wrongly -- a job that only runs between 10:00 and
      23:00 has a ten-minute tick *and* an eleven-hour quiet stretch, and a
      derivation that reports only the first would page every night;
    * the cadence is never published -- then the right-hand side of the
      comparison matches nothing, the rule stays permanently silent for that job,
      and the job is unmonitored while every test still passes.

The first is pinned by the derivation cases below, the second by the publication
ones. `tests/monitoring/test_scheduler_alert_rules_contract.py` covers the rule
side of the same join.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from apscheduler.events import EVENT_JOB_SUBMITTED, JobEvent
from apscheduler.triggers.cron import CronTrigger
from prometheus_client import REGISTRY

from src.scheduler.cadence import MONTH_SECONDS, WEEK_SECONDS, expected_interval_seconds
from src.scheduler.metrics import job_lifecycle_listener
from src.scheduler.registry import _publish_job_cadence_contract, job_specs

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_SOURCE = ROOT / "src" / "scheduler" / "registry.py"

LAST_RUN = "kbo_scheduler_job_last_run_timestamp"
CADENCE = "kbo_scheduler_job_expected_interval_seconds"

_MINUTE = 60
_HOUR = 3600
_DAY = 86400


def _published(name: str, job_id: str) -> float | None:
    """Return a published gauge sample, or ``None`` when the series does not exist."""
    return REGISTRY.get_sample_value(name, {"job_id": job_id})


class _FakeScheduler:
    """Stands in for a ``BlockingScheduler`` with jobs already registered."""

    def __init__(self, jobs: list[object]) -> None:
        self._jobs = jobs

    def get_jobs(self) -> list[object]:
        return self._jobs


def _job(job_id: str, trigger: object) -> SimpleNamespace:
    return SimpleNamespace(id=job_id, trigger=trigger)


def _field(name: str, expression: str) -> object:
    """Return a stand-in for one APScheduler cron field whose text is ``expression``."""

    class _Field:
        def __init__(self) -> None:
            self.name = name

        def __str__(self) -> str:
            return expression

    return _Field()


def _trigger(*fields: tuple[str, str]) -> SimpleNamespace:
    return SimpleNamespace(fields=[_field(name, expression) for name, expression in fields])


class TestTheDerivationCountsTheQuietStretch:
    """A cadence is the longest gap, not the average one.

    Every case here is a schedule that ticks often for part of the day and not at
    all for the rest. Reporting the tick interval would alert on a job that is
    behaving exactly as scheduled, which is how a rule gets muted and stays
    muted.
    """

    def test_a_daily_job_reports_a_day(self):
        assert expected_interval_seconds(CronTrigger(hour=3, minute=0)) == _DAY

    def test_a_fifteen_minute_job_reports_fifteen_minutes(self):
        assert expected_interval_seconds(CronTrigger(minute="*/15")) == 15 * _MINUTE

    def test_a_business_hours_job_includes_the_hours_it_is_closed(self):
        """11 hours closed + a 15-minute tick, not 15 minutes."""
        interval = expected_interval_seconds(CronTrigger(hour="10-23", minute="*/15"))

        assert interval == 10 * _HOUR + 15 * _MINUTE, interval

    def test_an_overnight_job_includes_the_whole_day_it_skips(self):
        """23:00-23:30 every ten seconds is 23 hours of nothing, then a burst."""
        interval = expected_interval_seconds(CronTrigger(hour=23, minute="0-30", second="*/10"))

        assert interval > 23 * _HOUR, interval

    def test_a_weekly_job_reports_a_week(self):
        assert expected_interval_seconds(CronTrigger(day_of_week="sun", hour=5, minute=30)) == WEEK_SECONDS

    def test_a_monthly_job_reports_a_month(self):
        assert expected_interval_seconds(CronTrigger(day="1", hour=2, minute=0)) == MONTH_SECONDS


class TestAnUnreadableTriggerReportsNoCadence:
    """``0`` is "unknown", and unknown must not become the tightest window.

    The alert rule compares against ``1.5 * <cadence>``, so a cadence of ``0``
    would make ``time() - last_run > 0`` true on every evaluation -- an alert
    that fires all day for a job nobody understands. The publication side skips
    a zero cadence for the same reason, and the cost of that choice (the job is
    unmonitored) is why the coverage test below exists.
    """

    def test_a_missing_trigger_has_no_cadence(self):
        assert expected_interval_seconds(None) == 0

    def test_an_object_without_cron_fields_has_no_cadence(self):
        assert expected_interval_seconds(SimpleNamespace()) == 0

    def test_an_unparseable_field_has_no_cadence(self):
        """A field the derivation cannot read is unknown, not zero-length.

        Returning a small positive number here would be worse than returning
        nothing: it would be indistinguishable from a real cadence and would
        produce a window that fires immediately.
        """
        assert expected_interval_seconds(_trigger(("hour", "not-a-range"), ("minute", "0"), ("second", "0"))) == 0


class TestEveryRegisteredJobGetsACadence:
    def test_no_registered_job_derives_a_zero_cadence(self):
        """A job the derivation cannot read is a job the staleness rule cannot watch."""
        unreadable = [
            job_id for _, trigger, job_id, _, _ in job_specs(CronTrigger) if expected_interval_seconds(trigger) <= 0
        ]

        assert not unreadable, (
            f"these registered jobs have no derivable cadence, so KBOSchedulerJobNotRunning "
            f"can never fire for them: {unreadable}"
        )

    def test_the_cadences_are_not_all_the_same(self):
        """A derivation that collapsed to one value would look complete and be useless.

        This is the check that a constant window would fail while every fixture
        still passed: some jobs are minutes apart, some are a month apart.
        """
        intervals = {expected_interval_seconds(trigger) for _, trigger, _, _, _ in job_specs(CronTrigger)}

        assert len(intervals) >= 3, sorted(intervals)
        assert max(intervals) >= 28 * _DAY, sorted(intervals)
        assert min(intervals) <= _HOUR, sorted(intervals)


class TestTheCadenceIsActuallyPublished:
    def test_every_job_the_scheduler_holds_is_published(self):
        """Registration is the only moment the schedule is known, so it is where this happens."""
        jobs = [
            _job("_test_cadence_daily", CronTrigger(hour=3, minute=0)),
            _job("_test_cadence_frequent", CronTrigger(minute="*/15")),
            _job("_test_cadence_monthly", CronTrigger(day="1", hour=2, minute=0)),
        ]

        _publish_job_cadence_contract(_FakeScheduler(jobs))

        for job in jobs:
            assert _published(LAST_RUN, job.id) is not None, f"{job.id} has no last-run series"
            assert _published(CADENCE, job.id) == expected_interval_seconds(job.trigger), job.id

    def test_a_job_with_no_derivable_cadence_gets_no_cadence_series(self):
        """Absent beats wrong: the rule ignores a job whose cadence is unknown.

        Publishing ``0`` would make the comparison true immediately, so the job
        would alert forever rather than never. Both are unmonitored; only one of
        them wakes somebody up every day.
        """
        job = _job("_test_cadence_unreadable", SimpleNamespace())

        _publish_job_cadence_contract(_FakeScheduler([job]))

        assert _published(LAST_RUN, job.id) is not None
        assert _published(CADENCE, job.id) is None

    def test_a_scheduler_that_cannot_list_jobs_is_not_fatal(self):
        """The publication runs during startup; a missing accessor must not stop it.

        Getting this wrong would take the whole scheduler down for a monitoring
        gauge, which is a strictly worse failure than an unmonitored job.
        """
        _publish_job_cadence_contract(SimpleNamespace())


class TestASubmissionIsWhatStampsTheGauge:
    """The behaviour, not the source text: a submitted job updates the age gauge.

    This is the pair to the source check below. The source check pins *where* the
    stamp happens (before the job body, so a job the fail-fast gate skips still
    counts as reached); this pins that it happens at all, on a real event object
    rather than on the string of an event name.
    """

    def test_a_submission_stamps_the_last_run_gauge(self):
        job_id = "_test_cadence_submitted"
        assert _published(LAST_RUN, job_id) is None

        job_lifecycle_listener(JobEvent(EVENT_JOB_SUBMITTED, job_id, "default"))

        stamped = _published(LAST_RUN, job_id)

        assert stamped is not None, "a submitted job left no last-run timestamp, so the staleness rule cannot see it"
        assert stamped > 0, stamped

    def test_a_later_submission_advances_the_timestamp(self):
        """A gauge that only ever gets created once would report a fixed age.

        The rule reads the *age*, so a stamp that never moves is indistinguishable
        from a job that stopped -- the alert would fire once and never resolve.
        """
        job_id = "_test_cadence_advanced"

        job_lifecycle_listener(JobEvent(EVENT_JOB_SUBMITTED, job_id, "default"))
        first = _published(LAST_RUN, job_id)
        job_lifecycle_listener(JobEvent(EVENT_JOB_SUBMITTED, job_id, "default"))
        second = _published(LAST_RUN, job_id)

        assert first is not None
        assert second is not None
        assert second >= first


class TestThePublicationRunsBeforeTheScheduleStarts:
    """Publishing after ``start()`` would leave the first minutes unwatched.

    More importantly, the call has to be *in* the startup path at all: a
    derivation that is correct, tested and never invoked is the failure this
    whole area was in -- a rule that reads a gauge nobody writes.
    """

    def test_the_startup_path_publishes_and_does_so_before_starting(self):
        body = REGISTRY_SOURCE.read_text(encoding="utf-8").split("def _start_scheduler", 1)[1]

        assert "_publish_job_cadence_contract(scheduler)" in body, (
            "_start_scheduler never publishes the cadence gauges, so the staleness rule is inert"
        )
        assert body.index("_publish_job_cadence_contract(scheduler)") < body.index("scheduler.start()")

    def test_the_listener_records_a_submission_as_a_run(self):
        """The last-run gauge is the other half of the rule, set before the job body runs.

        Recording on execution instead would leave a job that the database
        fail-fast gate skipped looking identical to one that never fired -- and
        the gate returns quietly by design, so this is the only trace it leaves.

        The listener body is sliced out before the assertion because the event
        constant is also imported at module scope: searching the whole file would
        pass on the import line alone, and that is the shape of check that keeps
        passing after the behaviour is gone.
        """
        source = (ROOT / "src" / "scheduler" / "metrics.py").read_text(encoding="utf-8")
        listener = source.split("def job_lifecycle_listener", 1)[1].split("\ndef ", 1)[0]
        submitted = listener.split("EVENT_JOB_SUBMITTED", 1)[1]

        assert "KBO_SCHEDULER_JOB_LAST_RUN_TIMESTAMP" in submitted


@pytest.mark.parametrize(("expression", "expected"), [("*/5", 5), ("10-23", 14), ("0", 1), ("1,2,3", 3)])
def test_the_value_counter_matches_the_expression(expression: str, expected: int):
    """The derivation counts values from cron text, so the counting is pinned directly.

    ``10-23`` is fourteen hours, not eleven, and a runner that reads it as a
    count of *parts* would produce a window eleven times too wide.
    """
    trigger = _trigger(("hour", expression), ("minute", "0"), ("second", "0"))

    quiet = (24 - expected) * _HOUR
    assert expected_interval_seconds(trigger) == quiet + _HOUR
