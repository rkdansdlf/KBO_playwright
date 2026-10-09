"""The queue's condition and the check's own health are different facts.

`crawl_dead_letter_recovery` reports through `alert_warning`, which keys on the
job name. That put two unrelated questions under one incident key: *did the check
work* (scheduler's business) and *is the queue in a bad state* (the domain's).
They need different responses, and merging them cost real information in both
directions -- one noisy tick overwrote whatever the queue was trying to say, and
an exhausted letter, which only an operator can clear, rode a warning key that
the next healthy tick resolved on its own.

So state moves to the ``dlq:`` namespace, and the tests here pin that separation
rather than merely the presence of the new keys.

The heartbeat tests cover the other half. A healthy queue produces no events,
and `apply_incidents` with no events and no resolve keys returns immediately --
so "the sweep ran and found nothing" and "nobody looked" were the same outcome.
An unreadable queue must therefore never resolve anything, and these tests fix
that at the one place where it was previously indistinguishable.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from src.services import dlq_incidents
from src.services.crawl_dead_letter_stats import (
    DEFAULT_STALE_RETRYING_SECONDS,
    DlqStats,
    publish_dlq_state_metrics,
    stale_retry_seconds,
)
from src.services.dlq_incidents import (
    DLQ_INCIDENT_BACKLOG,
    DLQ_INCIDENT_EXHAUSTED,
    DLQ_INCIDENT_KEYS,
    DLQ_INCIDENT_STRANDED,
    apply_dlq_incidents,
    dlq_incident_events,
)
from src.utils.metrics import (
    KBO_DLQ_LAST_SUCCESSFUL_SWEEP_TIMESTAMP,
    KBO_DLQ_SWEEP_FAILURES_TOTAL,
)

_NOW = datetime(2026, 10, 9, 3, 0, 0)


def _stats(**overrides: object) -> DlqStats:
    """Build a healthy reading unless a field is named."""
    fields: dict[str, object] = {
        "pending": 0,
        "due": 0,
        "retrying": 0,
        "stale_retrying": 0,
        "resolved": 0,
        "exhausted": 0,
        "ignored": 0,
        "oldest_due_at": None,
        "oldest_due_age_seconds": 0.0,
        "by_status_crawler": {},
    }
    fields.update(overrides)
    return DlqStats(**fields)  # type: ignore[arg-type]


class TestTheNamespaceIsSeparate:
    """The whole point: state and check health must not share a key."""

    def test_every_state_key_is_namespaced(self):
        for key in DLQ_INCIDENT_KEYS:
            assert key.startswith("dlq:"), key

    def test_no_state_key_collides_with_the_scheduler_check(self):
        """A collision here is the conflation, spelled out.

        `scheduler:crawl_dead_letter_recovery:warning` is what the check opens
        when it misbehaves. If a queue-state key ever landed there, the two
        would overwrite each other again.
        """
        scheduler_check_keys = {
            "scheduler:crawl_dead_letter_recovery:warning",
            "scheduler:crawl_dead_letter_recovery:success",
            "scheduler:crawl_dead_letter_recovery:failure",
        }

        assert not set(DLQ_INCIDENT_KEYS) & scheduler_check_keys

    def test_the_keys_are_distinct_from_each_other(self):
        """One condition per key, or reconciling one closes another."""
        assert len(set(DLQ_INCIDENT_KEYS)) == len(DLQ_INCIDENT_KEYS)

    def test_the_keys_are_stable_across_a_message_rewrite(self):
        """`incident_key` is semantic identity, so it must not embed a count."""
        stats = _stats(stale_retrying=1)
        first = dlq_incident_events(stats, stale_retry_threshold=1800)
        later = dlq_incident_events(_stats(stale_retrying=7), stale_retry_threshold=1800)

        assert [e.incident_key for e in first] == [e.incident_key for e in later] == [DLQ_INCIDENT_STRANDED]


class TestConditionsBecomeIncidents:
    def test_a_healthy_queue_opens_nothing(self):
        """The baseline. Everything else is a deviation from this."""
        assert dlq_incident_events(_stats(), stale_retry_threshold=1800) == []

    def test_a_stranded_letter_is_reported(self):
        events = dlq_incident_events(_stats(stale_retrying=3), stale_retry_threshold=1800)

        assert [e.incident_key for e in events] == [DLQ_INCIDENT_STRANDED]

    def test_an_exhausted_letter_is_reported(self):
        events = dlq_incident_events(_stats(exhausted=2), stale_retry_threshold=1800)

        assert [e.incident_key for e in events] == [DLQ_INCIDENT_EXHAUSTED]

    def test_an_ageing_backlog_is_reported(self):
        events = dlq_incident_events(_stats(due=5, oldest_due_age_seconds=90000.0), stale_retry_threshold=1800)

        assert [e.incident_key for e in events] == [DLQ_INCIDENT_BACKLOG]

    def test_a_backlog_below_the_threshold_is_not(self):
        """Otherwise a letter a day old pages, and the threshold means nothing."""
        just_under = dlq_incident_events(
            _stats(due=5, oldest_due_age_seconds=86399.0),
            stale_retry_threshold=1800,
        )

        assert just_under == []

    def test_conditions_are_reported_independently(self):
        """One broken condition must not hide another."""
        events = dlq_incident_events(
            _stats(stale_retrying=1, exhausted=1, due=2, oldest_due_age_seconds=90000.0),
            stale_retry_threshold=1800,
        )

        assert {e.incident_key for e in events} == set(DLQ_INCIDENT_KEYS)

    def test_an_exhausted_letter_is_an_error_not_a_warning(self):
        """It does not clear itself, so a warning understates it.

        The stranded and backlog conditions both resolve on their own once the
        system recovers. An exhausted letter cannot: the retry budget is spent
        and only `dlq retry` or `dlq ignore` changes it.
        """
        from src.notifications.alert_dto import AlertSeverity

        events = dlq_incident_events(_stats(exhausted=1), stale_retry_threshold=1800)

        assert events[0].severity is AlertSeverity.ERROR


class TestTheMessageMatchesTheReading:
    """An incident that misreports its own threshold is worse than none."""

    def test_the_staleness_threshold_in_the_message_is_the_one_used(self):
        events = dlq_incident_events(_stats(stale_retrying=1), stale_retry_threshold=5400)

        assert events[0].metadata["stale_threshold_seconds"] == 5400
        assert "5400초" in events[0].message

    def test_the_default_threshold_is_the_documented_one(self, monkeypatch):
        monkeypatch.delenv("DLQ_STALE_RETRYING_SECONDS", raising=False)

        assert stale_retry_seconds() == DEFAULT_STALE_RETRYING_SECONDS

    def test_the_threshold_override_is_honoured(self, monkeypatch):
        monkeypatch.setenv("DLQ_STALE_RETRYING_SECONDS", "5400")

        assert stale_retry_seconds() == 5400

    def test_a_nonsense_threshold_falls_back_rather_than_reporting_zero(self, monkeypatch):
        """Reporting "stuck for 0s" would read as a fresh, healthy reading."""
        monkeypatch.setenv("DLQ_STALE_RETRYING_SECONDS", "soon")

        assert stale_retry_seconds() == DEFAULT_STALE_RETRYING_SECONDS

    def test_every_incident_names_its_recovery_command(self):
        """An incident that cannot be acted on is a complaint."""
        events = dlq_incident_events(
            _stats(stale_retrying=1, exhausted=1, due=1, oldest_due_age_seconds=90000.0),
            stale_retry_threshold=1800,
        )

        for event in events:
            assert event.remediation, event.incident_key
            assert any("kbo dlq" in step for step in event.remediation), event.incident_key


class TestPublishingReconcilesTheNamespace:
    """A recovered queue must clear its own incident without an operator."""

    def test_a_healthy_reading_reconciles_rather_than_staying_silent(self, monkeypatch):
        captured: dict[str, object] = {}

        def fake_apply(events, **kwargs):
            captured["events"] = list(events)
            captured.update(kwargs)

        monkeypatch.setattr(dlq_incidents, "apply_incidents", fake_apply)

        apply_dlq_incidents(_stats(), stale_retry_threshold=1800)

        assert captured["events"] == []
        assert captured["reconcile_prefix"] == "dlq:"

    def test_reconciliation_covers_keys_the_module_no_longer_produces(self, monkeypatch):
        """A retired key is still closed by a healthy reading.

        Listing resolved keys by hand would leave a key that a later version
        stopped emitting open forever, reachable only by the manual command.
        """
        monkeypatch.setattr(dlq_incidents, "apply_incidents", lambda events, **kwargs: None)
        events = apply_dlq_incidents(_stats(stale_retrying=1), stale_retry_threshold=1800)

        assert dlq_incidents.resolved_dlq_keys(events) == [DLQ_INCIDENT_BACKLOG, DLQ_INCIDENT_EXHAUSTED]

    def test_a_notify_failure_does_not_break_the_job(self, monkeypatch):
        """The reading succeeded; reporting it must not undo that."""

        def boom(*_args, **_kwargs):
            raise RuntimeError("ledger unavailable")

        monkeypatch.setattr(dlq_incidents, "apply_incidents", boom)

        events = apply_dlq_incidents(_stats(exhausted=1), stale_retry_threshold=1800)

        assert [e.incident_key for e in events] == [DLQ_INCIDENT_EXHAUSTED]


class TestTheSweepHeartbeat:
    """`DLQ = 0` means two different things, and only the heartbeat separates them.

    A sweep that cannot reach the database leaves the previous gauges in place
    rather than zeroing them, so an empty queue and an unreadable one read
    identically. That is the ambiguity `kbo_crawl_dlq_last_successful_sweep_timestamp`
    exists to remove, and it only works if a failed read never records a success.
    """

    def test_a_successful_read_stamps_the_heartbeat(self, monkeypatch):
        """The positive case the other three contrast against."""
        stamped: list[float] = []
        monkeypatch.setattr(
            "src.services.crawl_dead_letter_stats.collect_dlq_stats",
            lambda **_kwargs: _stats(),
        )
        monkeypatch.setattr(
            "src.services.crawl_dead_letter_stats.record_dlq_sweep_succeeded",
            _recorder(stamped),
        )

        publish_dlq_state_metrics(now=_NOW.replace(tzinfo=UTC))

        assert stamped == [_NOW.replace(tzinfo=UTC).timestamp()]

    def test_a_failed_read_raises_so_the_caller_cannot_mistake_it(self):
        """Swallowing would let a failed read resolve incidents it never checked."""
        with pytest.raises(Exception):  # noqa: B017 - the driver error type varies
            publish_dlq_state_metrics(now=_NOW, session_factory=_failing_factory("unreachable"))

    def test_a_failed_read_does_not_stamp_the_heartbeat(self, monkeypatch):
        """The invariant. A fresh timestamp after a failure says "verified empty"."""
        stamped: list[float] = []
        monkeypatch.setattr(
            "src.services.crawl_dead_letter_stats.record_dlq_sweep_succeeded",
            _recorder(stamped),
        )

        with pytest.raises(Exception):  # noqa: B017 - the driver error type varies
            publish_dlq_state_metrics(now=_NOW, session_factory=_failing_factory("unreachable"))

        assert stamped == [], "a sweep that could not read the queue must not stamp a success"

    def test_a_failed_read_counts_a_failure(self, monkeypatch):
        counted: list[int] = []
        monkeypatch.setattr(
            "src.services.crawl_dead_letter_stats.record_dlq_sweep_failed",
            lambda: counted.append(1),
        )

        with pytest.raises(Exception):  # noqa: B017 - the driver error type varies
            publish_dlq_state_metrics(now=_NOW, session_factory=_failing_factory("unreachable"))

        assert counted == [1]


def _recorder(sink: list):
    """Return a stub that appends its one argument, so a call is observable."""

    def record(value):
        sink.append(value)

    return record


def _failing_factory(_reason: str):
    """Return a session factory whose every use raises, as an outage would.

    Mirrors how `collect_dlq_stats` opens a session -- `with factory() as
    session` -- so the failure happens at the same point a driver error would.
    """

    class _Failing:
        def __enter__(self):
            raise RuntimeError("database unreachable")

        def __exit__(self, *_exc):
            return False

    return _Failing()


class TestTheHeartbeatTimestampIsRealUtc:
    """A naive datetime read as local time is up to nine hours wrong.

    The gauge exists to say how stale a reading is, so a timezone error does not
    degrade the signal -- it inverts it, reporting a fresh sweep as nine hours
    old and hiding the stall entirely.
    """

    def test_a_naive_timestamp_is_read_as_utc(self, monkeypatch):
        captured: list[float] = []
        monkeypatch.setattr(
            "src.services.crawl_dead_letter_stats.record_dlq_sweep_succeeded",
            _recorder(captured),
        )
        monkeypatch.setattr(
            "src.services.crawl_dead_letter_stats.collect_dlq_stats",
            lambda **_kwargs: _stats(),
        )

        naive_utc = datetime(2026, 10, 9, 3, 0, 0)
        publish_dlq_state_metrics(now=naive_utc)

        expected = naive_utc.replace(tzinfo=UTC).timestamp()
        assert captured == [expected]

    def test_the_gauge_is_a_unix_timestamp(self):
        """Seconds, not a monotonic counter -- it has to compare against `time()`."""
        assert KBO_DLQ_LAST_SUCCESSFUL_SWEEP_TIMESTAMP._name.endswith("_timestamp")

    def test_the_failure_counter_is_separate_from_found_failures(self):
        """`kbo_crawl_dlq_failures_total` counts failures *in the queue*.

        This counts times the queue could not be read, which is the case that
        leaves every other gauge frozen and is therefore the least visible.

        Checked the way PromQL sees it: `prometheus_client` strips `_total` from
        a Counter's internal name and restores it on export, so asserting against
        `_name` would report a mismatch for a metric that scrapes correctly.
        The alert rule reads `kbo_crawl_dlq_sweep_failures_total`, and the
        promtool fixture for that rule is what proves the pair agrees.
        """
        internal = KBO_DLQ_SWEEP_FAILURES_TOTAL._name
        assert internal == "kbo_crawl_dlq_sweep_failures"
        assert f"{internal}_total" == "kbo_crawl_dlq_sweep_failures_total"
