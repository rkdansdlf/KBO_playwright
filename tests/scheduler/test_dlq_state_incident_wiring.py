"""The recovery job reports two separate facts under two separate keys.

`crawl_dead_letter_recovery` opens `scheduler:crawl_dead_letter_recovery:*` when
the *check* misbehaves, and `dlq:*` when the *queue* is in a bad state. Before
this separation both rode the job-name key, which lost information in both
directions: a noisy check overwrote whatever the queue was saying, and an
exhausted letter -- the one condition only an operator can clear -- rode a
warning that the next healthy tick resolved on its own.

These tests pin the wiring, because the derivation logic is already covered in
`tests/services/test_dlq_incidents.py`. What can break here is the job: calling
the derivation on a failed read, or dropping it, and neither shows up in the
service's own tests.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import patch

from src.scheduler.jobs import maintenance

if TYPE_CHECKING:
    from collections.abc import Iterator
    from unittest.mock import MagicMock

    from src.services.crawl_dead_letter_stats import DlqStats


class _NullLock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def _stats(**overrides: object) -> DlqStats:
    from src.services.crawl_dead_letter_stats import DlqStats

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


@contextmanager
def _job(reading: DlqStats | None) -> Iterator[MagicMock]:
    """Run the job against a queue reading, capturing the state-incident calls.

    ``None`` means the read failed, which is how `_refresh_dlq_metrics` reports
    an unreachable queue. Both alert channels and the DB touch are patched: the
    real `alert_success` opens a session, and the orphan sweep reaches for the
    run ledger, neither of which this test is about.
    """
    with (
        patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=[]),
        patch("src.scheduler.jobs.maintenance._refresh_dlq_metrics", return_value=reading),
        patch("src.scheduler.jobs.maintenance._sweep_orphaned_runs", return_value=_orphans()),
        patch("src.scheduler.jobs.maintenance.alert_warning"),
        patch("src.scheduler.jobs.maintenance.alert_success"),
        patch("src.services.dlq_incidents.apply_incidents") as apply_incidents,
    ):
        yield apply_incidents


def _orphans() -> SimpleNamespace:
    return SimpleNamespace(
        summary=lambda: "finalized=0, skipped_replays=0, failed=0",
        finalized=0,
        skipped_replays=0,
        failed=0,
        stale_threshold_seconds=21600,
    )


def _run(monkeypatch, reading: DlqStats | None) -> MagicMock:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with _job(reading) as apply_incidents:
        maintenance.crawl_dead_letter_recovery_job()
    return apply_incidents


class TestTheJobReportsQueueState:
    def test_a_bad_queue_opens_a_dlq_incident(self, monkeypatch) -> None:
        apply_incidents = _run(monkeypatch, _stats(stale_retrying=2))

        assert apply_incidents.call_count == 1
        events = apply_incidents.call_args.args[0]
        assert [e.incident_key for e in events] == ["dlq:stranded_retrying"]

    def test_state_is_reconciled_under_the_dlq_prefix(self, monkeypatch) -> None:
        """Namespace, not the whole ledger -- a scheduler incident is not ours."""
        apply_incidents = _run(monkeypatch, _stats(exhausted=1))

        assert apply_incidents.call_args.kwargs["reconcile_prefix"] == "dlq:"

    def test_a_healthy_queue_reconciles_too(self, monkeypatch) -> None:
        """The recovered case. Silence here would leave the incident open."""
        apply_incidents = _run(monkeypatch, _stats())

        assert apply_incidents.call_count == 1
        assert apply_incidents.call_args.args[0] == []


class TestAnUnreadableQueueIsNotAHealthyQueue:
    """The conflation this separation exists to prevent, at the job level.

    A failed read means the conditions were never evaluated. Resolving their
    incidents would report a recovery that did not happen, and a queue that
    cannot be read is emphatically not an empty one.
    """

    def test_a_failed_read_reconciles_nothing(self, monkeypatch) -> None:
        apply_incidents = _run(monkeypatch, None)

        apply_incidents.assert_not_called()

    def test_a_failed_read_still_reports_the_check_as_healthy(self, monkeypatch) -> None:
        """The check did run; it is the queue it could not read.

        This is the distinction the split exists for: the scheduler key says the
        job worked, and the missing `dlq:` update says the queue is unknown. A
        single key could not have said both.
        """
        monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
        with (
            patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=[]),
            patch("src.scheduler.jobs.maintenance._refresh_dlq_metrics", return_value=None),
            patch("src.scheduler.jobs.maintenance._sweep_orphaned_runs", return_value=_orphans()),
            patch("src.scheduler.jobs.maintenance.alert_success") as success,
            patch("src.services.dlq_incidents.apply_incidents") as apply_incidents,
        ):
            maintenance.crawl_dead_letter_recovery_job()

        success.assert_called_once()
        apply_incidents.assert_not_called()


class TestTheTwoChannelsAreIndependent:
    def test_a_bad_queue_does_not_warn_about_the_check(self, monkeypatch) -> None:
        """A queue full of letters is not a misbehaving scheduler.

        Keying both on the job name is what made an operator unable to tell
        which one they were looking at.
        """
        monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
        with (
            patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=[]),
            patch("src.scheduler.jobs.maintenance._refresh_dlq_metrics", return_value=_stats(exhausted=9)),
            patch("src.scheduler.jobs.maintenance._sweep_orphaned_runs", return_value=_orphans()),
            patch("src.scheduler.jobs.maintenance.alert_warning") as warn,
            patch("src.scheduler.jobs.maintenance.alert_success") as success,
            patch("src.services.dlq_incidents.apply_incidents") as apply_incidents,
        ):
            maintenance.crawl_dead_letter_recovery_job()

        apply_incidents.assert_called_once()
        warn.assert_not_called()
        success.assert_called_once()

    def test_a_failing_check_still_warns_about_itself(self, monkeypatch) -> None:
        """The coarse signal this job already owned is unchanged."""
        monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
        with (
            patch(
                "src.services.crawl_dead_letter_recovery.recover_stuck_retrying",
                return_value=[SimpleNamespace(action="failed")],
            ),
            patch("src.scheduler.jobs.maintenance._refresh_dlq_metrics", return_value=_stats()),
            patch("src.scheduler.jobs.maintenance._sweep_orphaned_runs", return_value=_orphans()),
            patch("src.scheduler.jobs.maintenance.alert_warning") as warn,
            patch("src.scheduler.jobs.maintenance.alert_success"),
            patch("src.services.dlq_incidents.apply_incidents") as apply_incidents,
        ):
            maintenance.crawl_dead_letter_recovery_job()

        warn.assert_called_once()
        apply_incidents.assert_called_once()
