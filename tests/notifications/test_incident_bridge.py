"""A swallowed incident-write failure has to leave a count behind.

`apply_incidents` exists so a check result survives alerting breaking, and it does
that by catching and logging persistence errors. The same property is what makes
the failure invisible: the caller is told nothing, so the check logs, returns
normally and reports the same verdict next run, while the incident it meant to
open or recover never existed.

Nothing inside the process can watch for this, because the ledger that would
carry the incident is the thing that failed. That is why the count has to exist
at all, and why the count is per call rather than per event: the call owns one
transaction, so a batch that fails partway is one lost batch and its length says
nothing about how much was lost.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from src.notifications import bridge
from src.utils import metrics

if TYPE_CHECKING:
    from collections.abc import Iterator


def _counter_value(counter: object) -> float:
    """Read one counter's current value.

    `prometheus_client` keeps the sample in `_value`; reading it directly is what
    lets a test assert an exact increment rather than a scrapes-and-regexes
    approximation.
    """
    return float(counter._value.get())


@pytest.fixture
def counters() -> Iterator[dict[str, float]]:
    """Snapshot the notification counters and restore them afterwards.

    The counters are process-global, so a test that increments one without
    resetting it makes every later assertion in the suite a delta comparison
    against an unknown baseline.
    """
    names = (
        metrics.KBO_NOTIFICATION_INCIDENT_APPLY_FAILURES_TOTAL,
        metrics.KBO_NOTIFICATION_DELIVERY_AUDIT_FAILURES_TOTAL,
    )
    before = {counter._name: _counter_value(counter) for counter in names}

    yield before

    for counter in names:
        counter._value.set(before[counter._name])


def _failing_session() -> object:
    """A session factory that fails on commit, as a broken write would."""
    session = MagicMock()
    session.__enter__.return_value = session
    session.commit.side_effect = bridge.SQLAlchemyError("permission denied")
    return session


def _working_session() -> object:
    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False
    return session


class TestAFailedWriteIsCounted:
    def test_a_commit_failure_increments_the_counter(self, counters: dict[str, float]) -> None:
        bridge.apply_incidents([MagicMock()], session_factory=_failing_session)

        assert _counter_value(metrics.KBO_NOTIFICATION_INCIDENT_APPLY_FAILURES_TOTAL) == (
            counters["kbo_notification_incident_apply_failures"] + 1
        )

    def test_the_failure_still_reaches_nobody(self, counters: dict[str, float]) -> None:
        """The whole point of the containment: the check keeps its result.

        If this raised, every caller would have to wrap the notification path,
        which is precisely the coupling `apply_incidents` was written to remove.
        """
        bridge.apply_incidents([MagicMock()], session_factory=_failing_session)

    def test_the_failure_is_not_a_delivery_audit_failure(self, counters: dict[str, float]) -> None:
        """Two different swallowed failures, two different counters.

        `kbo_notification_delivery_audit_failures_total` means a message went out
        and the row recording it did not. Merging the two would make an operator
        read an empty audit as "nothing was sent" when the ledger -- which is what
        records that anything was detected -- is the part that failed.
        """
        bridge.apply_incidents([MagicMock()], session_factory=_failing_session)

        assert (
            _counter_value(metrics.KBO_NOTIFICATION_DELIVERY_AUDIT_FAILURES_TOTAL)
            == (counters["kbo_notification_delivery_audit_failures"])
        )


class TestAHealthyWriteIsNotCounted:
    """The counter has to tell "nothing to record" apart from "could not record".

    Both look identical from outside the bridge, and only one of them is a
    problem.
    """

    def test_a_successful_apply_leaves_the_counter_alone(self, counters: dict[str, float]) -> None:
        with patch.object(bridge, "AlertPublisher") as publisher:
            bridge.apply_incidents([MagicMock()], session_factory=_working_session)

        publisher.assert_called_once()
        assert (
            _counter_value(metrics.KBO_NOTIFICATION_INCIDENT_APPLY_FAILURES_TOTAL)
            == (counters["kbo_notification_incident_apply_failures"])
        )

    def test_a_do_nothing_call_leaves_the_counter_alone(self, counters: dict[str, float]) -> None:
        """The early return for an empty batch must not look like a failure.

        `apply_incidents([], resolve_keys=[])` is how a healthy check with nothing
        to say looks, and counting it would inflate the signal that says the
        ledger is broken.
        """
        with patch.object(bridge, "AlertPublisher") as publisher:
            bridge.apply_incidents([], session_factory=_failing_session)

        publisher.assert_not_called()
        assert (
            _counter_value(metrics.KBO_NOTIFICATION_INCIDENT_APPLY_FAILURES_TOTAL)
            == (counters["kbo_notification_incident_apply_failures"])
        )

    def test_one_count_per_call_not_per_event(self, counters: dict[str, float]) -> None:
        """A batch that fails partway is one lost batch, not five lost events."""
        with patch.object(bridge, "AlertPublisher"):
            bridge.apply_incidents([MagicMock(), MagicMock(), MagicMock()], session_factory=_failing_session)

        assert _counter_value(metrics.KBO_NOTIFICATION_INCIDENT_APPLY_FAILURES_TOTAL) == (
            counters["kbo_notification_incident_apply_failures"] + 1
        )
