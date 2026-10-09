"""A queue mutation that resolves a letter must close the incident it opened.

BH10 asked for the dead letter chain to be observed rather than assumed:

    RUN-A partial -> DLQ enqueue -> incident -> replay RUN-B -> DLQ resolution
    -> incident state

The data half of that chain is already covered end to end by
`tests/crawlers/test_award_dead_letter_e2e.py`, which drives a real ledger
through a real replay. What no test covered is the *incident* half after
`c387f8b3` introduced `dlq:` keys: resolving a letter leaves the queue healthy,
but nothing re-derived the incidents on that path.

`dlq retry` therefore answered the exact question `dlq:exhausted` asked and the
incident kept claiming to be open until the next 30-minute recovery tick. Worse
for an exhausted letter specifically -- the one condition that only an operator
can clear -- the delay falls on the person who just did the clearing.

The fix derives incidents in `_refresh_metrics`, which every mutation path
already calls, so there is one place that decides what the queue looks like
rather than a per-command patch.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.cli import dlq_operator
from src.models.crawl_dead_letter import DlqStatus
from src.services.dlq_incidents import (
    DLQ_INCIDENT_BACKLOG,
    DLQ_INCIDENT_EXHAUSTED,
    DLQ_INCIDENT_STRANDED,
)
from src.services.crawl_dead_letter_stats import DlqStats


def _due() -> datetime:
    """Return a retry time that is unambiguously in the past.

    Relative to the real clock rather than pinned: `_cmd_retry` compares against
    `_utcnow()`, so a fixed date silently becomes a future time and the command
    exits with "retry not due" -- a failure that has nothing to do with what
    these tests assert and would appear only on certain days.
    """
    return datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=5)


def _stats(**overrides: object) -> DlqStats:
    """Build a queue reading, healthy unless a field is named."""
    fields: dict[str, object] = {
        "pending": 0,
        "due": 0,
        "retrying": 0,
        "stale_retrying": 0,
        "resolved": 4,
        "exhausted": 0,
        "ignored": 0,
        "oldest_due_at": None,
        "oldest_due_age_seconds": 0.0,
        "by_status_crawler": {},
    }
    fields.update(overrides)
    return DlqStats(**fields)  # type: ignore[arg-type]


def _args(**overrides: object) -> argparse.Namespace:
    fields: dict[str, object] = {"dlq_id": "dlq-1", "apply": True, "json": False, "reason": "test"}
    fields.update(overrides)
    return argparse.Namespace(**fields)


@pytest.fixture
def captured_incidents(monkeypatch: pytest.MonkeyPatch):
    """Record what the refresh path publishes, without a real incident ledger."""
    published: list[object] = []
    calls: list[dict[str, object]] = []

    def fake_publish(**_kwargs: object) -> DlqStats:
        return fake_publish.stats

    def fake_apply(events, **kwargs) -> None:
        published.extend(events)
        calls.append(kwargs)

    fake_publish.stats = _stats()  # type: ignore[attr-defined]
    monkeypatch.setattr(dlq_operator, "publish_dlq_state_metrics", fake_publish)
    monkeypatch.setattr("src.services.dlq_incidents.apply_incidents", fake_apply)
    return published, calls, fake_publish


class TestAMutationReDerivesTheIncidents:
    """The property the missing call broke: resolve the queue, close the alert."""

    def test_retry_closes_the_incident_it_answered(self, monkeypatch, captured_incidents) -> None:
        """`dlq retry` is the operator answering `dlq:exhausted`.

        Without this, the letter resolves and the ERROR incident stays open
        until the next recovery tick -- the alert outliving its own answer.
        """
        published, calls, _ = captured_incidents

        monkeypatch.setattr(
            dlq_operator,
            "_load_letter",
            lambda _dlq_id: SimpleNamespace(
                status=DlqStatus.PENDING.value,
                next_retry_at=_due(),
            ),
        )
        monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
        monkeypatch.setattr(
            dlq_operator,
            "retry_dead_letter",
            lambda *_a, **_k: SimpleNamespace(status=SimpleNamespace(value="resolved"), success=True),
        )

        assert dlq_operator._cmd_retry(_args()) == 0

        assert len(calls) == 1, "the mutation did not re-derive the incidents"
        assert calls[0]["reconcile_prefix"] == "dlq:"

    def test_the_staleness_threshold_in_the_message_is_the_one_that_selected(
        self, monkeypatch, captured_incidents
    ) -> None:
        """A message quoting the wrong cutoff would be worse than no message.

        Asserted through the derived event rather than the call kwargs: the
        threshold is consumed by `apply_dlq_incidents` and never reaches
        `apply_incidents`, so checking the kwargs of the latter would test the
        wrong layer. This drives a stranded letter and reads the number the
        operator would actually see -- which catches a wrong threshold wherever
        it entered.
        """
        published, _calls, fake_publish = captured_incidents
        fake_publish.stats = _stats(stale_retrying=1)  # type: ignore[attr-defined]

        monkeypatch.setattr(
            dlq_operator,
            "_load_letter",
            lambda _dlq_id: SimpleNamespace(status=DlqStatus.PENDING.value, next_retry_at=_due()),
        )
        monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
        monkeypatch.setenv("DLQ_STALE_RETRYING_SECONDS", "5400")
        monkeypatch.setattr(
            dlq_operator,
            "retry_dead_letter",
            lambda *_a, **_k: SimpleNamespace(status=SimpleNamespace(value="resolved"), success=True),
        )

        dlq_operator._cmd_retry(_args())

        assert [e.incident_key for e in published] == [DLQ_INCIDENT_STRANDED]
        assert published[0].metadata["stale_threshold_seconds"] == 5400


class TestAnUnreadableQueueDoesNotDeriveIncidents:
    """The failure mode that must not repeat here.

    `publish_dlq_state_metrics` raises when it cannot read the queue, and that
    is *not* a healthy reading. Deriving from a default or a previous value
    would resolve incidents for conditions nobody checked -- the same confusion
    B1/B2 removed from the scheduler path.
    """

    def test_an_unreadable_queue_does_not_touch_the_ledger(self, monkeypatch) -> None:
        """Asserted as "not called", not as "published nothing".

        The distinction is the whole risk: a fabricated healthy reading produces
        no events either, but `apply_incidents` reconciles by prefix, so an empty
        batch *closes* every `dlq:` incident. A test that only checked the events
        would pass while the incidents were being cleared on a sweep that
        observed nothing.
        """
        calls: list[object] = []
        monkeypatch.setattr(dlq_operator, "publish_dlq_state_metrics", _raising)
        monkeypatch.setattr(
            "src.services.dlq_incidents.apply_incidents",
            lambda events, **kwargs: calls.append((list(events), kwargs)),
        )

        dlq_operator._refresh_metrics()

        assert calls == [], "an unreadable queue must not reconcile the incident ledger"

    def test_a_none_reading_is_treated_as_no_reading(self, monkeypatch) -> None:
        """Found by an existing test double, and worth pinning.

        `publish_dlq_state_metrics` is typed `-> DlqStats` and raises on failure,
        so None cannot come from the real implementation. It must still not be
        treated as an empty queue: deriving from it would reconcile the
        namespace against a reading nobody took. It must also not raise, because
        a crash here fails a command whose mutation already committed.
        """
        calls: list[object] = []
        monkeypatch.setattr(dlq_operator, "publish_dlq_state_metrics", lambda **_k: None)
        monkeypatch.setattr(
            "src.services.dlq_incidents.apply_incidents",
            lambda events, **kwargs: calls.append(events),
        )

        dlq_operator._refresh_metrics()  # must not raise

        assert calls == []

    def test_the_failure_is_a_warning_not_an_error(self, monkeypatch, capsys) -> None:
        """The mutation is already committed.

        Reporting the command as failed would make the operator retry a mutation
        that worked, which is exactly the confusion a warning avoids.
        """
        monkeypatch.setattr(dlq_operator, "publish_dlq_state_metrics", _raising)

        dlq_operator._refresh_metrics()

        assert "failed to refresh DLQ metrics" in capsys.readouterr().err


class TestANotifyingFailureDoesNotFailTheCommand:
    """The ledger is a side channel here; the mutation is not."""

    def test_a_notify_failure_is_reported_but_not_raised(self, monkeypatch, caplog) -> None:
        """The guarantee lives in the callee, so that is where it is asserted.

        `_refresh_metrics` deliberately has no guard of its own -- adding one
        would imply `apply_dlq_incidents` is unreliable. So the contract to pin
        is that the callee swallows and logs: if it ever stopped, a committed
        mutation would start reporting failure and this test would fail.
        """
        import logging

        def boom(*_args, **_kwargs):
            raise RuntimeError("incident ledger unavailable")

        monkeypatch.setattr(dlq_operator, "publish_dlq_state_metrics", lambda **_k: _stats())
        monkeypatch.setattr("src.services.dlq_incidents.apply_incidents", boom)

        with caplog.at_level(logging.ERROR):
            dlq_operator._refresh_metrics()  # must not raise

        assert "Failed to apply DLQ state incidents" in caplog.text
        assert "incident ledger unavailable" in caplog.text


def _raising(**_kwargs: object):
    """Stand in for an unreachable queue, as a database outage would."""
    from src.db.engine import DB_SESSION_EXCEPTIONS

    raise DB_SESSION_EXCEPTIONS[0]("database unreachable")


class TestTheExhaustedPathIsTheOneThatMatters:
    """Exhausted is the condition with no self-recovery, so the delay is worst there."""

    def test_a_still_exhausted_queue_keeps_the_incident(self, monkeypatch, captured_incidents) -> None:
        """Resolving one letter must not close an alert about another.

        Reconciliation is by namespace, so this is the check that the prefix
        does not become a blunt instrument: a queue still holding exhausted work
        must keep saying so after an unrelated mutation.
        """
        published, _calls, fake_publish = captured_incidents
        fake_publish.stats = _stats(exhausted=2)  # type: ignore[attr-defined]

        monkeypatch.setattr(
            dlq_operator,
            "_load_letter",
            lambda _dlq_id: SimpleNamespace(status=DlqStatus.PENDING.value, next_retry_at=_due()),
        )
        monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
        monkeypatch.setattr(
            dlq_operator,
            "retry_dead_letter",
            lambda *_a, **_k: SimpleNamespace(status=SimpleNamespace(value="resolved"), success=True),
        )

        dlq_operator._cmd_retry(_args())

        assert [e.incident_key for e in published] == [DLQ_INCIDENT_EXHAUSTED]

    def test_a_fully_healthy_queue_opens_nothing(self, monkeypatch, captured_incidents) -> None:
        """The recovery case: the reconciliation is what closes the incident."""
        published, calls, _ = captured_incidents

        monkeypatch.setattr(
            dlq_operator,
            "_load_letter",
            lambda _dlq_id: SimpleNamespace(status=DlqStatus.PENDING.value, next_retry_at=_due()),
        )
        monkeypatch.setenv("KBO_ALLOW_DLQ_MUTATION", "1")
        monkeypatch.setattr(
            dlq_operator,
            "retry_dead_letter",
            lambda *_a, **_k: SimpleNamespace(status=SimpleNamespace(value="resolved"), success=True),
        )

        dlq_operator._cmd_retry(_args())

        assert published == []
        assert calls[0]["reconcile_prefix"] == "dlq:"

    def test_every_dlq_key_is_owned_by_the_namespace_reconciled(self) -> None:
        """A key outside `dlq:` would never be closed by this path.

        Reconciliation clears what it does not see, so a key filed elsewhere
        would be invisible to the one call site that refreshes after a mutation.
        """
        for key in (DLQ_INCIDENT_STRANDED, DLQ_INCIDENT_BACKLOG, DLQ_INCIDENT_EXHAUSTED):
            assert key.startswith("dlq:")
