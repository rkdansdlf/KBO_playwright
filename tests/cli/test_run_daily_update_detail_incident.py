"""The detail recovery incident has to close when its cause does.

``recovery:detail:<date>`` only ever opened. Three dates (20261014/17/18) stayed
OPEN long after their games were recovered, because the escalation helper
published the event and never handed the key back -- nothing else in the run
knew the key existed. These tests pin both directions: a repeated failure still
opens the incident, and a run that walks its date and leaves nothing missing
resolves it.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from src.cli.pipelines import run_daily_update as pipeline

DATE = "20261020"


def _ctx(**overrides: object) -> pipeline._RunContext:
    ctx = pipeline._RunContext(
        target_date=DATE,
        year=2026,
        month=10,
        today_kst=date(2026, 10, 24),
        runner=MagicMock(),
        write_contract=MagicMock(),
    )
    for name, value in overrides.items():
        setattr(ctx, name, value)
    return ctx


class _Recorder:
    """Capture one ``apply_incidents`` call instead of touching the ledger."""

    def __init__(self) -> None:
        self.events: list[object] = []
        self.resolve_keys: list[str] = []
        self.calls = 0

    def __call__(self, events: object, **kwargs: object) -> None:
        self.calls += 1
        self.events = list(events)  # type: ignore[arg-type]
        self.resolve_keys = [str(key) for key in (kwargs.get("resolve_keys") or ())]  # type: ignore[union-attr]


def _install(monkeypatch) -> _Recorder:
    recorder = _Recorder()
    monkeypatch.setattr(pipeline, "apply_incidents", recorder)
    return recorder


class TestTheDetailRecoveryIncidentClosesWhenItsCauseDoes:
    def test_a_repeated_failure_still_opens_the_incident(self, monkeypatch) -> None:
        """The escalation path keeps its own behaviour: the incident still opens."""
        recorder = _install(monkeypatch)

        pipeline._send_detail_recovery_escalation_alert(
            _ctx(detail_retry_escalation_game_ids=["20261020HTLG0", "20261020HTLG0"]),
        )

        assert [event.incident_key for event in recorder.events] == [f"recovery:detail:{DATE}"]
        assert recorder.events[0].metadata["game_ids"] == ["20261020HTLG0"]
        assert recorder.resolve_keys == []

    def test_a_run_that_leaves_nothing_missing_resolves_the_incident(self, monkeypatch) -> None:
        """Nothing missing on a date this run walked means the gap is gone."""
        recorder = _install(monkeypatch)

        pipeline._send_detail_recovery_escalation_alert(_ctx())

        assert recorder.events == []
        assert recorder.resolve_keys == [f"recovery:detail:{DATE}"]

    def test_a_below_threshold_failure_leaves_the_incident_alone(self, monkeypatch) -> None:
        """A failure under the escalation threshold is neither opened nor closed.

        Resolving here would close the key while a game on that date still has no
        box score, which is exactly the state the incident exists to report.
        """
        recorder = _install(monkeypatch)

        pipeline._send_detail_recovery_escalation_alert(
            _ctx(detail_still_missing={"20261020HTLG0"}),
        )

        assert recorder.calls == 0
