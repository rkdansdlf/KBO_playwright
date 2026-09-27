"""Tests for the snapshot drift check maintenance job."""

from __future__ import annotations

from unittest.mock import patch

from src.services.snapshot_replay import SnapshotValidationResult


class _NullLock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def _healthy() -> list[SnapshotValidationResult]:
    return [SnapshotValidationResult(1, "k", 3, 3, 0, False, True)]


def _drifted() -> list[SnapshotValidationResult]:
    return [SnapshotValidationResult(2, "k", 5, 3, -2, True, True)]


def _failed() -> list[SnapshotValidationResult]:
    return [SnapshotValidationResult(3, None, None, 0, None, False, False, "no parser")]


def _run(monkeypatch, results: list[SnapshotValidationResult]) -> object:
    from src.scheduler.jobs import maintenance

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with (
        patch("src.services.snapshot_replay.validate_recent_snapshots", return_value=results),
        patch("src.notifications.bridge.apply_incidents") as mock_apply,
    ):
        maintenance.snapshot_drift_check_job()
    return mock_apply


def test_healthy_resolves_incident(monkeypatch) -> None:
    mock_apply = _run(monkeypatch, _healthy())
    assert mock_apply.call_args.args[0] == []
    assert mock_apply.call_args.kwargs["resolve_keys"] == ["drift:snapshot"]


def test_drift_opens_error_incident(monkeypatch) -> None:
    mock_apply = _run(monkeypatch, _drifted())
    event = mock_apply.call_args.args[0][0]
    assert event.incident_key == "drift:snapshot"
    assert event.severity.value == "ERROR"
    assert event.remediation == ("kbo snapshot validate --snapshot-id 2",)


def test_failures_below_threshold_are_ok(monkeypatch) -> None:
    # default fail_max=5, a single failure stays within budget.
    mock_apply = _run(monkeypatch, _failed())
    assert mock_apply.call_args.args[0] == []
    assert mock_apply.call_args.kwargs["resolve_keys"] == ["drift:snapshot"]


def test_failures_over_threshold_open_warning(monkeypatch) -> None:
    from src.scheduler.jobs import maintenance

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    monkeypatch.setenv("SNAPSHOT_DRIFT_FAIL_MAX", "0")
    with (
        patch("src.services.snapshot_replay.validate_recent_snapshots", return_value=_failed()),
        patch("src.notifications.bridge.apply_incidents") as mock_apply,
    ):
        maintenance.snapshot_drift_check_job()
    event = mock_apply.call_args.args[0][0]
    assert event.severity.value == "WARNING"
    assert event.incident_key == "drift:snapshot"


def test_drift_max_override_relaxes_gate(monkeypatch) -> None:
    from src.scheduler.jobs import maintenance

    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    monkeypatch.setenv("SNAPSHOT_DRIFT_MAX", "1")
    with (
        patch("src.services.snapshot_replay.validate_recent_snapshots", return_value=_drifted()),
        patch("src.notifications.bridge.apply_incidents") as mock_apply,
    ):
        maintenance.snapshot_drift_check_job()
    assert mock_apply.call_args.args[0] == []


def test_job_registered_in_scheduler() -> None:
    from src.scheduler import registry

    assert hasattr(registry, "snapshot_drift_check_job")
