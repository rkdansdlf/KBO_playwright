"""Tests for the dead letter recovery maintenance job.

The job's alerts are conditional: it warns only when a sweep leaves work
exhausted or failed. That makes the *healthy* branch the one that has to clear a
warning from an earlier run, so most of these tests are about which branch
resolves rather than which one warns.
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


class _NullLock:
    """Context manager that acquires nothing (test double for scheduler locks)."""

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        pass

    def __enter__(self) -> _NullLock:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def _result(action: str) -> SimpleNamespace:
    return SimpleNamespace(action=action)


@contextmanager
def _alerts(results: list[SimpleNamespace]) -> Iterator[tuple[MagicMock, MagicMock]]:
    """Run the job against ``results``, capturing both alert channels.

    ``alert_success`` is patched rather than left live: the real one calls
    ``apply_incidents``, which opens a database session, so without this the
    assertions about state depend on the test database answering.
    """
    with (
        patch("src.services.crawl_dead_letter_recovery.recover_stuck_retrying", return_value=results),
        patch("src.scheduler.jobs.maintenance.alert_warning") as warn,
        patch("src.scheduler.jobs.maintenance.alert_success") as success,
    ):
        yield warn, success


def _run(monkeypatch, results: list[SimpleNamespace]) -> tuple[MagicMock, MagicMock]:
    monkeypatch.setattr(maintenance, "_scheduler_job_lock", _NullLock)
    with _alerts(results) as (warn, success):
        maintenance.crawl_dead_letter_recovery_job()
    return warn, success


def test_nothing_stuck_returns_without_alert(monkeypatch) -> None:
    warn, success = _run(monkeypatch, [])

    warn.assert_not_called()
    success.assert_called_once()


def test_exhausted_triggers_alert(monkeypatch) -> None:
    warn, success = _run(monkeypatch, [_result("exhausted"), _result("resolved")])

    warn.assert_called_once()
    assert "exhausted=1" in warn.call_args.args[1]
    # A failing sweep must not also report recovery.
    success.assert_not_called()


def test_resolved_only_does_not_alert(monkeypatch) -> None:
    warn, success = _run(monkeypatch, [_result("resolved")])

    warn.assert_not_called()
    success.assert_called_once()


class TestFailureClearsTheStaleWarning:
    """``alert_warning`` opens ``scheduler:crawl_dead_letter_recovery:warning``.

    Nothing else owns that key -- ``alert_failure`` writes ``:failed``, and the
    APScheduler lifecycle listener resolves ``<job_id>:failed`` -- so a run that
    finds nothing wrong is the only thing that can close it.
    """

    def test_a_clean_sweep_reports_recovery(self, monkeypatch) -> None:
        warn, success = _run(monkeypatch, [_result("resolved")])

        assert success.call_args.args[0] == "crawl_dead_letter_recovery"
        warn.assert_not_called()

    def test_an_empty_sweep_also_reports_recovery(self, monkeypatch) -> None:
        """The most common healthy run takes the early return."""
        _warn, success = _run(monkeypatch, [])

        assert success.call_args.args[0] == "crawl_dead_letter_recovery"

    def test_a_failing_sweep_leaves_the_warning_open(self, monkeypatch) -> None:
        _warn, success = _run(monkeypatch, [_result("failed")])

        success.assert_not_called()


def test_recovery_job_is_registered_in_scheduler() -> None:
    from src.scheduler import registry

    assert hasattr(registry, "crawl_dead_letter_recovery_job")
