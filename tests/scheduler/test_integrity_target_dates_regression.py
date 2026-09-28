"""Resilience contract for the 04:45 KST `data_integrity_check_job` target window.

The job imported `parse_date_str_lenient` from `src.utils.date_helpers` while
that function did not exist, so every scheduled run raised `ImportError` and the
job silently stopped running. Window shape and incident bookkeeping are covered
in `test_integrity_check_job.py`; this module covers only what that file cannot:

- an unparseable previous-day string must not stop the job, and
- the substituted date must be the *previous* KST day, never today. Checking
  today at 04:45 inspects a day whose 03:00 crawl has not landed yet and opens a
  fresh incident against partial data.
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from src.constants import KST
from src.orchestration.dto import StageExecutionStatus
from src.scheduler.jobs import maintenance as maintenance_jobs
from src.scheduler.jobs.daily import _failed_stage_ids
from src.scheduler.jobs.maintenance import _integrity_target_dates, data_integrity_check_job

_LOOKBACK_ENV = "INTEGRITY_RECHECK_LOOKBACK_DAYS"


def _yesterday_kst() -> date:
    return (datetime.now(KST) - timedelta(days=1)).date()


def _install_integrity_mocks(monkeypatch: pytest.MonkeyPatch, run: Any) -> None:
    monkeypatch.setattr("src.cli.reports.data_integrity_checker.run_integrity_checks", run, raising=False)
    monkeypatch.setattr("src.notifications.bridge.apply_incidents", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(
        maintenance_jobs,
        "_scheduler_job_lock",
        lambda *_a, **_k: contextlib.nullcontext(),
    )


class TestMalformedPreviousDay:
    @pytest.mark.parametrize("bad", ["NOT-A-DATE", "", "2026-13-45", "not a date at all"])
    def test_window_still_resolves(self, bad: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(_LOOKBACK_ENV, "0")
        monkeypatch.setattr(maintenance_jobs, "_previous_day_kst", lambda: bad)

        assert len(_integrity_target_dates()) == 1

    def test_fallback_is_previous_day_not_today(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The explicit fallback exists so a bad value never widens onto today."""
        monkeypatch.setenv(_LOOKBACK_ENV, "0")
        monkeypatch.setattr(maintenance_jobs, "_previous_day_kst", lambda: "NOT-A-DATE")

        parsed = datetime.strptime(_integrity_target_dates()[0], "%Y%m%d").date()

        assert parsed == _yesterday_kst()
        assert parsed != datetime.now(KST).date()

    def test_window_stays_contiguous_under_malformed_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(_LOOKBACK_ENV, "2")
        monkeypatch.setattr(maintenance_jobs, "_previous_day_kst", lambda: "NOT-A-DATE")

        dates = [datetime.strptime(d, "%Y%m%d").date() for d in _integrity_target_dates()]

        assert len(dates) == 3
        assert dates[0] == _yesterday_kst()
        assert dates[0] - dates[1] == timedelta(days=1)
        assert dates[1] - dates[2] == timedelta(days=1)

    def test_job_survives_malformed_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Drive the real 04:45 entry point: a bad value must not abort the job."""
        monkeypatch.setenv(_LOOKBACK_ENV, "1")
        monkeypatch.setattr(maintenance_jobs, "_previous_day_kst", lambda: "NOT-A-DATE")
        evaluated: list[str] = []

        def _run(target_date: str) -> Any:
            evaluated.append(target_date)
            return SimpleNamespace(results=[], failed_checks=0, total_checks=0)

        _install_integrity_mocks(monkeypatch, _run)

        data_integrity_check_job()

        assert len(evaluated) == 2


class TestFailedStageIdsHelper:
    """`_failed_stage_ids` is pure and must stay undecorated.

    It used to carry `@_with_lock_skip_guard`, which returns `None` when a lock
    is skipped, giving the caller's `_failed_stage_ids(report) - {"quality_gate"}`
    a live `None - set` TypeError path. The tenacity retry also turned a
    programmer error in this loop into a 5-minute wait plus a scheduler incident.
    """

    def test_does_not_swallow_into_none(self) -> None:
        stage = SimpleNamespace(stage_id="quality_gate", status=StageExecutionStatus.FAILED)
        result = _failed_stage_ids(SimpleNamespace(stage_results=[stage]))

        assert isinstance(result, set)
        assert result - {"quality_gate"} == set()

    def test_has_no_retry_wrapper(self) -> None:
        assert not hasattr(_failed_stage_ids, "retry")
        assert not hasattr(_failed_stage_ids, "__wrapped__")

    def test_tolerates_report_without_stage_results(self) -> None:
        assert _failed_stage_ids(object()) == set()
