"""Tests for the scheduler data-integrity job and its incident bookkeeping.

The job publishes date-scoped incident keys (``integrity:<check>:<YYYYMMDD>``), so
a date that stops being the target is never re-evaluated. These tests pin the
recheck window that lets a transient failure resolve itself instead of leaving a
permanently open incident.
"""

from __future__ import annotations

import os
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.constants import KST
from src.notifications.alert_dto import AlertSeverity
from src.scheduler.jobs import maintenance as module
from src.scheduler.jobs.maintenance import (
    _integrity_recheck_lookback_days,
    _integrity_target_dates,
    data_integrity_check_job,
)

#: Fixed instant for this module. ``_previous_day_kst()`` lives in
#: ``src.scheduler.jobs.live`` and resolves ``datetime.now(KST)``, so freezing it
#: pins the expected window to ``20260925`` regardless of the wall clock.
#: ``maintenance`` only imports the function, so patching ``datetime`` there
#: would not affect it — the patch must land on the defining module.
_FIXED_NOW = datetime(2026, 9, 26, 9, 0, tzinfo=KST)


class _FrozenDateTime:
    """Minimal ``datetime`` stand-in exposing only ``now``."""

    @staticmethod
    def now(tz: object = None) -> datetime:
        return _FIXED_NOW


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the recheck window so the hard-coded dates never rot."""
    monkeypatch.setattr("src.scheduler.jobs.live.datetime", _FrozenDateTime)


@pytest.fixture
def no_lock():
    """Bypass the tier lock and the dependency gate for job-body tests."""
    with (
        patch.object(module, "_scheduler_job_lock"),
        patch.object(module, "_with_lock_skip_guard", lambda fn: fn),
    ):
        yield


def _report(failed: list[str], total: int = 10):
    return SimpleNamespace(
        failed_checks=len(failed),
        total_checks=total,
        results=[SimpleNamespace(name=name, passed=name not in failed) for name in ("games_exist", "scores")],
    )


class TestRecheckWindow:
    def test_default_window_covers_previous_day_plus_two(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", raising=False)

        assert _integrity_recheck_lookback_days() == 2
        dates = _integrity_target_dates()
        assert len(dates) == 3
        # Newest first, contiguous calendar days.
        assert dates == ["20260925", "20260924", "20260923"]

    def test_window_is_configurable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", "0")

        assert _integrity_target_dates() == ["20260925"]

    def test_window_is_clamped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", "9999")

        assert _integrity_recheck_lookback_days() == module._INTEGRITY_RECHECK_LOOKBACK_MAX

    @pytest.mark.parametrize("raw", ["junk", "", "-3"])
    def test_invalid_window_values_are_safe(self, monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
        monkeypatch.setenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", raw)

        lookback = _integrity_recheck_lookback_days()
        assert 0 <= lookback <= module._INTEGRITY_RECHECK_LOOKBACK_MAX
        assert len(_integrity_target_dates()) == lookback + 1


class TestGamesExpectation:
    """Pin how the job reaches the checker's game-expectation gate.

    The job calls ``run_integrity_checks`` directly and never enters the CLI's
    override context, so ``games_are_expected`` reads the environment at the
    moment the check runs. That is the correct arrangement -- the documented
    escalation path is the environment variable, and it works on this path.

    These cases exist to keep it that way. The job is the only caller that does
    not go through ``main``, so it is the one place where a future change to
    ``games_are_expected`` -- reading it once at import, or gaining a default
    that overrides the environment -- would silently stop the strict gate from
    being honoured during a 04:45 KST run with nobody watching.
    """

    def test_strict_environment_reaches_the_check(self, no_lock, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INTEGRITY_EXPECT_GAMES", "1")
        seen: list[bool] = []

        def _fake_run(target_date: str):
            from src.cli.reports.data_integrity_checker import games_are_expected

            seen.append(games_are_expected())
            return _report(failed=[])

        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_fake_run),
            patch("src.notifications.bridge.apply_incidents", side_effect=lambda *a, **k: None),
        ):
            data_integrity_check_job()

        assert seen and all(seen), "the strict gate did not reach the check"

    def test_rest_day_does_not_fail_the_job_by_default(self, no_lock, monkeypatch: pytest.MonkeyPatch) -> None:
        """The permissive default is preserved: a rest day is not an incident."""
        monkeypatch.delenv("INTEGRITY_EXPECT_GAMES", raising=False)
        captured: dict[str, object] = {}

        def _rest_day_report(target_date: str):
            return SimpleNamespace(
                failed_checks=0,
                total_checks=10,
                results=[SimpleNamespace(name="games_exist", passed=True, message="rest day")],
            )

        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_rest_day_report),
            patch(
                "src.notifications.bridge.apply_incidents",
                side_effect=lambda events, *, resolve_keys=(), **kw: captured.update(events=list(events)),
            ),
        ):
            data_integrity_check_job()

        assert captured["events"] == []

    def test_environment_is_not_mutated_by_the_job(self, no_lock, monkeypatch: pytest.MonkeyPatch) -> None:
        """The job must leave the process environment as it found it."""
        monkeypatch.setenv("INTEGRITY_EXPECT_GAMES", "1")

        with (
            patch(
                "src.cli.reports.data_integrity_checker.run_integrity_checks",
                side_effect=lambda d: _report(failed=[]),
            ),
            patch("src.notifications.bridge.apply_incidents", side_effect=lambda *a, **k: None),
        ):
            data_integrity_check_job()

        assert os.environ["INTEGRITY_EXPECT_GAMES"] == "1"


class TestIncidentBookkeeping:
    def test_transient_failure_of_an_older_date_is_rechecked_and_resolved(self, no_lock) -> None:
        """A previously failing older date must be re-evaluated, not abandoned."""
        reports = {
            # Newest day passes everything.
            "20260925": _report(failed=[]),
            # An older day that failed transiently now passes; its key must resolve.
            "20260924": _report(failed=[]),
            "20260923": _report(failed=[]),
        }
        captured: dict[str, object] = {}

        def _fake_run(target_date: str):
            captured.setdefault("dates", []).append(target_date)  # type: ignore[union-attr]
            return reports[target_date]

        def _fake_apply(events, *, resolve_keys=(), **kwargs):
            captured["events"] = list(events)
            captured["resolve_keys"] = list(resolve_keys)

        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_fake_run),
            patch("src.notifications.bridge.apply_incidents", side_effect=_fake_apply),
        ):
            data_integrity_check_job()

        assert captured["dates"] == ["20260925", "20260924", "20260923"]
        assert captured["events"] == []
        # Every date/check pair is resolved, including the older ones.
        assert "integrity:games_exist:20260923" in captured["resolve_keys"]
        assert "integrity:scores:20260924" in captured["resolve_keys"]

    def test_failing_dates_open_and_passing_dates_resolve_in_one_batch(self, no_lock) -> None:
        reports = {
            "20260925": _report(failed=["games_exist"]),
            "20260924": _report(failed=[]),
            "20260923": _report(failed=[]),
        }
        captured: dict[str, object] = {}

        with (
            patch(
                "src.cli.reports.data_integrity_checker.run_integrity_checks",
                side_effect=lambda d: reports[d],
            ),
            patch(
                "src.notifications.bridge.apply_incidents",
                side_effect=lambda events, *, resolve_keys=(), **kw: captured.update(
                    events=list(events), resolve_keys=list(resolve_keys)
                ),
            ),
        ):
            data_integrity_check_job()

        opened = [e.incident_key for e in captured["events"]]
        assert opened == ["integrity:games_exist:20260925"]
        # A failing check for the newest day must not also be queued for recovery.
        assert "integrity:games_exist:20260925" not in captured["resolve_keys"]
        assert "integrity:scores:20260925" in captured["resolve_keys"]

    def test_failing_older_date_still_opens_an_incident(self, no_lock) -> None:
        reports = {
            "20260925": _report(failed=[]),
            "20260924": _report(failed=[]),
            "20260923": _report(failed=["games_exist"]),
        }
        captured: dict[str, object] = {}

        with (
            patch(
                "src.cli.reports.data_integrity_checker.run_integrity_checks",
                side_effect=lambda d: reports[d],
            ),
            patch(
                "src.notifications.bridge.apply_incidents",
                side_effect=lambda events, *, resolve_keys=(), **kw: captured.update(
                    events=list(events), resolve_keys=list(resolve_keys)
                ),
            ),
        ):
            data_integrity_check_job()

        assert [e.incident_key for e in captured["events"]] == ["integrity:games_exist:20260923"]
        assert captured["events"][0].severity is AlertSeverity.ERROR
        assert "20260923" in captured["events"][0].remediation[0]

    def test_check_failure_does_not_abort_the_window(self, no_lock) -> None:
        """One date blowing up must not stop the remaining dates from resolving."""
        seen: list[str] = []

        def _fake_run(target_date: str):
            seen.append(target_date)
            if target_date == "20260925":
                msg = "check runner exploded"
                raise RuntimeError(msg)
            return _report(failed=[])

        captured: dict[str, object] = {}

        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_fake_run),
            patch(
                "src.notifications.bridge.apply_incidents",
                side_effect=lambda events, *, resolve_keys=(), **kw: captured.update(
                    events=list(events), resolve_keys=list(resolve_keys)
                ),
            ),
        ):
            data_integrity_check_job()

        # The whole window was attempted despite the first date raising.
        assert seen == ["20260925", "20260924", "20260923"]
        # The unevaluated date is left alone: its state is unknown, not healthy.
        assert captured["events"] == []
        assert not [k for k in captured["resolve_keys"] if k.endswith(":20260925")]
        # The dates that did evaluate still resolve.
        assert "integrity:games_exist:20260924" in captured["resolve_keys"]
