"""Tests for re-validating integrity incidents the recent window has passed.

Incident keys are date-scoped (``integrity:<check>:<YYYYMMDD>``) and the job only
evaluates yesterday plus a bounded lookback. Once the window moved past a date,
its incident had no later chance to pass: ``winning_team_consistency`` for
20261005/06/07 and ``all_terminal_status`` for 20260930 were all OPEN against
data that had since become healthy. These tests pin the re-validation loop that
lets a repaired date close itself, and -- just as important -- pin that a key
whose check still fails, or cannot be evaluated at all, is left exactly as it is.
"""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.scheduler.jobs import maintenance as module
from src.scheduler.jobs.maintenance import (
    _integrity_stale_revalidation_limit,
    _revalidate_stale_integrity_incidents,
    _stale_integrity_incident_dates,
    data_integrity_check_job,
)


def _active(monkeypatch: pytest.MonkeyPatch, keys: list[str]) -> None:
    """Answer ``active_incidents`` from a fixed key list, without a database."""

    class _Manager:
        def __init__(self, session: object) -> None:
            self.session = session

        def active_incidents(self) -> list[SimpleNamespace]:
            return [SimpleNamespace(incident_key=key) for key in keys]

    monkeypatch.setattr("src.notifications.incident.IncidentManager", _Manager)
    monkeypatch.setattr(module, "SessionLocal", lambda: nullcontext(object()))


def _report(passed_names: list[str], failed_names: list[str]):
    return SimpleNamespace(
        failed_checks=len(failed_names),
        total_checks=len(passed_names) + len(failed_names),
        results=[SimpleNamespace(name=name, passed=True) for name in passed_names]
        + [SimpleNamespace(name=name, passed=False) for name in failed_names],
    )


class TestWhichIncidentsAreRevalidated:
    def test_incidents_outside_the_window_are_returned(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _active(monkeypatch, ["integrity:all_terminal_status:20260930"])

        assert _stale_integrity_incident_dates(set(), 20) == [
            ("integrity:all_terminal_status:20260930", "20260930"),
        ]

    def test_dates_inside_the_window_are_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The window already evaluates these; re-running them here would double the work."""
        _active(monkeypatch, ["integrity:all_terminal_status:20260930"])

        assert _stale_integrity_incident_dates({"20260930"}, 20) == []

    def test_non_integrity_keys_are_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Only the keys this loop knows how to re-derive are touched."""
        _active(monkeypatch, ["gap:SPLITS", "dlq:exhausted", "rag:identity"])

        assert _stale_integrity_incident_dates(set(), 20) == []

    def test_a_key_whose_tail_is_not_a_date_is_left_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A key this function cannot date is skipped, never guessed at."""
        _active(monkeypatch, ["integrity:season_stat_team_code", "integrity:scores:2026-10-20"])

        assert _stale_integrity_incident_dates(set(), 20) == []

    def test_the_oldest_date_comes_first(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A backlog must drain from the longest-waiting date, not the newest."""
        _active(
            monkeypatch,
            [
                "integrity:scores_populated:20261020",
                "integrity:all_terminal_status:20260930",
                "integrity:winning_team_consistency:20261005",
            ],
        )

        dates = [date for _, date in _stale_integrity_incident_dates(set(), 20)]

        assert dates == ["20260930", "20261005", "20261020"]

    def test_the_limit_bounds_one_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _active(
            monkeypatch,
            [f"integrity:scores_populated:202610{day:02d}" for day in range(1, 11)],
        )

        selected = _stale_integrity_incident_dates(set(), 3)

        assert len(selected) == 3
        assert [date for _, date in selected] == ["20261001", "20261002", "20261003"]

    def test_a_zero_limit_selects_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _active(monkeypatch, ["integrity:scores_populated:20261020"])

        assert _stale_integrity_incident_dates(set(), 0) == []

    @pytest.mark.parametrize("raw", ["junk", "", "-3"])
    def test_invalid_limits_are_safe(self, monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
        monkeypatch.setenv("INTEGRITY_STALE_REVALIDATION_LIMIT", raw)

        limit = _integrity_stale_revalidation_limit()

        assert 0 <= limit <= module._INTEGRITY_STALE_REVALIDATION_MAX

    def test_the_limit_is_clamped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("INTEGRITY_STALE_REVALIDATION_LIMIT", "99999")

        assert _integrity_stale_revalidation_limit() == module._INTEGRITY_STALE_REVALIDATION_MAX


class TestAnUnreadableLedgerCostsOnlyTheAddition:
    """The window check must survive a ledger this loop cannot read.

    Listing the backlog is an addition to the run, not a replacement for it. An
    unmigrated ``notification_incidents`` table or a dropped connection used to
    take the whole job down with it -- the run lost its primary evaluation
    because the optional one could not be read.
    """

    def test_the_selection_returns_nothing_instead_of_raising(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _boom():
            message = "no such table: notification_incidents"
            raise RuntimeError(message)

        monkeypatch.setattr(module, "SessionLocal", _boom)

        assert _stale_integrity_incident_dates(set(), 20) == []

    def test_the_job_still_evaluates_its_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(module, "_integrity_target_dates", lambda: ["20261023"])
        captured: dict[str, list[str]] = {}
        checked: list[str] = []

        def _boom():
            message = "no such table: notification_incidents"
            raise RuntimeError(message)

        def _fake_run(target_date: str):
            checked.append(target_date)
            return _report(["all_terminal_status"], [])

        def _capture(events, **kwargs):
            captured["resolve_keys"] = list(kwargs.get("resolve_keys") or ())

        monkeypatch.setattr(module, "SessionLocal", _boom)
        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_fake_run),
            patch("src.notifications.bridge.apply_incidents", side_effect=_capture),
            patch.object(module, "_scheduler_job_lock"),
        ):
            data_integrity_check_job()

        assert checked == ["20261023"]
        assert captured["resolve_keys"] == ["integrity:all_terminal_status:20261023"]


class TestWhatTheRevalidationDoes:
    def test_a_passing_check_hands_its_key_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            module,
            "_stale_integrity_incident_dates",
            lambda within, limit: [("integrity:all_terminal_status:20260930", "20260930")],
        )
        resolve_keys: list[str] = []

        with patch(
            "src.cli.reports.data_integrity_checker.run_integrity_checks",
            return_value=_report(["all_terminal_status", "scores_populated"], []),
        ):
            _revalidate_stale_integrity_incidents(set(), resolve_keys)

        assert sorted(resolve_keys) == [
            "integrity:all_terminal_status:20260930",
            "integrity:scores_populated:20260930",
        ]

    def test_a_failing_check_leaves_the_key_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Re-publishing a known failure would re-notify without new information."""
        monkeypatch.setattr(
            module,
            "_stale_integrity_incident_dates",
            lambda within, limit: [("integrity:child_stats_exist:20261020", "20261020")],
        )
        resolve_keys: list[str] = []

        with patch(
            "src.cli.reports.data_integrity_checker.run_integrity_checks",
            return_value=_report([], ["child_stats_exist"]),
        ):
            _revalidate_stale_integrity_incidents(set(), resolve_keys)

        assert resolve_keys == []

    def test_a_check_that_raises_leaves_every_key_alone(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An unevaluated date is unknown, not healthy, so nothing may be closed."""
        monkeypatch.setattr(
            module,
            "_stale_integrity_incident_dates",
            lambda within, limit: [("integrity:child_stats_exist:20261020", "20261020")],
        )
        resolve_keys: list[str] = []

        with patch(
            "src.cli.reports.data_integrity_checker.run_integrity_checks",
            side_effect=RuntimeError("database went away"),
        ):
            _revalidate_stale_integrity_incidents(set(), resolve_keys)

        assert resolve_keys == []

    def test_one_bad_date_does_not_block_the_rest(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            module,
            "_stale_integrity_incident_dates",
            lambda within, limit: [
                ("integrity:scores_populated:20261001", "20261001"),
                ("integrity:scores_populated:20261002", "20261002"),
            ],
        )
        resolve_keys: list[str] = []

        def _run(target_date: str):
            if target_date == "20261001":
                message = "boom"
                raise RuntimeError(message)
            return _report(["scores_populated"], [])

        with patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_run):
            _revalidate_stale_integrity_incidents(set(), resolve_keys)

        assert resolve_keys == ["integrity:scores_populated:20261002"]


class TestTheJobRevalidatesStaleIncidents:
    def test_the_job_closes_a_repaired_date_the_window_has_passed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A healed 20260930 resolves even though the window now starts in October."""
        monkeypatch.setenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", "0")
        monkeypatch.setattr(module, "_integrity_target_dates", lambda: ["20261023"])
        _active(monkeypatch, ["integrity:all_terminal_status:20260930"])
        captured: dict[str, list[str]] = {}

        def _fake_run(target_date: str):
            return _report(["all_terminal_status"], [])

        def _capture(events, **kwargs):
            captured["resolve_keys"] = list(kwargs.get("resolve_keys") or ())

        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_fake_run),
            patch("src.notifications.bridge.apply_incidents", side_effect=_capture),
            patch.object(module, "_scheduler_job_lock"),
        ):
            data_integrity_check_job()

        assert "integrity:all_terminal_status:20260930" in captured["resolve_keys"]

    def test_the_job_keeps_recent_failures_open(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The loop must not close a key its own check still fails."""
        monkeypatch.setenv("INTEGRITY_RECHECK_LOOKBACK_DAYS", "0")
        monkeypatch.setattr(module, "_integrity_target_dates", lambda: ["20261020"])
        _active(monkeypatch, ["integrity:child_stats_exist:20261020"])
        captured: dict[str, list[str]] = {}

        def _fake_run(target_date: str):
            return _report([], ["child_stats_exist"])

        def _capture(events, **kwargs):
            captured["events"] = list(events)
            captured["resolve_keys"] = list(kwargs.get("resolve_keys") or ())

        with (
            patch("src.cli.reports.data_integrity_checker.run_integrity_checks", side_effect=_fake_run),
            patch("src.notifications.bridge.apply_incidents", side_effect=_capture),
            patch.object(module, "_scheduler_job_lock"),
        ):
            data_integrity_check_job()

        assert "integrity:child_stats_exist:20261020" not in captured["resolve_keys"]
        assert captured["events"], "the failing check must still publish"
