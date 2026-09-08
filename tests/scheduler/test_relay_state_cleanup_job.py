"""Tests for the scheduled relay state cleanup job."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.scheduler.jobs.maintenance import relay_state_cleanup_job


def _make_summary(**kwargs: Any) -> MagicMock:
    """Create a mock RelayStateSummary with sensible defaults."""
    summary = MagicMock()
    summary.total_games = kwargs.get("total_games", 0)
    summary.total_pbp_rows = kwargs.get("total_pbp_rows", 0)
    summary.total_events = kwargs.get("total_events", 0)
    summary.unknown_source_games = kwargs.get("unknown_source_games", 0)
    summary.source_mismatch_games = kwargs.get("source_mismatch_games", 0)
    summary.redundant_source_games = kwargs.get("redundant_source_games", 0)
    summary.allowed_source_games = kwargs.get("allowed_source_games", 0)
    summary.source_breakdown = kwargs.get("source_breakdown", {})
    summary.issues = kwargs.get("issues", [])
    summary.game_level_issues = kwargs.get("game_level_issues", {})
    summary.unclassified_event_games = kwargs.get("unclassified_event_games", 0)
    return summary


def _run_job(captured: dict[str, Any], *, raises: Exception | None = None) -> None:
    """Run relay_state_cleanup_job with mocked dependencies."""

    def fake_audit_relay_source_states(sample_size: int | None = None):
        captured["sample_size"] = sample_size
        return _make_summary(
            total_games=100,
            unknown_source_games=5,
            source_mismatch_games=3,
            redundant_source_games=2,
        )

    def fake_fix_unknown_sources(dry_run=True, sample_size=None):
        return {"action": "fix_unknown_sources", "games": ["g1", "g2"], "dry_run": dry_run, "affected_rows": 2}

    def fake_fix_source_mismatch(dry_run=True, sample_size=None):
        return {"action": "fix_source_mismatch", "games": ["g3"], "dry_run": dry_run}

    def fake_remove_redundant_sources(dry_run=True, sample_size=None):
        return {"action": "remove_redundant", "games": ["g4"], "dry_run": dry_run}

    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.scheduler.jobs.maintenance.alert_success") as mock_alert_success,
        patch("src.scheduler.jobs.maintenance.alert_failure") as mock_alert_failure,
        patch(
            "scripts.maintenance.fix_relay_state.audit_relay_source_states", side_effect=fake_audit_relay_source_states
        ),
        patch("scripts.maintenance.fix_relay_state.fix_unknown_sources", side_effect=fake_fix_unknown_sources),
        patch("scripts.maintenance.fix_relay_state.fix_source_mismatch", side_effect=fake_fix_source_mismatch),
        patch(
            "scripts.maintenance.fix_relay_state.remove_redundant_sources", side_effect=fake_remove_redundant_sources
        ),
        patch("scripts.maintenance.fix_relay_state.fix_unclassified_events") as mock_fix_unclass,
        patch("scripts.maintenance.fix_relay_state.print_summary"),
    ):
        mock_lock.return_value.__enter__.return_value = None
        mock_fix_unclass.return_value = {"action": "fix_unclassified", "affected_rows": 0, "dry_run": False}

        if raises is not None:
            with patch("scripts.maintenance.fix_relay_state.audit_relay_source_states", side_effect=raises):
                relay_state_cleanup_job()
        else:
            relay_state_cleanup_job()

        captured["alert_success_called"] = mock_alert_success.called
        captured["alert_failure_called"] = mock_alert_failure.called


def test_relay_state_cleanup_runs_audit_with_sample_size() -> None:
    """Test that cleanup job audits with sample_size=10000."""
    captured: dict[str, Any] = {}
    _run_job(captured)

    assert captured["sample_size"] == 10000


def test_relay_state_cleanup_calls_fix_unknown_sources() -> None:
    """Test that cleanup calls fix_unknown_sources when unknown sources exist."""
    captured: dict[str, Any] = {}

    def check_unknown(dry_run=True, sample_size=None):
        captured["fix_unknown_called"] = True
        return {"action": "fix_unknown_sources", "games": [], "dry_run": dry_run, "affected_rows": 0}

    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.scheduler.jobs.maintenance.alert_success"),
        patch("src.scheduler.jobs.maintenance.alert_failure"),
        patch(
            "scripts.maintenance.fix_relay_state.audit_relay_source_states",
            return_value=_make_summary(total_games=10, unknown_source_games=1),
        ),
        patch("scripts.maintenance.fix_relay_state.fix_unknown_sources", side_effect=check_unknown),
        patch("scripts.maintenance.fix_relay_state.fix_source_mismatch", return_value={"action": "none"}),
        patch("scripts.maintenance.fix_relay_state.remove_redundant_sources", return_value={"action": "none"}),
        patch("scripts.maintenance.fix_relay_state.fix_unclassified_events", return_value={"action": "none"}),
        patch("scripts.maintenance.fix_relay_state.print_summary"),
    ):
        mock_lock.return_value.__enter__.return_value = None
        relay_state_cleanup_job()

    assert captured.get("fix_unknown_called") is True


def test_relay_state_cleanup_success_alert_sent() -> None:
    """Test that success alert is sent after successful cleanup."""
    captured: dict[str, Any] = {}
    _run_job(captured)

    assert captured["alert_success_called"] is True
    assert captured["alert_failure_called"] is False


def test_relay_state_cleanup_failure_alert_sent() -> None:
    """Test that failure alert is sent when an exception occurs."""
    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.scheduler.jobs.maintenance.alert_success") as mock_success,
        patch("src.scheduler.jobs.maintenance.alert_failure") as mock_failure,
        patch("scripts.maintenance.fix_relay_state.audit_relay_source_states", side_effect=Exception("Test error")),
        patch("scripts.maintenance.fix_relay_state.print_summary"),
    ):
        mock_lock.return_value.__enter__.return_value = None
        relay_state_cleanup_job()

    assert mock_success.called is False
    assert mock_failure.called is True


def test_relay_state_cleanup_skips_fixes_when_no_issues() -> None:
    """Test that cleanup doesn't call fix functions when no issues found."""
    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.scheduler.jobs.maintenance.alert_success"),
        patch("src.scheduler.jobs.maintenance.alert_failure"),
        patch(
            "scripts.maintenance.fix_relay_state.audit_relay_source_states",
            return_value=_make_summary(
                total_games=10, unknown_source_games=0, source_mismatch_games=0, redundant_source_games=0
            ),
        ),
        patch("scripts.maintenance.fix_relay_state.fix_unknown_sources") as mock_fix_unknown,
        patch("scripts.maintenance.fix_relay_state.fix_source_mismatch") as mock_fix_mismatch,
        patch("scripts.maintenance.fix_relay_state.remove_redundant_sources") as mock_remove,
        patch("scripts.maintenance.fix_relay_state.fix_unclassified_events") as mock_fix_unclass,
        patch("scripts.maintenance.fix_relay_state.print_summary"),
    ):
        mock_lock.return_value.__enter__.return_value = None
        relay_state_cleanup_job()

    mock_fix_unknown.assert_not_called()
    mock_fix_mismatch.assert_not_called()
    mock_remove.assert_not_called()
    mock_fix_unclass.assert_not_called()


def test_relay_state_cleanup_uses_dry_run_mode() -> None:
    """Test that fix functions are called with dry_run=True (safe observation mode)."""
    captured: dict[str, Any] = {}

    def check_dry_run(dry_run=True, **kwargs):
        captured["dry_run"] = dry_run
        return {"action": "fix_unknown_sources", "games": [], "dry_run": dry_run, "affected_rows": 0}

    with (
        patch("src.scheduler.jobs.maintenance._scheduler_job_lock") as mock_lock,
        patch("src.scheduler.jobs.maintenance.alert_success"),
        patch("src.scheduler.jobs.maintenance.alert_failure"),
        patch(
            "scripts.maintenance.fix_relay_state.audit_relay_source_states",
            return_value=_make_summary(total_games=10, unknown_source_games=1),
        ),
        patch("scripts.maintenance.fix_relay_state.fix_unknown_sources", side_effect=check_dry_run),
        patch("scripts.maintenance.fix_relay_state.fix_source_mismatch", return_value={"action": "none"}),
        patch("scripts.maintenance.fix_relay_state.remove_redundant_sources", return_value={"action": "none"}),
        patch("scripts.maintenance.fix_relay_state.fix_unclassified_events", return_value={"action": "none"}),
        patch("scripts.maintenance.fix_relay_state.print_summary"),
    ):
        mock_lock.return_value.__enter__.return_value = None
        relay_state_cleanup_job()

    assert captured["dry_run"] is True  # Job uses dry_run=True for safety (observation only)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
