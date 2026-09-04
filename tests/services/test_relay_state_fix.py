"""fix_relay_state.py 단위 테스트.

릴레이 소스 상태 정리 스크립트의 각 함수를 테스트합니다.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.maintenance.fix_relay_state import (
    RelayStateSummary,
    RelayStateIssue,
    _analyze_game_sources,
    _get_games_by_criterion,
    print_summary,
    audit_relay_source_states,
    fix_unknown_sources,
    fix_unclassified_events,
)
from src.scheduler.jobs.daily import (
    JobStatus,
    JobResult,
    _register_job,
    _update_job_status,
    _can_run_job,
    get_job_status_summary,
    clear_job_registry,
)


# === Relay State Summary / Issue tests ===

def test_relay_state_summary_dataclass() -> None:
    """Test RelayStateSummary dataclass."""
    summary = RelayStateSummary()
    assert summary.total_games == 0
    assert summary.total_pbp_rows == 0
    assert summary.issues == []
    assert summary.source_breakdown == {}


def test_relay_state_issue_dataclass() -> None:
    """Test RelayStateIssue dataclass."""
    issue = RelayStateIssue(issue_type="unknown", source_name="test", count=1)
    assert issue.issue_type == "unknown"
    assert issue.source_name == "test"
    assert issue.count == 1


# === _analyze_game_sources tests ===

def test_analyze_game_sources_empty() -> None:
    """Test _analyze_game_sources returns empty results."""
    mock_session = MagicMock()
    mock_session.execute.return_value.all.return_value = []

    sources, has_unclassified, has_mismatch, has_redundant = _analyze_game_sources(mock_session, "FAKE_GAME")

    assert sources == set()
    assert has_unclassified is False
    assert has_mismatch is False
    assert has_redundant is False


def test_analyze_game_sources_with_data() -> None:
    """Test _analyze_game_sources with mock data."""
    mock_session = MagicMock()
    mock_session.execute.return_value.all.return_value = [
        type('Row', (), {'source_name': 'naver', 'event_type': 'batting', 'provider_log_id': 'naver_001'})(),
        type('Row', (), {'source_name': 'jumper', 'event_type': 'unknown', 'provider_log_id': 'naver_002'})(),
    ]

    sources, has_unclassified, has_mismatch, has_redundant = _analyze_game_sources(mock_session, "FAKE_GAME")

    assert 'naver' in sources
    assert 'jumper' in sources
    assert has_unclassified is True
    assert has_redundant is True  # 'jumper' is a known redundant prefix


# === _get_games_by_criterion tests ===

def test_get_games_by_criterion_unknown_source() -> None:
    """Test _get_games_by_criterion with unknown_source."""
    summary = RelayStateSummary()
    summary.game_level_issues = {
        'game1': ['unknown_source:test'],
        'game2': ['redundant'],
    }

    result = _get_games_by_criterion(summary, 'unknown_source')

    assert 'game1' in result
    assert 'game2' not in result


def test_get_games_by_criterion_all() -> None:
    """Test _get_games_by_criterion with all."""
    summary = RelayStateSummary()
    summary.game_level_issues = {
        'game1': ['unknown_source:test'],
        'game2': ['redundant'],
    }

    result = _get_games_by_criterion(summary, 'all')

    assert len(result) == 2


def test_get_games_by_criterion_empty() -> None:
    """Test _get_games_by_criterion returns empty list for unknown criterion."""
    summary = RelayStateSummary()
    result = _get_games_by_criterion(summary, 'nonexistent')
    assert result == []


def test_get_games_by_criterion_source_mismatch() -> None:
    """Test _get_games_by_criterion with source_mismatch."""
    summary = RelayStateSummary()
    summary.game_level_issues = {
        'game1': ['source_mismatch'],
        'game2': ['redundant'],
    }

    result = _get_games_by_criterion(summary, 'source_mismatch')
    assert 'game1' in result
    assert 'game2' not in result


def test_get_games_by_criterion_redundant() -> None:
    """Test _get_games_by_criterion with redundant."""
    summary = RelayStateSummary()
    summary.game_level_issues = {
        'game1': ['source_mismatch'],
        'game2': ['redundant'],
    }

    result = _get_games_by_criterion(summary, 'redundant')
    assert 'game2' in result
    assert 'game1' not in result


# === print_summary test ===

def test_print_summary() -> None:
    """Test print_summary output."""
    summary = RelayStateSummary(
        total_games=100,
        total_pbp_rows=5000,
        total_events=10000,
        unknown_source_games=5,
        source_mismatch_games=3,
        redundant_source_games=2,
    )

    import logging
    with patch.object(logging.getLogger('scripts.maintenance.fix_relay_state'), 'info') as mock_info:
        print_summary(summary)
        assert mock_info.called


# === fix_*_sources tests (with mocked audit) ===

def test_fix_unknown_sources_dry_run_no_issues() -> None:
    """Test fix_unknown_sources in dry-run mode with no issues."""
    with patch('scripts.maintenance.fix_relay_state.audit_relay_source_states') as mock_audit:
        summary = RelayStateSummary()
        summary.game_level_issues = {}
        summary.source_breakdown = {}
        mock_audit.return_value = summary

        result = fix_unknown_sources(dry_run=True)
        assert result['action'] == 'none'
        assert result['dry_run'] is True


def test_fix_unclassified_events_dry_run_no_issues() -> None:
    """Test fix_unclassified_events in dry-run mode with no issues."""
    with patch('scripts.maintenance.fix_relay_state.SessionLocal') as mock_session_local:
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.query.return_value.filter.return_value.all.return_value = []
        mock_session_local.return_value = mock_session

        result = fix_unclassified_events(dry_run=True)
        assert result['action'] == 'none'


# === Job dependency tracking tests (from src.scheduler.jobs.daily) ===

def test_job_status_enum() -> None:
    """Test JobStatus enum values."""
    assert JobStatus.SUCCESS.value == 'success'
    assert JobStatus.FAILURE.value == 'failure'
    assert JobStatus.RUNNING.value == 'running'
    assert JobStatus.SKIPPED.value == 'skipped'


def test_job_result_dataclass() -> None:
    """Test JobResult dataclass."""
    result = JobResult(job_name='test', status=JobStatus.SUCCESS, dependencies=['dep1'])
    assert result.job_name == 'test'
    assert result.status == JobStatus.SUCCESS
    assert result.dependencies == ['dep1']
    assert result.details == {}
    assert result.message == ''


def test_dependency_tracking_functions() -> None:
    """Test that dependency tracking functions work correctly."""
    clear_job_registry()

    _register_job('test_job', dependencies=['dep1', 'dep2'])
    _update_job_status('test_job', JobStatus.SUCCESS, 'Test completed')

    result = get_job_status_summary()
    assert 'test_job' in result
    assert result['test_job']['status'] == 'success'
    assert result['test_job']['dependencies'] == ['dep1', 'dep2']


def test_can_run_job_with_success_dependency() -> None:
    """Test that _can_run_job returns True when dependency is successful."""
    clear_job_registry()

    _register_job('dep_job')
    _update_job_status('dep_job', JobStatus.SUCCESS, 'Dependency completed')
    _register_job('main_job', dependencies=['dep_job'])

    can_run, reason = _can_run_job('main_job')
    assert can_run is True
    assert reason == ''


def test_can_run_job_with_failed_dependency() -> None:
    """Test that _can_run_job returns False when dependency failed."""
    clear_job_registry()

    _register_job('dep_job')
    _update_job_status('dep_job', JobStatus.FAILURE, 'Dependency failed')
    _register_job('main_job', dependencies=['dep_job'])

    can_run, reason = _can_run_job('main_job')
    assert can_run is False
    assert 'failure' in reason.lower()


def test_can_run_job_with_unknown_dependency() -> None:
    """Test that _can_run_job returns False when dependency is not registered."""
    clear_job_registry()

    _register_job('main_job', dependencies=['unknown_dep'])

    can_run, reason = _can_run_job('main_job')
    assert can_run is False
    assert 'not registered' in reason


def test_can_run_job_with_all_success_dependencies() -> None:
    """Test _can_run_job with multiple successful dependencies."""
    clear_job_registry()

    _register_job('dep1')
    _update_job_status('dep1', JobStatus.SUCCESS, 'Done')
    _register_job('dep2')
    _update_job_status('dep2', JobStatus.SUCCESS, 'Done')
    _register_job('main_job', dependencies=['dep1', 'dep2'])

    can_run, reason = _can_run_job('main_job')
    assert can_run is True


def test_can_run_job_with_partial_failure() -> None:
    """Test _can_run_job when one of multiple dependencies failed."""
    clear_job_registry()

    _register_job('dep1')
    _update_job_status('dep1', JobStatus.SUCCESS, 'Done')
    _register_job('dep2')
    _update_job_status('dep2', JobStatus.FAILURE, 'Failed')
    _register_job('main_job', dependencies=['dep1', 'dep2'])

    can_run, reason = _can_run_job('main_job')
    assert can_run is False


def test_job_registry_clear() -> None:
    """Test that clear_job_registry empties the registry."""
    clear_job_registry()

    _register_job('test_job')
    _update_job_status('test_job', JobStatus.SUCCESS)

    assert len(get_job_status_summary()) > 0

    clear_job_registry()

    assert get_job_status_summary() == {}


def test_update_job_status_with_details() -> None:
    """Test _update_job_status with details dict."""
    clear_job_registry()

    _register_job('test_job')
    _update_job_status('test_job', JobStatus.SUCCESS, 'Done', details={'key': 'value'})

    result = get_job_status_summary()
    assert result['test_job']['details'] == {'key': 'value'}


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
