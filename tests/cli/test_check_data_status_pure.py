"""Unit tests for check_data_status pure functions."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.cli.check_data_status import (
    _collect_status_warnings,
    _env_enabled,
    _validate_schedule_counts,
)


class TestValidateScheduleCounts:
    def test_all_above_threshold(self) -> None:
        type_counts = {"preseason": 50, "regular": 730, "postseason": 10}
        result = _validate_schedule_counts(790, 0, type_counts)
        assert result == []

    def test_preseason_below(self) -> None:
        type_counts = {"preseason": 30, "regular": 720, "postseason": 7}
        result = _validate_schedule_counts(757, 0, type_counts)
        assert len(result) == 1
        assert "preseason" in result[0]
        assert "30 < 42" in result[0]

    def test_regular_below(self) -> None:
        type_counts = {"preseason": 42, "regular": 700, "postseason": 7}
        result = _validate_schedule_counts(749, 0, type_counts)
        assert len(result) == 1
        assert "regular" in result[0]

    def test_postseason_below(self) -> None:
        type_counts = {"preseason": 42, "regular": 720, "postseason": 3}
        result = _validate_schedule_counts(765, 0, type_counts)
        assert len(result) == 1
        assert "postseason: 3 < 7" in result[0]

    def test_operational_fallback(self) -> None:
        result = _validate_schedule_counts(0, 700, {})
        assert result == []

    def test_multiple_below(self) -> None:
        type_counts = {"preseason": 10, "regular": 100, "postseason": 1}
        result = _validate_schedule_counts(111, 0, type_counts)
        assert len(result) == 3

    def test_missing_season_type_counted_as_zero(self) -> None:
        type_counts = {}
        result = _validate_schedule_counts(0, 0, type_counts)
        assert len(result) == 3


class TestEnvEnabled:
    @pytest.mark.parametrize("value", ["1", "true", "yes", "on", "enabled"])
    def test_enabled_values(self, value: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_VAR", value)
        assert _env_enabled("TEST_VAR", "1") is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off"])
    def test_disabled_values(self, value: str, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_VAR", value)
        assert _env_enabled("TEST_VAR", "1") is False

    def test_default_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_VAR", raising=False)
        assert _env_enabled("TEST_VAR", "1") is True

    def test_default_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_VAR", raising=False)
        assert _env_enabled("TEST_VAR", "0") is False

    def test_whitespace_stripped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_VAR", "  true  ")
        assert _env_enabled("TEST_VAR", "1") is True


class TestCollectStatusWarnings:
    def test_no_warnings(self) -> None:
        schedule_stats = {"total": 700, "warnings": []}
        futures_stats = {"batting": 100}
        pregame_pitcher_stats = {
            "preview_missing_starters": 0,
            "sync_candidate_games": 0,
            "oci_sync_ready": True,
        }
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert result == []

    def test_no_schedules(self) -> None:
        schedule_stats = {"total": 0, "warnings": []}
        futures_stats = {"batting": 100}
        pregame_pitcher_stats = {}
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert result == ["No schedules found"]

    def test_schedule_warnings_extended(self) -> None:
        schedule_stats = {
            "total": 700,
            "warnings": ["preseason: 30 < 42"],
        }
        futures_stats = {"batting": 100}
        pregame_pitcher_stats = {}
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert result == ["preseason: 30 < 42"]

    def test_futures_missing(self) -> None:
        schedule_stats = {"total": 700, "warnings": []}
        futures_stats = {"batting": 0}
        pregame_pitcher_stats = {}
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert "No Futures batting data found" in result

    def test_pregame_missing_starters(self) -> None:
        schedule_stats = {"total": 700, "warnings": []}
        futures_stats = {"batting": 100}
        pregame_pitcher_stats = {
            "preview_missing_starters": 5,
            "sync_candidate_games": 0,
            "oci_sync_ready": True,
        }
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert "pitcher fields are missing" in result[0]

    def test_oci_not_ready(self) -> None:
        schedule_stats = {"total": 700, "warnings": []}
        futures_stats = {"batting": 100}
        pregame_pitcher_stats = {
            "preview_missing_starters": 0,
            "sync_candidate_games": 10,
            "oci_sync_ready": False,
        }
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert "OCI sync is not ready" in result[0]

    def test_multiple_warnings(self) -> None:
        schedule_stats = {
            "total": 0,
            "warnings": ["regular: 700 < 720"],
        }
        futures_stats = {"batting": 0}
        pregame_pitcher_stats = {
            "preview_missing_starters": 3,
            "sync_candidate_games": 5,
            "oci_sync_ready": False,
        }
        result = _collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats)
        assert len(result) == 5
        assert "No schedules found" in result
        assert "regular: 700 < 720" in result
        assert "No Futures batting data found" in result
