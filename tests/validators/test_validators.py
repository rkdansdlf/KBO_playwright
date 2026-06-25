"""Tests for src.validators modules."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.validators.quality_gate import AGGREGATE_TEAM_CODES, INVALID_TEAM_CODES, QualityGate
from src.validators.standings_integrity import validate_standings_integrity


class TestQualityGate:
    def test_aggregate_team_codes_defined(self) -> None:
        assert "합계" in AGGREGATE_TEAM_CODES
        assert "TOTAL" in AGGREGATE_TEAM_CODES
        assert "" in AGGREGATE_TEAM_CODES

    def test_invalid_team_codes_include_aggregate(self) -> None:
        for code in AGGREGATE_TEAM_CODES:
            assert code in INVALID_TEAM_CODES

    def test_invalid_team_codes_include_all_star(self) -> None:
        assert "EA" in INVALID_TEAM_CODES
        assert "WE" in INVALID_TEAM_CODES

    def test_result_ok(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate._result(season=2025, league="REGULAR", checked_players=100)

        assert result["ok"] is True
        assert result["error"] is None
        assert result["mismatches"] == []

    def test_result_with_error(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate._result(season=2025, league="REGULAR", error="DB failure")

        assert result["ok"] is False
        assert result["error"] == "DB failure"

    def test_result_with_mismatches(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate._result(
            season=2025,
            league="REGULAR",
            mismatches=[{"player_id": 1, "field": "HR"}],
        )

        assert result["ok"] is False
        assert len(result["mismatches"]) == 1


class TestStandingsIntegrity:
    def test_historical_date_skipped(self) -> None:
        session = MagicMock()
        result = validate_standings_integrity(session, date(2019, 6, 15))

        assert result["ok"] is True
        assert result["checked_teams"] == 0
        assert "Historical" in result.get("note", "")

    def test_post_season_date_skipped(self) -> None:
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.order_by.return_value.first.return_value = None

        result = validate_standings_integrity(session, date(2025, 11, 15))

        assert "ok" in result

    def test_valid_date_proceeds(self) -> None:
        session = MagicMock()
        session.query.return_value.join.return_value.filter.return_value.order_by.return_value.first.return_value = None

        result = validate_standings_integrity(session, date(2025, 6, 15))

        assert "ok" in result
        assert "checked_date" in result
