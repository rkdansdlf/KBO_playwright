"""Tests for quality_gate and standings_integrity validators."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from src.validators.quality_gate import AGGREGATE_TEAM_CODES, INVALID_TEAM_CODES, QualityGate
from src.validators.standings_integrity import validate_standings_integrity


class TestQualityGateConstants:
    def test_aggregate_codes(self) -> None:
        assert "합계" in AGGREGATE_TEAM_CODES
        assert "TOTAL" in AGGREGATE_TEAM_CODES
        assert "" in AGGREGATE_TEAM_CODES

    def test_invalid_codes_include_all(self) -> None:
        for code in AGGREGATE_TEAM_CODES:
            assert code in INVALID_TEAM_CODES
        assert "EA" in INVALID_TEAM_CODES
        assert "WE" in INVALID_TEAM_CODES


class TestQualityGateResult:
    def test_result_ok(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate._result(season=2025, league="REGULAR", checked_players=100)
        assert result["ok"] is True
        assert result["error"] is None

    def test_result_with_error(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate._result(season=2025, league="REGULAR", error="No data")
        assert result["ok"] is False
        assert result["error"] == "No data"

    def test_result_with_mismatches(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate._result(
            season=2025,
            league="REGULAR",
            mismatches=[{"player_id": 1, "field": "HR"}],
        )
        assert result["ok"] is False
        assert len(result["mismatches"]) == 1


class TestQualityGateValidation:
    def test_validate_season_batting_no_season_ids(self) -> None:
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []

        gate = QualityGate(session)
        result = gate.validate_season_batting(2099)

        assert result["ok"] is False
        assert "No Regular Season IDs" in result["error"]

    def test_validate_season_batting_wrong_league(self) -> None:
        session = MagicMock()
        gate = QualityGate(session)
        result = gate.validate_season_batting(2025, league="POSTSEASON")

        assert result["ok"] is True
        assert result["league"] == "POSTSEASON"


class TestStandingsIntegrity:
    def test_historical_date_skipped(self) -> None:
        session = MagicMock()
        result = validate_standings_integrity(session, date(2019, 6, 15))

        assert result["ok"] is True
        assert result["checked_teams"] == 0

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
