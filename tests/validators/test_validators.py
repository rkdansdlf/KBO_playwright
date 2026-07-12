"""Tests for src.validators modules."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.validators.game_data_validator import validate_game_data
from src.validators.quality_gate import (
    AGGREGATE_TEAM_CODES,
    INVALID_TEAM_CODES,
    QualityGate,
    _batting_pa_mismatch,
    _team_stat_mismatch,
)
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


class TestGameDataValidator:
    def test_valid_game_data(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {
                "home": {"code": "LG", "name": "LG 트윈스"},
                "away": {"code": "SSG", "name": "SSG 랜더스"},
            },
            "hitters": {"home": [{"name": "김철수"}], "away": [{"name": "이영희"}]},
            "pitchers": {"home": [{"name": "박민수"}], "away": [{"name": "최지�"}]},
        }
        is_valid, errors, warnings = validate_game_data(game_data)
        assert is_valid is True
        assert len(errors) == 0

    def test_missing_game_id(self) -> None:
        game_data = {
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert "Missing game_id" in errors

    def test_missing_game_date(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert "Missing game_date" in errors

    def test_missing_team_code(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {}, "away": {}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("home team code" in e for e in errors)
        assert any("away team code" in e for e in errors)

    def test_invalid_team_code(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "INVALID"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("Invalid home team code" in e for e in errors)

    def test_missing_hitter_rows(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [], "away": [{}]},
            "pitchers": {"home": [{}], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("No hitter rows for home" in e for e in errors)

    def test_missing_pitcher_rows(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {"home": {"code": "LG"}, "away": {"code": "SSG"}},
            "hitters": {"home": [{}], "away": [{}]},
            "pitchers": {"home": [], "away": [{}]},
        }
        is_valid, errors, _ = validate_game_data(game_data)
        assert is_valid is False
        assert any("No pitcher rows for home" in e for e in errors)

    def test_line_score_mismatch(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {
                "home": {"code": "LG", "score": 5, "line_score": [1, 2, 3]},
                "away": {"code": "SSG", "score": 3, "line_score": [1, 1]},
            },
            "hitters": {
                "home": [{"stats": {"runs": 1}}],
                "away": [{"stats": {"runs": 1}}],
            },
            "pitchers": {"home": [{"stats": {}}], "away": [{"stats": {}}]},
        }
        _, _, warnings = validate_game_data(game_data)
        assert any("home line score" in w for w in warnings)

    def test_hitter_mismatch(self) -> None:
        game_data = {
            "game_id": "20260624LGSS0",
            "game_date": "2026-06-24",
            "teams": {
                "home": {"code": "LG", "score": 5, "line_score": [2, 3]},
                "away": {"code": "SSG", "score": 3, "line_score": [1, 1, 1]},
            },
            "hitters": {
                "home": [{"stats": {"runs": 10}}],
                "away": [{"stats": {"runs": 1}}],
            },
            "pitchers": {"home": [{"stats": {}}], "away": [{"stats": {}}]},
        }
        _, _, warnings = validate_game_data(game_data)
        assert any("home hitter runs" in w for w in warnings)


class TestQualityGatePureFunctions:
    def test_batting_pa_mismatch_respects_absolute_and_relative_tolerances(self) -> None:
        assert _batting_pa_mismatch(2, 100) is False
        assert _batting_pa_mismatch(5, 1000) is False
        assert _batting_pa_mismatch(6, 1000) is True

    def test_team_stat_mismatch_respects_default_tolerance(self) -> None:
        assert _team_stat_mismatch(5) is False
        assert _team_stat_mismatch(6) is True

    def test_valid_team_code_filters_returns_tuple(self) -> None:
        from sqlalchemy import column

        gate = QualityGate(MagicMock())
        result = gate._valid_team_code_filters(
            type(
                "M",
                (),
                {
                    "canonical_team_code": column("ctc"),
                    "team_code": column("tc"),
                },
            ),
        )
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_resolve_pitching_cumulative_outs_direct(self) -> None:
        row = MagicMock()
        row.innings_outs = 54
        row.extra_stats = None
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 54

    def test_resolve_pitching_cumulative_outs_from_extra_stats(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {"innings_outs": 45}
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 45

    def test_resolve_pitching_cumulative_outs_from_innings_pitched(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {}
        row.innings_pitched = 6.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 18

    def test_resolve_pitching_cumulative_outs_one_third(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {}
        row.innings_pitched = 6.33
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 19

    def test_resolve_pitching_cumulative_outs_two_thirds(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {}
        row.innings_pitched = 6.66
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 20

    def test_resolve_pitching_cumulative_outs_none_ip(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {}
        row.innings_pitched = None
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result is None

    def test_resolve_pitching_cumulative_outs_zero_ip(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {}
        row.innings_pitched = 0.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 0

    def test_resolve_pitching_cumulative_outs_extra_stats_precedence(self) -> None:
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {"innings_outs": 30}
        row.innings_pitched = 5.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 30

    def test_pitching_outs_no_mismatch_within_tolerance(self) -> None:
        row = MagicMock()
        row.player_id = "p1"
        row.outs = 100
        result = QualityGate._pitching_outs_mismatch(row, 99)
        assert result is None

    def test_pitching_outs_mismatch_exceeds_max(self) -> None:
        row = MagicMock()
        row.player_id = "p1"
        row.outs = 150
        result = QualityGate._pitching_outs_mismatch(row, 100)
        assert result is not None
        assert result["issue"] == "Transactional Outs > Cumulative Outs"

    def test_pitching_outs_within_one_percent(self) -> None:
        row = MagicMock()
        row.player_id = "p1"
        row.outs = 100
        result = QualityGate._pitching_outs_mismatch(row, 99)
        assert result is None

    def test_pitching_outs_none_cumulative_outs_small_diff(self) -> None:
        row = MagicMock()
        row.player_id = "p1"
        row.outs = 3
        result = QualityGate._pitching_outs_mismatch(row, None)
        assert result is None

    def test_pitching_outs_none_cumulative_outs_big_diff(self) -> None:
        row = MagicMock()
        row.player_id = "p1"
        row.outs = 150
        result = QualityGate._pitching_outs_mismatch(row, None)
        assert result is not None

    def test_missing_pitching_cumulative_record(self) -> None:
        row = MagicMock()
        row.player_id = "p1"
        row.outs = 5
        row.wins = 2
        result = QualityGate._missing_pitching_cumulative_record(row)
        assert result["issue"] == "Missing cumulative record"


class TestQualityGateValidationNoDb:
    def test_validate_season_batting_wrong_league(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate.validate_season_batting(2025, "PLAYOFF")
        assert result["ok"] is True
        assert result["league"] == "PLAYOFF"

    def test_validate_season_pitching_wrong_league(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate.validate_season_pitching(2025, "PLAYOFF")
        assert result["ok"] is True

    def test_validate_season_pa_formula_wrong_league(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate.validate_season_pa_formula(2025, "PLAYOFF")
        assert result["ok"] is True

    def test_validate_season_team_batting_wrong_league(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate.validate_season_team_batting(2025, "PLAYOFF")
        assert result["ok"] is True

    def test_validate_season_team_pitching_wrong_league(self) -> None:
        gate = QualityGate(MagicMock())
        result = gate.validate_season_team_pitching(2025, "PLAYOFF")
        assert result["ok"] is True

    def test_validate_season_batting_no_season_ids(self) -> None:
        gate = QualityGate(MagicMock())
        gate.session.execute.return_value.scalars.return_value.all.return_value = []
        result = gate.validate_season_batting(2099)
        assert result["ok"] is False
        assert "error" in result

    def test_validate_season_pitching_no_season_ids(self) -> None:
        gate = QualityGate(MagicMock())
        gate.session.execute.return_value.scalars.return_value.all.return_value = []
        result = gate.validate_season_pitching(2099)
        assert result["ok"] is False

    def test_validate_season_pa_formula_no_season_ids(self) -> None:
        gate = QualityGate(MagicMock())
        gate.session.execute.return_value.scalars.return_value.all.return_value = []
        result = gate.validate_season_pa_formula(2099)
        assert result["ok"] is False

    def test_validate_season_team_batting_no_season_ids(self) -> None:
        gate = QualityGate(MagicMock())
        gate.session.execute.return_value.scalars.return_value.all.return_value = []
        result = gate.validate_season_team_batting(2099)
        assert result["ok"] is False

    def test_validate_season_team_pitching_no_season_ids(self) -> None:
        gate = QualityGate(MagicMock())
        gate.session.execute.return_value.scalars.return_value.all.return_value = []
        result = gate.validate_season_team_pitching(2099)
        assert result["ok"] is False


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
