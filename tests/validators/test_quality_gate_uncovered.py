from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.constants import IP_FRAC_THIRD, IP_FRAC_TWO_THIRDS, MAX_OUTS
from src.validators.quality_gate import QualityGate, run_quality_gate


class TestResolvePitchingCumulativeOuts:
    def test_innings_outs_direct(self):
        row = MagicMock()
        row.innings_outs = 150
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 150

    def test_extra_stats_innings_outs(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {"innings_outs": 120}
        row.innings_pitched = None
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 120

    def test_innings_pitched_whole(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = None
        row.innings_pitched = 5.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 15

    def test_innings_pitched_one_third(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = None
        row.innings_pitched = 5.33
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 16

    def test_innings_pitched_two_thirds(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = None
        row.innings_pitched = 5.66
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 17

    def test_innings_pitched_none(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = None
        row.innings_pitched = None
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result is None

    def test_innings_pitched_zero(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = None
        row.innings_pitched = 0.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 0

    def test_extra_stats_takes_precedence_over_ip(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {"innings_outs": 99}
        row.innings_pitched = 5.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 99

    def test_empty_extra_stats_falls_through(self):
        row = MagicMock()
        row.innings_outs = None
        row.extra_stats = {}
        row.innings_pitched = 3.0
        result = QualityGate._resolve_pitching_cumulative_outs(row)
        assert result == 9


class TestPitchingOutsMismatch:
    def test_no_mismatch_within_tolerance(self):
        row = MagicMock()
        row.player_id = 1
        row.outs = 150
        result = QualityGate._pitching_outs_mismatch(row, 148)
        assert result is None

    def test_mismatch_exceeds_max_outs(self):
        row = MagicMock()
        row.player_id = 1
        row.outs = 200
        result = QualityGate._pitching_outs_mismatch(row, 100)
        assert result is not None
        assert result["player_id"] == 1
        assert result["issue"] == "Transactional Outs > Cumulative Outs"

    def test_mismatch_within_one_percent(self):
        row = MagicMock()
        row.player_id = 1
        row.outs = 606
        result = QualityGate._pitching_outs_mismatch(row, 600)
        assert result is None

    def test_mismatch_exceeds_one_percent(self):
        row = MagicMock()
        row.player_id = 2
        row.outs = 610
        result = QualityGate._pitching_outs_mismatch(row, 600)
        assert result is not None
        assert result["player_id"] == 2

    def test_none_cumulative_outs_with_big_diff(self):
        row = MagicMock()
        row.player_id = 3
        row.outs = 100
        result = QualityGate._pitching_outs_mismatch(row, None)
        assert result is not None

    def test_none_cumulative_outs_with_small_diff(self):
        row = MagicMock()
        row.player_id = 3
        row.outs = 2
        result = QualityGate._pitching_outs_mismatch(row, None)
        assert result is None

    def test_zero_cumulative_outs(self):
        row = MagicMock()
        row.player_id = 4
        row.outs = 10
        result = QualityGate._pitching_outs_mismatch(row, 0)
        assert result is not None


class TestMissingPitchingCumulativeRecord:
    def test_returns_dict(self):
        row = MagicMock()
        row.player_id = 42
        row.outs = 100
        row.wins = 5
        result = QualityGate._missing_pitching_cumulative_record(row)
        assert result["player_id"] == 42
        assert result["issue"] == "Missing cumulative record"
        assert result["transactional"] == {"outs": 100, "wins": 5}


class TestValidTeamCodeFilters:
    def test_returns_tuple(self):
        from src.models.player import PlayerSeasonBatting

        filters = QualityGate._valid_team_code_filters(PlayerSeasonBatting)
        assert isinstance(filters, tuple)
        assert len(filters) == 3


def _mock_execute_result(rows):
    result = MagicMock()
    result.all.return_value = rows
    result.scalars.return_value.all.return_value = [int(r) for r in rows] if rows and isinstance(rows[0], int) else rows
    return result


class TestValidateSeasonBatting:
    def test_with_cumulative_player_match(self):
        session = MagicMock()
        cum_row = MagicMock()
        cum_row.player_id = 1
        cum_row.plate_appearances = 500
        cum_row.hits = 130
        cum_row.runs = 70
        cum_row.home_runs = 15
        trans_row = MagicMock()
        trans_row.player_id = 1
        trans_row.pa = 500
        trans_row.hits = 130
        trans_row.runs = 70
        trans_row.hr = 15
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([cum_row]),
            _mock_execute_result([trans_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_batting(2025)
        assert result["ok"] is True
        assert result["checked_players"] == 1

    def test_with_null_player_id_skipped(self):
        session = MagicMock()
        cum_row = MagicMock()
        cum_row.player_id = 1
        cum_row.plate_appearances = 500
        cum_row.hits = 130
        cum_row.runs = 70
        cum_row.home_runs = 15
        trans_null = MagicMock()
        trans_null.player_id = None
        trans_null.pa = 100
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([cum_row]),
            _mock_execute_result([trans_null]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_batting(2025)
        assert result["ok"] is True

    def test_missing_cumulative_record(self):
        session = MagicMock()
        trans_row = MagicMock()
        trans_row.player_id = 99
        trans_row.pa = 200
        trans_row.hits = 50
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([]),
            _mock_execute_result([trans_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_batting(2025)
        assert result["ok"] is False
        assert any(m["issue"] == "Missing cumulative record" for m in result["mismatches"])

    def test_pa_mismatch_detected(self):
        session = MagicMock()
        cum_row = MagicMock()
        cum_row.player_id = 1
        cum_row.plate_appearances = 400
        cum_row.hits = 100
        cum_row.runs = 50
        cum_row.home_runs = 10
        trans_row = MagicMock()
        trans_row.player_id = 1
        trans_row.pa = 500
        trans_row.hits = 130
        trans_row.runs = 70
        trans_row.hr = 15
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([cum_row]),
            _mock_execute_result([trans_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_batting(2025)
        assert result["ok"] is False
        assert any("Transactional PA > Cumulative PA" in m["issue"] for m in result["mismatches"])


class TestValidateSeasonPitching:
    def test_with_no_season_ids(self):
        session = MagicMock()
        session.execute.return_value = _mock_execute_result([])
        gate = QualityGate(session)
        result = gate.validate_season_pitching(2099)
        assert result["ok"] is False
        assert "No Regular Season IDs" in result["error"]

    def test_wrong_league(self):
        session = MagicMock()
        gate = QualityGate(session)
        result = gate.validate_season_pitching(2025, league="POSTSEASON")
        assert result["ok"] is True
        assert result["league"] == "POSTSEASON"

    def test_missing_cumulative_pitching_record(self):
        session = MagicMock()
        trans_row = MagicMock()
        trans_row.player_id = 99
        trans_row.outs = 100
        trans_row.wins = 5
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([]),
            _mock_execute_result([trans_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pitching(2025)
        assert result["ok"] is False
        assert any(m["issue"] == "Missing cumulative record" for m in result["mismatches"])

    def test_null_player_id_skipped(self):
        session = MagicMock()
        cum_row = MagicMock()
        cum_row.player_id = 1
        cum_row.innings_outs = 150
        cum_row.innings_pitched = None
        cum_row.extra_stats = None
        trans_null = MagicMock()
        trans_null.player_id = None
        trans_null.outs = 100
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([cum_row]),
            _mock_execute_result([trans_null]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pitching(2025)
        assert result["ok"] is True

    def test_pitching_outs_mismatch_detected(self):
        session = MagicMock()
        cum_row = MagicMock()
        cum_row.player_id = 1
        cum_row.innings_outs = 100
        cum_row.innings_pitched = None
        cum_row.extra_stats = None
        trans_row = MagicMock()
        trans_row.player_id = 1
        trans_row.outs = 500
        trans_row.wins = 20
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([cum_row]),
            _mock_execute_result([trans_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pitching(2025)
        assert result["ok"] is False

    def test_pitching_outs_match_no_mismatch(self):
        session = MagicMock()
        cum_row = MagicMock()
        cum_row.player_id = 1
        cum_row.innings_outs = 600
        cum_row.innings_pitched = None
        cum_row.extra_stats = None
        trans_row = MagicMock()
        trans_row.player_id = 1
        trans_row.outs = 603
        trans_row.wins = 15
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([cum_row]),
            _mock_execute_result([trans_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pitching(2025)
        assert result["ok"] is True


class TestValidateSeasonPaFormula:
    def test_wrong_league(self):
        session = MagicMock()
        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025, league="POSTSEASON")
        assert result["ok"] is True
        assert result["league"] == "POSTSEASON"

    def test_no_season_ids(self):
        session = MagicMock()
        session.execute.return_value = _mock_execute_result([])
        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2099)
        assert result["ok"] is False
        assert "No Regular Season IDs" in result["error"]

    def test_pa_formula_match(self):
        session = MagicMock()
        row = MagicMock()
        row.player_id = 1
        row.pa = 500
        row.ab = 420
        row.bb = 50
        row.hbp = 10
        row.sh = 12
        row.sf = 8
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)
        assert result["ok"] is True
        assert result["checked_players"] == 1

    def test_pa_formula_mismatch(self):
        session = MagicMock()
        row = MagicMock()
        row.player_id = 1
        row.pa = 500
        row.ab = 420
        row.bb = 50
        row.hbp = 10
        row.sh = 10
        row.sf = 5
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)
        assert result["ok"] is False
        assert any(m["issue"] == "PA formula mismatch" for m in result["mismatches"])

    def test_null_player_id_skipped(self):
        session = MagicMock()
        row = MagicMock()
        row.player_id = None
        row.pa = 100
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_pa_formula(2025)
        assert result["ok"] is True


BATTING_STAT_FIELDS = [
    "plate_appearances",
    "at_bats",
    "runs",
    "hits",
    "doubles",
    "triples",
    "home_runs",
    "rbi",
    "stolen_bases",
    "caught_stealing",
    "walks",
    "strikeouts",
    "intentional_walks",
    "hbp",
    "sacrifice_hits",
    "sacrifice_flies",
    "gdp",
]

PITCHING_STAT_FIELDS = [
    "wins",
    "losses",
    "saves",
    "holds",
    "runs_allowed",
    "earned_runs",
    "hits_allowed",
    "home_runs_allowed",
    "walks_allowed",
    "strikeouts",
    "innings_outs",
    "intentional_walks",
    "hit_batters",
    "tbf",
    "complete_games",
    "shutouts",
    "wild_pitches",
    "balks",
    "sacrifices_allowed",
    "sacrifice_flies_allowed",
]


def _set_attrs(mock_obj, attrs_dict):
    for k, v in attrs_dict.items():
        setattr(mock_obj, k, v)


class TestValidateSeasonTeamBatting:
    def test_wrong_league(self):
        session = MagicMock()
        gate = QualityGate(session)
        result = gate.validate_season_team_batting(2025, league="POSTSEASON")
        assert result["ok"] is True
        assert result["league"] == "POSTSEASON"

    def test_no_season_ids(self):
        session = MagicMock()
        session.execute.return_value = _mock_execute_result([])
        gate = QualityGate(session)
        result = gate.validate_season_team_batting(2099)
        assert result["ok"] is False
        assert "No Regular Season IDs" in result["error"]

    def test_empty_team_map(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "ZZ"
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_batting(2025)
        assert result["ok"] is True

    def test_no_player_records_for_team(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        stat_vals = dict.fromkeys(BATTING_STAT_FIELDS, 100)
        _set_attrs(team_row, stat_vals)
        player_agg_row = MagicMock()
        player_agg_row.team_code = "SS"
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_agg_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_batting(2025)
        assert result["ok"] is False
        assert any("No player season batting" in m["issue"] for m in result["mismatches"])

    def test_team_batting_mismatch_detected(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        team_row.games = 144
        team_stats = {
            "plate_appearances": 6000,
            "at_bats": 5000,
            "runs": 700,
            "hits": 1500,
            "doubles": 300,
            "triples": 40,
            "home_runs": 150,
            "rbi": 700,
            "stolen_bases": 80,
            "caught_stealing": 30,
            "walks": 800,
            "strikeouts": 1100,
            "intentional_walks": 50,
            "hbp": 60,
            "sacrifice_hits": 70,
            "sacrifice_flies": 40,
            "gdp": 120,
        }
        _set_attrs(team_row, team_stats)
        player_row = MagicMock()
        player_row.team_code = "DB"
        player_stats = dict(team_stats)
        player_stats["hits"] = 1400
        _set_attrs(player_row, player_stats)
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_batting(2025)
        assert result["ok"] is False
        assert any("Team batting stats mismatch" in m["issue"] for m in result["mismatches"])

    def test_team_batting_match_no_mismatch(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        team_row.games = 144
        stat_vals = dict.fromkeys(BATTING_STAT_FIELDS, 100)
        _set_attrs(team_row, stat_vals)
        player_row = MagicMock()
        player_row.team_code = "DB"
        _set_attrs(player_row, stat_vals)
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_batting(2025)
        assert result["ok"] is True


class TestValidateSeasonTeamPitching:
    def test_wrong_league(self):
        session = MagicMock()
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2025, league="POSTSEASON")
        assert result["ok"] is True

    def test_no_season_ids(self):
        session = MagicMock()
        session.execute.return_value = _mock_execute_result([])
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2099)
        assert result["ok"] is False

    def test_empty_team_map(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "ZZ"
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2025)
        assert result["ok"] is True

    def test_no_player_records_for_team(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        team_row.innings_pitched = 1200.0
        team_row.innings_outs = 3600
        stat_vals = {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"}
        _set_attrs(team_row, stat_vals)
        player_agg_row = MagicMock()
        player_agg_row.team_code = "SS"
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_agg_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2025)
        assert result["ok"] is False
        assert any("No player season pitching" in m["issue"] for m in result["mismatches"])

    def test_team_pitching_match(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        team_row.innings_pitched = 300.0
        team_row.innings_outs = 900
        stat_vals = {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"}
        _set_attrs(team_row, stat_vals)
        player_row = MagicMock()
        player_row.team_code = "DB"
        player_row.innings_outs = 900
        _set_attrs(player_row, {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"})
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2025)
        assert result["ok"] is True

    def test_innings_pitched_mismatch(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        team_row.innings_pitched = 0.0
        team_row.innings_outs = 0
        stat_vals = {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"}
        _set_attrs(team_row, stat_vals)
        player_row = MagicMock()
        player_row.team_code = "DB"
        player_row.innings_outs = 500
        _set_attrs(player_row, {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"})
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2025)
        assert result["ok"] is False

    def test_stat_field_mismatch(self):
        session = MagicMock()
        team_row = MagicMock()
        team_row.team_id = "DB"
        team_row.innings_pitched = 300.0
        team_row.innings_outs = 900
        team_stats = {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"}
        team_stats["wins"] = 200
        _set_attrs(team_row, team_stats)
        player_row = MagicMock()
        player_row.team_code = "DB"
        player_row.innings_outs = 900
        player_stats = {f: 100 for f in PITCHING_STAT_FIELDS if f != "innings_outs"}
        player_stats["wins"] = 50
        _set_attrs(player_row, player_stats)
        session.execute.side_effect = [
            _mock_execute_result([1]),
            _mock_execute_result([team_row]),
            _mock_execute_result([player_row]),
        ]
        gate = QualityGate(session)
        result = gate.validate_season_team_pitching(2025)
        assert result["ok"] is False


class TestRunQualityGate:
    def test_all_ok(self):
        session = MagicMock()
        with patch.object(QualityGate, "validate_season_batting", return_value={"ok": True}):
            with patch.object(QualityGate, "validate_season_pitching", return_value={"ok": True}):
                with patch.object(QualityGate, "validate_season_pa_formula", return_value={"ok": True}):
                    with patch.object(QualityGate, "validate_season_team_batting", return_value={"ok": True}):
                        with patch.object(QualityGate, "validate_season_team_pitching", return_value={"ok": True}):
                            result = run_quality_gate(session, 2025)
        assert result["ok"] is True

    def test_any_failure_means_not_ok(self):
        session = MagicMock()
        with patch.object(QualityGate, "validate_season_batting", return_value={"ok": False}):
            with patch.object(QualityGate, "validate_season_pitching", return_value={"ok": True}):
                with patch.object(QualityGate, "validate_season_pa_formula", return_value={"ok": True}):
                    with patch.object(QualityGate, "validate_season_team_batting", return_value={"ok": True}):
                        with patch.object(QualityGate, "validate_season_team_pitching", return_value={"ok": True}):
                            result = run_quality_gate(session, 2025)
        assert result["ok"] is False

    def test_returns_all_keys(self):
        session = MagicMock()
        with patch.object(QualityGate, "validate_season_batting", return_value={"ok": True}):
            with patch.object(QualityGate, "validate_season_pitching", return_value={"ok": True}):
                with patch.object(QualityGate, "validate_season_pa_formula", return_value={"ok": True}):
                    with patch.object(QualityGate, "validate_season_team_batting", return_value={"ok": True}):
                        with patch.object(QualityGate, "validate_season_team_pitching", return_value={"ok": True}):
                            result = run_quality_gate(session, 2025)
        for key in ("batting", "pitching", "pa_formula", "team_batting", "team_pitching", "ok"):
            assert key in result
