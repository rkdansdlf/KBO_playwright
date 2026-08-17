"""Tests for PBP State Machine & Cross-Reconciliation Validator."""

from __future__ import annotations

import pytest

from src.validators.pbp_state_machine import (
    reconcile_pbp_with_boxscore,
    validate_pbp_state_machine,
)
from src.validators.rules.batting_rules import validate_pa_components_formula
from src.validators.rules.pitching_rules import (
    format_outs_to_ip,
    parse_ip_to_outs,
    validate_innings_outs_consistency,
)
from src.validators.stat_validator import ValidationSeverity


class TestPitchingIPOutsParsing:
    def test_parse_ip_to_outs_integers_and_floats(self) -> None:
        assert parse_ip_to_outs(5) == 15
        assert parse_ip_to_outs(5.0) == 15
        assert parse_ip_to_outs(5.1) == 16
        assert parse_ip_to_outs(5.2) == 17
        assert parse_ip_to_outs(6.0) == 18
        assert parse_ip_to_outs(5.333) == 16
        assert parse_ip_to_outs(5.667) == 17

    def test_parse_ip_to_outs_strings(self) -> None:
        assert parse_ip_to_outs("5.0") == 15
        assert parse_ip_to_outs("5.1") == 16
        assert parse_ip_to_outs("5.2") == 17
        assert parse_ip_to_outs("5 1/3") == 16
        assert parse_ip_to_outs("5 2/3") == 17
        assert parse_ip_to_outs("1/3") == 1
        assert parse_ip_to_outs("2/3") == 2
        assert parse_ip_to_outs(None) is None
        assert parse_ip_to_outs("") is None

    def test_format_outs_to_ip(self) -> None:
        assert format_outs_to_ip(15) == "5.0"
        assert format_outs_to_ip(16) == "5.1"
        assert format_outs_to_ip(17) == "5.2"
        assert format_outs_to_ip(27) == "9.0"

    def test_validate_innings_outs_consistency(self) -> None:
        valid_record = {"innings_outs": 17, "innings_pitched": "5.2"}
        assert len(validate_innings_outs_consistency(valid_record)) == 0

        mismatched_record = {"innings_outs": 16, "innings_pitched": "5.2"}
        results = validate_innings_outs_consistency(mismatched_record)
        assert len(results) == 1
        assert results[0].rule_id == "PIT-005"
        assert results[0].expected == 17
        assert results[0].actual == 16


class TestBattingPAFormulaExceptionHandling:
    def test_standard_pa_formula_exact(self) -> None:
        rec = {
            "plate_appearances": 4,
            "at_bats": 3,
            "walks": 1,
            "hbp": 0,
            "sacrifice_hits": 0,
            "sacrifice_flies": 0,
        }
        assert len(validate_pa_components_formula(rec)) == 0

    def test_pa_formula_with_catcher_interference_field(self) -> None:
        rec = {
            "plate_appearances": 4,
            "at_bats": 3,
            "walks": 0,
            "hbp": 0,
            "sacrifice_hits": 0,
            "sacrifice_flies": 0,
            "catcher_interference": 1,
        }
        assert len(validate_pa_components_formula(rec)) == 0

    def test_pa_formula_single_diff_generates_warning(self) -> None:
        rec = {
            "plate_appearances": 5,
            "at_bats": 4,
            "walks": 0,
            "hbp": 0,
            "sacrifice_hits": 0,
            "sacrifice_flies": 0,
        }
        results = validate_pa_components_formula(rec)
        assert len(results) == 1
        assert results[0].severity == ValidationSeverity.WARNING
        assert "diff=+1" in results[0].message


class TestPBPStateMachineValidator:
    def test_valid_pbp_sequence(self) -> None:
        events = [
            {"event_seq": 1, "inning": 1, "half": "TOP", "outs": 0, "away_score": 0, "home_score": 0},
            {"event_seq": 2, "inning": 1, "half": "TOP", "outs": 1, "away_score": 1, "home_score": 0},
            {"event_seq": 3, "inning": 1, "half": "TOP", "outs": 2, "away_score": 1, "home_score": 0},
            {"event_seq": 4, "inning": 1, "half": "TOP", "outs": 3, "away_score": 1, "home_score": 0},
            {"event_seq": 5, "inning": 1, "half": "BOT", "outs": 0, "away_score": 1, "home_score": 0},
            {"event_seq": 6, "inning": 1, "half": "BOT", "outs": 3, "away_score": 1, "home_score": 2},
        ]
        report = validate_pbp_state_machine(events, game_id="20240501LGHH0", expected_final_score=(1, 2))
        assert report.is_valid is True
        assert len(report.violations) == 0

    def test_duplicate_event_seq_detected(self) -> None:
        events = [
            {"event_seq": 1, "inning": 1, "half": "TOP", "outs": 0},
            {"event_seq": 1, "inning": 1, "half": "TOP", "outs": 1},
        ]
        report = validate_pbp_state_machine(events)
        assert report.is_valid is False
        assert any(v.rule_id == "PBP-001" for v in report.violations)

    def test_reverted_inning_detected(self) -> None:
        events = [
            {"event_seq": 1, "inning": 2, "half": "TOP", "outs": 1},
            {"event_seq": 2, "inning": 1, "half": "BOT", "outs": 2},
        ]
        report = validate_pbp_state_machine(events)
        assert report.is_valid is False
        assert any(v.rule_id == "PBP-002" for v in report.violations)

    def test_decreasing_score_detected(self) -> None:
        events = [
            {"event_seq": 1, "inning": 1, "half": "TOP", "away_score": 3, "home_score": 0},
            {"event_seq": 2, "inning": 1, "half": "TOP", "away_score": 2, "home_score": 0},
        ]
        report = validate_pbp_state_machine(events)
        assert report.is_valid is False
        assert any(v.rule_id == "PBP-005" for v in report.violations)

    def test_final_score_mismatch_detected(self) -> None:
        events = [
            {"event_seq": 1, "inning": 1, "half": "TOP", "away_score": 1, "home_score": 0},
            {"event_seq": 2, "inning": 9, "half": "BOT", "away_score": 2, "home_score": 3},
        ]
        report = validate_pbp_state_machine(events, expected_final_score=(2, 4))
        assert report.is_valid is False
        assert any(v.rule_id == "PBP-007" for v in report.violations)


class TestPBPBoxscoreCrossReconciliation:
    def test_strikeout_and_run_reconciliation_pass(self) -> None:
        batting = [{"strikeouts": 5, "runs": 3}, {"strikeouts": 3, "runs": 2}]
        pitching = [{"strikeouts": 4, "runs_allowed": 2}, {"strikeouts": 4, "runs_allowed": 3}]
        pbp = [{"event_seq": 1}]
        results = reconcile_pbp_with_boxscore(
            pbp,
            batting,
            pitching,
            game_id="20240501LGHH0",
            scoreboard_runs=(3, 2),
        )
        assert len(results) == 0

    def test_strikeout_mismatch_detected(self) -> None:
        batting = [{"strikeouts": 5}]
        pitching = [{"strikeouts": 8}]
        pbp = [{"event_seq": 1}]
        results = reconcile_pbp_with_boxscore(pbp, batting, pitching, game_id="20240501LGHH0")
        assert len(results) == 1
        assert results[0].rule_id == "REC-001"
        assert "Total batting SO (5) != total pitching SO (8)" in results[0].message
