"""Fault Injection & Mutation Testing Suite for KBO Data Quality Validators.

Verifies the anti-fragility and regression detection capability of all quality layers
by deliberately injecting corrupted, mutated, and anomalous data payloads.
"""

from __future__ import annotations

import pytest

from src.monitoring.crawler_selector_gate import (
    SelectorCheck,
    SelectorTarget,
    evaluate_html_target,
)
from src.validators.golden_game_oracle import GoldenGameOracle, verify_game_against_oracle
from src.validators.lineup_rules import validate_lineup_appearances
from src.validators.pbp_state_machine import (
    reconcile_pbp_with_boxscore,
    validate_pbp_state_machine,
)
from src.validators.rules.batting_rules import (
    validate_hits_le_at_bats,
    validate_pa_components_formula,
)
from src.validators.rules.pitching_rules import (
    parse_ip_to_outs,
    validate_earned_runs_le_runs,
    validate_innings_outs_consistency,
)
from src.validators.source_manifest_census import compare_source_manifest_against_db
from src.validators.stat_validator import ValidationSeverity


class TestFaultInjectionLayer0SourceCensus:
    def test_missing_game_injection_detected(self) -> None:
        """Inject a missing parent game in DB and ensure census catches it."""
        source_manifest = {
            "20240501LGHH0": {"game_id": "20240501LGHH0", "status": "COMPLETED"},
            "20240501NCSS0": {"game_id": "20240501NCSS0", "status": "COMPLETED"},
            "20240501KTOB0": {"game_id": "20240501KTOB0", "status": "COMPLETED"},
        }
        # Injected corruption: 20240501NCSS0 was dropped by crawler
        db_games = [
            {"game_id": "20240501LGHH0", "status": "COMPLETED"},
            {"game_id": "20240501KTOB0", "status": "COMPLETED"},
        ]
        report = compare_source_manifest_against_db(source_manifest, db_games, year=2024)
        assert report.ok is False
        assert "20240501NCSS0" in report.missing_in_db
        assert report.coverage_ratio < 1.0


class TestFaultInjectionLayer3LineupAppearance:
    def test_starter_missing_batting_injection_detected(self) -> None:
        """Inject starter with no batting record and ensure LIN-001 catches it."""
        lineup = [
            {"player_id": 101, "player_name": "선발타자", "batting_order": 3, "is_starter": 1, "position": "3B"},
        ]
        batting_mutated: list[dict] = []  # Corrupted: batting row missing
        pitching: list[dict] = []

        results = validate_lineup_appearances(lineup, batting_mutated, pitching, game_id="20240501LGHH0")
        assert len(results) == 1
        assert results[0].rule_id == "LIN-001"
        assert results[0].severity == ValidationSeverity.ERROR


class TestFaultInjectionLayer4PBPStateMachine:
    def test_score_decrease_mutation_detected(self) -> None:
        """Inject a score regression (e.g. away score 4 -> 3) and ensure PBP-005 catches it."""
        mutated_events = [
            {"event_seq": 1, "inning": 1, "half": "TOP", "outs": 0, "away_score": 4, "home_score": 0},
            {"event_seq": 2, "inning": 1, "half": "TOP", "outs": 1, "away_score": 3, "home_score": 0},  # Mutated
        ]
        report = validate_pbp_state_machine(mutated_events, game_id="20240501LGHH0")
        assert report.is_valid is False
        assert any(v.rule_id == "PBP-005" for v in report.violations)

    def test_out_count_overflow_mutation_detected(self) -> None:
        """Inject an out count > 3 and ensure PBP-004 catches it."""
        mutated_events = [
            {"event_seq": 1, "inning": 2, "half": "BOT", "outs": 4, "away_score": 1, "home_score": 1},  # Mutated
        ]
        report = validate_pbp_state_machine(mutated_events, game_id="20240501LGHH0")
        assert report.is_valid is False
        assert any(v.rule_id == "PBP-004" for v in report.violations)

    def test_pbp_walk_off_terminal_state_accepted(self) -> None:
        """Verify that a walk-off hit with 1 out in 9th/extra innings does NOT trigger false out errors."""
        walk_off_events = [
            {"event_seq": 1, "inning": 9, "half": "BOT", "outs": 0, "away_score": 3, "home_score": 3},
            {"event_seq": 2, "inning": 9, "half": "BOT", "outs": 1, "away_score": 3, "home_score": 3},
            {"event_seq": 3, "inning": 9, "half": "BOT", "outs": 1, "away_score": 3, "home_score": 4},  # Walk-off!
        ]
        report = validate_pbp_state_machine(walk_off_events, game_id="20240501LGHH0", expected_final_score=(3, 4))
        assert report.is_valid is True
        assert len(report.violations) == 0


class TestFaultInjectionLayer5CrossReconciliation:
    def test_strikeout_mutation_detected(self) -> None:
        """Inject strikeout mismatch between pitcher and batting sum and ensure REC-001 catches it."""
        batting = [{"strikeouts": 9}]
        pitching_mutated = [{"strikeouts": 12}]  # Mutated SO
        results = reconcile_pbp_with_boxscore([{"event_seq": 1}], batting, pitching_mutated, game_id="20240501LGHH0")
        assert len(results) == 1
        assert results[0].rule_id == "REC-001"
        assert results[0].severity == ValidationSeverity.ERROR


class TestFaultInjectionBaseballFormulas:
    def test_hits_greater_than_ab_mutation_detected(self) -> None:
        """Inject Hits > At-Bats (impossible baseball stat) and ensure BAT-001 catches it."""
        mutated_record = {"hits": 4, "at_bats": 3}
        results = validate_hits_le_at_bats(mutated_record)
        assert len(results) == 1
        assert results[0].rule_id == "BAT-001"
        assert results[0].severity == ValidationSeverity.ERROR

    def test_earned_runs_greater_than_runs_mutation_detected(self) -> None:
        """Inject ER > R (impossible pitching stat) and ensure PIT-001 catches it."""
        mutated_record = {"earned_runs": 5, "runs_allowed": 3}
        results = validate_earned_runs_le_runs(mutated_record)
        assert len(results) == 1
        assert results[0].rule_id == "PIT-001"
        assert results[0].severity == ValidationSeverity.ERROR

    def test_pa_formula_large_unexplained_gap_mutation_detected(self) -> None:
        """Inject large unexplained gap in PA (e.g. PA 6 != 3AB + 0BB) and ensure BAT-004 raises ERROR."""
        mutated_record = {
            "plate_appearances": 6,
            "at_bats": 3,
            "walks": 0,
            "hbp": 0,
            "sacrifice_hits": 0,
            "sacrifice_flies": 0,
        }
        results = validate_pa_components_formula(mutated_record)
        assert len(results) == 1
        assert results[0].rule_id == "BAT-004"
        assert results[0].severity == ValidationSeverity.ERROR


class TestFaultInjectionLayer7SemanticSelectorGate:
    def test_corrupted_header_mutation_detected(self) -> None:
        """Inject DOM shift missing required '타율' column and ensure header_missing is caught."""
        corrupted_html = "<table><thead><tr><th>타순</th><th>이름</th><th>타수</th><th>안타</th></tr></thead></table>"
        target = SelectorTarget(
            name="boxscore_headers",
            source="inline",
            source_type="inline",
            checks=[
                SelectorCheck(
                    name="headers",
                    selector="th",
                    min_count=4,
                    required_headers=("타순", "이름", "타수", "타율"),  # "타율" is missing!
                ),
            ],
        )
        result = evaluate_html_target(target, corrupted_html)
        assert result.ok is False
        assert any(i.category == "header_missing" for i in result.issues)


class TestFaultInjectionGoldenGameOracle:
    def test_golden_game_oracle_mismatch_detected(self) -> None:
        """Verify that parsed game deviating from Golden Oracle triggers failure."""
        oracle = GoldenGameOracle(
            game_id="20240405LGKT0",
            season=2024,
            category="standard_9inn",
            description="Standard 9-inning regular season game",
            home_team="KT",
            away_team="LG",
            final_score=(7, 8),
            innings=9,
            total_outs=54,
            winning_team="KT",
            lineup_player_count=20,
            batting_totals={"hits": 15},
        )
        # Mutated parsed game with wrong score (6, 8) instead of (7, 8)
        mutated_parsed = {
            "away_score": 6,
            "home_score": 8,
            "total_outs": 54,
            "lineups": [{} for _ in range(22)],
            "batting_totals": {"hits": 15},
        }
        violations = verify_game_against_oracle(mutated_parsed, oracle)
        assert len(violations) == 1
        assert violations[0].rule_id == "GOL-001"
