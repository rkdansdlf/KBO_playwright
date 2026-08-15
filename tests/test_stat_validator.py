"""Unit tests for the Stat Validation Framework and Batting/Pitching validation rules."""

from __future__ import annotations

from src.validators import (
    ValidationSeverity,
    create_default_stat_validator,
)


def test_valid_batting_record() -> None:
    """A mathematically clean batting line should produce 0 errors and 0 warnings."""
    validator = create_default_stat_validator()
    record = {
        "player_id": 12345,
        "player_name": "홍길동",
        "plate_appearances": 4,
        "at_bats": 3,
        "hits": 2,
        "doubles": 1,
        "triples": 0,
        "home_runs": 0,
        "total_bases": 3,  # 1 single + 1 double(2) = 3
        "walks": 1,
        "hbp": 0,
        "sacrifice_hits": 0,
        "sacrifice_flies": 0,
        "avg": 0.667,
        "obp": 0.750,
    }
    results = validator.validate_batting(record)
    assert len(results) == 0


def test_bat_001_hits_exceeds_at_bats() -> None:
    """BAT-001: Hits > At Bats must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "김타자",
        "at_bats": 3,
        "hits": 4,  # Impossible!
    }
    results = validator.validate_batting(record)
    assert len(results) == 1
    res = results[0]
    assert res.rule_id == "BAT-001"
    assert res.severity == ValidationSeverity.ERROR
    assert res.is_blocking is True
    assert "exceeds At-Bats" in res.message


def test_bat_002_extra_bases_exceeds_hits() -> None:
    """BAT-002: 2B + 3B + HR > Hits must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "이타자",
        "at_bats": 4,
        "hits": 2,
        "doubles": 2,
        "home_runs": 1,  # 2 + 1 = 3 extra base hits > 2 hits
    }
    results = validator.validate_batting(record)
    assert any(r.rule_id == "BAT-002" and r.severity == ValidationSeverity.ERROR for r in results)


def test_bat_003_total_bases_mismatch() -> None:
    """BAT-003: Total bases mismatch must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "박타자",
        "at_bats": 4,
        "hits": 2,
        "doubles": 1,  # 1 single(1) + 1 double(2) = 3 TB
        "home_runs": 0,
        "total_bases": 5,  # Incorrectly recorded as 5
    }
    results = validator.validate_batting(record)
    assert any(r.rule_id == "BAT-003" and r.severity == ValidationSeverity.ERROR for r in results)


def test_bat_004_pa_formula_warning() -> None:
    """BAT-004: PA formula mismatch triggers a WARNING."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "최타자",
        "plate_appearances": 5,
        "at_bats": 3,
        "walks": 1,
        "hbp": 0,
        "sacrifice_hits": 0,
        "sacrifice_flies": 0,  # 3+1 = 4 != 5 PA
    }
    results = validator.validate_batting(record)
    assert any(r.rule_id == "BAT-004" and r.severity == ValidationSeverity.WARNING for r in results)


def test_bat_005_avg_inconsistency() -> None:
    """BAT-005: AVG significantly different from H/AB triggers a WARNING."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "정타자",
        "at_bats": 10,
        "hits": 3,  # 3/10 = 0.300
        "avg": 0.450,  # Huge mismatch
    }
    results = validator.validate_batting(record)
    assert any(r.rule_id == "BAT-005" and r.severity == ValidationSeverity.WARNING for r in results)


def test_valid_pitching_record() -> None:
    """A clean pitching line should produce 0 errors and 0 warnings."""
    validator = create_default_stat_validator()
    record = {
        "player_id": 67890,
        "player_name": "박투수",
        "innings_pitched": 6.0,
        "innings_outs": 18,
        "batters_faced": 24,
        "hits_allowed": 5,
        "home_runs_allowed": 1,
        "runs_allowed": 2,
        "earned_runs": 2,
        "walks_allowed": 2,
        "strikeouts": 7,
    }
    results = validator.validate_pitching(record)
    assert len(results) == 0


def test_pit_001_earned_runs_exceeds_runs() -> None:
    """PIT-001: ER > R must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "김투수",
        "runs_allowed": 2,
        "earned_runs": 4,  # Impossible!
    }
    results = validator.validate_pitching(record)
    assert len(results) == 1
    res = results[0]
    assert res.rule_id == "PIT-001"
    assert res.severity == ValidationSeverity.ERROR
    assert res.is_blocking is True


def test_pit_002_hits_exceeds_batters_faced() -> None:
    """PIT-002: Hits Allowed > Batters Faced must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "이투수",
        "batters_faced": 5,
        "hits_allowed": 7,  # Impossible!
    }
    results = validator.validate_pitching(record)
    assert any(r.rule_id == "PIT-002" and r.severity == ValidationSeverity.ERROR for r in results)


def test_pit_003_walks_exceeds_batters_faced() -> None:
    """PIT-003: Walks Allowed > Batters Faced must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "최투수",
        "batters_faced": 3,
        "walks_allowed": 5,  # Impossible!
    }
    results = validator.validate_pitching(record)
    assert any(r.rule_id == "PIT-003" and r.severity == ValidationSeverity.ERROR for r in results)


def test_pit_004_hr_exceeds_hits() -> None:
    """PIT-004: HR Allowed > Hits Allowed must trigger an ERROR."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "정투수",
        "hits_allowed": 1,
        "home_runs_allowed": 2,  # Impossible!
    }
    results = validator.validate_pitching(record)
    assert any(r.rule_id == "PIT-004" and r.severity == ValidationSeverity.ERROR for r in results)


def test_pit_005_innings_outs_mismatch() -> None:
    """PIT-005: Inning outs mismatch against IP notation triggers a WARNING."""
    validator = create_default_stat_validator()
    record = {
        "player_name": "한투수",
        "innings_pitched": 5.1,  # 5 full innings + 1 out = 16 outs
        "innings_outs": 15,  # Mismatch!
    }
    results = validator.validate_pitching(record)
    assert any(r.rule_id == "PIT-005" and r.severity == ValidationSeverity.WARNING for r in results)
