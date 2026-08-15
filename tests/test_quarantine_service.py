"""Unit tests for QuarantineService and comprehensive game detail validation."""

from __future__ import annotations

import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.quarantine import QuarantinedRecord
from src.services.quarantine_service import QuarantineService
from src.validators.game_data_validator import validate_game_detail_comprehensive
from src.validators.stat_validator import ValidationResult, ValidationSeverity


def _get_in_memory_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_quarantine_blocking_failures() -> None:
    """QuarantineService should only record blocking ERROR failures into quarantined_records."""
    session = _get_in_memory_session()
    service = QuarantineService(session)

    results = [
        ValidationResult(
            validator="batting_rules",
            rule_id="BAT-001",
            entity_type="batting",
            field_name="hits",
            expected="<= 3",
            actual=5,
            severity=ValidationSeverity.ERROR,
            game_id="20260815LGKIA0",
            entity_id=123,
            message="Hits exceeds AB",
        ),
        ValidationResult(
            validator="batting_rules",
            rule_id="BAT-004",
            entity_type="batting",
            field_name="plate_appearances",
            expected=4,
            actual=5,
            severity=ValidationSeverity.WARNING,
            game_id="20260815LGKIA0",
            message="PA formula mismatch",
        ),
    ]
    raw_payload = {"game_id": "20260815LGKIA0", "raw_test": True}

    saved = service.quarantine_validation_failures(results, raw_payload, game_id="20260815LGKIA0")
    assert len(saved) == 1
    assert saved[0].rule_id == "BAT-001"
    assert saved[0].severity == "ERROR"
    assert saved[0].status == "PENDING"

    pending = service.get_pending_quarantines(game_id="20260815LGKIA0")
    assert len(pending) == 1
    assert pending[0].entity_type == "batting"

    # Test resolving
    success = service.mark_resolved(pending[0].id, status="RECONCILED")
    assert success is True

    pending_after = service.get_pending_quarantines(game_id="20260815LGKIA0")
    assert len(pending_after) == 0


def test_validate_game_detail_comprehensive_valid() -> None:
    """A valid game payload should pass comprehensive validation."""
    game_payload = {
        "game_id": "20260815LGKIA0",
        "game_date": "2026-08-15",
        "teams": {
            "home": {"code": "KIA", "score": 3, "line_score": ["1", "0", "2", "0", "0", "0", "0", "0", "0"]},
            "away": {"code": "LG", "score": 1, "line_score": ["0", "1", "0", "0", "0", "0", "0", "0", "0"]},
        },
        "hitters": {
            "home": [
                {
                    "player_name": "김도영",
                    "player_id": 52600,
                    "stats": {"at_bats": 3, "hits": 2, "runs": 2, "doubles": 1, "home_runs": 0, "walks": 1},
                },
                {
                    "player_name": "나성범",
                    "player_id": 62900,
                    "stats": {"at_bats": 4, "hits": 1, "runs": 1, "doubles": 0, "home_runs": 1, "walks": 0},
                },
            ],
            "away": [
                {
                    "player_name": "문보경",
                    "player_id": 51100,
                    "stats": {"at_bats": 4, "hits": 1, "runs": 1, "doubles": 0, "home_runs": 0, "walks": 0},
                },
            ],
        },
        "pitchers": {
            "home": [
                {
                    "player_name": "양현종",
                    "player_id": 77637,
                    "stats": {
                        "innings_pitched": 7.0,
                        "innings_outs": 21,
                        "batters_faced": 26,
                        "runs_allowed": 1,
                        "earned_runs": 1,
                        "hits_allowed": 4,
                    },
                },
            ],
            "away": [
                {
                    "player_name": "켈리",
                    "player_id": 69102,
                    "stats": {
                        "innings_pitched": 6.0,
                        "innings_outs": 18,
                        "batters_faced": 25,
                        "runs_allowed": 3,
                        "earned_runs": 3,
                        "hits_allowed": 6,
                    },
                },
            ],
        },
    }

    is_valid, results = validate_game_detail_comprehensive(game_payload)
    assert is_valid is True
    assert not any(r.is_blocking for r in results)


def test_validate_game_detail_comprehensive_invalid_batting() -> None:
    """An impossible stat line (e.g. Hits > AB) should fail comprehensive validation."""
    game_payload = {
        "game_id": "20260815LGKIA0",
        "game_date": "2026-08-15",
        "teams": {
            "home": {"code": "KIA", "score": 2, "line_score": ["2", "0", "0"]},
            "away": {"code": "LG", "score": 0, "line_score": ["0", "0", "0"]},
        },
        "hitters": {
            "home": [
                {
                    "player_name": "김도영",
                    "stats": {"at_bats": 2, "hits": 4, "runs": 2},  # 4 Hits in 2 AB!
                },
            ],
            "away": [],
        },
        "pitchers": {"home": [], "away": []},
    }

    is_valid, results = validate_game_detail_comprehensive(game_payload, allow_partial=True)
    assert is_valid is False
    assert any(r.rule_id == "BAT-001" and r.is_blocking for r in results)
