"""Unit tests for src.pipeline.auto_healer_service."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameInningScore
from src.pipeline import auto_healer_service
from src.pipeline.auto_healer_service import AutoHealerService
from src.pipeline.dto import DefectItem, DefectReport, HealingStatus, PipelineDefectType
from src.repositories import game_status
from src.services.quality_hub import UnifiedQualityReport


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


@pytest.fixture(autouse=True)
def _pin_season(monkeypatch):
    """Fixtures use 2024 games; pin the season so the current-season guard is deterministic."""
    monkeypatch.setattr(auto_healer_service, "current_season_year", lambda today=None: 2024)
    monkeypatch.setattr(game_status, "current_season_year", lambda today=None: 2024)


def test_heal_stuck_game(db_session) -> None:
    g = Game(
        game_id="20240401LGSS0",
        game_date=date(2024, 4, 1),
        home_team="SS",
        away_team="LG",
        home_score=5,
        away_score=3,
        game_status="SCHEDULED",
    )
    db_session.add(g)
    db_session.flush()

    healer = AutoHealerService(db_session)
    summary = healer.heal_stuck_game("20240401LGSS0")

    assert summary.status == "SUCCESS"
    assert summary.action_taken == "update_status_to_COMPLETED"

    db_session.refresh(g)
    assert g.game_status == "COMPLETED"


def test_heal_score_mismatch(db_session) -> None:
    g = Game(
        game_id="20240402LGSS0",
        game_date=date(2024, 4, 2),
        home_team="SS",
        away_team="LG",
        home_score=99,  # Mismatched board score
        away_score=99,
        game_status="COMPLETED",
    )
    inn1 = GameInningScore(game_id="20240402LGSS0", team_side="home", inning=1, runs=4)
    inn2 = GameInningScore(game_id="20240402LGSS0", team_side="away", inning=1, runs=2)
    db_session.add_all([g, inn1, inn2])
    db_session.flush()

    healer = AutoHealerService(db_session)
    summary = healer.heal_score_mismatch("20240402LGSS0")

    assert summary.status == "SUCCESS"
    db_session.refresh(g)
    assert g.home_score == 4
    assert g.away_score == 2


def test_heal_from_defect_report(db_session) -> None:
    g = Game(
        game_id="20240403LGSS0",
        game_date=date(2024, 4, 3),
        home_team="SS",
        away_team="LG",
        home_score=None,
        away_score=None,
        game_status="SCHEDULED",
    )
    db_session.add(g)
    db_session.flush()

    defect = DefectItem(
        game_id="20240403LGSS0",
        defect_type=PipelineDefectType.STUCK_SCHEDULED,
    )
    report = DefectReport(target_date="2024-04-03", defects=[defect])

    healer = AutoHealerService(db_session)
    actions = healer.heal_from_defect_report(report)

    assert len(actions) == 1
    assert actions[0].status == "SUCCESS"
    db_session.refresh(g)
    assert g.game_status == "UNRESOLVED_MISSING"  # scores absent != cancelled (issue #32)


def test_heal_stuck_game_skips_pre_2001(db_session) -> None:
    """Pre-2001 rows must never be auto-healed (issue #32)."""
    g = Game(
        game_id="19950501LGSS0",
        game_date=date(1995, 5, 1),
        home_team="SS",
        away_team="LG",
        home_score=None,
        away_score=None,
        game_status="SCHEDULED",
    )
    db_session.add(g)
    db_session.flush()

    healer = AutoHealerService(db_session)
    summary = healer.heal_stuck_game("19950501LGSS0")

    assert summary.status == "SKIPPED"
    db_session.refresh(g)
    assert g.game_status == "SCHEDULED"


def test_heal_stuck_game_skips_past_season(db_session) -> None:
    """A past (post-2001) season game is SKIPPED by automatic healing and left untouched."""
    g = Game(
        game_id="20230501LGSS0",
        game_date=date(2023, 5, 1),
        home_team="SS",
        away_team="LG",
        home_score=None,
        away_score=None,
        game_status="SCHEDULED",
    )
    db_session.add(g)
    db_session.flush()

    summary = AutoHealerService(db_session).heal_stuck_game("20230501LGSS0")

    assert summary.status == "SKIPPED"
    assert "Historical guard" in (summary.error_message or "")
    db_session.refresh(g)
    assert g.game_status == "SCHEDULED"


@pytest.mark.parametrize(
    ("defect_type", "expected_action"),
    [
        (PipelineDefectType.MISSING_STATS, "defer_to_stats_recalc"),
        (PipelineDefectType.UNVERIFIED_PBP, "defer_to_pbp_recrawl"),
    ],
)
def test_a_handoff_to_another_job_is_not_reported_as_a_repair(
    db_session,
    defect_type: PipelineDefectType,
    expected_action: str,
) -> None:
    """Neither defect has a repair in this service, so neither can be SUCCESS."""
    summary = AutoHealerService(db_session).heal_defect(DefectItem(game_id="20240404LGSS0", defect_type=defect_type))

    assert summary.status == HealingStatus.DEFERRED
    assert summary.healed is False
    assert summary.action_taken == expected_action
    assert summary.details["remediation_owner"]


def test_quality_hints_are_recorded_without_claiming_execution(db_session) -> None:
    """A remediation hint is a note, not an action that was carried out."""
    report = UnifiedQualityReport(
        timestamp="2024-04-04T00:00:00",
        overall_status="WARN",
        quality_score=80,
        remediation_hints=["recalculate standings for 2024"],
    )

    actions = AutoHealerService(db_session).heal_from_quality_report(report)

    assert len(actions) == 1
    assert actions[0].status == HealingStatus.DEFERRED
    assert actions[0].healed is False
    assert actions[0].action_taken == "record_remediation_hint"
    assert actions[0].details["hint"] == "recalculate standings for 2024"
