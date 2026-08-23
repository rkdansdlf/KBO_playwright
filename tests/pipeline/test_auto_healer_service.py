"""Unit tests for src.pipeline.auto_healer_service."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameInningScore
from src.pipeline.auto_healer_service import AutoHealerService
from src.pipeline.dto import DefectItem, DefectReport, PipelineDefectType


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
    assert g.game_status == "CANCELLED"  # because scores were None
