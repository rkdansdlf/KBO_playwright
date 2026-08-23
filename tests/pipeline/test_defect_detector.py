"""Unit tests for src.pipeline.defect_detector."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameBattingStat, GameInningScore, GamePitchingStat
from src.pipeline.defect_detector import PipelineDefectDetector
from src.pipeline.dto import PipelineDefectType


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


def test_find_stuck_games(db_session) -> None:
    g1 = Game(
        game_id="20240401LGSS0",
        game_date=date(2024, 4, 1),
        home_team="SS",
        away_team="LG",
        game_status="SCHEDULED",
    )
    g2 = Game(
        game_id="20240401KTNC0",
        game_date=date(2024, 4, 1),
        home_team="NC",
        away_team="KT",
        game_status="COMPLETED",
        home_score=5,
        away_score=3,
    )
    db_session.add_all([g1, g2])
    db_session.flush()

    detector = PipelineDefectDetector(db_session)
    defects = detector.find_stuck_games(target_date=date(2024, 4, 2))

    assert len(defects) == 1
    assert defects[0].game_id == "20240401LGSS0"
    assert defects[0].defect_type == PipelineDefectType.STUCK_SCHEDULED


def test_find_score_mismatches(db_session) -> None:
    g = Game(
        game_id="20240402LGSS0",
        game_date=date(2024, 4, 2),
        home_team="SS",
        away_team="LG",
        game_status="COMPLETED",
        home_score=6,  # mismatch with 5
        away_score=3,
    )
    inn1 = GameInningScore(game_id="20240402LGSS0", team_side="home", inning=1, runs=5)
    inn2 = GameInningScore(game_id="20240402LGSS0", team_side="away", inning=1, runs=3)
    db_session.add_all([g, inn1, inn2])
    db_session.flush()

    detector = PipelineDefectDetector(db_session)
    defects = detector.find_score_mismatches(target_date=date(2024, 4, 2))

    assert len(defects) == 1
    assert defects[0].game_id == "20240402LGSS0"
    assert defects[0].defect_type == PipelineDefectType.SCORE_MISMATCH


def test_find_missing_player_stats(db_session) -> None:
    g = Game(
        game_id="20240403LGSS0",
        game_date=date(2024, 4, 3),
        home_team="SS",
        away_team="LG",
        game_status="COMPLETED",
        home_score=4,
        away_score=2,
    )
    # Only adding batting stat, missing pitching stat
    b = GameBattingStat(
        game_id="20240403LGSS0",
        team_side="home",
        player_id=101,
        player_name="선수",
        team_code="SS",
        appearance_seq=1,
    )
    db_session.add_all([g, b])
    db_session.flush()

    detector = PipelineDefectDetector(db_session)
    defects = detector.find_missing_player_stats(target_date=date(2024, 4, 3))

    assert len(defects) == 1
    assert defects[0].game_id == "20240403LGSS0"
    assert defects[0].defect_type == PipelineDefectType.MISSING_STATS
    assert "pitching" in defects[0].details["missing"]
