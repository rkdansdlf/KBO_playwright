"""Tests for Data Quality Monitoring & Extended Gap Reporting."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.cli.gap_report import (
    check_futures_gaps,
    check_milestones_gaps,
    check_notices_gaps,
    check_splits_gaps,
)
from src.models.base import Base
from src.models.futures_schedule import FuturesGameSchedule
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    yield session
    session.close()


def test_gap_check_functions(db_session: Session) -> None:
    """Test extended gap checking functions for notices, milestones, futures, splits."""
    db_session.add(
        KboPressRelease(
            notice_id="101",
            category="공시",
            title="KBO 올스타전 라인업 발표",
            published_date=date(2026, 8, 9),
            source_url="https://example.com/101",
        )
    )
    db_session.add(
        PlayerMilestone(
            season=2026,
            player_id="P1",
            player_name="최형우",
            milestone_category="1600타점",
            current_val=1598,
            target_val=1600,
            remaining_val=2,
        )
    )
    db_session.add(
        FuturesGameSchedule(
            game_id="F1",
            season=2026,
            game_date=date(2026, 8, 9),
            away_team="고양",
            home_team="한화",
        )
    )
    db_session.add(
        PlayerSplitsStat(
            season=2026,
            player_id="P1",
            player_name="최형우",
            split_type="scoring_position",
            split_key="득점권시",
        )
    )
    db_session.commit()

    with patch("src.cli.gap_report.SessionLocal", return_value=db_session):
        notices_gap = check_notices_gaps()
        assert notices_gap["total_notices"] == 1

        milestones_gap = check_milestones_gaps()
        assert milestones_gap["total_milestones"] == 1
        assert milestones_gap["ok"] is True

        futures_gap = check_futures_gaps()
        assert futures_gap["total_futures_games"] == 1
        assert futures_gap["ok"] is True

        splits_gap = check_splits_gaps()
        assert splits_gap["total_splits"] == 1
        assert splits_gap["ok"] is True
