"""Tests for extended RagKnowledgeIndexer."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.models.base import Base
from src.models.futures_schedule import FuturesGameSchedule
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat
from src.services.rag_indexer import RagKnowledgeIndexer

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


def test_index_incremental_all(db_session: Session) -> None:
    """Test RagKnowledgeIndexer.index_incremental_all across all 4 categories."""
    db_session.add(
        KboPressRelease(
            notice_id="101",
            category="공시",
            title="KBO 올스타전 예매 개시",
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
            game_id="F100",
            season=2026,
            game_date=date(2026, 8, 9),
            away_team="고양",
            home_team="한화",
            away_score=4,
            home_score=2,
            game_status="COMPLETED",
        )
    )
    db_session.add(
        PlayerSplitsStat(
            season=2026,
            player_id="P1",
            player_name="최형우",
            team_code="KIA",
            split_type="scoring_position",
            split_key="득점권시",
            avg=0.350,
            ops=1.020,
        )
    )
    db_session.commit()

    indexer = RagKnowledgeIndexer(db_session)
    counts = indexer.index_incremental_all(season=2026)

    assert counts["press_releases"] == 1
    assert counts["milestones"] == 1
    assert counts["futures_schedules"] == 1
    assert counts["player_splits"] == 1
    assert counts["total_chunks"] == 4
