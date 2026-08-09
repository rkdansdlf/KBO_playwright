"""Tests for extended FastAPI REST API routers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.api.app import app
from src.models.base import Base
from src.models.futures_schedule import FuturesGameSchedule
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def test_client() -> TestClient:
    """FastAPI TestClient fixture."""
    return TestClient(app)


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture shared across threads."""
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


def test_get_notices_api(test_client: TestClient, db_session: Session) -> None:
    """Test GET /api/v1/notices endpoint."""
    db_session.add(
        KboPressRelease(
            notice_id="100",
            category="공시/공지",
            title="KBO 리그 경기일정 변경",
            published_date=date(2026, 8, 9),
            source_url="https://example.com/100",
        )
    )
    db_session.commit()

    @contextmanager
    def _mock_db():
        yield db_session

    with patch("src.api.routers.notices.get_db_session", side_effect=_mock_db):
        response = test_client.get("/api/v1/notices")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["notices"][0]["title"] == "KBO 리그 경기일정 변경"


def test_get_milestones_api(test_client: TestClient, db_session: Session) -> None:
    """Test GET /api/v1/milestones endpoint."""
    db_session.add(
        PlayerMilestone(
            season=2026,
            player_id="P1",
            player_name="최형우",
            team_code="KIA",
            milestone_category="1600타점",
            current_val=1598,
            target_val=1600,
            remaining_val=2,
            is_achieved=False,
        )
    )
    db_session.commit()

    @contextmanager
    def _mock_db():
        yield db_session

    with patch("src.api.routers.milestones.get_db_session", side_effect=_mock_db):
        response = test_client.get("/api/v1/milestones?season=2026")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["milestones"][0]["player_name"] == "최형우"


def test_get_futures_schedule_api(test_client: TestClient, db_session: Session) -> None:
    """Test GET /api/v1/futures/schedule endpoint."""
    db_session.add(
        FuturesGameSchedule(
            game_id="F20260809",
            season=2026,
            game_date=date(2026, 8, 9),
            away_team="고양",
            home_team="한화",
            away_score=5,
            home_score=3,
        )
    )
    db_session.commit()

    @contextmanager
    def _mock_db():
        yield db_session

    with patch("src.api.routers.futures.get_db_session", side_effect=_mock_db):
        response = test_client.get("/api/v1/futures/schedule?season=2026")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["schedules"][0]["away_team"] == "고양"


def test_get_player_splits_api(test_client: TestClient, db_session: Session) -> None:
    """Test GET /api/v1/players/{player_id}/splits endpoint."""
    db_session.add(
        PlayerSplitsStat(
            season=2026,
            player_id="78224",
            player_name="김도영",
            team_code="KIA",
            split_type="scoring_position",
            split_key="득점권시",
            avg=0.375,
            ops=1.050,
        )
    )
    db_session.commit()

    @contextmanager
    def _mock_db():
        yield db_session

    with patch("src.api.routers.players.get_db_session", side_effect=_mock_db):
        response = test_client.get("/api/v1/players/78224/splits?season=2026")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["splits"][0]["player_name"] == "김도영"
