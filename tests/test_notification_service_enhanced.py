"""Tests for Enhanced NotificationService (Dynamic Matchup, WPA Hero, Hot Push)."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.game import Game
from src.services.notification_service import NotificationService

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    yield session
    session.close()


def test_send_today_all_pregame_alerts(db_session: Session) -> None:
    """Test send_today_all_pregame_alerts dynamic matchup lookup."""
    db_session.add(
        Game(
            game_id="20260809LGKIA0",
            game_date=date(2026, 8, 9),
            away_team="LG",
            home_team="KIA",
            game_status="SCHEDULED",
        )
    )
    db_session.commit()

    service = NotificationService(db_session)

    with patch("src.utils.alerting.TelegramBotClient.send_message", return_value=True) as mock_send:
        res = service.send_today_all_pregame_alerts(target_date="20260809", season=2026, channels=["telegram"])
        assert res["game_count"] == 1
        assert len(res["dispatches"]) == 1
        mock_send.assert_called_once()


def test_send_postgame_wpa_hero_alert(db_session: Session) -> None:
    """Test send_postgame_wpa_hero_alert."""
    db_session.add(
        Game(
            game_id="20260809LGKIA0",
            game_date=date(2026, 8, 9),
            away_team="LG",
            home_team="KIA",
            away_score=3,
            home_score=5,
            game_status="COMPLETED",
        )
    )
    db_session.commit()

    service = NotificationService(db_session)

    with patch("src.utils.alerting.TelegramBotClient.send_message", return_value=True) as mock_send:
        res = service.send_postgame_wpa_hero_alert(game_id="20260809LGKIA0", season=2026, channels=["telegram"])
        assert res["winner"] == "KIA"
        assert res["dispatched_channels"]["telegram"] is True
        mock_send.assert_called_once()


def test_send_emergency_notice_alert(db_session: Session) -> None:
    """Test send_emergency_notice_alert real-time hot push."""
    service = NotificationService(db_session)

    with patch("src.utils.alerting.TelegramBotClient.send_message", return_value=True) as mock_send:
        res = service.send_emergency_notice_alert(
            title="[공시] 잠실 경기 우천 취소 안내",
            content="오늘 예정된 LG vs KIA 잠실 경기는 우천으로 순연되었습니다.",
            source_url="https://example.com/notice/1",
            channels=["telegram"],
        )
        assert res["dispatched_channels"]["telegram"] is True
        mock_send.assert_called_once()
