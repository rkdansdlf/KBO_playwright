"""Tests for NotificationService using TelegramBotClient."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.player_milestone import PlayerMilestone
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


def test_notification_service_dry_run(db_session: Session) -> None:
    """Test notification service milestone summary generation."""
    db_session.add(
        PlayerMilestone(
            season=2026,
            player_id="P100",
            player_name="최형우",
            team_code="KIA",
            milestone_category="통산 타점",
            current_val=1598,
            target_val=1600,
            remaining_val=2,
            is_achieved=False,
        )
    )
    db_session.commit()

    service = NotificationService(db_session)

    with patch("src.utils.alerting.TelegramBotClient.send_message", return_value=True) as mock_send:
        res = service.send_milestone_daily_summary(season=2026, channels=["telegram"])
        assert res["milestone_count"] == 1
        assert res["dispatched_channels"]["telegram"] is True
        mock_send.assert_called_once()


def test_game_preview_report_dispatch(db_session: Session) -> None:
    """Test game preview report dispatch via TelegramBotClient."""
    service = NotificationService(db_session)

    with patch("src.utils.alerting.TelegramBotClient.send_message", return_value=True) as mock_send:
        res = service.send_game_preview_report(away_team="LG", home_team="KIA", season=2026, channels=["telegram"])
        assert res["matchup"] == "LG vs KIA"
        assert res["dispatched_channels"]["telegram"] is True
        mock_send.assert_called_once()
