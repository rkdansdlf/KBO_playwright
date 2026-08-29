"""Unit tests for HistoricalBoxscoreIngestor."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.game import Game
from src.services.historical_1982_pilot_service import Historical1982PilotService
from src.services.historical_boxscore_ingestor import HistoricalBoxscoreIngestor

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def session() -> Generator[Session, None, None]:
    """Provide isolated in-memory test database session."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    s = factory()
    try:
        yield s
    finally:
        s.close()
        engine.dispose()


def test_1982_boxscore_audit_report(session: Session) -> None:
    """HistoricalBoxscoreIngestor should accurately report data availability without synthetic counts."""
    session.query(Game).filter(Game.game_id.like("1982%")).delete()
    session.commit()

    pilot_svc = Historical1982PilotService(session)
    pilot_svc.seed_1982_fixtures()
    session.commit()

    boxscore_svc = HistoricalBoxscoreIngestor(session)
    cleaned = boxscore_svc.cleanup_synthetic_records(1982)
    assert cleaned >= 0

    report = boxscore_svc.audit_historical_boxscore_integrity(1982)
    assert report.total_games in (240, 242)
    assert report.boxscore_secured_games == 0
    assert report.source_verified_batting_games == 0
    assert report.source_verified_pitching_games == 0
