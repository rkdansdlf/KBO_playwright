"""Unit tests for HistoricalBoxscoreIngestor."""

from __future__ import annotations

import pytest

from src.db.engine import SessionLocal, init_db
from src.models.game import Game
from src.services.historical_1982_pilot_service import Historical1982PilotService
from src.services.historical_boxscore_ingestor import HistoricalBoxscoreIngestor


@pytest.fixture(autouse=True)
def _setup_db():
    init_db()


def test_1982_boxscore_audit_report() -> None:
    """HistoricalBoxscoreIngestor should accurately report data availability without synthetic counts."""
    with SessionLocal() as session:
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
