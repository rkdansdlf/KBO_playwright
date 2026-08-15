"""Unit tests for HistoricalBoxscoreIngestor."""

from __future__ import annotations

import pytest

from src.db.engine import SessionLocal, init_db
from src.services.historical_1982_pilot_service import Historical1982PilotService
from src.services.historical_boxscore_ingestor import HistoricalBoxscoreIngestor


@pytest.fixture(autouse=True)
def _setup_db():
    init_db()


def test_1982_boxscore_seeding_and_audit() -> None:
    """HistoricalBoxscoreIngestor should generate valid line scores matching game totals."""
    with SessionLocal() as session:
        pilot_svc = Historical1982PilotService(session)
        pilot_svc.seed_1982_fixtures()
        session.commit()

        boxscore_svc = HistoricalBoxscoreIngestor(session)
        inns, bats, pits = boxscore_svc.seed_1982_season_boxscores()
        session.commit()

        assert inns == 240 * 18  # 240 games * 2 sides * 9 innings = 4320 rows
        assert bats > 0
        assert pits > 0

        report = boxscore_svc.audit_1982_boxscore_integrity()
        assert report.total_games == 240
        assert report.boxscore_secured_games == 240
        assert report.score_sums_match_count == 240
        assert report.batting_stats_games > 0
        assert report.pitching_stats_games > 0
        assert report.is_valid is True
