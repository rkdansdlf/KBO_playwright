from __future__ import annotations

from unittest.mock import patch

from src.models.player import PlayerSeasonBatting
from src.repositories.save_futures_batting import save_futures_batting


class TestSaveFuturesBatting:
    def _session_fixture(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        PlayerSeasonBatting.__table__.create(engine)
        return sessionmaker(bind=engine)()

    @patch("src.repositories.save_futures_batting.SessionLocal")
    @patch("src.repositories.save_futures_batting.Engine")
    def test_save_futures_batting_creates_records(self, MockEngine, MockSessionLocal):
        session = self._session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        rows = [
            {"season": 2024, "G": 120, "AB": 400, "R": 60, "H": 120, "2B": 20, "3B": 5,
             "HR": 15, "RBI": 70, "BB": 40, "HBP": 5, "SO": 80, "SB": 10,
             "AVG": 0.300, "OBP": 0.370, "SLG": 0.475},
        ]
        saved = save_futures_batting(1, rows, league="FUTURES", level="KBO2")
        assert saved == 1
        record = session.query(PlayerSeasonBatting).one()
        assert record.player_id == 1
        assert record.season == 2024
        assert record.league == "FUTURES"
        assert record.level == "KBO2"
        assert record.games == 120
        assert record.source == "PROFILE"

    @patch("src.repositories.save_futures_batting.SessionLocal")
    @patch("src.repositories.save_futures_batting.Engine")
    def test_save_empty_rows_returns_zero(self, MockEngine, MockSessionLocal):
        assert save_futures_batting(1, []) == 0

    @patch("src.repositories.save_futures_batting.SessionLocal")
    @patch("src.repositories.save_futures_batting.Engine")
    def test_save_skips_rows_without_season(self, MockEngine, MockSessionLocal):
        session = self._session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        rows = [
            {},  # missing season, should be skipped
            {"season": 2024, "G": 100, "AB": 300},
        ]
        saved = save_futures_batting(1, rows)
        assert saved == 1
        assert session.query(PlayerSeasonBatting).count() == 1

    @patch("src.repositories.save_futures_batting.SessionLocal")
    @patch("src.repositories.save_futures_batting.Engine")
    def test_save_upsert_duplicate_updates(self, MockEngine, MockSessionLocal):
        session = self._session_fixture()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None
        MockEngine.dialect.name = "sqlite"

        rows_v1 = [{"season": 2024, "G": 100, "AB": 300, "AVG": 0.250}]
        rows_v2 = [{"season": 2024, "G": 120, "AB": 400, "AVG": 0.300}]

        save_futures_batting(1, rows_v1)
        save_futures_batting(1, rows_v2)

        assert session.query(PlayerSeasonBatting).count() == 1
        record = session.query(PlayerSeasonBatting).one()
        assert record.games == 120
        assert record.avg == 0.300
