from __future__ import annotations

from datetime import date
from unittest.mock import patch

from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories.game_status import refresh_game_status_for_date, update_game_status


def _fake_canonicalize(gid):
    if not gid:
        return None, None
    return str(gid).strip().upper(), str(gid).strip().upper()


class TestGameStatus:
    def _setup_game_tables(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        engine = create_engine("sqlite:///:memory:")
        Game.__table__.create(engine)
        GameMetadata.__table__.create(engine)
        GameInningScore.__table__.create(engine)
        GameLineup.__table__.create(engine)
        GameBattingStat.__table__.create(engine)
        GamePitchingStat.__table__.create(engine)
        return engine, sessionmaker(bind=engine)()

    @patch("src.repositories.game_status.SessionLocal")
    @patch("src.repositories.game_status._canonicalize_game_id", side_effect=_fake_canonicalize)
    def test_update_game_status_updates_record(self, MockCanon, MockSessionLocal):
        engine, session = self._setup_game_tables()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        g = Game(game_id="20241015LGSSG0", game_date=date(2024, 10, 15))
        session.add(g)
        session.commit()

        result = update_game_status("20241015LGSSG0", "completed")
        assert result is True

        session.refresh(g)
        assert g.game_status == "completed"

    @patch("src.repositories.game_status.SessionLocal")
    @patch("src.repositories.game_status._canonicalize_game_id", side_effect=_fake_canonicalize)
    def test_update_game_status_invalid_id(self, MockCanon, MockSessionLocal):
        engine, session = self._setup_game_tables()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        result = update_game_status("NONEXISTENT", "completed")
        assert result is False

    @patch("src.repositories.game_status.SessionLocal")
    @patch("src.repositories.game_status._canonicalize_game_id", side_effect=_fake_canonicalize)
    def test_update_game_status_empty_params(self, MockCanon, MockSessionLocal):
        engine, session = self._setup_game_tables()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        assert update_game_status("", "completed") is False
        assert update_game_status("20241015LGSSG0", "") is False

    @patch("src.repositories.game_status.refresh_game_status_for_date")
    def test_refresh_game_status_called(self, mock_refresh):
        mock_refresh.return_value = {
            "target_date": "20241015",
            "total": 1,
            "updated": 0,
            "status_counts": {"completed": 1},
        }
        result = refresh_game_status_for_date("20241015")
        assert result["total"] == 1

    @patch("src.repositories.game_status.SessionLocal")
    def test_refresh_game_status_derives_completed(self, MockSessionLocal):
        engine, session = self._setup_game_tables()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        g = Game(game_id="20241015LGSSG0", game_date=date(2024, 10, 15),
                 home_score=5, away_score=3, game_status="unresolved")
        session.add(g)
        session.add(GameInningScore(game_id="20241015LGSSG0", team_side="home",
                                     inning=1, runs=5))
        session.add(GameInningScore(game_id="20241015LGSSG0", team_side="away",
                                     inning=1, runs=3))
        session.commit()

        result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
        assert result["total"] == 1
        assert result["updated"] == 1
        assert result["status_counts"].get("COMPLETED") == 1

    @patch("src.repositories.game_status.SessionLocal")
    def test_refresh_game_status_invalid_date(self, MockSessionLocal):
        result = refresh_game_status_for_date("not-a-date")
        assert result["total"] == 0
        assert result["updated"] == 0

    @patch("src.repositories.game_status.SessionLocal")
    def test_refresh_game_status_no_games_for_date(self, MockSessionLocal):
        engine, session = self._setup_game_tables()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        result = refresh_game_status_for_date("20241015", today=date(2024, 10, 16))
        assert result["total"] == 0
