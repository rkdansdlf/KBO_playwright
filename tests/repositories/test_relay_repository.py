from __future__ import annotations

from unittest.mock import patch

from src.repositories.relay_repository import get_game_relay_summary, save_relay_data


class TestRelayRepository:
    @patch("src.repositories.relay_repository.save_normalized_relay_data")
    def test_save_relay_data_flattens_innings(self, mock_save):
        mock_save.return_value = 3

        innings_data = [
            {
                "inning": 1,
                "half": "top",
                "plays": [
                    {"pitcher": "Kim", "batter": "Lee", "description": "Strikeout", "event_type": "strikeout", "result": "O"},
                    {"pitcher": "Kim", "batter": "Park", "description": "Walk", "event_type": "walk", "result": "BB"},
                ],
            },
            {
                "inning": 1,
                "half": "bottom",
                "plays": [
                    {"pitcher": "Choi", "batter": "Yoon", "description": "Single", "event_type": "hit", "result": "S"},
                ],
            },
        ]

        result = save_relay_data("20241015LGSSG0", innings_data)
        assert result == 3
        mock_save.assert_called_once()
        args, kwargs = mock_save.call_args
        assert args[0] == "20241015LGSSG0"
        assert len(kwargs["raw_pbp_rows"]) == 3
        assert kwargs["raw_pbp_rows"][0]["inning"] == 1
        assert kwargs["raw_pbp_rows"][0]["inning_half"] == "top"
        assert kwargs["raw_pbp_rows"][2]["inning_half"] == "bottom"

    @patch("src.repositories.relay_repository.save_normalized_relay_data")
    def test_save_relay_data_empty_input(self, mock_save):
        mock_save.return_value = 0
        result = save_relay_data("20241015LGSSG0", [])
        assert result == 0
        mock_save.assert_called_once()

    @patch("src.repositories.relay_repository.save_normalized_relay_data")
    def test_save_relay_data_none_input(self, mock_save):
        mock_save.return_value = 0
        result = save_relay_data("20241015LGSSG0", None)
        assert result == 0

    @patch("src.repositories.relay_repository.SessionLocal")
    def test_get_game_relay_summary_no_plays(self, MockSessionLocal):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from src.models.game import GamePlayByPlay

        engine = create_engine("sqlite:///:memory:")
        GamePlayByPlay.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        summary = get_game_relay_summary("NONEXISTENT")
        assert summary["game_id"] == "NONEXISTENT"
        assert summary["total_plays"] == 0
        assert summary["innings"] == 0
        assert summary["event_types"] == {}

    @patch("src.repositories.relay_repository.SessionLocal")
    def test_get_game_relay_summary_with_plays(self, MockSessionLocal):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from src.models.game import GamePlayByPlay

        engine = create_engine("sqlite:///:memory:")
        GamePlayByPlay.__table__.create(engine)
        session = sessionmaker(bind=engine)()
        MockSessionLocal.return_value.__enter__.return_value = session
        MockSessionLocal.return_value.__exit__.return_value = None

        session.add_all([
            GamePlayByPlay(game_id="G1", inning=1, inning_half="top", event_type="batting"),
            GamePlayByPlay(game_id="G1", inning=1, inning_half="bottom", event_type="strikeout"),
            GamePlayByPlay(game_id="G1", inning=2, inning_half="top", event_type="home_run"),
        ])
        session.commit()

        summary = get_game_relay_summary("G1")
        assert summary["total_plays"] == 3
        assert summary["innings"] == 3
        assert summary["event_types"]["batting"] == 1
        assert summary["event_types"]["strikeout"] == 1
        assert summary["event_types"]["home_run"] == 1
