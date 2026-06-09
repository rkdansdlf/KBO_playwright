from __future__ import annotations

from unittest.mock import patch

from src.services.schedule_collection_service import ScheduleSaveResult, save_schedule_games


class TestScheduleSaveResult:
    def test_discovered_property(self):
        result = ScheduleSaveResult(games=[{"id": 1}], saved_games=[], failed_games=[], filtered_games=[])
        assert result.discovered == 1

    def test_empty_discovered(self):
        result = ScheduleSaveResult(games=[], saved_games=[], failed_games=[], filtered_games=[])
        assert result.discovered == 0


class TestSaveScheduleGames:
    def test_empty_games(self):
        result = save_schedule_games([])
        assert result.saved == 0
        assert result.failed == 0
        assert result.filtered == 0

    def test_saves_valid_game(self):
        game = {"game_id": "20240315LGSS0", "home_team": "LG", "away_team": "SS"}
        with patch("src.services.schedule_collection_service.validate_schedule_game_payload", return_value=(True, None)):
            with patch("src.services.schedule_collection_service.save_schedule_game", return_value=True):
                result = save_schedule_games([game])
                assert result.saved == 1
                assert result.failed == 0
                assert result.filtered == 0

    def test_filters_invalid_game(self):
        game = {"game_id": "20240315LGSS0"}
        with patch("src.services.schedule_collection_service.validate_schedule_game_payload", return_value=(False, "missing_team")):
            result = save_schedule_games([game])
            assert result.saved == 0
            assert result.filtered == 1
            assert result.failed == 1
            assert result.filtered_games[0]["failure_reason"] == "missing_team"

    def test_failed_save(self):
        game = {"game_id": "20240315LGSS0", "home_team": "LG", "away_team": "SS"}
        with patch("src.services.schedule_collection_service.validate_schedule_game_payload", return_value=(True, None)):
            with patch("src.services.schedule_collection_service.save_schedule_game", return_value=False):
                result = save_schedule_games([game])
                assert result.saved == 0
                assert result.failed == 1

    def test_mixed_results(self):
        games = [
            {"game_id": "G1", "home_team": "LG", "away_team": "SS"},
            {"game_id": "G2"},
        ]
        with patch("src.services.schedule_collection_service.validate_schedule_game_payload") as mock_val:
            mock_val.side_effect = [(True, None), (False, "missing_team")]
            with patch("src.services.schedule_collection_service.save_schedule_game", return_value=True):
                result = save_schedule_games(games)
                assert result.saved == 1
                assert result.filtered == 1
                assert result.failed == 1
