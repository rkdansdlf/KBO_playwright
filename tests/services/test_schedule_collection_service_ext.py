from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

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
        with patch(
            "src.services.schedule_collection_service.validate_schedule_game_payload",
            return_value=(True, None),
        ):
            with patch("src.services.schedule_collection_service.save_schedule_game", return_value=True):
                result = save_schedule_games([game])
                assert result.saved == 1
                assert result.failed == 0
                assert result.filtered == 0

    def test_filters_invalid_game(self):
        game = {"game_id": "20240315LGSS0"}
        with patch(
            "src.services.schedule_collection_service.validate_schedule_game_payload",
            return_value=(False, "missing_team"),
        ):
            result = save_schedule_games([game])
            assert result.saved == 0
            assert result.filtered == 1
            assert result.failed == 1
            assert result.filtered_games[0]["failure_reason"] == "missing_team"

    def test_failed_save(self):
        game = {"game_id": "20240315LGSS0", "home_team": "LG", "away_team": "SS"}
        with patch(
            "src.services.schedule_collection_service.validate_schedule_game_payload",
            return_value=(True, None),
        ):
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

    def test_saves_with_shared_session(self):
        games = [
            {"game_id": "G1", "home_team": "LG", "away_team": "SS"},
            {"game_id": "G2", "home_team": "OB", "away_team": "NC"},
        ]
        mock_session = MagicMock()
        with (
            patch("src.services.schedule_collection_service.validate_schedule_game_payload", return_value=(True, None)),
            patch("src.services.schedule_collection_service.save_schedule_game", return_value=True) as mock_save,
        ):
            result = save_schedule_games(games, session=mock_session)
            assert result.saved == 2
            assert mock_save.call_count == 2
            for call_item in mock_save.call_args_list:
                assert call_item.kwargs["session"] is mock_session

    def test_batch_transaction_rolls_back_on_mid_write_sqlalchemy_error(self):
        games = [
            {"game_id": "G1", "home_team": "LG", "away_team": "SS"},
            {"game_id": "G2", "home_team": "OB", "away_team": "NC"},
            {"game_id": "G3", "home_team": "KT", "away_team": "HT"},
        ]
        with patch(
            "src.services.schedule_collection_service.validate_schedule_game_payload",
            return_value=(True, None),
        ):
            # First game succeeds, second raises SQLAlchemyError mid-batch
            with patch(
                "src.services.schedule_collection_service.save_schedule_game",
                side_effect=[True, SQLAlchemyError("db connection dropped")],
            ):
                result = save_schedule_games(games)

        # Entire batch must be marked as failed, not leaving game 1 counted as saved
        assert result.saved == 0
        assert result.failed == 3
        assert len(result.saved_games) == 0
        assert len(result.failed_games) == 3

    def test_batch_transaction_rolls_back_when_commit_fails_at_context_exit(self):
        from contextlib import contextmanager

        games = [
            {"game_id": "G1", "home_team": "LG", "away_team": "SS"},
            {"game_id": "G2", "home_team": "OB", "away_team": "NC"},
        ]
        mock_session = MagicMock()
        mock_session.commit.side_effect = SQLAlchemyError("commit failed: constraint violation deferred")

        @contextmanager
        def _mock_get_db_session():
            try:
                yield mock_session
                mock_session.commit()
            except SQLAlchemyError:
                mock_session.rollback()
                raise

        with (
            patch(
                "src.services.schedule_collection_service.validate_schedule_game_payload",
                return_value=(True, None),
            ),
            patch("src.services.schedule_collection_service.save_schedule_game", return_value=True),
            patch("src.db.engine.get_db_session", _mock_get_db_session),
        ):
            result = save_schedule_games(games)

        assert result.saved == 0
        assert result.failed == 2
        assert len(result.saved_games) == 0
        assert len(result.failed_games) == 2
        mock_session.rollback.assert_called_once()
