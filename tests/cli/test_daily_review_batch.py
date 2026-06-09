from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.daily_review_batch import main


class TestDailyReviewBatchCLI:
    def test_main_no_games(self):
        with patch("src.cli.daily_review_batch.refresh_game_status_for_date") as mock_status, \
             patch("src.cli.daily_review_batch.SessionLocal") as mock_sesh, \
             patch("src.cli.daily_review_batch.write_refresh_manifest") as mock_manifest:
            mock_status.return_value = {"updated": 0}
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015"])
            assert result == 0
            mock_manifest.assert_called_once()

    def test_main_with_games(self):
        with patch("src.cli.daily_review_batch.refresh_game_status_for_date") as mock_status, \
             patch("src.cli.daily_review_batch.SessionLocal") as mock_sesh, \
             patch("src.cli.daily_review_batch.ContextAggregator") as MockAgg, \
             patch("src.cli.daily_review_batch.write_refresh_manifest"):
            mock_status.return_value = {"updated": 0}
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_game.away_team = "LG"
            mock_game.home_team = "SS"
            mock_game.game_date = MagicMock()
            mock_game.game_date.strftime.return_value = "20251015"
            mock_game.game_date.year = 2025
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_game]
            mock_sesh.return_value.__enter__.return_value = mock_session
            mock_agg = MagicMock()
            mock_agg.get_crucial_moments.return_value = [{"event": "HR"}]
            MockAgg.return_value = mock_agg

            result = main(["--date", "20251015"])
            assert result == 0
