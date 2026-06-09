from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.daily_highlight_batch import main


class TestDailyHighlightBatchCLI:
    def test_main_no_games(self):
        with patch("src.cli.daily_highlight_batch.SessionLocal") as mock_sesh, \
             patch("src.cli.daily_highlight_batch.HighlightAggregator"), \
             patch("src.cli.daily_highlight_batch.datetime") as mock_dt:
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015"])
            assert result == 0

    def test_main_dry_run(self):
        with patch("src.cli.daily_highlight_batch.SessionLocal") as mock_sesh, \
             patch("src.cli.daily_highlight_batch.HighlightAggregator"), \
             patch("src.cli.daily_highlight_batch.datetime") as mock_dt:
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = [mock_game]
            mock_sesh.return_value.__enter__.return_value = mock_session

            result = main(["--date", "20251015", "--dry-run"])
            assert result == 0

    def test_main_force(self):
        with patch("src.cli.daily_highlight_batch.SessionLocal") as mock_sesh, \
             patch("src.cli.daily_highlight_batch.HighlightAggregator") as MockAgg, \
             patch("src.cli.daily_highlight_batch.datetime") as mock_dt:
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_game = MagicMock()
            mock_game.game_id = "20251015LGHH0"
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = [mock_game]
            mock_sesh.return_value.__enter__.return_value = mock_session
            mock_agg = MagicMock()
            mock_agg.aggregate_game_highlights.return_value = [MagicMock()]
            MockAgg.return_value = mock_agg

            result = main(["--date", "20251015", "--force", "--no-sync", "--no-notify"])
            assert result == 0
            mock_agg.aggregate_game_highlights.assert_called_once_with("20251015LGHH0")
