from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.daily_preview_batch import main


class TestDailyPreviewBatchCLI:
    def test_main_no_previews(self):
        with patch("src.cli.daily_preview_batch.PreviewCrawler") as MockCrawler, \
             patch("src.cli.daily_preview_batch.write_refresh_manifest") as mock_manifest, \
             patch("src.cli.daily_preview_batch.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "20251015"
            mock_instance = MagicMock()
            mock_instance.crawl_preview_for_date = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_manifest.return_value = "/tmp/manifest.json"

            result = main(["--date", "20251015"])
            assert result == 0
            mock_instance.crawl_preview_for_date.assert_called_once_with("20251015")

    def test_main_with_previews(self):
        with patch("src.cli.daily_preview_batch.PreviewCrawler") as MockCrawler, \
             patch("src.cli.daily_preview_batch.SessionLocal"), \
             patch("src.cli.daily_preview_batch.ContextAggregator"), \
             patch("src.cli.daily_preview_batch.save_pregame_lineups") as mock_save, \
             patch("src.cli.daily_preview_batch.write_refresh_manifest"), \
             patch("src.cli.daily_preview_batch.datetime") as mock_dt:
            mock_dt.strptime.return_value.date.return_value = MagicMock()
            mock_dt.strptime.return_value.date.return_value.year = 2025
            mock_instance = MagicMock()
            mock_instance.crawl_preview_for_date = AsyncMock(
                return_value=[{"game_id": "20251015LGHH0", "away_team_name": "LG", "home_team_name": "SS"}]
            )
            MockCrawler.return_value = mock_instance
            mock_save.return_value = True

            result = main(["--date", "20251015"])
            assert result == 0
