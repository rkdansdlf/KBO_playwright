from __future__ import annotations

from unittest.mock import AsyncMock, patch

from src.cli.crawl_futures import main


class TestCrawlFuturesCLI:
    def test_main_season(self):
        with (
            patch("src.cli.crawl_futures.gather_active_player_ids", new_callable=AsyncMock) as mock_gather,
            patch("src.cli.crawl_futures.PlayerRepository"),
            patch("src.cli.crawl_futures.AsyncPlaywrightPool"),
        ):
            mock_gather.return_value = {}

            result = main(["--season", "2025"])

            mock_gather.assert_called_once_with(2025, 2.0)
            assert result["season"] == 2025

    def test_main_with_limit(self):
        with (
            patch("src.cli.crawl_futures.gather_active_player_ids", new_callable=AsyncMock) as mock_gather,
            patch("src.cli.crawl_futures.PlayerRepository"),
            patch("src.cli.crawl_futures.AsyncPlaywrightPool"),
        ):
            mock_gather.return_value = {"1": {"position": "hitter", "name": "A"}}

            result = main(["--season", "2025", "--limit", "1", "--concurrency", "1"])

            assert result["processed"] == 1

    def test_main_with_player_ids(self):
        with (
            patch("src.cli.crawl_futures.gather_active_player_ids") as mock_gather,
            patch("src.cli.crawl_futures.PlayerRepository"),
            patch("src.cli.crawl_futures.AsyncPlaywrightPool"),
        ):
            result = main(["--season", "2025", "--player-ids", "123,456", "--concurrency", "1"])

            mock_gather.assert_not_called()
            assert result["season"] == 2025

    def test_main_json_summary(self):
        with (
            patch("src.cli.crawl_futures.gather_active_player_ids", new_callable=AsyncMock) as mock_gather,
            patch("src.cli.crawl_futures.PlayerRepository"),
            patch("src.cli.crawl_futures.AsyncPlaywrightPool"),
        ):
            mock_gather.return_value = {}

            result = main(["--season", "2025", "--json-summary"])

            assert result["season"] == 2025
            assert result["processed"] == 0
