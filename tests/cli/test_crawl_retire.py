from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_retire import main


class TestCrawlRetireCLI:
    def test_main_default(self):
        with patch("src.cli.crawl_retire.determine_inactive_ids", new_callable=AsyncMock) as mock_determine, \
             patch("src.cli.crawl_retire.RetiredPlayerDetailCrawler"), \
             patch("src.cli.crawl_retire.PlayerRepository"):
            mock_determine.return_value = set()
            main(["--end-year", "2024"])
            mock_determine.assert_called_once()

    def _make_crawler_mock(self):
        m = MagicMock()
        m.fetch_player = AsyncMock(return_value={"hitter": None, "pitcher": None, "photo_url": None})
        m.close = AsyncMock()
        return m

    def test_main_with_limit(self):
        with patch("src.cli.crawl_retire.determine_inactive_ids", new_callable=AsyncMock) as mock_determine, \
             patch("src.cli.crawl_retire.RetiredPlayerDetailCrawler") as MockCrawler, \
             patch("src.cli.crawl_retire.PlayerRepository"):
            mock_determine.return_value = {"100", "200", "300"}
            MockCrawler.return_value = self._make_crawler_mock()
            main(["--end-year", "2024", "--limit", "2"])

    def test_main_with_seed_file(self):
        with patch("builtins.open") as mock_open, \
             patch("src.cli.crawl_retire.RetiredPlayerDetailCrawler") as MockCrawler, \
             patch("src.cli.crawl_retire.PlayerRepository"):
            mock_open.return_value.__enter__.return_value = ["100\n", "200\n"]
            MockCrawler.return_value = self._make_crawler_mock()
            main(["--seed-file", "/fake/path.txt"])
