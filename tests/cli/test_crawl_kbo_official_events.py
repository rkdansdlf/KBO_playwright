from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_kbo_official_events import main


class TestCrawlKboOfficialEventsCLI:
    def test_main_dry_run(self):
        with patch("src.cli.crawl_kbo_official_events.KboEventCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance

            result = main([])

            assert result == 0
            MockCrawler.assert_called_once()
            mock_instance.run.assert_called_once_with(save=False)

    def test_main_save(self):
        with patch("src.cli.crawl_kbo_official_events.KboEventCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance

            result = main(["--save"])

            assert result == 0
            mock_instance.run.assert_called_once_with(save=True)

    def test_main_with_url(self):
        with patch("src.cli.crawl_kbo_official_events.KboEventCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance

            result = main(["--url", "https://example.com"])

            assert result == 0
            MockCrawler.assert_called_once_with(base_url="https://example.com")
