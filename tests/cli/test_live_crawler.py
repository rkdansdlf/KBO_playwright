from unittest.mock import patch

from src.cli.live_crawler import main


class TestLiveCrawler:
    def test_run_once(self):
        with patch("src.cli.live_crawler.asyncio.run") as mock_run:
            mock_run.return_value = None
            result = main(["--run-once"])
            assert result == 0

    def test_run_once_no_sync(self):
        with patch("src.cli.live_crawler.asyncio.run") as mock_run:
            mock_run.return_value = None
            result = main(["--run-once", "--no-sync"])
            assert result == 0

    def test_dynamic_mode(self):
        with patch("src.cli.live_crawler.asyncio.run") as mock_run:
            mock_run.return_value = None
            result = main(["--dynamic", "--interval", "1"])
            assert result == 0
