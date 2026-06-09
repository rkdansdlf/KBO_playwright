from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_congestion import main


class TestCrawlCongestionCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_congestion.CongestionCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with()
            mock_instance.run.assert_called_once_with(game_date=None, save=True)

    def test_main_save_with_game_date(self):
        with patch("src.cli.crawl_congestion.CongestionCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save", "--game-date", "20260603"])
            MockCrawler.assert_called_once_with()
            call_kwargs = mock_instance.run.call_args.kwargs
            assert call_kwargs["save"] is True
            assert str(call_kwargs["game_date"]) == "2026-06-03"

    def test_main_no_args(self):
        with patch("src.cli.crawl_congestion.CongestionCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main([])
            mock_instance.run.assert_called_once_with(game_date=None, save=False)
