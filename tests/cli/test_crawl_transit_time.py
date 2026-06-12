from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_transit_time import JAMSIL_ORIGINS, main


class TestCrawlTransitTimeCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_transit_time.TransitTimeCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with(origins=JAMSIL_ORIGINS)
            mock_instance.run.assert_called_once_with(game_date=None, save=True)

    def test_main_save_with_game_date(self):
        with patch("src.cli.crawl_transit_time.TransitTimeCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save", "--game-date", "20260603"])
            MockCrawler.assert_called_once_with(origins=JAMSIL_ORIGINS)
            call_kwargs = mock_instance.run.call_args.kwargs
            assert call_kwargs["save"] is True
            import datetime

            assert call_kwargs["game_date"] == datetime.date(2026, 6, 3)

    def test_main_with_origin_filter(self):
        with patch("src.cli.crawl_transit_time.TransitTimeCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save", "--origin", "잠실역_2호선_7번출구"])
            expected_origins = [o for o in JAMSIL_ORIGINS if o["label"] == "잠실역_2호선_7번출구"]
            MockCrawler.assert_called_once_with(origins=expected_origins)
            mock_instance.run.assert_called_once_with(game_date=None, save=True)

    def test_main_no_args(self):
        with patch("src.cli.crawl_transit_time.TransitTimeCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main([])
            MockCrawler.assert_called_once_with(origins=JAMSIL_ORIGINS)
            mock_instance.run.assert_called_once_with(game_date=None, save=False)

    def test_main_unknown_origin(self):
        with patch("src.cli.crawl_transit_time.TransitTimeCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--origin", "NONEXISTENT"])
            MockCrawler.assert_not_called()
            mock_instance.run.assert_not_called()
