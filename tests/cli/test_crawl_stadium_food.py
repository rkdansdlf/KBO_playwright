from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_stadium_food import main
from src.crawlers.sweep_outcome import CrawlSweepOutcome


class TestCrawlStadiumFoodCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_stadium_food.FoodCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with()
            mock_instance.run.assert_called_once_with(save=True, team_filter=None)

    def test_main_save_with_team(self):
        with patch("src.cli.crawl_stadium_food.FoodCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            main(["--save", "--team", "KT"])
            mock_instance.run.assert_called_once_with(save=True, team_filter="KT")

    def test_main_no_args(self):
        with patch("src.cli.crawl_stadium_food.FoodCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            main([])
            mock_instance.run.assert_called_once_with(save=False, team_filter=None)

    def test_main_returns_the_crawlers_outcome(self):
        """`crawl_p1p2_data_job` aggregates this value.

        While the entrypoint returned nothing the job could only ask whether the
        call returned, which is how a sweep that parsed nothing came to alert
        success. The outcome is what replaces that inference, so it has to be the
        value the crawler recorded.
        """
        outcome = CrawlSweepOutcome(crawler="food", status="partial", written=3, failed=1)

        with patch("src.cli.crawl_stadium_food.FoodCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            mock_instance.last_run_outcome = outcome
            MockCrawler.return_value = mock_instance

            assert main(["--save"]) is outcome
