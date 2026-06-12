from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_phase1_extra import main


class TestCrawlPhase1ExtraCLI:
    def test_main_broadcast(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "broadcast", "--save"]),
            patch("src.crawlers.broadcast_crawler.BroadcastCrawler") as MockCrawler,
        ):
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main()
            mock_instance.run.assert_called_once_with(save=True)

    def test_main_mvp(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "mvp", "--save"]),
            patch("src.crawlers.game_mvp_crawler.GameMvpCrawler") as MockCrawler,
        ):
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main()
            mock_instance.run.assert_called_once_with(game_ids=None, save=True)

    def test_main_seed_stadium(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "seed_stadium"]),
            patch("src.cli.crawl_phase1_extra.seed_stadium_info") as mock_seed,
        ):
            main()
            mock_seed.assert_called_once()

    def test_main_all_crawlers(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "crawlers", "--save"]),
            patch("src.crawlers.broadcast_crawler.BroadcastCrawler") as MockBC,
            patch("src.crawlers.game_mvp_crawler.GameMvpCrawler") as MockMC,
            patch("src.crawlers.injury_crawler.InjuryCrawler") as MockIC,
            patch("src.crawlers.foreign_player_crawler.ForeignPlayerCrawler") as MockFC,
            patch("src.crawlers.manager_change_crawler.ManagerChangeCrawler") as MockMCC,
        ):
            for m in [MockBC, MockMC, MockIC, MockFC, MockMCC]:
                inst = MagicMock()
                inst.run = AsyncMock()
                m.return_value = inst

            main()
            MockBC.return_value.run.assert_called_once_with(save=True)
            MockMC.return_value.run.assert_called_once_with(game_ids=None, save=True)
