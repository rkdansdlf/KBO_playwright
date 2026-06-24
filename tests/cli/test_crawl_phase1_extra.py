from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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

    def test_main_injury(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "injury"]),
            patch("src.cli.crawl_phase1_extra.run_injury", new_callable=AsyncMock) as mock_run,
        ):
            main()
            mock_run.assert_called_once_with(save=False)

    def test_main_foreign_player(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "foreign"]),
            patch("src.cli.crawl_phase1_extra.run_foreign_player", new_callable=AsyncMock) as mock_run,
        ):
            main()
            mock_run.assert_called_once_with(save=False)

    def test_main_manager_change(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "manager"]),
            patch("src.cli.crawl_phase1_extra.run_manager_change", new_callable=AsyncMock) as mock_run,
        ):
            main()
            mock_run.assert_called_once_with(save=False)

    def test_main_fan_culture(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "fan_culture"]),
            patch("src.cli.crawl_phase1_extra.run_fan_culture", new_callable=AsyncMock) as mock_run,
        ):
            main()
            mock_run.assert_called_once_with(save=False)

    def test_main_all(self):
        with (
            patch("sys.argv", ["crawl_phase1_extra", "--type", "all"]),
            patch("src.cli.crawl_phase1_extra.run_all", new_callable=AsyncMock) as mock_run,
        ):
            main()
            mock_run.assert_called_once_with(save=False)

    def test_main_no_type_prints_help(self, capsys):
        with patch("sys.argv", ["crawl_phase1_extra"]):
            with pytest.raises(SystemExit):
                main()
            captured = capsys.readouterr()
            assert "usage" in captured.out.lower() or "required" in captured.out.lower()

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
