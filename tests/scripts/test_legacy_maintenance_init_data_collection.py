from unittest.mock import AsyncMock, patch

from scripts.legacy.maintenance.init_data_collection import build_arg_parser, step5_collect_schedule


class TestBuildArgParser:
    def test_defaults(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.season_year == 2024
        assert args.schedule_month == 10
        assert args.skip_profiles is False


class TestStep5CollectSchedule:
    @patch("scripts.legacy.maintenance.init_data_collection.ScheduleCrawler")
    def test_empty(self, mock_crawler_cls):
        import asyncio
        mock_crawler = AsyncMock()
        mock_crawler_cls.return_value = mock_crawler
        mock_crawler.crawl_schedule = AsyncMock(return_value=[])

        games = asyncio.run(step5_collect_schedule(2025, 4))
        assert games == []

    @patch("scripts.legacy.maintenance.init_data_collection.ScheduleCrawler")
    def test_with_games(self, mock_crawler_cls):
        import asyncio
        mock_crawler = AsyncMock()
        mock_crawler_cls.return_value = mock_crawler
        mock_crawler.crawl_schedule = AsyncMock(return_value=[{"game_id": "G1"}])

        games = asyncio.run(step5_collect_schedule(2025, 4))
        assert len(games) == 1
