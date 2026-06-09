from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_operation_notices import main


class TestCrawlOperationNoticesCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_operation_notices.OperationNoticeLGCrawler") as MockLg, \
             patch("src.cli.crawl_operation_notices.OperationNoticeDoosanCrawler") as MockDoosan:
            mock_lg = MagicMock()
            mock_lg.run = AsyncMock()
            MockLg.return_value = mock_lg
            mock_do = MagicMock()
            mock_do.run = AsyncMock()
            MockDoosan.return_value = mock_do

            with patch.dict("src.cli.crawl_operation_notices.TEAM_CRAWLERS", {
                "LG": (MockLg, "LG트윈스공식"),
                "OB": (MockDoosan, "두산베어스공식"),
            }, clear=True):
                main(["--save"])

            MockLg.assert_called_once_with(max_pages=5)
            mock_lg.run.assert_called_once_with(save=True, stop_at_external_id=None)
            MockDoosan.assert_called_once_with(max_pages=5)
            mock_do.run.assert_called_once_with(save=True, stop_at_external_id=None)

    def test_main_team_lg(self):
        with patch("src.cli.crawl_operation_notices.OperationNoticeLGCrawler") as MockLg, \
             patch("src.cli.crawl_operation_notices.OperationNoticeDoosanCrawler"):
            mock_lg = MagicMock()
            mock_lg.run = AsyncMock()
            MockLg.return_value = mock_lg

            with patch.dict("src.cli.crawl_operation_notices.TEAM_CRAWLERS", {
                "LG": (MockLg, "LG트윈스공식"),
            }, clear=True):
                main(["--team", "LG", "--save"])

            MockLg.assert_called_once_with(max_pages=5)
            mock_lg.run.assert_called_once_with(save=True, stop_at_external_id=None)

    def test_main_source_naver(self):
        with patch("src.crawlers.operation_notice_naver_crawler.OperationNoticeNaverCrawler") as MockNaver:
            mock_n = MagicMock()
            mock_n.run = AsyncMock()
            MockNaver.return_value = mock_n

            main(["--source", "naver", "--save"])

            MockNaver.assert_called_once_with(days_back=3)
            mock_n.run.assert_called_once_with(save=True)
