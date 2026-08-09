"""CLI smoke tests for src.cli.crawl_awards."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

from src.cli.crawl_awards import main


class TestCrawlAwardsCli:
    def _fake_crawler(self) -> Mock:
        instance = Mock()
        instance.run = AsyncMock(return_value=42)
        instance.close = AsyncMock()
        return instance

    def test_dry_run_does_not_save(self) -> None:
        instance = self._fake_crawler()
        with patch("src.cli.crawl_awards.AwardCrawler", return_value=instance):
            main(["--dry-run"])

        instance.run.assert_awaited_once_with(save=False, types=None)
        instance.close.assert_awaited_once()

    def test_default_saves(self) -> None:
        instance = self._fake_crawler()
        with patch("src.cli.crawl_awards.AwardCrawler", return_value=instance):
            main([])

        instance.run.assert_awaited_once_with(save=True, types=None)

    def test_type_filter_passed(self) -> None:
        instance = self._fake_crawler()
        with patch("src.cli.crawl_awards.AwardCrawler", return_value=instance):
            main(["--type", "MVP", "--dry-run"])

        instance.run.assert_awaited_once_with(save=False, types={"MVP"})

    def test_save_and_dry_run_conflict_prefers_save(self) -> None:
        instance = self._fake_crawler()
        with patch("src.cli.crawl_awards.AwardCrawler", return_value=instance):
            main(["--save", "--dry-run"])

        instance.run.assert_awaited_once_with(save=True, types=None)
