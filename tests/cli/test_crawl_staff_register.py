from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.crawl_staff_register import main, run_crawler


class TestCrawlStaffRegisterCLI:
    def test_main_team(self):
        with patch("src.cli.crawl_staff_register.StaffRegisterCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_all_teams = AsyncMock(return_value=[])
            mock_instance.save_to_db = MagicMock()
            MockCrawler.return_value = mock_instance

            with pytest.raises(SystemExit) as exc:
                main(["--team", "LG"])
            assert exc.value.code == 0

            MockCrawler.assert_called_once_with(headless=True)
            mock_instance.crawl_all_teams.assert_called_once_with(team_codes=["LG"])
            mock_instance.save_to_db.assert_called_once_with([], dry_run=False)

    def test_main_all_teams(self):
        with patch("src.cli.crawl_staff_register.StaffRegisterCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_all_teams = AsyncMock(return_value=[])
            mock_instance.save_to_db = MagicMock()
            MockCrawler.return_value = mock_instance

            with pytest.raises(SystemExit) as exc:
                main(["--all-teams"])
            assert exc.value.code == 0

            mock_instance.crawl_all_teams.assert_called_once()

    def test_main_dry_run(self):
        with patch("src.cli.crawl_staff_register.StaffRegisterCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_all_teams = AsyncMock(return_value=[])
            mock_instance.save_to_db = MagicMock()
            MockCrawler.return_value = mock_instance

            with pytest.raises(SystemExit) as exc:
                main(["--team", "SS", "--dry-run"])
            assert exc.value.code == 0

            mock_instance.save_to_db.assert_called_once_with([], dry_run=True)


class TestRunCrawler:
    @pytest.mark.asyncio
    async def test_invalid_team_returns_error_without_creating_crawler(self):
        args = MagicMock(all_teams=False, team="bad", dry_run=False, sync_oci=False)

        with patch("src.cli.crawl_staff_register.StaffRegisterCrawler") as crawler_cls:
            result = await run_crawler(args)

        assert result == 1
        crawler_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_sync_oci_skips_when_no_player_ids(self):
        args = MagicMock(all_teams=False, team="lg", dry_run=False, sync_oci=True)
        crawler = MagicMock()
        crawler.crawl_all_teams = AsyncMock(return_value=[{"player_id": None}])

        with patch("src.cli.crawl_staff_register.StaffRegisterCrawler", return_value=crawler):
            result = await run_crawler(args)

        assert result == 0
        crawler.save_to_db.assert_called_once_with([{"player_id": None}], dry_run=False)

    @pytest.mark.asyncio
    async def test_dry_run_does_not_attempt_oci_sync(self):
        args = MagicMock(all_teams=False, team="LG", dry_run=True, sync_oci=True)
        crawler = MagicMock()
        crawler.crawl_all_teams = AsyncMock(return_value=[{"player_id": "123"}])

        with patch("src.cli.crawl_staff_register.StaffRegisterCrawler", return_value=crawler):
            result = await run_crawler(args)

        assert result == 0
        crawler.save_to_db.assert_called_once_with([{"player_id": "123"}], dry_run=True)
