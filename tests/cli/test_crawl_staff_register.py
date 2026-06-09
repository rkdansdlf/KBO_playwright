from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.crawl_staff_register import main


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
