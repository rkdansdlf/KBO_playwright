"""Unit tests for scripts/crawl_2009_game_details.py."""

from unittest.mock import MagicMock, patch

import scripts.crawl_2009_game_details as mod


class TestCrawl2009GameDetails:
    def test_crawl_2009_details_importable(self):
        assert callable(mod.crawl_2009_details)

    def test_crawl_2009_details_runs_with_mocks(self):
        with (
            patch.object(mod, "sync_playwright") as mock_pw,
            patch.object(mod, "SessionLocal") as mock_session_cls,
            patch.object(mod, "save_game_detail"),
            patch.object(mod, "PlayerIdResolver"),
        ):
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_browser = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_browser
            mock_browser.chromium.launch.return_value = mock_browser
            mock_browser.new_page.return_value = mock_page
            mock_page.query_selector.return_value = None  # Table not found, exits safely

            mod.crawl_2009_details()
            assert mock_page.goto.called
