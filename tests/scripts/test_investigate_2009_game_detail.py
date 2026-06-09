import sys
import types
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _mock_playwright():
    for mod_name in ["playwright", "playwright.sync_api"]:
        m = types.ModuleType(mod_name)
        if mod_name == "playwright.sync_api":
            m.sync_playwright = MagicMock()
        sys.modules[mod_name] = m
    yield


class TestInvestigate2009GameDetail:
    def test_investigate_2009_game_detail(self):
        with patch("scripts.investigate_2009_game_detail.sync_playwright") as mock_pw, \
             patch("scripts.investigate_2009_game_detail.time.sleep"):
            mock_browser = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_page.return_value = mock_page
            mock_page.query_selector.return_value = None
            mock_page.query_selector_all.return_value = []
            from scripts.investigate_2009_game_detail import investigate_2009_game_detail
            investigate_2009_game_detail()
            mock_page.goto.assert_called_once()
