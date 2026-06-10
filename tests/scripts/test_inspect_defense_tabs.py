from unittest.mock import MagicMock, patch

import pytest


class TestInspectDefenseTabs:
    def test_inspect_defense_tabs(self):
        module = pytest.importorskip("scripts.inspect_defense_tabs")
        with patch.object(module, "sync_playwright") as mock_pw:
            mock_browser = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_page.return_value = mock_page
            mock_page.query_selector_all.return_value = []
            from scripts.inspect_defense_tabs import inspect_defense_tabs

            inspect_defense_tabs()
            mock_page.goto.assert_called_once()
