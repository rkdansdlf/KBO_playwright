import importlib
from unittest.mock import MagicMock, patch


class TestInspectCatcherTable:
    def test_inspect_catcher_table(self):
        module = importlib.import_module("scripts.inspect_catcher_table")
        with patch.object(module, "sync_playwright") as mock_pw:
            mock_browser = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_page.return_value = mock_page
            mock_page.query_selector_all.return_value = []
            from scripts.inspect_catcher_table import inspect_catcher_table

            inspect_catcher_table()
            mock_page.goto.assert_called_once()
