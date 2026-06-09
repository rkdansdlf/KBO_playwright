from unittest.mock import MagicMock, patch


class TestInspectDefenseDropdowns:
    def test_inspect_defense_dropdowns(self):
        with patch("scripts.inspect_defense_dropdowns.sync_playwright") as mock_pw:
            mock_browser = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_page.return_value = mock_page
            mock_page.query_selector_all.return_value = []
            from scripts.inspect_defense_dropdowns import inspect_defense_dropdowns
            inspect_defense_dropdowns()
            mock_page.goto.assert_called_once()
