from unittest.mock import MagicMock, patch


class TestDumpDefenseHTML:
    def test_dump_defense_html(self):
        with patch("scripts.dump_defense_html.sync_playwright") as mock_pw:
            mock_browser = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_page.return_value = mock_page
            mock_page.query_selector_all.return_value = []
            from scripts.dump_defense_html import dump_defense_html
            dump_defense_html()
            mock_page.goto.assert_called_once()
