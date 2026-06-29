from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from playwright.sync_api import Error as PlaywrightError

from src.crawlers.baserunning_stats_crawler import crawl_baserunning_stats, save_baserunning_stats


@pytest.fixture
def mock_browser():
    mock_playwright = MagicMock()
    mock_browser = MagicMock()
    mock_context = MagicMock()
    mock_page = MagicMock()

    mock_playwright.chromium.launch.return_value = mock_browser
    mock_browser.new_context.return_value = mock_context
    mock_context.new_page.return_value = mock_page

    return mock_playwright, mock_browser, mock_context, mock_page


class TestCrawlBaserunningStats:
    @patch("src.crawlers.baserunning_stats_crawler.sync_playwright")
    def test_returns_empty_on_goto_failure(self, mock_sync_pw):
        mock_pw = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        mock_pw.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page
        mock_page.goto.side_effect = PlaywrightError("timeout")
        mock_sync_pw.return_value.__enter__.return_value = mock_pw

        result = crawl_baserunning_stats(year=2024, max_retries=1)
        assert result == []

    @patch("src.crawlers.baserunning_stats_crawler.sync_playwright")
    def test_parses_table_rows(self, mock_sync_pw):
        mock_pw = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        mock_pw.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page
        mock_page.goto.return_value = None
        mock_sync_pw.return_value.__enter__.return_value = mock_pw

        def make_cell(text):
            cell = MagicMock()
            cell.inner_text.return_value = text
            cell.query_selector.return_value = None
            return cell

        mock_table = MagicMock()
        mock_tbody = MagicMock()
        mock_row = MagicMock()
        mock_row.query_selector_all.return_value = [
            make_cell("1"),
            make_cell("Kim"),
            make_cell("LG"),
            make_cell("100"),
            make_cell("20"),
            make_cell("15"),
            make_cell("5"),
            make_cell("0.750"),
            make_cell("1"),
            make_cell("2"),
        ]
        mock_tbody.query_selector_all.return_value = [mock_row]
        mock_table.query_selector.return_value = mock_tbody
        mock_page.query_selector_all.return_value = [mock_table]

        result = crawl_baserunning_stats(year=2024)
        assert len(result) == 1
        assert result[0]["player_name"] == "Kim"
        assert result[0]["team_id"] == "LG"
        assert result[0]["stolen_bases"] == 15
        assert result[0]["games"] == 100

    @pytest.mark.slow
    @patch("src.crawlers.baserunning_stats_crawler.sync_playwright")
    def test_safe_int_handles_dash(self, mock_sync_pw):
        mock_pw = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        mock_pw.chromium.launch.return_value = mock_browser
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page
        mock_page.goto.return_value = None
        mock_sync_pw.return_value.__enter__.return_value = mock_pw

        def make_cell(text):
            cell = MagicMock()
            cell.inner_text.return_value = text
            cell.query_selector.return_value = None
            return cell

        mock_table = MagicMock()
        mock_tbody = MagicMock()
        mock_row = MagicMock()
        mock_row.query_selector_all.return_value = [
            make_cell("1"),
            make_cell("Park"),
            make_cell("SS"),
            make_cell("-"),
            make_cell("-"),
            make_cell("-"),
            make_cell("-"),
            make_cell("-"),
            make_cell("-"),
            make_cell("-"),
        ]
        mock_tbody.query_selector_all.return_value = [mock_row]
        mock_table.query_selector.return_value = mock_tbody
        mock_page.query_selector_all.return_value = [mock_table]

        result = crawl_baserunning_stats(year=2024)
        assert len(result) == 1
        assert result[0]["games"] == 0
        assert result[0]["stolen_bases"] == 0


class TestSaveBaserunningStats:
    @patch("src.crawlers.baserunning_stats_crawler.crawl_baserunning_stats")
    @patch("src.crawlers.baserunning_stats_crawler.sqlite3.connect")
    def test_saves_with_player_id_from_crawl(self, mock_connect, mock_crawl):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_crawl.return_value = [
            {
                "player_id": "12345",
                "player_name": "Kim",
                "team_id": "LG",
                "year": 2024,
                "games": 100,
                "stolen_base_attempts": 20,
                "stolen_bases": 15,
                "caught_stealing": 5,
                "stolen_base_percentage": 0.75,
                "out_on_base": 1,
                "picked_off": 2,
            },
        ]

        save_baserunning_stats([], year=2024, db_path=":memory:")

        mock_cursor.execute.assert_any_call(mock_cursor.execute.call_args[0][0], mock_cursor.execute.call_args[0][1])
        assert mock_cursor.execute.call_count >= 1

    @patch("src.crawlers.baserunning_stats_crawler.crawl_baserunning_stats")
    @patch("src.crawlers.baserunning_stats_crawler.sqlite3.connect")
    def test_handles_empty_data(self, mock_connect, mock_crawl):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_crawl.return_value = []

        save_baserunning_stats([], year=2024, db_path=":memory:")
        mock_conn.close.assert_called_once()
