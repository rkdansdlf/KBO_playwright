"""Tests for playwright_helpers — page navigation utilities."""

from unittest.mock import MagicMock, patch

import pytest

from src.utils.playwright_helpers import goto_next_page


class TestGotoNextPage:
    def test_no_pagination_returns_false(self):
        page = MagicMock()
        page.query_selector.return_value = None
        assert not goto_next_page(page)

    def test_no_next_link_returns_false(self):
        page = MagicMock()
        pagination = MagicMock()
        pagination.query_selector_all.return_value = []
        page.query_selector.return_value = pagination
        assert not goto_next_page(page)

    def test_next_link_in_text_clicks(self):
        page = MagicMock()
        link = MagicMock()
        link.inner_text.return_value = "다음"
        link.get_attribute.return_value = "/page2"
        pagination = MagicMock()
        pagination.query_selector_all.return_value = [link]
        page.query_selector.return_value = pagination
        result = goto_next_page(page)
        assert result
        link.click.assert_called_once()
        page.wait_for_load_state.assert_called_once_with("networkidle", timeout=30000)

    def test_greater_than_sign_clicks(self):
        page = MagicMock()
        link = MagicMock()
        link.inner_text.return_value = ">"
        link.get_attribute.return_value = "/page2"
        pagination = MagicMock()
        pagination.query_selector_all.return_value = [link]
        page.query_selector.return_value = pagination
        result = goto_next_page(page)
        assert result
        link.click.assert_called_once()

    def test_javascript_href_skipped(self):
        page = MagicMock()
        link = MagicMock()
        link.inner_text.return_value = "다음"
        link.get_attribute.return_value = "javascript:__doPostBack('ctl00$...')"
        pagination = MagicMock()
        pagination.query_selector_all.return_value = [link]
        page.query_selector.return_value = pagination
        result = goto_next_page(page)
        assert not result
        link.click.assert_not_called()

    def test_policy_delay_called(self):
        page = MagicMock()
        link = MagicMock()
        link.inner_text.return_value = "다음"
        link.get_attribute.return_value = "/page2"
        pagination = MagicMock()
        pagination.query_selector_all.return_value = [link]
        page.query_selector.return_value = pagination
        policy = MagicMock()
        result = goto_next_page(page, policy)
        assert result
        policy.delay.assert_called_once()

    def test_exception_returns_false(self):
        page = MagicMock()
        page.query_selector.side_effect = Exception("fail")
        assert not goto_next_page(page)
