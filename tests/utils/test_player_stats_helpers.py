"""Tests for player_stats_helpers — table row extraction via JS."""

from unittest.mock import MagicMock

from src.utils.player_stats_helpers import extract_rows_fast


class TestExtractRowsFast:
    def test_returns_empty_list_when_no_table(self):
        page = MagicMock()
        page.evaluate.return_value = None
        result = extract_rows_fast(page, "table", "td:nth-child(2) a")
        assert result == []

    def test_returns_payload(self):
        page = MagicMock()
        expected = [{"cells": ["1", "홍길동"], "linkText": "보기", "linkHref": "/player/1"}]
        page.evaluate.return_value = expected
        result = extract_rows_fast(page, "table", "td:nth-child(2) a")
        assert result == expected

    def test_custom_selector_passed(self):
        page = MagicMock()
        page.evaluate.return_value = []
        extract_rows_fast(page, "table.stats", "td:nth-child(3) a")
        args = page.evaluate.call_args[0][1]
        assert args["selector"] == "table.stats"
        assert args["linkQuery"] == "td:nth-child(3) a"

    def test_exception_returns_none(self):
        page = MagicMock()
        page.evaluate.side_effect = Exception("JS error")
        result = extract_rows_fast(page)
        assert result is None
