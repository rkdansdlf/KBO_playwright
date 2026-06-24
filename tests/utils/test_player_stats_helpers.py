from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.utils.player_stats_helpers import extract_rows_fast


class TestExtractRowsFast:
    def test_returns_none_on_no_table(self) -> None:
        page = MagicMock()
        page.evaluate.return_value = None
        result = extract_rows_fast(page)
        assert result == []

    def test_returns_none_on_empty(self) -> None:
        page = MagicMock()
        page.evaluate.return_value = []
        result = extract_rows_fast(page)
        assert result == []

    def test_returns_rows_on_success(self) -> None:
        page = MagicMock()
        page.evaluate.return_value = [
            {"cells": ["1", "PlayerA", "LG"], "linkText": "PlayerA", "linkHref": "/player/123"},
        ]
        result = extract_rows_fast(page)
        assert result is not None
        assert len(result) == 1
        assert result[0]["cells"] == ["1", "PlayerA", "LG"]

    def test_returns_none_on_exception(self) -> None:
        from playwright.sync_api import Error as PlaywrightError

        page = MagicMock()
        page.evaluate.side_effect = PlaywrightError("timeout")
        result = extract_rows_fast(page)
        assert result is None

    def test_custom_selector(self) -> None:
        page = MagicMock()
        page.evaluate.return_value = []
        extract_rows_fast(page, selector="table.custom")
        page.evaluate.assert_called_once()
        args = page.evaluate.call_args
        assert args[0][1]["selector"] == "table.custom"
