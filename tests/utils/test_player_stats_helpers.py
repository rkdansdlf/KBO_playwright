from __future__ import annotations

from src.utils.player_stats_helpers import extract_rows_fast


class TestExtractRowsFast:
    def test_function_exists(self):
        assert callable(extract_rows_fast)

    def test_default_selector(self):
        import inspect
        sig = inspect.signature(extract_rows_fast)
        params = sig.parameters
        assert "selector" in params
        assert params["selector"].default == "table"

    def test_default_link_query(self):
        import inspect
        sig = inspect.signature(extract_rows_fast)
        params = sig.parameters
        assert "link_query" in params
        assert params["link_query"].default == "td:nth-child(2) a"
