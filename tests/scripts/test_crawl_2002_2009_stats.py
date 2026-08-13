import importlib
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _mock_missing_modules():  # noqa: PLR0912
    created_modules: set[str] = set()
    for mod_name in [
        "src.repositories.safe_pitching_repository",
        "src.repositories.safe_batting_repository",
    ]:
        if mod_name not in sys.modules:
            m = types.ModuleType(mod_name)
            m.save_pitching_stats_safe = MagicMock()
            m.save_batting_stats_safe = MagicMock()
            sys.modules[mod_name] = m
            created_modules.add(mod_name)
    _mocked_team_codes = None
    for mod_name in [
        "src.crawlers.player_batting_all_series_crawler",
        "src.crawlers.player_pitching_all_series_crawler",
    ]:
        if mod_name not in sys.modules:
            m = types.ModuleType(mod_name)
            m._build_batting_data = MagicMock()
            m.get_series_mapping = MagicMock(return_value={"regular": {}})
            m._build_pitching_data = MagicMock()
            sys.modules[mod_name] = m
            created_modules.add(mod_name)
        else:
            m = sys.modules[mod_name]
            if not hasattr(m, "_build_pitching_data"):
                m._build_pitching_data = MagicMock()
            if not hasattr(m, "get_series_mapping"):
                m.get_series_mapping = MagicMock(return_value={"regular": {}})
    if "src.utils.team_codes" not in sys.modules:
        m = types.ModuleType("src.utils.team_codes")
        m.resolve_team_code = MagicMock()
        m.build_kbo_game_id = MagicMock()
        sys.modules["src.utils.team_codes"] = m
        _mocked_team_codes = m
    try:
        importlib.import_module("playwright.sync_api")
    except ImportError:
        for mod_name in ["playwright", "playwright.sync_api"]:
            if mod_name in sys.modules:
                continue
            m_pw_sync = types.ModuleType(mod_name)
            m_pw_sync.sync_playwright = MagicMock()
            m_pw_sync.Error = Exception
            m_pw_sync.TimeoutError = TimeoutError
            sys.modules[mod_name] = m_pw_sync
            created_modules.add(mod_name)
    yield
    for mod_name in created_modules:
        sys.modules.pop(mod_name, None)
    if _mocked_team_codes is not None:
        sys.modules.pop("src.utils.team_codes", None)


class TestCrawl20022009Stats:
    def test_main_imports(self):
        from scripts.crawl_2002_2009_stats import crawl_stats_for_year, main

        assert callable(main)
        assert callable(crawl_stats_for_year)

    def test_main_executes(self):
        with (
            patch("scripts.crawl_2002_2009_stats.sync_playwright") as mock_pw,
            patch("scripts.crawl_2002_2009_stats.save_batting_stats_safe"),
            patch("scripts.crawl_2002_2009_stats.save_pitching_stats_safe"),
            patch("scripts.crawl_2002_2009_stats.time.sleep"),
        ):
            mock_browser = MagicMock()
            mock_context = MagicMock()
            mock_page = MagicMock()
            mock_pw.return_value.__enter__.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page
            from scripts.crawl_2002_2009_stats import main

            main()
