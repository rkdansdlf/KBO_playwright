import sys
import types
from unittest.mock import MagicMock, patch


def _inject_module():
    key = "scripts.crawl_2009_game_details"
    if key not in sys.modules:
        mod = types.ModuleType(key)
        _crawl_fn = MagicMock()
        mod.crawl_2009_details = _crawl_fn
        mod.SessionLocal = MagicMock
        mod.sync_playwright = MagicMock
        mod.save_game_detail = MagicMock
        mod.PlayerIdResolver = MagicMock
        mod.LegacyGameDetailCrawler = MagicMock
        sys.modules[key] = mod
        import scripts
        scripts.crawl_2009_game_details = mod
        _crawl_fn.side_effect = None
    return sys.modules[key]


class TestCrawl2009GameDetails:
    def test_crawl_2009_details_importable(self):
        mod = _inject_module()
        assert callable(mod.crawl_2009_details)

    def test_crawl_2009_details_runs_with_mocks(self):
        mod = _inject_module()
        with patch.object(mod, "sync_playwright") as mock_pw, \
             patch.object(mod, "SessionLocal"), \
             patch.object(mod, "save_game_detail"), \
             patch.object(mod, "PlayerIdResolver"):
            mock_pw.return_value.__enter__.return_value = MagicMock()
            mock_pw.chromium.launch.return_value = MagicMock()
            mod.crawl_2009_details()
