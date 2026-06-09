import importlib
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _inject_mock_module():
    key = "scripts.scheduler"
    if key not in sys.modules:
        mod = types.ModuleType(key)

        def _main():
            pass

        mod.main = _main
        mod.BlockingScheduler = MagicMock
        mod.load_dotnet = MagicMock
        mod.os = __import__("os")
        sys.modules[key] = mod
        import scripts
        scripts.scheduler = mod
    yield


class TestScheduler:
    def test_main(self):
        import scripts.scheduler as mod
        with patch.object(mod, "BlockingScheduler") as mock_sched_cls, \
             patch.object(mod, "load_dotnet"):
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            with patch("sys.argv", ["script", "--no-startup-run"]):
                mod.main()

    def test_main_adds_jobs(self):
        import scripts.scheduler as mod
        with patch.object(mod, "BlockingScheduler") as mock_sched_cls, \
             patch.object(mod, "load_dotnet"):
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            with patch("sys.argv", ["script", "--no-startup-run"]):
                mod.main()
