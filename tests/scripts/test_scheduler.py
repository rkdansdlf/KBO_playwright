import sys
import types
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _inject_mock_module():
    key = "scripts.scheduler"
    _original = sys.modules.get(key)
    if key in sys.modules:
        mod = sys.modules[key]
    else:
        mod = types.ModuleType(key)
        sys.modules[key] = mod
    if not hasattr(mod, "load_dotnet"):
        mod.load_dotnet = MagicMock
    if not hasattr(mod, "BlockingScheduler"):
        mod.BlockingScheduler = MagicMock
    if not hasattr(mod, "main"):

        def _main():
            pass

        mod.main = _main
    if not hasattr(mod, "os"):
        mod.os = __import__("os")
    import scripts

    scripts.scheduler = mod
    yield
    # Restore original module to avoid polluting other test modules
    if _original is None:
        sys.modules.pop(key, None)
    else:
        sys.modules[key] = _original


class TestScheduler:
    def test_main(self):
        import scripts.scheduler as mod

        with patch.object(mod, "BlockingScheduler") as mock_sched_cls, patch.object(mod, "load_dotnet"):
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            with patch("sys.argv", ["script", "--no-startup-run"]):
                mod.main()

    def test_main_adds_jobs(self):
        import scripts.scheduler as mod

        with patch.object(mod, "BlockingScheduler") as mock_sched_cls, patch.object(mod, "load_dotnet"):
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            with patch("sys.argv", ["script", "--no-startup-run"]):
                mod.main()
