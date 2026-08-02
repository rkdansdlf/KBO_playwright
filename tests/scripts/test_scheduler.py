from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "scheduler.py"


@pytest.fixture(autouse=True)
def _inject_mock_module():
    key = "scripts.scheduler"
    _original = sys.modules.get(key)
    import scripts

    _original_attr = getattr(scripts, "scheduler", None)
    _had_original_attr = hasattr(scripts, "scheduler")
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

    scripts.scheduler = mod
    yield
    # Restore original module to avoid polluting other test modules
    if _original is None:
        sys.modules.pop(key, None)
    else:
        sys.modules[key] = _original
    if _had_original_attr:
        scripts.scheduler = _original_attr
    else:
        delattr(scripts, "scheduler")


def _load_real_scheduler():
    spec = importlib.util.spec_from_file_location("scripts_scheduler_real", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestScheduler:
    def test_main(self):
        import scripts.scheduler as mod

        with (
            patch.object(mod, "BlockingScheduler") as mock_sched_cls,
            patch.object(mod, "load_dotnet"),
            patch.object(mod, "_ensure_single_scheduler_instance", create=True),
            patch.object(mod, "start_metrics_server", create=True),
        ):
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            with patch("sys.argv", ["script", "--no-startup-run"]):
                mod.main()

    def test_main_adds_jobs(self):
        import scripts.scheduler as mod

        with (
            patch.object(mod, "BlockingScheduler") as mock_sched_cls,
            patch.object(mod, "load_dotnet"),
            patch.object(mod, "_ensure_single_scheduler_instance", create=True),
            patch.object(mod, "start_metrics_server", create=True),
        ):
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            with patch("sys.argv", ["script", "--no-startup-run"]):
                mod.main()

    def test_auto_heal_games_job(self):
        mod = _load_real_scheduler()
        with patch("src.cli.auto_healer.run_healer_async", new_callable=MagicMock) as mock_healer:
            mock_healer.return_value = 0
            with patch("asyncio.run", return_value=0) as mock_asyncio_run:
                mod.auto_heal_games_job()
                mock_asyncio_run.assert_called_once()

    def test_run_auto_heal_once_flag(self):
        mod = _load_real_scheduler()
        with patch.object(mod, "auto_heal_games_job") as mock_job:
            with patch("sys.argv", ["script", "--run-auto-heal-once"]):
                args = mod.build_arg_parser().parse_args(["--run-auto-heal-once"])
                res = mod._dispatch_single_run(args)
                assert res is True
                mock_job.assert_called_once()

    def test_data_integrity_check_job(self):
        mod = _load_real_scheduler()
        report = MagicMock(failed_checks=0, total_checks=5)
        with patch("src.cli.data_integrity_checker.run_integrity_checks", return_value=report) as mock_check:
            mod.data_integrity_check_job()
            mock_check.assert_called_once()

    def test_run_integrity_check_once_flag(self):
        mod = _load_real_scheduler()
        with patch.object(mod, "data_integrity_check_job") as mock_job:
            args = mod.build_arg_parser().parse_args(["--run-integrity-check-once"])
            res = mod._dispatch_single_run(args)
            assert res is True
            mock_job.assert_called_once()

    def test_update_oci_sync_lag_metrics_triggers_recovery(self):
        mod = _load_real_scheduler()
        mod._last_oci_auto_sync_trigger = 0.0
        lag_res = {"overall_max_lag_seconds": 30000.0}  # Exceeds default 21600s
        with (
            patch("src.sync.lag_monitor.check_and_resync_lagging_tables", return_value=lag_res),
            patch.object(mod, "sync_from_oci_job") as mock_sync,
        ):
            mod.update_oci_sync_lag_metrics()
            mock_sync.assert_called_once()
