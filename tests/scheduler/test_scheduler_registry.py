"""Unit tests for src/scheduler/registry.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.scheduler.registry import (
    _dispatch_single_run,
    build_arg_parser,
    main,
)


def test_build_arg_parser():
    parser = build_arg_parser()
    args = parser.parse_args(["--run-once", "--limit", "10", "--no-startup-run"])
    assert args.run_once is True
    assert args.limit == 10
    assert args.no_startup_run is True


def test_dispatch_single_run():
    parser = build_arg_parser()

    args_run_once = parser.parse_args(["--run-once"])
    with patch("src.scheduler.registry.crawl_daily_games") as mock_daily:
        assert _dispatch_single_run(args_run_once) is True
        mock_daily.assert_called_once()

    args_pregame = parser.parse_args(["--run-pregame-once"])
    with patch("src.scheduler.registry.crawl_pregame_refresh") as mock_pregame:
        assert _dispatch_single_run(args_pregame) is True
        mock_pregame.assert_called_once()

    args_retire = parser.parse_args(["--run-retire-once", "--limit", "5"])
    with patch("src.scheduler.registry.crawl_retired_players_job") as mock_retire:
        assert _dispatch_single_run(args_retire) is True
        mock_retire.assert_called_once_with(limit=5)

    args_auto_heal = parser.parse_args(["--run-auto-heal-once"])
    with patch("src.scheduler.registry.auto_heal_games_job") as mock_heal:
        assert _dispatch_single_run(args_auto_heal) is True
        mock_heal.assert_called_once()

    args_integrity = parser.parse_args(["--run-integrity-check-once"])
    with patch("src.scheduler.registry.data_integrity_check_job") as mock_integrity:
        assert _dispatch_single_run(args_integrity) is True
        mock_integrity.assert_called_once()

    args_none = parser.parse_args([])
    assert _dispatch_single_run(args_none) is False


def test_main_single_run():
    with (
        patch("src.scheduler.registry._dispatch_single_run", return_value=True) as mock_dispatch,
        patch("src.scheduler.registry._start_scheduler") as mock_start,
    ):
        main(["--run-once"])
        mock_dispatch.assert_called_once()
        mock_start.assert_not_called()


def test_live_refresh_jobs_register_the_measured_grace_window(monkeypatch):
    """The 10s live-refresh jobs must not discard ticks that overlap a running cycle.

    A refresh cycle measured 70-152s while the grace window was 5s, so up to ~15 ticks
    per cycle were dropped and the job read as dead whenever the scheduler fell behind.
    """
    import scripts.scheduler as scripts_scheduler

    from src.scheduler import registry

    added = {}

    class _FakeScheduler:
        def add_job(self, fn, **kwargs):
            if kwargs.get("id"):
                added[kwargs["id"]] = kwargs

        def add_listener(self, *args, **kwargs):
            return None

        def start(self):
            return None

    def _fake_scheduler_cls(**kwargs):
        return _FakeScheduler()

    # ``_start_scheduler`` resolves the class through ``sys.modules`` and prefers
    # ``scripts.scheduler``. Patching only the registry global would let the real
    # blocking scheduler start, which hangs the test instead of failing it.
    monkeypatch.setattr(scripts_scheduler, "BlockingScheduler", _fake_scheduler_cls)
    monkeypatch.setattr(registry, "BlockingScheduler", _fake_scheduler_cls)
    monkeypatch.setattr(registry, "_SCHEDULER_REF", None, raising=False)
    monkeypatch.setenv("STARTUP_RUN", "0")

    with patch("signal.signal"):
        registry._start_scheduler(registry.build_arg_parser().parse_args(["--no-startup-run"]))

    for job_id in ("crawl_live_refresh_day", "crawl_live_refresh_night"):
        registered = added[job_id]
        assert registered["misfire_grace_time"] == registry.LIVE_MISFIRE_GRACE_SECONDS
        assert registered["misfire_grace_time"] >= 300
