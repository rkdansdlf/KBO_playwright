from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import scripts.scheduler as scheduler
import src.cli.generate_quality_report as generate_quality_report


pytestmark = pytest.mark.integration


class _Outcome:
    def __init__(self, exc):
        self._exc = exc

    def exception(self):
        return self._exc


def _retry_state(exc):
    return SimpleNamespace(
        outcome=_Outcome(exc),
        fn=SimpleNamespace(__name__="sample_job"),
        attempt_number=3,
    )


def test_alert_failure_publishes_incident(monkeypatch):
    captured = []
    import src.scheduler.alerting as alerting

    monkeypatch.setattr(alerting, "apply_incidents", lambda events, **kw: captured.append((events, kw)))

    result = scheduler.alert_failure(_retry_state(RuntimeError("boom")))

    assert result is None
    events, _kwargs = captured[0]
    assert len(events) == 1
    assert events[0].incident_key == "scheduler:sample_job:failed"
    assert events[0].source.value == "scheduler"
    assert "boom" in events[0].message


def test_alert_failure_handles_alert_error_gracefully(monkeypatch):
    import src.notifications.bridge as bridge

    def _broken_session_factory():
        raise OSError("db down")

    monkeypatch.setattr(bridge, "SessionLocal", _broken_session_factory)

    result = scheduler.alert_failure(_retry_state(ValueError("alert transport failed too")))

    assert result is None


def test_alert_success_resolves_and_respects_notify_flag(monkeypatch):
    captured = []
    import src.scheduler.alerting as alerting

    monkeypatch.setattr(alerting, "apply_incidents", lambda events, **kw: captured.append((events, kw)))

    monkeypatch.delenv("NOTIFY_SUCCESS", raising=False)
    scheduler.alert_success("sample_job")

    events, kwargs = captured[0]
    assert events == []
    assert set(kwargs["resolve_keys"]) == {
        "scheduler:sample_job:warning",
        "scheduler:sample_job:failed",
    }
    assert kwargs["dry_run"] is True

    monkeypatch.setenv("NOTIFY_SUCCESS", "1")
    scheduler.alert_success("sample_job")
    assert captured[1][1]["dry_run"] is False


def test_alert_warning_publishes_incident(monkeypatch):
    captured = []
    import src.scheduler.alerting as alerting

    monkeypatch.setattr(alerting, "apply_incidents", lambda events, **kw: captured.append(events))

    scheduler.alert_warning("crawl_p1p2_data_job", "lock contention")

    events = captured[0]
    assert len(events) == 1
    assert events[0].incident_key == "scheduler:crawl_p1p2_data_job:warning"
    assert events[0].severity.value == "WARNING"
    assert "lock contention" in events[0].message


def test_live_refresh_uses_bounded_default_shard(monkeypatch):
    calls = []

    async def _fake_live_cycle(**kwargs):
        calls.append(kwargs)
        return {}

    monkeypatch.delenv("LIVE_REFRESH_MAX_GAMES_PER_CYCLE", raising=False)
    monkeypatch.setattr(scheduler, "_should_skip_live_for_pregame", lambda: False)
    monkeypatch.setattr(scheduler, "LAST_LIVE_RUN_TIME", None)
    monkeypatch.setattr(scheduler, "LAST_LIVE_POLL_INTERVAL", None)
    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", _fake_live_cycle)

    scheduler.crawl_live_refresh()

    assert calls == [{"max_active_games": 1, "detail_snapshot_background": True}]


def test_crawl_p0_non_game_job_invokes_unified_cli(monkeypatch):
    from src.cli import crawl_p0_data

    calls = []
    fixed_now = datetime(2026, 6, 5, 6, 20, tzinfo=scheduler.KST)

    class _FrozenDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now

    monkeypatch.setattr(scheduler, "datetime", _FrozenDateTime)
    monkeypatch.setattr(crawl_p0_data, "main", lambda argv: calls.append(list(argv)) or {"events": 1})
    monkeypatch.setattr(scheduler, "alert_success", lambda *_args, **_kwargs: None)

    # The dependency gate requires crawl_daily_games to be registered SUCCESS.
    from src.scheduler.jobs.daily import JobStatus, _register_job, _update_job_status, clear_job_registry

    clear_job_registry()
    _register_job("crawl_daily_games", dependencies=[])
    _update_job_status("crawl_daily_games", JobStatus.SUCCESS, "test setup")
    try:
        scheduler.crawl_p0_non_game_job()
    finally:
        clear_job_registry()

    assert calls == [["--type", "all", "--save", "--days", "3", "--season", "2026"]]


def test_health_and_freshness_checks_use_canonical_table_names():
    from src.cli import health_check, monitor_data_freshness
    from src.models.roster_transaction import RosterTransaction
    from src.models.standings import TeamStandingsDaily
    from src.models.team_event import TeamEvent
    from src.models.ticket_open_rule import TicketOpenRule
    from src.models.ticket_price import TicketPrice

    assert monitor_data_freshness.DOMAIN_TABLE_CHECKS["event"][0] == TeamEvent.__tablename__
    assert monitor_data_freshness.DOMAIN_TABLE_CHECKS["roster"][0] == RosterTransaction.__tablename__
    assert monitor_data_freshness.DOMAIN_TABLE_CHECKS["ticket"][0] == TicketPrice.__tablename__

    health_tables = {table for table, _date_col in health_check.TABLE_CHECKS}
    assert TeamEvent.__tablename__ in health_tables
    assert RosterTransaction.__tablename__ in health_tables
    assert TicketPrice.__tablename__ in health_tables
    assert TicketOpenRule.__tablename__ in health_tables
    assert TeamStandingsDaily.__tablename__ in health_tables


def test_main_registers_morning_jobs_with_expected_cron(monkeypatch, tmp_path):
    scheduled = []

    class _FakeTrigger:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def __repr__(self):  # pragma: no cover - debug helper only
            return f"FakeCronTrigger({self.kwargs})"

    class _FakeScheduler:
        def add_job(self, func, trigger, **kwargs):
            scheduled.append((getattr(func, "__name__", str(func)), trigger, kwargs))

        def add_listener(self, callback, mask):
            pass

        def start(self):
            return None

    monkeypatch.setattr(scheduler, "CronTrigger", _FakeTrigger)
    monkeypatch.setattr(scheduler, "BlockingScheduler", lambda timezone=None: _FakeScheduler())
    monkeypatch.setenv("STARTUP_RUN", "0")
    monkeypatch.setattr(scheduler, "crawl_pregame_refresh", lambda: None)
    monkeypatch.setattr(scheduler, "crawl_live_refresh", lambda: None)
    monkeypatch.setattr(scheduler, "crawl_phase1_extra_job", lambda: None)
    monkeypatch.setattr(scheduler, "crawl_p0_non_game_job", lambda: None)
    monkeypatch.setattr(scheduler, "_SCHEDULER_PID_FILE", tmp_path / "scheduler.pid")

    scheduler.main(["--no-startup-run"])

    ids_to_trigger = {kwargs["id"]: trigger.kwargs for _, trigger, kwargs in scheduled if "id" in kwargs}
    ids_to_kwargs = {kwargs["id"]: kwargs for _, _, kwargs in scheduled if "id" in kwargs}

    assert ids_to_trigger["crawl_p0_non_game"] == {"hour": 6, "minute": 20}
    assert ids_to_trigger["crawl_pregame_refresh"] == {"hour": "10-23", "minute": "*/15"}
    assert ids_to_trigger["crawl_live_refresh_day"] == {"hour": "12-22", "second": "*/10"}
    assert ids_to_trigger["crawl_live_refresh_night"] == {"hour": 23, "minute": "0-30", "second": "*/10"}
    assert ids_to_kwargs["crawl_live_refresh_day"]["max_instances"] == 1
    assert ids_to_kwargs["crawl_live_refresh_night"]["max_instances"] == 1


def test_job_lifecycle_listener_publishes_failure_incident(monkeypatch):
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
    from src.scheduler import metrics

    captured = []
    monkeypatch.setattr(metrics, "apply_incidents", lambda events, **kw: captured.append((events, kw)))
    monkeypatch.setattr(metrics.sentry_sdk, "capture_exception", lambda exc: None)
    metrics.job_start_times.clear()

    err_event = SimpleNamespace(
        code=EVENT_JOB_ERROR,
        job_id="crawl_live_refresh_day",
        exception=RuntimeError("DPY-6000: connection refused"),
    )
    metrics.job_lifecycle_listener(err_event)

    events, _kwargs = captured[0]
    assert events[0].incident_key == "scheduler:crawl_live_refresh_day:failed"
    assert events[0].severity.value == "ERROR"
    assert "DPY-6000" in events[0].message
    assert events[0].metadata["job"] == "crawl_live_refresh_day"

    ok_event = SimpleNamespace(code=EVENT_JOB_EXECUTED, job_id="crawl_live_refresh_day")
    metrics.job_lifecycle_listener(ok_event)

    assert captured[1][1]["resolve_keys"] == ["scheduler:crawl_live_refresh_day:failed"]


def test_job_lifecycle_listener_ignores_error_without_exception(monkeypatch):
    from apscheduler.events import EVENT_JOB_ERROR
    from src.scheduler import metrics

    captured = []
    monkeypatch.setattr(metrics, "apply_incidents", lambda events, **kw: captured.append(events))
    metrics.job_start_times.clear()

    metrics.job_lifecycle_listener(SimpleNamespace(code=EVENT_JOB_ERROR, job_id="x", exception=None))

    assert captured == []
