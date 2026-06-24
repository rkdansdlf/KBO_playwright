from datetime import datetime
from types import SimpleNamespace

import pytest

import scripts.scheduler as scheduler
import src.cli.generate_quality_report as generate_quality_report


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


def test_alert_failure_sends_alert_and_does_not_raise(monkeypatch):
    sent = []
    exc = RuntimeError("boom")

    monkeypatch.setattr(
        scheduler.SlackWebhookClient,
        "send_error_alert",
        lambda message: sent.append(message) or True,
    )

    result = scheduler.alert_failure(_retry_state(exc))

    assert result is None
    assert len(sent) == 1
    assert "sample_job" in sent[0]
    assert "boom" in sent[0]


def test_alert_failure_handles_alert_error_gracefully(monkeypatch):
    exc = ValueError("alert transport failed too")

    def _raise_alert(_message):
        raise OSError("slack down")

    monkeypatch.setattr(scheduler.SlackWebhookClient, "send_error_alert", _raise_alert)

    result = scheduler.alert_failure(_retry_state(exc))

    assert result is None


def test_alert_success_is_optional_and_non_blocking(monkeypatch):
    calls = []

    monkeypatch.delenv("NOTIFY_SUCCESS", raising=False)
    monkeypatch.setattr(
        scheduler.SlackWebhookClient,
        "send_alert",
        lambda message: calls.append(message) or True,
    )

    scheduler.alert_success("sample_job")
    assert calls == []

    monkeypatch.setenv("NOTIFY_SUCCESS", "1")

    def _raise_success(_message):
        calls.append("called")
        raise OSError("slack down")

    monkeypatch.setattr(scheduler.SlackWebhookClient, "send_alert", _raise_success)
    scheduler.alert_success("sample_job")

    assert calls == ["called"]


def test_alert_success_includes_optional_details(monkeypatch):
    calls = []

    monkeypatch.setenv("NOTIFY_SUCCESS", "1")
    monkeypatch.setattr(
        scheduler.SlackWebhookClient,
        "send_alert",
        lambda message: calls.append(message) or True,
    )

    scheduler.alert_success("sample_job", "detail_failures=incomplete_detail=1")

    assert calls == ["✅ KBO Job sample_job completed successfully.\ndetail_failures=incomplete_detail=1"]


def test_sync_from_oci_job_runs_hydration_for_current_year(monkeypatch):
    calls = []
    fixed_now = datetime(2026, 1, 2, 5, 0, tzinfo=scheduler.KST)

    class _FrozenDateTime:
        @staticmethod
        def now(tz=None):
            return fixed_now

    monkeypatch.setattr(scheduler, "datetime", _FrozenDateTime)
    monkeypatch.setattr(scheduler, "_run_hydration", lambda year: calls.append(year))

    scheduler.sync_from_oci_job()

    assert calls == [2026]


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

    assert calls == [{"sync_to_oci": False, "max_active_games": 1, "detail_snapshot_background": True}]


def test_pregame_refresh_queues_realtime_oci_sync_without_inline_sync(monkeypatch):
    run_calls = []
    submit_calls = []
    summary_calls = []

    monkeypatch.setenv("OCI_DB_URL", "postgresql://oci-host/kbo")
    monkeypatch.setenv("PREGAME_SYNC_TO_OCI", "1")
    monkeypatch.setattr(scheduler, "_pregame_target_dates", lambda: ["20260605"])
    monkeypatch.setattr(scheduler, "alert_success", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        scheduler,
        "_submit_realtime_oci_sync",
        lambda sync_kind, game_ids: submit_calls.append((sync_kind, list(game_ids))) or True,
    )

    def _fake_summary(target_date):
        summary_calls.append(target_date)
        if len(summary_calls) == 1:
            return 5, 5, 5
        return 5, 0, 0

    async def _fake_preview_batch(target_date, *, sync_to_oci=None):
        run_calls.append((target_date, sync_to_oci))
        return ["20260605WOOB0", "20260605HHLT0"]

    monkeypatch.setattr(scheduler, "_pregame_refresh_summary", _fake_summary)
    monkeypatch.setattr(scheduler, "run_preview_batch", _fake_preview_batch)

    scheduler.crawl_pregame_refresh()

    assert run_calls == [("20260605", False)]
    assert submit_calls == [("pregame", ["20260605WOOB0", "20260605HHLT0"])]


def test_live_refresh_queues_realtime_oci_sync_without_inline_sync(monkeypatch):
    run_calls = []
    submit_calls = []

    async def _fake_live_cycle(**kwargs):
        run_calls.append(kwargs)
        return {
            "active": True,
            "game_ids_playing": ["20260605WOOB0", "20260605HHLT0"],
        }

    monkeypatch.setenv("OCI_DB_URL", "postgresql://oci-host/kbo")
    monkeypatch.setattr(scheduler, "_should_skip_live_for_pregame", lambda: False)
    monkeypatch.setattr(scheduler, "_get_live_poll_interval_seconds", lambda: 0)
    monkeypatch.setattr(scheduler, "_live_refresh_max_games_per_cycle", lambda: 1)
    monkeypatch.setattr(scheduler, "LAST_LIVE_RUN_TIME", None)
    monkeypatch.setattr(scheduler, "LAST_LIVE_POLL_INTERVAL", None)
    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", _fake_live_cycle)
    monkeypatch.setattr(
        scheduler,
        "_submit_realtime_oci_sync",
        lambda sync_kind, game_ids: submit_calls.append((sync_kind, list(game_ids))) or True,
    )

    scheduler.crawl_live_refresh()

    assert run_calls == [{"sync_to_oci": False, "max_active_games": 1, "detail_snapshot_background": True}]
    assert submit_calls == [("live", ["20260605WOOB0", "20260605HHLT0"])]


def test_realtime_oci_sync_submitter_skips_when_worker_is_still_running(monkeypatch):
    thread_calls = []

    def _fake_thread(**kwargs):
        thread_calls.append(kwargs)
        raise AssertionError("Thread should not be created while realtime sync lock is held")

    monkeypatch.setenv("OCI_DB_URL", "postgresql://oci-host/kbo")
    monkeypatch.setattr(scheduler, "Thread", _fake_thread)

    assert scheduler.REALTIME_OCI_SYNC_LOCK.acquire(blocking=False)
    try:
        assert scheduler._submit_realtime_oci_sync("live", ["20260605WOOB0"]) is False
    finally:
        scheduler.REALTIME_OCI_SYNC_LOCK.release()

    assert thread_calls == []


def test_realtime_oci_sync_worker_isolates_game_failures_and_releases_lock(monkeypatch):
    calls = []

    class _ImmediateThread:
        def __init__(self, *, target, name, daemon):
            self.target = target
            calls.append(("thread", name, daemon))

        def start(self):
            self.target()

    class _FakeSession:
        def __init__(self, name):
            self.name = name

        def __enter__(self):
            calls.append(("session_enter", self.name))
            return self.name

        def __exit__(self, exc_type, exc, tb):
            calls.append(("session_exit", self.name, exc_type))
            return False

    class _PerGameSyncer:
        def __init__(self, oci_url, session):
            self.session = session
            calls.append(("syncer", oci_url, session))

        def sync_specific_game(self, game_id):
            calls.append(("sync_specific_game", game_id, self.session))
            if game_id == "20260605AAA0":
                raise RuntimeError("operation timed out")

        def close(self):
            calls.append(("close", self.session))

    session_names = []

    def _session_factory():
        name = f"session-{len(session_names) + 1}"
        session_names.append(name)
        return _FakeSession(name)

    monkeypatch.setenv("OCI_DB_URL", "postgresql://oci-host/kbo")
    monkeypatch.setattr(scheduler, "Thread", _ImmediateThread)
    monkeypatch.setattr(scheduler, "SessionLocal", _session_factory)
    monkeypatch.setattr(scheduler, "OCISync", _PerGameSyncer)

    assert scheduler._submit_realtime_oci_sync("live", ["20260605BBB0", "20260605AAA0"]) is True

    assert [call for call in calls if call[0] == "sync_specific_game"] == [
        ("sync_specific_game", "20260605AAA0", "session-1"),
        ("sync_specific_game", "20260605BBB0", "session-2"),
    ]
    assert [call for call in calls if call[0] == "close"] == [
        ("close", "session-1"),
        ("close", "session-2"),
    ]
    assert scheduler.REALTIME_OCI_SYNC_LOCK.acquire(blocking=False)
    scheduler.REALTIME_OCI_SYNC_LOCK.release()


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

    scheduler.crawl_p0_non_game_job()

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


def test_main_registers_morning_jobs_with_expected_cron(monkeypatch):
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

    scheduler.main(["--no-startup-run"])

    ids_to_trigger = {kwargs["id"]: trigger.kwargs for _, trigger, kwargs in scheduled if "id" in kwargs}
    ids_to_kwargs = {kwargs["id"]: kwargs for _, _, kwargs in scheduled if "id" in kwargs}

    assert ids_to_trigger["crawl_p0_non_game"] == {"hour": 6, "minute": 20}
    assert ids_to_trigger["crawl_pregame_refresh"] == {"hour": "10-23", "minute": "*/15"}
    assert ids_to_trigger["crawl_live_refresh_day"] == {"hour": "12-22", "second": "*/10"}
    assert ids_to_trigger["crawl_live_refresh_night"] == {"hour": 23, "minute": "0-30", "second": "*/10"}
    assert ids_to_kwargs["crawl_live_refresh_day"]["max_instances"] == 1
    assert ids_to_kwargs["crawl_live_refresh_night"]["max_instances"] == 1
