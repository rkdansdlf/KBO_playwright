from datetime import datetime
from types import SimpleNamespace

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
    monkeypatch.setattr(scheduler, "run_live_crawler_cycle", _fake_live_cycle)

    scheduler.crawl_live_refresh()

    assert calls == [{"max_active_games": 1}]


def test_generate_daily_report_job_forces_morning_summary_notify(monkeypatch):
    calls = []
    monkeypatch.setattr(scheduler, "_previous_day_kst", lambda: "20260513")

    def _fake_report_main(argv):
        calls.append(list(argv))
        return 0

    monkeypatch.setattr(generate_quality_report, "main", _fake_report_main)
    scheduler.generate_daily_report_job()

    assert calls == [["--date", "20260513", "--force-notify"]]


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
    monkeypatch.setattr(scheduler, "crawl_daily_games", lambda: None)
    monkeypatch.setattr(scheduler, "crawl_pregame_refresh", lambda: None)
    monkeypatch.setattr(scheduler, "crawl_live_refresh", lambda: None)
    monkeypatch.setattr(scheduler, "crawl_all_futures_profiles", lambda: None)
    monkeypatch.setattr(scheduler, "sync_from_oci_job", lambda: None)
    monkeypatch.setattr(scheduler, "generate_daily_report_job", lambda: None)

    scheduler.main(["--no-startup-run"])

    ids_to_trigger = {kwargs["id"]: trigger.kwargs for _, trigger, kwargs in scheduled if "id" in kwargs}
    ids_to_kwargs = {kwargs["id"]: kwargs for _, _, kwargs in scheduled if "id" in kwargs}

    assert ids_to_trigger["sync_from_oci"] == {"hour": 5, "minute": 0}
    assert ids_to_trigger["generate_quality_report"] == {"hour": 5, "minute": 15}
    assert ids_to_trigger["crawl_games_regular"] == {"hour": 3, "minute": 0}
    assert ids_to_trigger["crawl_futures_profile"] == {"day_of_week": "sun", "hour": 5, "minute": 0}
    assert ids_to_trigger["crawl_pregame_refresh"] == {"hour": "10-23", "minute": "*/15"}
    assert ids_to_trigger["crawl_live_refresh_day"] == {"hour": "12-22", "minute": "*/2"}
    assert ids_to_trigger["crawl_live_refresh_night"] == {"hour": 23, "minute": "0-30/2"}
    assert ids_to_kwargs["crawl_live_refresh_day"]["max_instances"] == 1
    assert ids_to_kwargs["crawl_live_refresh_night"]["max_instances"] == 1
