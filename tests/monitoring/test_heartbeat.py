"""Contract tests for the external dead-man's switch.

The switch exists because the alert path cannot observe its own host: Prometheus
and Alertmanager run in the same Docker stack as the scheduler and the API, so a
host-wide stop takes all three down together. On 2026-11-09 nothing left the
machine to say the machine had stopped.

Two properties are load-bearing and are tested behaviourally rather than by
reading the source:

1. The job must ping while the database is down. A heartbeat silenced by a
   database outage would report a dead host, which is the wrong fault.
2. The ping URL must not reach the log. It is a credential: anyone holding it
   can keep the switch quiet by pinging on the host's behalf.
"""

from __future__ import annotations

import logging

import httpx
import pytest
from apscheduler.triggers.cron import CronTrigger

from src.monitoring import heartbeat
from src.monitoring.heartbeat import send_heartbeat
from src.scheduler import locks
from src.scheduler.cadence import expected_interval_seconds
from src.scheduler.jobs.sentinel import heartbeat_ping_job
from src.scheduler.registry import job_specs
from src.utils import log_masking

_URL = "https://hc-ping.com/6b1f0c2e-6a4d-4c1e-9a1f-3f0d2b7c8e91"
_UUID = "6b1f0c2e-6a4d-4c1e-9a1f-3f0d2b7c8e91"


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate the registry, the environment, and the DB-gate memo.

    The gate memo is reset even though this job is not gated: if a future edit
    adds ``_with_db_fail_fast_guard``, the test below fails, and the memo it left
    behind must not decide another test's probe.
    """
    monkeypatch.setattr(log_masking, "_REGISTERED_SECRETS", ())
    monkeypatch.delenv(heartbeat.HEARTBEAT_URL_ENV, raising=False)
    locks._reset_db_gate()


def _forbid_request(*_args: object, **_kwargs: object) -> httpx.Response:
    raise AssertionError("no request should be sent")


class TestSendingAPing:
    def test_an_unset_url_sends_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(heartbeat.httpx, "get", _forbid_request)

        assert send_heartbeat() is False

    def test_a_blank_url_is_treated_as_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Compose files disable optional features by exporting an empty value.
        monkeypatch.setenv(heartbeat.HEARTBEAT_URL_ENV, "   ")
        monkeypatch.setattr(heartbeat.httpx, "get", _forbid_request)

        assert send_heartbeat() is False

    def test_the_configured_url_is_pinged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen: list[str] = []

        def _get(url: str, *, timeout: float) -> httpx.Response:
            seen.append(url)
            return httpx.Response(200)

        monkeypatch.setenv(heartbeat.HEARTBEAT_URL_ENV, _URL)
        monkeypatch.setattr(heartbeat.httpx, "get", _get)

        assert send_heartbeat() is True
        assert seen == [_URL]

    def test_a_rejected_ping_is_not_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(heartbeat.httpx, "get", lambda *_a, **_kw: httpx.Response(404))

        assert send_heartbeat(_URL) is False

    def test_a_redirect_counts_as_a_failed_ping(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The URL is the credential, so following a redirect would hand it to
        # whatever the response names. Not following it means the switch did not
        # record the ping, which is a failure and not a success.
        monkeypatch.setattr(heartbeat.httpx, "get", lambda *_a, **_kw: httpx.Response(302))

        assert send_heartbeat(_URL) is False

    def test_a_transport_error_is_swallowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def _get(*_args: object, **_kwargs: object) -> httpx.Response:
            raise httpx.ConnectTimeout("timed out")

        monkeypatch.setattr(heartbeat.httpx, "get", _get)

        assert send_heartbeat(_URL) is False

    def test_a_failure_names_the_class_but_not_the_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        def _get(*_args: object, **_kwargs: object) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

        monkeypatch.setattr(heartbeat.httpx, "get", _get)

        with caplog.at_level(logging.WARNING, logger="src.monitoring.heartbeat"):
            send_heartbeat(_URL)

        assert "ConnectError" in caplog.text
        assert _UUID not in caplog.text
        assert _URL not in caplog.text


class TestTheUrlDoesNotReachTheLog:
    def test_it_is_registered_before_the_request(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The leaking record is the one httpx writes *while sending*, so the
        # registration has to happen first. Asserting after the call would pass
        # even if the mask were installed one line too late.
        observed: list[str] = []

        def _get(url: str, *, timeout: float) -> httpx.Response:
            observed.append(log_masking.mask_text(f"HTTP Request: GET {url}"))
            return httpx.Response(200)

        monkeypatch.setattr(heartbeat.httpx, "get", _get)

        send_heartbeat(_URL)

        assert observed == ["HTTP Request: GET ***"]

    def test_the_installed_mask_covers_a_ping_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        original = logging.getLogRecordFactory()
        try:
            monkeypatch.setattr(heartbeat.httpx, "get", lambda *_a, **_kw: httpx.Response(200))
            send_heartbeat(_URL)
            log_masking.install_secret_masking()

            with caplog.at_level(logging.INFO, logger="httpx"):
                logging.getLogger("httpx").info("HTTP Request: GET %s", _URL)
        finally:
            logging.setLogRecordFactory(original)

        assert _UUID not in caplog.text
        assert "HTTP Request: GET ***" in caplog.text


class TestTheScheduledJob:
    def test_it_does_nothing_when_the_switch_is_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(heartbeat.httpx, "get", _forbid_request)

        heartbeat_ping_job()

    def test_it_pings_when_the_switch_is_on(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[str] = []
        monkeypatch.setenv(heartbeat.HEARTBEAT_URL_ENV, _URL)
        monkeypatch.setattr(heartbeat.httpx, "get", lambda url, **_kw: calls.append(url) or httpx.Response(200))

        heartbeat_ping_job()

        assert calls == [_URL]

    def test_it_pings_while_the_database_is_down(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The one fault this job must not inherit.

        If the DB fail-fast gate were applied, the gate would ask this probe and
        skip the job -- turning a database outage into a "host is dead" alert
        from the external service. The database already has its own alert path.
        """
        calls: list[str] = []
        monkeypatch.setattr(locks, "database_reachable", lambda **_kw: False)
        monkeypatch.setenv(heartbeat.HEARTBEAT_URL_ENV, _URL)
        monkeypatch.setattr(heartbeat.httpx, "get", lambda url, **_kw: calls.append(url) or httpx.Response(200))

        heartbeat_ping_job()

        assert calls == [_URL], "the heartbeat was gated on the database it is meant to outlive"

    def test_it_raises_no_incident(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The incident ledger lives in the database this job outlives."""
        monkeypatch.setattr(
            "src.scheduler.jobs.sentinel.apply_incidents",
            _forbid_request,
            raising=True,
        )
        monkeypatch.setattr(heartbeat.httpx, "get", lambda *_a, **_kw: httpx.Response(500))

        heartbeat_ping_job()

    def test_it_is_registered_and_has_a_readable_cadence(self) -> None:
        # A job nobody schedules is a job nobody runs, and a cadence the metrics
        # cannot derive is a job whose silence no alert watches.
        triggers = {job_id: trigger for _, trigger, job_id, _, _ in job_specs(CronTrigger)}

        assert "heartbeat_ping" in triggers
        assert expected_interval_seconds(triggers["heartbeat_ping"]) == 600
