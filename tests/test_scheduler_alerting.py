from types import SimpleNamespace

import pytest

import scripts.scheduler as scheduler


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


def test_alert_failure_sends_alert_and_reraises_original(monkeypatch):
    sent = []
    exc = RuntimeError("boom")

    monkeypatch.setattr(
        scheduler.SlackWebhookClient,
        "send_error_alert",
        lambda message: sent.append(message) or True,
    )

    with pytest.raises(RuntimeError) as raised:
        scheduler.alert_failure(_retry_state(exc))

    assert raised.value is exc
    assert len(sent) == 1
    assert "sample_job" in sent[0]
    assert "boom" in sent[0]


def test_alert_failure_preserves_job_failure_when_alerting_fails(monkeypatch):
    exc = ValueError("alert transport failed too")

    def _raise_alert(_message):
        raise OSError("slack down")

    monkeypatch.setattr(scheduler.SlackWebhookClient, "send_error_alert", _raise_alert)

    with pytest.raises(ValueError) as raised:
        scheduler.alert_failure(_retry_state(exc))

    assert raised.value is exc


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
