"""Tests for alert routing, cooldown and suppression policy."""

from __future__ import annotations

import pytest

from src.notifications.alert_dto import AlertSeverity, AlertSource
from src.notifications.dto import NotificationPriority
from src.notifications.policy import (
    DEFAULT_COOLDOWN_SECONDS,
    DESTINATION_ENV_MAP,
    cooldown_seconds,
    min_notify_severity,
    resolve_chat_id,
    severity_to_priority,
    should_notify_severity,
)


class TestSeverityMapping:
    def test_severity_to_priority_is_monotonic(self) -> None:
        assert severity_to_priority(AlertSeverity.INFO) == NotificationPriority.LOW
        assert severity_to_priority(AlertSeverity.WARNING) == NotificationPriority.NORMAL
        assert severity_to_priority(AlertSeverity.ERROR) == NotificationPriority.HIGH
        assert severity_to_priority(AlertSeverity.CRITICAL) == NotificationPriority.CRITICAL

    def test_default_cooldowns_decrease_with_severity(self) -> None:
        cooldowns = [DEFAULT_COOLDOWN_SECONDS[s] for s in AlertSeverity]
        assert cooldowns == sorted(cooldowns, reverse=True)


class TestDeliveryFloor:
    def test_default_floor_is_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALERT_MIN_SEVERITY", raising=False)
        assert min_notify_severity() == AlertSeverity.WARNING
        assert should_notify_severity(AlertSeverity.INFO) is False
        assert should_notify_severity(AlertSeverity.WARNING) is True
        assert should_notify_severity(AlertSeverity.CRITICAL) is True

    def test_floor_can_be_lowered_to_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALERT_MIN_SEVERITY", "info")
        assert should_notify_severity(AlertSeverity.INFO) is True

    def test_floor_can_be_raised_to_critical(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALERT_MIN_SEVERITY", "critical")
        assert should_notify_severity(AlertSeverity.ERROR) is False
        assert should_notify_severity(AlertSeverity.CRITICAL) is True

    def test_invalid_floor_falls_back_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALERT_MIN_SEVERITY", "bogus")
        assert min_notify_severity() == AlertSeverity.WARNING


class TestCooldown:
    def test_defaults_returned_without_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALERT_COOLDOWN_ERROR_SECONDS", raising=False)
        assert cooldown_seconds(AlertSeverity.ERROR) == DEFAULT_COOLDOWN_SECONDS[AlertSeverity.ERROR]

    def test_env_override_applies(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALERT_COOLDOWN_CRITICAL_SECONDS", "42")
        assert cooldown_seconds(AlertSeverity.CRITICAL) == 42

    def test_invalid_env_falls_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALERT_COOLDOWN_WARNING_SECONDS", "not-a-number")
        assert cooldown_seconds(AlertSeverity.WARNING) == DEFAULT_COOLDOWN_SECONDS[AlertSeverity.WARNING]


class TestDestinationResolution:
    def test_explicit_override_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TELEGRAM_CHAT_ID_INTEGRITY", "from-env")
        assert resolve_chat_id("integrity", explicit="explicit") == "explicit"

    def test_source_env_is_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TELEGRAM_CHAT_ID_INTEGRITY", "integrity-chat")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "default-chat")
        assert resolve_chat_id("integrity") == "integrity-chat"

    def test_falls_back_to_default_chat(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TELEGRAM_CHAT_ID_DRIFT", raising=False)
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "default-chat")
        assert resolve_chat_id("drift") == "default-chat"

    def test_unknown_token_falls_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "default-chat")
        assert resolve_chat_id("unknown-token") == "default-chat"

    def test_no_configuration_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        assert resolve_chat_id("integrity") is None

    def test_legacy_gap_categories_remain_routable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TELEGRAM_CHAT_ID_PA_FORMULA", "pa-chat")
        assert resolve_chat_id("PA_FORMULA") == "pa-chat"


class TestDestinationRegistryCoverage:
    def test_every_alert_source_has_a_destination_entry(self) -> None:
        for source in AlertSource:
            assert source.value in DESTINATION_ENV_MAP, f"{source} has no destination env mapping"


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
