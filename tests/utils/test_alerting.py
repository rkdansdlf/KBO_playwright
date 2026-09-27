"""Transport adapter tests: Telegram/Slack delivery, retry and tri-state outcomes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.utils.alerting import (
    GAP_CATEGORY_ENV_MAP,
    GAP_EMOJI_MAP,
    DeliveryOutcome,
    DeliveryResult,
    SlackWebhookClient,
    TelegramBotClient,
)


def _sent(channel: str = "telegram") -> DeliveryResult:
    return DeliveryResult(channel=channel, outcome=DeliveryOutcome.SENT, attempts=1)


def _failed(channel: str = "telegram") -> DeliveryResult:
    return DeliveryResult(channel=channel, outcome=DeliveryOutcome.FAILED, attempts=3, error="boom")


def _skipped(channel: str = "telegram") -> DeliveryResult:
    return DeliveryResult(channel=channel, outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED)


class TestTelegramBotClient:
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.httpx.post")
    def test_send_message_success(self, mock_post, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        assert TelegramBotClient.send_message("Hello") is True

    @patch("src.utils.alerting.os.getenv")
    def test_send_message_missing_token(self, mock_getenv):
        mock_getenv.return_value = None
        assert TelegramBotClient.send_message("Hello") is False

    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.httpx.post")
    def test_send_message_error_returns_false(self, mock_post, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
            "ALERT_DELIVERY_BACKOFF_SECONDS": "0",
        }.get(k, d)

        mock_post.side_effect = OSError("Network error")
        assert TelegramBotClient.send_message("Hello") is False


class TestDeliveryOutcome:
    """The tri-state distinguishes 'sent' from 'unconfigured'."""

    @patch("src.utils.alerting.os.getenv")
    def test_unconfigured_is_skipped_not_failed(self, mock_getenv):
        mock_getenv.return_value = None
        result = TelegramBotClient.deliver("Hello")
        assert result.outcome is DeliveryOutcome.SKIPPED_UNCONFIGURED
        assert result.delivered is False
        assert result.skipped is True

    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.httpx.post")
    def test_delivered_is_sent(self, mock_post, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
        }.get(k, d)
        mock_post.return_value = MagicMock(status_code=200)

        result = TelegramBotClient.deliver("Hello")
        assert result.outcome is DeliveryOutcome.SENT
        assert result.delivered is True


class TestRetry:
    """Transient failures are retried with bounded backoff."""

    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.httpx.post")
    def test_retries_then_succeeds(self, mock_post, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
            "ALERT_DELIVERY_ATTEMPTS": "3",
            "ALERT_DELIVERY_BACKOFF_SECONDS": "0",
        }.get(k, d)

        mock_post.side_effect = [OSError("transient"), MagicMock(status_code=200)]
        result = TelegramBotClient.deliver("Hello")

        assert result.outcome is DeliveryOutcome.SENT
        assert result.attempts == 2
        assert mock_post.call_count == 2

    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.httpx.post")
    def test_exhausts_attempts_and_fails(self, mock_post, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
            "ALERT_DELIVERY_ATTEMPTS": "3",
            "ALERT_DELIVERY_BACKOFF_SECONDS": "0",
        }.get(k, d)

        mock_post.side_effect = OSError("down")
        result = TelegramBotClient.deliver("Hello")

        assert result.outcome is DeliveryOutcome.FAILED
        assert result.attempts == 3
        assert mock_post.call_count == 3

    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.httpx.post")
    def test_non_2xx_is_retried(self, mock_post, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
            "ALERT_DELIVERY_ATTEMPTS": "2",
            "ALERT_DELIVERY_BACKOFF_SECONDS": "0",
        }.get(k, d)

        mock_post.return_value = MagicMock(status_code=500)
        result = TelegramBotClient.deliver("Hello")

        assert result.outcome is DeliveryOutcome.FAILED
        assert mock_post.call_count == 2


class TestSlackWebhookClient:
    @patch("src.utils.alerting.TelegramBotClient.deliver")
    def test_send_alert_uses_telegram_first(self, mock_telegram):
        mock_telegram.return_value = _sent()
        assert SlackWebhookClient.send_alert("test") is True
        mock_telegram.assert_called_once_with("test")

    @patch("src.utils.alerting.TelegramBotClient.deliver")
    @patch("src.utils.alerting.os.getenv")
    def test_send_alert_logs_when_no_alerting(self, mock_getenv, mock_telegram):
        mock_telegram.return_value = _failed()
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": None,
            "TELEGRAM_BOT_TOKEN": None,
        }.get(k, d)

        assert SlackWebhookClient.send_alert("test") is True

    def test_gap_emoji_map_has_known_keys(self):
        assert GAP_EMOJI_MAP["FRESHNESS"] == "\u2757"
        assert GAP_EMOJI_MAP["P0"] == "\u26a1"
        assert "STANDINGS" in GAP_EMOJI_MAP

    def test_gap_category_env_map_has_known_keys(self):
        assert GAP_CATEGORY_ENV_MAP["FRESHNESS"] == "TELEGRAM_CHAT_ID_FRESHNESS"
        assert GAP_CATEGORY_ENV_MAP["RELAY"] == "TELEGRAM_CHAT_ID_RELAY"

    def test_legacy_gap_map_matches_canonical_policy(self):
        from src.notifications.policy import DESTINATION_ENV_MAP

        for category, env_name in GAP_CATEGORY_ENV_MAP.items():
            assert DESTINATION_ENV_MAP[category.lower()] == env_name

    @patch("src.utils.alerting.TelegramBotClient.deliver")
    def test_send_gap_alert_telegram_success(self, mock_telegram):
        mock_telegram.return_value = _sent()
        SlackWebhookClient.send_gap_alert("FRESHNESS", "Fresh data arrived")
        mock_telegram.assert_called_once()

    @patch("src.utils.alerting.TelegramBotClient.deliver")
    @patch("src.utils.alerting.os.getenv")
    def test_send_gap_alert_with_details_truncates(self, mock_getenv, mock_telegram):
        mock_telegram.return_value = _sent()
        mock_getenv.return_value = None

        details = [f"detail_{i}" for i in range(20)]
        SlackWebhookClient.send_gap_alert("P0", "P0 gap", details=details)
        mock_telegram.assert_called_once()

    @patch("src.utils.alerting.TelegramBotClient.send_message")
    def test_send_error_alert_forwards_to_telegram(self, mock_telegram):
        mock_telegram.return_value = True
        SlackWebhookClient.send_error_alert("Traceback line 1")
        mock_telegram.assert_called_once()


class TestSlackFallback:
    """Slack webhook fallback paths."""

    def test_no_config_returns_true(self, monkeypatch):
        from src.utils.alerting import SlackWebhookClient

        monkeypatch.setenv("ALERT_DELIVERY_BACKOFF_SECONDS", "0")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
        result = SlackWebhookClient.send_alert("test")
        assert result is True

    def test_slack_http_error_returns_false(self, monkeypatch):
        from src.utils.alerting import SlackWebhookClient

        monkeypatch.setenv("ALERT_DELIVERY_BACKOFF_SECONDS", "0")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "")
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://fake-webhook.test/hook")
        monkeypatch.setattr(
            "src.utils.alerting.httpx.post",
            lambda *a, **kw: (_ for _ in ()).throw(OSError("fail")),
        )
        result = SlackWebhookClient.send_alert("test")
        assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
