from __future__ import annotations

import urllib.error
from unittest.mock import MagicMock, patch

from src.utils.alerting import SlackWebhookClient


class TestSendAlertSlackPaths:
    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_slack_success_returns_true(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = SlackWebhookClient.send_alert("test message")
        assert result is True

    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_slack_with_blocks(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        blocks = [{"type": "section", "text": {"type": "plain_text", "text": "test"}}]
        result = SlackWebhookClient.send_alert("test", blocks=blocks)
        assert result is True

    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_slack_204_returns_true(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status = 204
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = SlackWebhookClient.send_alert("test")
        assert result is True

    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_slack_exception_returns_false(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_urlopen.side_effect = urllib.error.URLError("connection refused")
        result = SlackWebhookClient.send_alert("test")
        assert result is False


class TestSendGapAlertSlackFallback:
    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_gap_alert_slack_success(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_CHAT_ID_FRESHNESS": None,
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = SlackWebhookClient.send_gap_alert("FRESHNESS", "stale data")
        assert result is True

    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_gap_alert_no_slack_no_telegram(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": None,
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        result = SlackWebhookClient.send_gap_alert("RELAY", "missing relay")
        assert result is True

    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_gap_alert_slack_exception(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_CHAT_ID_FRESHNESS": None,
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_urlopen.side_effect = urllib.error.URLError("fail")
        result = SlackWebhookClient.send_gap_alert("FRESHNESS", "error")
        assert result is False


class TestSendErrorAlertSlackFallback:
    @patch("src.utils.alerting.urllib.request.urlopen")
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.TelegramBotClient.send_message", return_value=False)
    def test_error_alert_slack_fallback(self, mock_telegram, mock_getenv, mock_urlopen):
        mock_getenv.side_effect = lambda k, d=None: {
            "SLACK_WEBHOOK_URL": "https://hooks.slack.com/test",
            "TELEGRAM_BOT_TOKEN": "",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = SlackWebhookClient.send_error_alert("Critical error traceback")
        assert result is True
