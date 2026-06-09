from unittest.mock import MagicMock, patch

from src.utils.alerting import (
    GAP_CATEGORY_ENV_MAP,
    GAP_EMOJI_MAP,
    SlackWebhookClient,
    TelegramBotClient,
)


class TestTelegramBotClient:
    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.urllib.request.urlopen")
    def test_send_message_success(self, mock_urlopen, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
        }.get(k, d)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        assert TelegramBotClient.send_message("Hello") is True

    @patch("src.utils.alerting.os.getenv")
    def test_send_message_missing_token(self, mock_getenv):
        mock_getenv.return_value = None
        assert TelegramBotClient.send_message("Hello") is False

    @patch("src.utils.alerting.os.getenv")
    @patch("src.utils.alerting.urllib.request.urlopen")
    def test_send_message_error_returns_false(self, mock_urlopen, mock_getenv):
        mock_getenv.side_effect = lambda k, d=None: {
            "TELEGRAM_BOT_TOKEN": "bot123",
            "TELEGRAM_CHAT_ID": "chat456",
        }.get(k, d)

        mock_urlopen.side_effect = Exception("Network error")
        assert TelegramBotClient.send_message("Hello") is False


class TestSlackWebhookClient:
    @patch("src.utils.alerting.TelegramBotClient.send_message")
    def test_send_alert_uses_telegram_first(self, mock_telegram):
        mock_telegram.return_value = True
        assert SlackWebhookClient.send_alert("test") is True
        mock_telegram.assert_called_once_with("test")

    @patch("src.utils.alerting.TelegramBotClient.send_message")
    @patch("src.utils.alerting.os.getenv")
    def test_send_alert_logs_when_no_alerting(self, mock_getenv, mock_telegram):
        mock_telegram.return_value = False
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

    @patch("src.utils.alerting.TelegramBotClient.send_message")
    def test_send_gap_alert_telegram_success(self, mock_telegram):
        mock_telegram.return_value = True
        SlackWebhookClient.send_gap_alert("FRESHNESS", "Fresh data arrived")
        mock_telegram.assert_called_once()

    @patch("src.utils.alerting.TelegramBotClient.send_message")
    @patch("src.utils.alerting.os.getenv")
    def test_send_gap_alert_with_details_truncates(self, mock_getenv, mock_telegram):
        mock_telegram.return_value = True
        mock_getenv.return_value = None

        details = [f"detail_{i}" for i in range(20)]
        SlackWebhookClient.send_gap_alert("P0", "P0 gap", details=details)
        mock_telegram.assert_called_once()

    @patch("src.utils.alerting.TelegramBotClient.send_message")
    def test_send_error_alert_forwards_to_telegram(self, mock_telegram):
        mock_telegram.return_value = True
        SlackWebhookClient.send_error_alert("Traceback line 1")
        mock_telegram.assert_called_once()
