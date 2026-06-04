import json
import logging
import os
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

GAP_EMOJI_MAP: dict[str, str] = {
    "FRESHNESS": "\u2757",
    "P0": "\u26a1",
    "STALENESS": "\u23f3",
    "RELAY": "\U0001f4be",
    "PROFILE": "\U0001f464",
    "ID_RESOLUTION": "\U0001f50d",
}

GAP_CATEGORY_ENV_MAP: dict[str, str] = {
    "FRESHNESS": "TELEGRAM_CHAT_ID_FRESHNESS",
    "P0": "TELEGRAM_CHAT_ID_P0",
    "STALENESS": "TELEGRAM_CHAT_ID_STALENESS",
    "RELAY": "TELEGRAM_CHAT_ID_RELAY",
    "PROFILE": "TELEGRAM_CHAT_ID_PROFILE",
    "ID_RESOLUTION": "TELEGRAM_CHAT_ID_ID_RESOLUTION",
}


class TelegramBotClient:
    """Sends notifications via Telegram Bot API."""

    @staticmethod
    def send_message(message: str, chat_id: str | None = None) -> bool:
        """
        Sends an alert message to a Telegram chat.
        Uses TELEGRAM_CHAT_ID by default, or the provided chat_id override.
        Requires TELEGRAM_BOT_TOKEN.
        """
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            return False

        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})

        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 200
        except Exception as e:
            logger.exception("Failed to send Telegram message")
            print(f"[TELEGRAM-ERROR] Failed to send message: {e}")
            return False


class SlackWebhookClient:
    """Sends notifications. Now prioritizes Telegram if configured."""

    @staticmethod
    def send_alert(message: str, blocks: list = None) -> bool:
        """
        Sends an alert message.
        Tries Telegram first, falls back to Slack if configured.
        """
        # Try Telegram first
        if TelegramBotClient.send_message(message):
            return True

        # Fallback to Slack
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            if not os.getenv("TELEGRAM_BOT_TOKEN"):
                print(f"[ALERT-SKIP] No alerting (Slack/Telegram) configured. Message: {message}")
            return True

        payload: dict[str, Any] = {"text": message}
        if blocks:
            payload["blocks"] = blocks

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})

        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status in (200, 204)
        except Exception as e:
            logger.exception("Failed to send Slack webhook")
            print(f"[ALERT-ERROR] Failed to send Slack webhook: {e}")
            return False

    @staticmethod
    def send_gap_alert(gap_type: str, summary: str, details: list[str] | None = None) -> bool:
        """Send a gap-type-aware alert with optional per-category Telegram chat routing."""
        emoji = GAP_EMOJI_MAP.get(gap_type, "\u26a0\ufe0f")
        header = f"<b>{emoji} KBO {gap_type} Gap</b>\n{summary}"
        body = ""
        if details:
            body = "\n".join(f"\u2022 {d}" for d in details[:15])
            if len(details) > 15:
                body += f"\n... and {len(details) - 15} more"
        message = header + ("\n\n" + body if body else "")

        chat_env = GAP_CATEGORY_ENV_MAP.get(gap_type)
        chat_id = os.getenv(chat_env) if chat_env else None

        if TelegramBotClient.send_message(message, chat_id=chat_id):
            return True

        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            return True
        slack_msg = f"*{emoji} KBO {gap_type} Gap*\n{summary}"
        payload = {"text": slack_msg}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status in (200, 204)
        except Exception as e:
            logger.exception("Failed to send Slack gap alert")
            print(f"[ALERT-ERROR] Failed to send Slack webhook: {e}")
            return False

    @staticmethod
    def send_error_alert(traceback_msg: str) -> bool:
        """Convenience to format and send a critical error trace."""
        message = f"<b>🚨 KBO Pipeline Critical Error</b>\n\n<pre>{traceback_msg[:3000]}</pre>"
        if TelegramBotClient.send_message(message):
            return True

        # Slack legacy fallback
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "🚨 KBO Pipeline Critical Error"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```\n{traceback_msg[:2000]}\n```"}},
        ]
        return SlackWebhookClient.send_alert("🚨 KBO Pipeline Error encountered", blocks=blocks)
