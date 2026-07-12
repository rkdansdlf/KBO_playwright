"""유틸리티: alerting."""

from __future__ import annotations

import logging
import os
from http import HTTPStatus
from typing import Any

import httpx

logger = logging.getLogger(__name__)

ALERTING_EXCEPTIONS = (httpx.HTTPError, OSError, TimeoutError, ValueError, TypeError)
GAP_ALERT_DETAIL_LIMIT = 15

GAP_EMOJI_MAP: dict[str, str] = {
    "FRESHNESS": "\u2757",
    "P0": "\u26a1",
    "STALENESS": "\u23f3",
    "RELAY": "\U0001f4be",
    "PROFILE": "\U0001f464",
    "ID_RESOLUTION": "\U0001f50d",
    "PA_FORMULA": "\U0001f4ca",
    "TEAM_STATS": "\U0001f3c1",
    "STANDINGS": "\U0001f3c5",
}

GAP_CATEGORY_ENV_MAP: dict[str, str] = {
    "FRESHNESS": "TELEGRAM_CHAT_ID_FRESHNESS",
    "P0": "TELEGRAM_CHAT_ID_P0",
    "STALENESS": "TELEGRAM_CHAT_ID_STALENESS",
    "RELAY": "TELEGRAM_CHAT_ID_RELAY",
    "PROFILE": "TELEGRAM_CHAT_ID_PROFILE",
    "ID_RESOLUTION": "TELEGRAM_CHAT_ID_ID_RESOLUTION",
    "PA_FORMULA": "TELEGRAM_CHAT_ID_PA_FORMULA",
    "TEAM_STATS": "TELEGRAM_CHAT_ID_TEAM_STATS",
    "STANDINGS": "TELEGRAM_CHAT_ID_STANDINGS",
}


class TelegramBotClient:
    """Send notifications via Telegram Bot API."""

    @staticmethod
    def send_message(message: str, chat_id: str | None = None) -> bool:
        """Send an alert message to a Telegram chat.

        Uses TELEGRAM_CHAT_ID by default, or the provided chat_id override.
        Requires TELEGRAM_BOT_TOKEN.

        Args:
            message: Message.
            chat_id: Chat ID.
            message: Message.
            chat_id: Chat ID.

        """
        token = os.getenv("TELEGRAM_BOT_TOKEN")

        chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            return False

        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            response = httpx.post(url, json=payload, timeout=10)
            return bool(response.status_code == HTTPStatus.OK)
        except ALERTING_EXCEPTIONS:
            logger.exception("Failed to send Telegram message")
            return False


class SlackWebhookClient:
    """Send notifications. Now prioritizes Telegram if configured."""

    @staticmethod
    def send_alert(message: str, blocks: list[Any] | None = None) -> bool:
        """Send an alert message.

        Tries Telegram first, falls back to Slack if configured.

        Args:
            message: Message.
            blocks: Blocks.
            message: Message.
            blocks: Blocks.

        """
        # Try Telegram first

        if TelegramBotClient.send_message(message):
            return True

        # Fallback to Slack
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            if not os.getenv("TELEGRAM_BOT_TOKEN"):
                logger.info("[ALERT-SKIP] No alerting (Slack/Telegram) configured. Message: %s", message)
            return True

        payload: dict[str, Any] = {"text": message}
        if blocks:
            payload["blocks"] = blocks

        try:
            response = httpx.post(webhook_url, json=payload, timeout=5)
        except ALERTING_EXCEPTIONS:
            logger.exception("Failed to send Slack webhook")
            return False
        else:
            return response.status_code in (HTTPStatus.OK, HTTPStatus.NO_CONTENT)

    @staticmethod
    def send_gap_alert(gap_type: str, summary: str, details: list[str] | None = None) -> bool:
        """Send a gap-type-aware alert with optional per-category Telegram chat routing.

        Args:
            gap_type: Gap Type.
            summary: Summary.
            details: Details.
            gap_type: Gap Type.
            summary: Summary.
            details: Details.

        """
        emoji = GAP_EMOJI_MAP.get(gap_type, "\u26a0\ufe0f")

        header = f"<b>{emoji} KBO {gap_type} Gap</b>\n{summary}"
        body = ""
        if details:
            body = "\n".join(f"\u2022 {d}" for d in details[:15])
            if len(details) > GAP_ALERT_DETAIL_LIMIT:
                body += f"\n... and {len(details) - GAP_ALERT_DETAIL_LIMIT} more"
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
        try:
            response = httpx.post(webhook_url, json=payload, timeout=5)
        except ALERTING_EXCEPTIONS:
            logger.exception("Failed to send Slack gap alert")
            return False
        else:
            return response.status_code in (HTTPStatus.OK, HTTPStatus.NO_CONTENT)

    @staticmethod
    def send_error_alert(traceback_msg: str) -> bool:
        """Format and send and send a critical error trace.

        Args:
            traceback_msg: Traceback Msg.
            traceback_msg: Traceback Msg.

        """
        message = f"<b>🚨 KBO Pipeline Critical Error</b>\n\n<pre>{traceback_msg[:3000]}</pre>"

        if TelegramBotClient.send_message(message):
            return True

        # Slack legacy fallback
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "🚨 KBO Pipeline Critical Error"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```\n{traceback_msg[:2000]}\n```"}},
        ]
        return SlackWebhookClient.send_alert("🚨 KBO Pipeline Error encountered", blocks=blocks)
