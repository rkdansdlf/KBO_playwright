import os
import json
import urllib.request
from typing import Dict, Any, List, Optional

class TelegramBotClient:
    """Sends notifications via Telegram Bot API."""

    @staticmethod
    def send_message(message: str) -> bool:
        """
        Sends an alert message to a Telegram chat.
        Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.
        """
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            return False

        # Prepare payload
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=data,
            headers={'Content-Type': 'application/json'}
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 200
        except Exception as e:
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
            
        payload: Dict[str, Any] = {"text": message}
        if blocks:
            payload["blocks"] = blocks

        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            webhook_url, 
            data=data, 
            headers={'Content-Type': 'application/json'}
        )
        
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status in (200, 204)
        except Exception as e:
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
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 KBO Pipeline Critical Error"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```\n{traceback_msg[:2000]}\n```"
                }
            }
        ]
        return SlackWebhookClient.send_alert("🚨 KBO Pipeline Error encountered", blocks=blocks)
