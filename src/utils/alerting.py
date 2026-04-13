import os
import json
import urllib.request
from typing import Dict, Any

class SlackWebhookClient:
    """Sends webhook notifications to a configured URL (e.g. Slack/Discord)."""
    
    @staticmethod
    def send_alert(message: str, blocks: list = None) -> bool:
        """
        Sends an alert message. Assumes environment variable SLACK_WEBHOOK_URL is set.
        Returns True if successful or Webhook is unconfigured (safe-skip), False on error.
        """
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            print(f"[ALERT-SKIP] No SLACK_WEBHOOK_URL configured. Message: {message}")
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
            print(f"[ALERT-ERROR] Failed to send webhook: {e}")
            return False

    @staticmethod
    def send_error_alert(traceback_msg: str) -> bool:
        """Convenience to format and send a critical error trace."""
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
                    "text": f"```\n{traceback_msg[:2000]}\n```" # Slack block text limit is 3000
                }
            }
        ]
        return SlackWebhookClient.send_alert("🚨 KBO Pipeline Error encountered", blocks=blocks)
