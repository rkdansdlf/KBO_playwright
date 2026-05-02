import logging
from typing import Optional
from src.utils.alerting import SlackWebhookClient

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("fallback_monitor")

class FallbackMonitor:
    """
    모니터링 및 알림 시스템을 담당하는 클래스.
    폴백 이벤트 발생 시 로그 기록 및 알림(Slack 등)을 수행합니다.
    """

    @staticmethod
    def log_fallback(year: int, series: str, stat_type: str, reason: str, player_count: Optional[int] = None):
        """
        폴백 이벤트 로그 기록 및 알림 전송.
        """
        msg = f"🔄 [FALLBACK TRIGGERED] {year} {series} {stat_type} fallback initiated. Reason: {reason}"
        if player_count is not None:
            msg += f" (Processed {player_count} players)"
            
        logger.warning(msg)
        
        # 슬랙 알림 전송 (SLACK_WEBHOOK_URL 환경변수 설정 시)
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⚠️ KBO Fallback System Triggered"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Year:* {year}"},
                    {"type": "mrkdwn", "text": f"*Series:* {series}"},
                    {"type": "mrkdwn", "text": f"*Type:* {stat_type}"},
                    {"type": "mrkdwn", "text": f"*Players:* {player_count or 'Unknown'}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Reason:* {reason}"
                }
            }
        ]
        
        SlackWebhookClient.send_alert(msg, blocks=blocks)
