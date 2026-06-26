"""모니터링: fallback monitor."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime

# 로깅 설정
from src.constants import KST
from src.utils.alerting import SlackWebhookClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("fallback_monitor")


class FallbackMonitor:
    """
    모니터링 및 알림 시스템을 담당하는 클래스.

    폴백 이벤트 발생 시 로그 기록 및 알림(Slack 등)을 수행합니다.
    """

    @staticmethod
    def log_fallback(year: int, series: str, stat_type: str, reason: str, player_count: int | None = None) -> None:
        """폴백 이벤트 로그 기록 및 알림 전송."""
        msg = f"🔄 [FALLBACK TRIGGERED] {year} {series} {stat_type} fallback initiated. Reason: {reason}"
        if player_count is not None:
            msg += f" (Processed {player_count} players)"

        logger.warning(msg)

        # 슬랙 알림 전송 (SLACK_WEBHOOK_URL 환경변수 설정 시)
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "⚠️ KBO Fallback System Triggered"}},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Year:* {year}"},
                    {"type": "mrkdwn", "text": f"*Series:* {series}"},
                    {"type": "mrkdwn", "text": f"*Type:* {stat_type}"},
                    {"type": "mrkdwn", "text": f"*Players:* {player_count or 'Unknown'}"},
                ],
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Reason:* {reason}"}},
        ]

        SlackWebhookClient.send_alert(msg, blocks=blocks)

    @staticmethod
    def save_audit_backup(
        player_id: str,
        type_name: str,
        original_data: dict,
        calculated_data: dict | None = None,
        player_name: str | None = None,
    ) -> str:
        """
        수정 전 데이터 스냅샷을 logs/audit_fixes/ 폴더에 {date}_{player_id}_{type}.json 형태로 저장합니다.

        기존 백업 파일이 있으면 list에 append하여 덮어쓰기를 방지합니다.
        """
        from pathlib import Path

        class DateTimeEncoder(json.JSONEncoder):
            """DateTimeEncoder class."""

            def default(self, o: object) -> object:
                """
                Handles the default operation.

                Args:
                    o: O.

                Returns:
                    object instance.

                """
                if isinstance(o, (datetime, date)):
                    return o.isoformat()
                return super().default(o)

        project_root = Path(__file__).resolve().parent.parent.parent
        backup_dir = project_root / "logs" / "audit_fixes"
        backup_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now(KST).strftime("%Y%m%d")
        file_path = backup_dir / f"{date_str}_{player_id}_{type_name}.json"

        snapshots = []
        if file_path.exists():
            try:
                with file_path.open(encoding="utf-8") as f:
                    content = json.load(f)
                    snapshots = content if isinstance(content, list) else [content]
            except (OSError, ValueError) as e:
                logger.warning("Failed to read existing backup file %s: %s", file_path, e)

        # 새 스냅샷 추가
        snapshot = {
            "timestamp": datetime.now(KST).isoformat(),
            "player_id": str(player_id),
            "type": type_name,
            "original": original_data,
        }
        if player_name is not None:
            snapshot["player_name"] = player_name
        if calculated_data is not None:
            snapshot["calculated"] = calculated_data

        snapshots.append(snapshot)

        with file_path.open("w", encoding="utf-8") as f:
            json.dump(snapshots, f, cls=DateTimeEncoder, ensure_ascii=False, indent=2)

        return str(file_path)

    @staticmethod
    def save_audit_event(category: str, event_type: str, data: dict) -> None:
        """Saves an audit event (e.g. warning, abort) to logs/audit_fixes/{date}_{event_type}_{category}.json."""
        from pathlib import Path

        project_root = Path(__file__).resolve().parent.parent.parent
        backup_dir = project_root / "logs" / "audit_fixes"
        backup_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now(KST).strftime("%Y%m%d")
        file_path = backup_dir / f"{date_str}_{event_type}_{category.lower()}.json"

        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
