"""유틸리티: alerting."""

from __future__ import annotations

import logging
import os
from http import HTTPStatus
from pathlib import Path
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

    @staticmethod
    def send_quarantine_alert(report: object) -> bool:
        """Send an alert for a DB file quarantine event.

        Args:
            report: SqliteIntegrityReport object.

        """
        db_path = getattr(report, "database_path", None) or "N/A"
        quarantine_dir = getattr(report, "quarantine_dir", None) or "N/A"
        moved_files = getattr(report, "moved_files", ())
        files_str = ", ".join(Path(str(f)).name for f in moved_files) if moved_files else "None"
        reason = getattr(report, "reason", None) or getattr(report, "error", None) or "Unknown error"

        message = (
            "🚨 <b>[DB Integrity Guard] DB 파일 격리(Quarantine) 발생</b>\n\n"
            f"• <b>DB 경로:</b> <code>{db_path}</code>\n"
            f"• <b>격리 위치:</b> <code>{quarantine_dir}</code>\n"
            f"• <b>격리 파일:</b> {files_str}\n"
            f"• <b>사유:</b> {reason}"
        )
        return SlackWebhookClient.send_alert(message)

    @staticmethod
    def send_hydration_alert(
        year: int,
        summary: dict[str, int],
        quarantine_dir: str | None = None,
        *,
        is_quarantine_recovery: bool = False,
    ) -> bool:
        """Send an alert when OCI hydration completes.

        Args:
            year: Season year.
            summary: Dictionary of restored row counts per table label.
            quarantine_dir: Optional quarantine directory path.
            is_quarantine_recovery: True if hydration was triggered due to quarantine.

        """
        total_restored = sum(summary.values()) if summary else 0
        header = "🔄 <b>[DB Self-Healing] OCI 하이드레이션 복구 완료</b>\n"
        quarantine_info = ""
        if is_quarantine_recovery or quarantine_dir:
            quarantine_info = (
                "⚠️ <b>사유:</b> DB 격리(Quarantine) 후 자동 하이드레이션 진행\n"
                f"📁 <b>격리 위치:</b> <code>{quarantine_dir or 'N/A'}</code>\n"
            )

        rows_summary = []
        if summary:
            for key, count in summary.items():
                if count > 0:
                    rows_summary.append(f"• {key}: {count:,}건")

        body = (
            f"{quarantine_info}"
            f"• <b>대상 시즌:</b> {year}년\n"
            f"• <b>총 복구 레코드:</b> {total_restored:,}건\n\n"
            "<b>테이블별 복구 현황:</b>\n" + ("\n".join(rows_summary) if rows_summary else "• (복구 내역 없음)")
        )
        message = header + "\n" + body
        return SlackWebhookClient.send_alert(message)

    @staticmethod
    def send_gap_summary_alert(report: dict[str, Any], chat_id: str | None = None) -> bool:
        """Send a daily gap report summary notification.

        Args:
            report: Unified gap report dictionary.
            chat_id: Optional Telegram chat ID override.

        """
        generated_at = report.get("generated_at", "")
        gaps = report.get("gaps", {})

        def _sev(gap: dict[str, Any]) -> str:
            if gap.get("error"):
                return "error"
            if gap.get("alert") is False:
                return "ok"
            if not gap.get("ok", True):
                return "warning"
            return "ok"

        def _icon(sev: str) -> str:
            return "✅" if sev == "ok" else "⚠️" if sev == "warning" else "❌"

        # 수집 누락 3대 항목
        relay = gaps.get("RELAY", {})
        relay_sev = _sev(relay)
        relay_txt = f"{relay.get('missing_count', 0)}건 누락" if relay_sev != "ok" else "0건 누락 (정상)"

        profile = gaps.get("PROFILE", {})
        profile_sev = _sev(profile)
        profile_txt = f"{profile.get('missing_count', 0)}명 누락" if profile_sev != "ok" else "0명 누락 (정상)"

        id_res = gaps.get("ID_RESOLUTION", {})
        id_sev = _sev(id_res)
        id_txt = f"{id_res.get('total', 0)}건 NULL" if id_sev != "ok" else "0건 NULL (정상)"

        # 나머지 품질 항목
        fresh = gaps.get("FRESHNESS", {})
        fresh_sev = _sev(fresh)
        fresh_txt = f"{fresh.get('total_issues', 0)}건 이슈" if fresh_sev != "ok" else "정상"

        standings = gaps.get("STANDINGS", {})
        standings_sev = _sev(standings)
        standings_txt = f"{standings.get('mismatches', 0)}건 불일치" if standings_sev != "ok" else "정상"

        pa = gaps.get("PA_FORMULA", {})
        pa_sev = _sev(pa)
        pa_txt = f"{pa.get('violation_count', 0)}건 위반" if pa_sev != "ok" else "0건 위반 (정상)"

        team = gaps.get("TEAM_STATS", {})
        team_sev = _sev(team)
        team_txt = f"{team.get('total', 0)}건 불일치" if team_sev != "ok" else "0건 불일치 (정상)"

        stale = gaps.get("STALENESS", {})
        stale_sev = _sev(stale)
        stale_txt = f"{stale.get('stale_count', 0)}건 지연" if stale_sev != "ok" else "정상"

        team_code = gaps.get("SEASON_TEAM_CODE", {})
        team_code_sev = _sev(team_code)
        team_code_txt = (
            f"{team_code.get('total_null', 0)}건 NULL (임계치 {team_code.get('alert_threshold_rate', 10.0)}%)"
            if team_code_sev != "ok"
            else "정상"
        )

        ok_count = sum(
            1 for g in (relay, profile, id_res, fresh, standings, pa, team, stale, team_code) if _sev(g) == "ok"
        )
        warn_count = sum(
            1 for g in (relay, profile, id_res, fresh, standings, pa, team, stale, team_code) if _sev(g) == "warning"
        )
        err_count = sum(
            1 for g in (relay, profile, id_res, fresh, standings, pa, team, stale, team_code) if _sev(g) == "error"
        )

        summary_line = f"정상 {ok_count}개"
        if warn_count:
            summary_line += f", 경고 {warn_count}개"
        if err_count:
            summary_line += f", 오류 {err_count}개"

        message = (
            "📊 <b>KBO 데이터 수율 (Gap Report) 일일 요약</b>\n"
            f"📅 일시: <code>{generated_at[:19]}</code>\n\n"
            "<b>[수집 누락 현황]</b>\n"
            f"• {_icon(relay_sev)} 💾 <b>문자중계 (RELAY):</b> {relay_txt}\n"
            f"• {_icon(profile_sev)} 👤 <b>선수 사진 (PROFILE):</b> {profile_txt}\n"
            f"• {_icon(id_sev)} 🔍 <b>NULL 선수 ID (ID_RESOLUTION):</b> {id_txt}\n\n"
            "<b>[데이터 품질 현황]</b>\n"
            f"• {_icon(fresh_sev)} ⚡ <b>P0 데이터 (FRESHNESS):</b> {fresh_txt}\n"
            f"• {_icon(standings_sev)} 🏆 <b>순위표 (STANDINGS):</b> {standings_txt}\n"
            f"• {_icon(pa_sev)} 📊 <b>타석 공통 공식 (PA_FORMULA):</b> {pa_txt}\n"
            f"• {_icon(team_sev)} 🏁 <b>팀 통계 (TEAM_STATS):</b> {team_txt}\n"
            f"• {_icon(stale_sev)} ⏳ <b>데이터 신선도 (STALENESS):</b> {stale_txt}\n"
            f"• {_icon(team_code_sev)} 🏷️ <b>팀 코드 (SEASON_TEAM_CODE):</b> {team_code_txt}\n\n"
            f"<b>전체 상태:</b> {summary_line}"
        )

        if chat_id and TelegramBotClient.send_message(message, chat_id=chat_id):
            return True
        return SlackWebhookClient.send_alert(message)
