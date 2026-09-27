"""유틸리티: alerting.

Transport adapters for the in-process alert manager. These classes speak HTTP to
Telegram and Slack and nothing more — routing, dedup, cooldown and lifecycle live
in :mod:`src.notifications`. Application code should publish an
:class:`src.notifications.alert_dto.AlertEvent` instead of calling these
directly; a repository lint gate enforces that boundary.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from enum import StrEnum
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx

from src.utils.metrics import record_notification_dispatch

logger = logging.getLogger(__name__)

ALERTING_EXCEPTIONS = (httpx.HTTPError, OSError, TimeoutError, ValueError, TypeError)
GAP_ALERT_DETAIL_LIMIT = 15

DEFAULT_ALERT_DELIVERY_ATTEMPTS = 3
DEFAULT_ALERT_DELIVERY_BACKOFF_SECONDS = 0.5

#: Deprecated: kept importable for compatibility. The canonical routing table now
#: lives in :data:`src.notifications.policy.DESTINATION_ENV_MAP`.
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

#: Deprecated: see :data:`GAP_EMOJI_MAP`.
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


class DeliveryOutcome(StrEnum):
    """Tri-state delivery result.

    Distinguishing ``SKIPPED_UNCONFIGURED`` from ``SENT`` is what lets the alert
    manager send a recovery notice exactly once: an unconfigured channel must
    never be mistaken for a successful delivery.
    """

    SENT = "SENT"
    SKIPPED_UNCONFIGURED = "SKIPPED_UNCONFIGURED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class DeliveryResult:
    """Outcome of a single transport delivery attempt."""

    channel: str
    outcome: DeliveryOutcome
    attempts: int = 0
    duration_seconds: float = 0.0
    error: str | None = None

    @property
    def delivered(self) -> bool:
        """Return whether the message reached the channel."""
        return self.outcome is DeliveryOutcome.SENT

    @property
    def skipped(self) -> bool:
        """Return whether delivery was skipped because the channel is unconfigured."""
        return self.outcome is DeliveryOutcome.SKIPPED_UNCONFIGURED


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _post_with_retry(
    url: str,
    *,
    payload: dict[str, Any],
    timeout: float,
    attempts: int,
    backoff: float,
) -> tuple[bool, int, str | None]:
    """POST a JSON payload with bounded exponential backoff.

    Returns ``(ok, attempts_used, error)``. The explicit ``timeout`` is the hang
    guard: a stalled endpoint can never block a scheduler job indefinitely.
    """
    last_error: str | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = httpx.post(url, json=payload, timeout=timeout)
        except ALERTING_EXCEPTIONS as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            if response.status_code in (HTTPStatus.OK, HTTPStatus.NO_CONTENT):
                return True, attempt, None
            last_error = f"HTTP {response.status_code}"
        if attempt < attempts:
            time.sleep(backoff * (2 ** (attempt - 1)))
    return False, attempts, last_error


def _delivery_attempts() -> int:
    return max(1, _env_int("ALERT_DELIVERY_ATTEMPTS", DEFAULT_ALERT_DELIVERY_ATTEMPTS))


def _delivery_backoff() -> float:
    return max(0.0, _env_float("ALERT_DELIVERY_BACKOFF_SECONDS", DEFAULT_ALERT_DELIVERY_BACKOFF_SECONDS))


class TelegramBotClient:
    """Send notifications via Telegram Bot API."""

    @staticmethod
    def deliver(message: str, chat_id: str | None = None) -> DeliveryResult:
        """Deliver a message to Telegram, retrying transient failures."""
        start = time.monotonic()
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        resolved_chat = chat_id or os.getenv("TELEGRAM_CHAT_ID")

        if not token or not resolved_chat:
            result = DeliveryResult(channel="telegram", outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED)
            record_notification_dispatch("telegram", result.outcome.value, 0.0)
            return result

        payload = {"chat_id": resolved_chat, "text": message, "parse_mode": "HTML"}
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        ok, attempts, error = _post_with_retry(
            url,
            payload=payload,
            timeout=10.0,
            attempts=_delivery_attempts(),
            backoff=_delivery_backoff(),
        )
        duration = time.monotonic() - start
        outcome = DeliveryOutcome.SENT if ok else DeliveryOutcome.FAILED
        record_notification_dispatch("telegram", outcome.value, duration)
        if not ok:
            logger.warning("Telegram delivery failed after %d attempt(s): %s", attempts, error)
        return DeliveryResult(
            channel="telegram",
            outcome=outcome,
            attempts=attempts,
            duration_seconds=duration,
            error=error,
        )

    @staticmethod
    def send_message(message: str, chat_id: str | None = None) -> bool:
        """Send a message to Telegram, returning whether it was delivered.

        Compatibility façade over :meth:`deliver`.
        """
        return TelegramBotClient.deliver(message, chat_id=chat_id).delivered


class SlackWebhookClient:
    """Send notifications, preferring Telegram when configured."""

    @staticmethod
    def deliver(message: str, blocks: list[Any] | None = None) -> DeliveryResult:
        """Deliver via Telegram, then Slack, reporting the actual outcome."""
        telegram = TelegramBotClient.deliver(message)
        if telegram.delivered:
            return telegram

        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            outcome = DeliveryOutcome.SKIPPED_UNCONFIGURED
            if not os.getenv("TELEGRAM_BOT_TOKEN"):
                logger.info("[ALERT-SKIP] No alerting (Slack/Telegram) configured. Message: %s", message)
            else:
                outcome = DeliveryOutcome.FAILED
            record_notification_dispatch("slack", outcome.value, 0.0)
            return DeliveryResult(channel="slack", outcome=outcome)

        payload: dict[str, Any] = {"text": message}
        if blocks:
            payload["blocks"] = blocks

        start = time.monotonic()
        ok, attempts, error = _post_with_retry(
            webhook_url,
            payload=payload,
            timeout=5.0,
            attempts=_delivery_attempts(),
            backoff=_delivery_backoff(),
        )
        duration = time.monotonic() - start
        outcome = DeliveryOutcome.SENT if ok else DeliveryOutcome.FAILED
        record_notification_dispatch("slack", outcome.value, duration)
        return DeliveryResult(
            channel="slack",
            outcome=outcome,
            attempts=attempts,
            duration_seconds=duration,
            error=error,
        )

    @staticmethod
    def deliver_webhook(message: str, blocks: list[Any] | None = None) -> DeliveryResult:
        """Deliver directly to the configured Slack webhook without a Telegram leg.

        The dispatcher needs a Slack-only path so that a CRITICAL fan-out to
        ``(Telegram, Slack)`` does not send Telegram twice.
        """
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            result = DeliveryResult(channel="slack", outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED)
            record_notification_dispatch("slack", result.outcome.value, 0.0)
            return result

        payload: dict[str, Any] = {"text": message}
        if blocks:
            payload["blocks"] = blocks

        start = time.monotonic()
        ok, attempts, error = _post_with_retry(
            webhook_url,
            payload=payload,
            timeout=5.0,
            attempts=_delivery_attempts(),
            backoff=_delivery_backoff(),
        )
        duration = time.monotonic() - start
        outcome = DeliveryOutcome.SENT if ok else DeliveryOutcome.FAILED
        record_notification_dispatch("slack", outcome.value, duration)
        return DeliveryResult(
            channel="slack",
            outcome=outcome,
            attempts=attempts,
            duration_seconds=duration,
            error=error,
        )

    @staticmethod
    def send_alert(message: str, blocks: list[Any] | None = None) -> bool:
        """Send an alert, returning ``False`` only on a genuine delivery failure.

        Compatibility façade preserving the historical fail-open behaviour: an
        unconfigured channel is not treated as a failure.
        """
        return SlackWebhookClient.deliver(message, blocks=blocks).outcome is not DeliveryOutcome.FAILED

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

        from src.notifications.policy import resolve_chat_id

        chat_id = resolve_chat_id(gap_type)

        telegram = TelegramBotClient.deliver(message, chat_id=chat_id)
        if telegram.delivered:
            return True

        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            return not telegram.delivered and telegram.outcome is DeliveryOutcome.SKIPPED_UNCONFIGURED

        slack_msg = f"*{emoji} KBO {gap_type} Gap*\n{summary}"
        ok, _, error = _post_with_retry(
            webhook_url,
            payload={"text": slack_msg},
            timeout=5.0,
            attempts=_delivery_attempts(),
            backoff=_delivery_backoff(),
        )
        record_notification_dispatch("slack", "SENT" if ok else "FAILED", 0.0)
        if not ok:
            logger.warning("Slack gap alert failed: %s", error)
        return ok

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


class GenericWebhookClient:
    """POST alert messages as JSON to a generic ``ALERT_WEBHOOK_URL``."""

    @staticmethod
    def deliver(message: str, *, payload: dict[str, Any] | None = None) -> DeliveryResult:
        """Deliver a JSON payload to the configured generic webhook."""
        url = os.getenv("ALERT_WEBHOOK_URL")
        if not url:
            result = DeliveryResult(channel="webhook", outcome=DeliveryOutcome.SKIPPED_UNCONFIGURED)
            record_notification_dispatch("webhook", result.outcome.value, 0.0)
            return result

        body = {"text": message}
        if payload:
            body.update(payload)

        start = time.monotonic()
        ok, attempts, error = _post_with_retry(
            url,
            payload=body,
            timeout=5.0,
            attempts=_delivery_attempts(),
            backoff=_delivery_backoff(),
        )
        duration = time.monotonic() - start
        outcome = DeliveryOutcome.SENT if ok else DeliveryOutcome.FAILED
        record_notification_dispatch("webhook", outcome.value, duration)
        return DeliveryResult(
            channel="webhook",
            outcome=outcome,
            attempts=attempts,
            duration_seconds=duration,
            error=error,
        )
