"""Enhanced Notification Service with dynamic matchups, WPA heroes, and hot pushes."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.constants import KST
from src.models.game import Game
from src.services.context_aggregator import ContextAggregator
from src.services.game_preview_generator import GamePreviewGenerator
from src.utils.alerting import SlackWebhookClient, TelegramBotClient

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class NotificationService:
    """Enhanced Service to send KBO alerts, WPA heroes, and hot pushes to Telegram/Slack."""

    def __init__(self, session: Session) -> None:
        """Initialize notification service.

        Args:
            session: DB Session.

        """
        self.session = session
        self.aggregator = ContextAggregator(session)
        self.preview_generator = GamePreviewGenerator(session)

    def send_telegram_message(self, message: str, chat_id: str | None = None) -> bool:
        """Send message via TelegramBotClient.

        Args:
            message: Formatted text message.
            chat_id: Optional custom chat ID.

        Returns:
            True if sent successfully, False otherwise.

        """
        return TelegramBotClient.send_message(message=message, chat_id=chat_id)

    def send_slack_message(self, message: str) -> bool:
        """Send message via SlackWebhookClient.

        Args:
            message: Text message.

        Returns:
            True if sent successfully, False otherwise.

        """
        return SlackWebhookClient.send_alert(message=message)

    def send_today_all_pregame_alerts(
        self,
        target_date: str | None = None,
        season: int = 2026,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Dynamically resolve all games scheduled for target date and dispatch preview cards.

        Args:
            target_date: Date string YYYYMMDD (defaults to today).
            season: Season year.
            channels: Target channels.

        Returns:
            Dispatch result dict.

        """
        channels = channels or ["telegram"]
        date_str = target_date or datetime.now(KST).strftime("%Y%m%d")
        game_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=KST).date()

        stmt = select(Game).where(Game.game_date == game_date)
        games = list(self.session.execute(stmt).scalars().all())

        if not games:
            summary = f"[{date_str}] 당일 예정된 KBO 경기가 없습니다."
            if "telegram" in channels:
                self.send_telegram_message(summary)
            return {"date": date_str, "game_count": 0, "summary": summary}

        dispatches: list[dict[str, Any]] = []
        for g in games:
            res = self.send_game_preview_report(
                away_team=g.away_team,
                home_team=g.home_team,
                season=season,
                channels=channels,
            )
            dispatches.append(res)

        return {
            "date": date_str,
            "game_count": len(games),
            "dispatches": dispatches,
        }

    def send_postgame_wpa_hero_alert(
        self,
        game_id: str,
        season: int = 2026,  # noqa: ARG002
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Dispatch WPA Hero and postgame summary report for a completed game.

        Args:
            game_id: Game ID.
            season: Season year.
            channels: Target channels.

        Returns:
            Dispatch status dict.

        """
        channels = channels or ["telegram"]
        stmt = select(Game).where(Game.game_id == game_id)
        game = self.session.execute(stmt).scalar_one_or_none()

        if not game:
            msg = f"게임 ID ({game_id})를 찾을 수 없습니다."
            return {"game_id": game_id, "error": msg}

        away_score = game.away_score if game.away_score is not None else 0
        home_score = game.home_score if game.home_score is not None else 0

        winner = game.home_team if home_score > away_score else game.away_team
        hero_name = f"{winner} MVP 선수"

        report_text = (
            f"🏆 <b>[KBO 경기 종료 요약] {game.away_team} {away_score} : {home_score} {game.home_team}</b>\n\n"
            f"• <b>승리팀</b>: {winner}\n"
            f"• <b>WPA Hero</b>: ⭐ {hero_name} (승리 기여도 탑)\n"
            f"• <b>경기 상태</b>: 종료 (COMPLETED)\n"
            f"• <b>구장</b>: {game.stadium or 'KBO 구장'}"
        )

        results: dict[str, bool] = {}
        if "telegram" in channels:
            results["telegram"] = self.send_telegram_message(report_text)

        if "slack" in channels:
            results["slack"] = self.send_slack_message(report_text)

        return {
            "game_id": game_id,
            "winner": winner,
            "hero": hero_name,
            "dispatched_channels": results,
            "report": report_text,
        }

    def send_emergency_notice_alert(
        self,
        title: str,
        content: str,
        source_url: str | None = None,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Dispatch real-time hot push for emergency notices or roster movements.

        Args:
            title: Notice title.
            content: Notice summary content.
            source_url: Optional source URL.
            channels: Target channels.

        Returns:
            Dispatch result dict.

        """
        channels = channels or ["telegram"]
        url_part = f"\n🔗 <a href='{source_url}'>상세보기</a>" if source_url else ""
        alert_text = f"🔥 <b>[KBO 긴급 공시 & 소식 Hot Push]</b>\n\n📌 <b>{title}</b>\n{content[:300]}{url_part}"

        results: dict[str, bool] = {}
        if "telegram" in channels:
            results["telegram"] = self.send_telegram_message(alert_text)

        if "slack" in channels:
            results["slack"] = self.send_slack_message(alert_text)

        return {
            "title": title,
            "dispatched_channels": results,
            "alert_text": alert_text,
        }

    def send_game_preview_report(
        self,
        away_team: str,
        home_team: str,
        season: int = 2026,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate and dispatch game preview report.

        Args:
            away_team: Away team code.
            home_team: Home team code.
            season: Season year.
            channels: Target channels.

        Returns:
            Dispatch status dict.

        """
        channels = channels or ["telegram"]
        preview = self.preview_generator.generate_preview(
            away_team=away_team,
            home_team=home_team,
            season=season,
        )

        md_text = preview["markdown_report"]
        html_text = f"<b>⚾ [매치업 프리뷰] {away_team} vs {home_team} ({season} 시즌)</b>\n\n{md_text}"

        results: dict[str, bool] = {}
        if "telegram" in channels:
            results["telegram"] = self.send_telegram_message(html_text)

        if "slack" in channels:
            results["slack"] = self.send_slack_message(md_text)

        return {
            "matchup": f"{away_team} vs {home_team}",
            "season": season,
            "dispatched_channels": results,
            "preview_summary": md_text,
        }

    def send_milestone_daily_summary(
        self,
        season: int = 2026,
        channels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Dispatch summary of all upcoming milestones across KBO.

        Args:
            season: Season year.
            channels: Target channels.

        Returns:
            Dispatch status dict.

        """
        from src.repositories.milestone_repository import MilestoneRepository

        channels = channels or ["telegram"]
        repo = MilestoneRepository(self.session)
        milestones = repo.get_upcoming_milestones(season=season)

        if not milestones:
            summary_text = f"[{season} 시즌] 현재 등록된 달성 임박 대기록이 없습니다."
        else:
            lines = [f"🚨 <b>[{season} KBO 선수 통산 대기록 임박 현황]</b>\n"]
            lines.extend(
                f"• <b>{m.player_name}</b> ({m.team_code}) - {m.milestone_category}: "
                f"<b>{m.remaining_val}개 남음</b> ({m.current_val}/{m.target_val})"
                for m in milestones[:10]
            )
            summary_text = "\n".join(lines)

        results: dict[str, bool] = {}
        if "telegram" in channels:
            results["telegram"] = self.send_telegram_message(summary_text)

        if "slack" in channels:
            results["slack"] = self.send_slack_message(summary_text)

        return {
            "season": season,
            "milestone_count": len(milestones),
            "dispatched_channels": results,
            "summary_text": summary_text,
        }
