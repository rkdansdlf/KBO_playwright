"""
SLA tracker: measure data freshness SLAs over time.
Tracks PBP coverage rate, game completion rate, detail capture rate.
Generates daily/weekly/monthly SLA reports.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

try:
    from datetime import UTC
except ImportError:
    UTC = UTC


from src.models.game import Game, GameBattingStat, GameLineup, GameMetadata, GamePitchingStat, GamePlayByPlay
from src.utils.alerting import SlackWebhookClient
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class SlaTracker:
    def __init__(self, session: Session) -> None:
        self.session = session

    def compute_daily_sla(self, target_date: str) -> dict[str, Any]:
        if isinstance(target_date, str) and len(target_date) == 8:
            query_date = date(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:8]))
        else:
            query_date = target_date

        games = self.session.query(Game).filter(Game.game_date == query_date).all()
        total = len(games)
        if total == 0:
            return {"date": target_date, "total": 0}

        completed = [g for g in games if g.game_status in COMPLETED_LIKE_GAME_STATUSES]
        if not completed:
            return {
                "date": target_date,
                "total": total,
                "completed": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "completion_rate": 0,
            }

        # PBP coverage: completed games with PBP
        pbp_count = (
            self.session.query(GamePlayByPlay.game_id)
            .filter(GamePlayByPlay.game_id.in_([g.game_id for g in completed]))
            .distinct()
            .count()
        )

        # Detail coverage: completed games with all details
        full_detail = 0
        for g in completed:
            has_meta = self.session.query(GameMetadata).filter(GameMetadata.game_id == g.game_id).count() > 0
            has_lineup = self.session.query(GameLineup).filter(GameLineup.game_id == g.game_id).count() > 0
            has_batting = self.session.query(GameBattingStat).filter(GameBattingStat.game_id == g.game_id).count() > 0
            has_pitching = (
                self.session.query(GamePitchingStat).filter(GamePitchingStat.game_id == g.game_id).count() > 0
            )
            if has_meta and has_lineup and has_batting and has_pitching:
                full_detail += 1

        return {
            "date": target_date,
            "total": total,
            "completed": len(completed),
            "pbp_coverage": round(pbp_count / len(completed), 3) if completed else 0,
            "detail_coverage": round(full_detail / len(completed), 3) if completed else 0,
            "completion_rate": round(len(completed) / total, 3) if total else 0,
        }

    def compute_weekly_sla(self, end_date: str, days: int = 7) -> list[dict]:
        end = datetime.strptime(end_date, "%Y%m%d")
        results = []
        for i in range(days):
            d = (end - timedelta(days=i)).strftime("%Y%m%d")
            results.append(self.compute_daily_sla(d))
        return results

    def print_weekly_report(self, end_date: str) -> None:
        sla_data = self.compute_weekly_sla(end_date, days=7)
        if not sla_data or sla_data[0]["total"] == 0:
            logger.info("[SLA] No data for week ending %s", end_date)
            return

        logger.info("\n%s", "=" * 60)
        logger.info("  SLA Report (Week ending %s)", end_date)
        logger.info("%s", "=" * 60)
        logger.info("%-12s %6s %10s %7s %7s %8s", "Date", "Games", "Completed", "Comp%", "PBP%", "Detail%")
        logger.info("%s %s %s %s %s %s", "-" * 12, "-" * 6, "-" * 10, "-" * 7, "-" * 7, "-" * 8)

        totals = {"games": 0, "completed": 0}
        for s in sla_data:
            if s["total"] == 0:
                continue
            totals["games"] += s["total"]
            totals["completed"] += s["completed"]
            logger.info(
                "  %-10s %6d %10d %6.0f%% %6.0f%% %7.0f%%",
                s["date"],
                s["total"],
                s["completed"],
                s["completion_rate"] * 100,
                s["pbp_coverage"] * 100,
                s["detail_coverage"] * 100,
            )

        overall_rate = totals["completed"] / totals["games"] if totals["games"] > 0 else 0
        logger.info("%s %s %s %s %s %s", "-" * 12, "-" * 6, "-" * 10, "-" * 7, "-" * 7, "-" * 8)
        logger.info("  %-10s %6s %10s %s", "TOTAL", totals["games"], totals["completed"], format(overall_rate, ">6.0%"))
        logger.info("")

    def send_weekly_sla_report(self, end_date: str | None = None) -> None:
        """
        Compute the past 7-day SLA data and send a summary to Telegram/Slack.
        If end_date is None, uses today (UTC).
        """
        if end_date is None:
            end_date = datetime.now(UTC).strftime("%Y%m%d")

        sla_data = self.compute_weekly_sla(end_date, days=7)
        active = [s for s in sla_data if s.get("total", 0) > 0]

        if not active:
            return  # No games this week — skip alert

        total_games = sum(s["total"] for s in active)
        total_completed = sum(s["completed"] for s in active)
        avg_pbp = sum(s.get("pbp_coverage", 0) for s in active) / len(active)
        avg_detail = sum(s.get("detail_coverage", 0) for s in active) / len(active)
        overall_rate = total_completed / total_games if total_games else 0

        # Find low-completion days (< 80%)
        low_days = [s for s in active if s.get("completion_rate", 1.0) < 0.80]
        low_days_text = ""
        if low_days:
            low_days_text = "\n⚠️ 낮은 완료율 날짜:\n"
            for s in low_days:
                low_days_text += f"  • {s['date']}: {s['completion_rate']:.0%} ({s['completed']}/{s['total']}경기)\n"

        start_date = min(s["date"] for s in active)
        msg = (
            f"<b>📊 주간 SLA 리포트 ({start_date}~{end_date})</b>\n"
            f"총 경기: {total_games} | 완료: {total_completed}\n"
            f"평균 완료율: {overall_rate:.1%}\n"
            f"평균 PBP 커버리지: {avg_pbp:.1%}\n"
            f"평균 상세 커버리지: {avg_detail:.1%}"
            f"{low_days_text}"
        )
        SlackWebhookClient.send_alert(msg)


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.now(UTC).strftime("%Y%m%d"))
    args = parser.parse_args()

    with SessionLocal() as session:
        tracker = SlaTracker(session)
        tracker.print_weekly_report(args.date)
