"""
SLA tracker: measure data freshness SLAs over time.
Tracks PBP coverage rate, game completion rate, detail capture rate.
Generates daily/weekly/monthly SLA reports.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from src.models.game import Game, GameBattingStat, GameLineup, GameMetadata, GamePitchingStat, GamePlayByPlay
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.safe_print import safe_print as print


class SlaTracker:
    def __init__(self, session: Session):
        self.session = session

    def compute_daily_sla(self, target_date: str) -> dict:
        from datetime import date as date_type

        if isinstance(target_date, str) and len(target_date) == 8:
            query_date = date_type(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:8]))
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

    def print_weekly_report(self, end_date: str):
        sla_data = self.compute_weekly_sla(end_date, days=7)
        if not sla_data or sla_data[0]["total"] == 0:
            print(f"[SLA] No data for week ending {end_date}")
            return

        print(f"\n{'=' * 60}")
        print(f"  SLA Report (Week ending {end_date})")
        print(f"{'=' * 60}")
        print(f"{'Date':<12} {'Games':>6} {'Completed':>10} {'Comp%':>7} {'PBP%':>7} {'Detail%':>8}")
        print(f"{'-' * 12} {'-' * 6} {'-' * 10} {'-' * 7} {'-' * 7} {'-' * 8}")

        totals = {"games": 0, "completed": 0}
        for s in sla_data:
            if s["total"] == 0:
                continue
            totals["games"] += s["total"]
            totals["completed"] += s["completed"]
            print(
                f"  {s['date']:<10} {s['total']:>6} {s['completed']:>10} "
                f"{s['completion_rate']:>6.0%} {s['pbp_coverage']:>6.0%} {s['detail_coverage']:>7.0%}"
            )

        if totals["games"] > 0:
            overall_rate = totals["completed"] / totals["games"]
        else:
            overall_rate = 0
        print(f"{'-' * 12} {'-' * 6} {'-' * 10} {'-' * 7} {'-' * 7} {'-' * 8}")
        print(f"  {'TOTAL':<10} {totals['games']:>6} {totals['completed']:>10} {overall_rate:>6.0%}")
        print()


if __name__ == "__main__":
    import argparse
    from datetime import datetime, timezone

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y%m%d"))
    args = parser.parse_args()

    with SessionLocal() as session:
        tracker = SlaTracker(session)
        tracker.print_weekly_report(args.date)
