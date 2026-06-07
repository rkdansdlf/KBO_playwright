"""
Clutch/WPA aggregator.
Computes per-player, per-season clutch metrics from GameEvent WPA data.
"""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.models.game import Game, GameEvent
from src.models.season import KboSeason
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.safe_print import safe_print as print


class ClutchAggregator:
    def __init__(self, session: Session):
        self.session = session

    def aggregate(self, year: int) -> list[dict]:
        events = (
            self.session.query(GameEvent)
            .join(Game, GameEvent.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(
                KboSeason.season_year == year,
                KboSeason.league_type_name.in_(["정규시즌", "Regular Season"]),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                GameEvent.wpa.isnot(None),
            )
            .all()
        )

        if not events:
            print(f"[Clutch] {year}년 WPA 데이터 없음.")
            return []

        # Aggregate by batter
        batter_stats: dict[int, dict] = defaultdict(
            lambda: {
                "wpa_sum": 0.0,
                "wpa_abs_sum": 0.0,
                "count": 0,
                "high_leverage_wpa": 0.0,
                "high_leverage_count": 0,
            }
        )

        for e in events:
            bid = e.batter_id
            if not bid:
                continue

            bs = batter_stats[bid]
            we_before = e.win_expectancy_before or 0.5
            leverage = abs(we_before - 0.5)  # Lower = higher leverage (close game)

            bs["wpa_sum"] += e.wpa or 0.0
            bs["wpa_abs_sum"] += abs(e.wpa or 0.0)
            bs["count"] += 1

            # High leverage: win expectancy close to 0.5 (±0.15)
            if leverage <= 0.15:
                bs["high_leverage_wpa"] += e.wpa or 0.0
                bs["high_leverage_count"] += 1

        results = []
        for bid, bs in batter_stats.items():
            avg_wpa = round(bs["wpa_sum"] / bs["count"], 4) if bs["count"] > 0 else 0.0
            clutch = round(bs["high_leverage_wpa"], 4)
            results.append(
                {
                    "season": year,
                    "batter_id": bid,
                    "wpa_sum": round(bs["wpa_sum"], 4),
                    "wpa_abs_sum": round(bs["wpa_abs_sum"], 4),
                    "event_count": bs["count"],
                    "avg_wpa": avg_wpa,
                    "high_leverage_wpa": clutch,
                    "high_leverage_count": bs["high_leverage_count"],
                }
            )

        results.sort(key=lambda r: r["wpa_sum"], reverse=True)
        return results

    def persist_to_extra_stats(self, year: int):
        results = self.aggregate(year)
        if not results:
            return

        from src.models.player import PlayerSeasonBatting

        for r in results:
            pid = r["batter_id"]
            if not pid:
                continue
            bat = (
                self.session.query(PlayerSeasonBatting)
                .filter(
                    PlayerSeasonBatting.season == year,
                    PlayerSeasonBatting.player_id == pid,
                )
                .first()
            )
            if not bat:
                continue

            extra = bat.extra_stats or {}
            extra["wpa_sum"] = r["wpa_sum"]
            extra["avg_wpa"] = r["avg_wpa"]
            extra["high_leverage_wpa"] = r["high_leverage_wpa"]
            extra["clutch"] = r["high_leverage_wpa"]  # Clutch = WPA in high leverage
            bat.extra_stats = extra

        try:
            self.session.commit()
            print(f"[Clutch] {len(results)} batters updated for {year}.")
        except SQLAlchemyError as e:
            err_str = str(e)
            self.session.rollback()
            if "foreign key" in err_str.lower() or "Foreign key" in err_str:
                print(f"[Clutch] FK constraint ({err_str[:80]}...), trying raw SQL fallback...")
                import json

                from sqlalchemy import text

                for r in results:
                    pid = r["batter_id"]
                    if not pid:
                        continue
                    raw = self.session.execute(
                        text("SELECT extra_stats FROM player_season_batting WHERE season=:s AND player_id=:p"),
                        {"s": year, "p": pid},
                    ).scalar()
                    extra = json.loads(raw) if raw else {}
                    extra["wpa_sum"] = r["wpa_sum"]
                    extra["avg_wpa"] = r["avg_wpa"]
                    extra["high_leverage_wpa"] = r["high_leverage_wpa"]
                    extra["clutch"] = r["high_leverage_wpa"]
                    self.session.execute(
                        text("UPDATE player_season_batting SET extra_stats=:e WHERE season=:s AND player_id=:p"),
                        {"e": json.dumps(extra, ensure_ascii=False), "s": year, "p": pid},
                    )
                self.session.commit()
                print(f"[Clutch] {len(results)} batters updated via raw SQL.")
            else:
                print(f"[Clutch] Non-FK error: {err_str[:200]}")
                raise

    def print_report(self, year: int, top_n: int = 10):
        results = self.aggregate(year)
        if not results:
            return

        print(f"\n{'=' * 60}")
        print(f"  KBO {year}년 Clutch/WPA Top {top_n}")
        print(f"{'=' * 60}")
        print(f"{'순위':>4} {'BatterID':>9} {'WPA합계':>8} {'평균WPA':>8} {'Clutch':>8} {'고레버리지':>9}")
        print(f"{'-' * 60}")
        for i, r in enumerate(results[:top_n]):
            print(
                f"  {i + 1:>2}  {r['batter_id']:>9} {r['wpa_sum']:>8.4f} {r['avg_wpa']:>8.4f} "
                f"{r['high_leverage_wpa']:>8.4f} {r['high_leverage_count']:>9}"
            )

        print(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.now().year)
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--persist", action="store_true")
    args = parser.parse_args()

    with SessionLocal() as session:
        agg = ClutchAggregator(session)
        if args.persist:
            agg.persist_to_extra_stats(args.year)
        if args.report:
            agg.print_report(args.year)
        else:
            results = agg.aggregate(args.year)
            print(f"Computed clutch for {len(results)} batters in {args.year}")
