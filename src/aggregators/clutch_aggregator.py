"""Clutch/WPA aggregator.
Computes per-player, per-season clutch metrics from GameEvent WPA data.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.models.game import Game, GameEvent
from src.models.season import KboSeason
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ClutchAggregator:
    """ClutchAggregator class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    def aggregate(self, year: int) -> list[dict]:
        """Aggregates aggregate.

        Args:
            year: Season year.

        Returns:
            List of results.

        """
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
            logger.info("[Clutch] %s년 WPA 데이터 없음.", year)
            return []

        # Aggregate by batter
        batter_stats: dict[int, dict] = defaultdict(
            lambda: {
                "wpa_sum": 0.0,
                "wpa_abs_sum": 0.0,
                "count": 0,
                "high_leverage_wpa": 0.0,
                "high_leverage_count": 0,
            },
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
                },
            )

        results.sort(key=lambda r: r["wpa_sum"], reverse=True)
        return results

    def persist_to_extra_stats(self, year: int) -> None:
        """Handles the persist to extra stats operation.

        Args:
            year: Season year.

        """
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
            logger.info("[Clutch] %s batters updated for %s.", len(results), year)
        except SQLAlchemyError as e:
            err_str = str(e)
            self.session.rollback()
            if "foreign key" in err_str.lower() or "Foreign key" in err_str:
                logger.exception("[Clutch] FK constraint (%s...), trying raw SQL fallback...", err_str[:80])
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
                logger.exception("[Clutch] %s batters updated via raw SQL.", len(results))
            else:
                logger.exception("[Clutch] Non-FK error: %s", err_str[:200])
                raise

    def print_report(self, year: int, top_n: int = 10) -> None:
        """Prints print report.

        Args:
            year: Season year.
            top_n: Top N.

        """
        results = self.aggregate(year)
        if not results:
            return

        logger.info("\n%s", "=" * 60)
        logger.info("  KBO %s년 Clutch/WPA Top %s", year, top_n)
        logger.info("%s", "=" * 60)
        logger.info("%4s %9s %8s %8s %8s %9s", "순위", "BatterID", "WPA합계", "평균WPA", "Clutch", "고레버리지")
        logger.info("%s", "-" * 60)
        for i, r in enumerate(results[:top_n]):
            logger.info(
                "  %2d  %9s %8.4f %8.4f %8.4f %9d",
                i + 1,
                r["batter_id"],
                r["wpa_sum"],
                r["avg_wpa"],
                r["high_leverage_wpa"],
                r["high_leverage_count"],
            )

        logger.info("%s", "=" * 60)


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.now(KST).year)
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
            logger.info("Computed clutch for %s batters in %s", len(results), args.year)
