"""
Park Factor calculator for KBO stadiums.

Calculates run factor (PF) and home run factor (HRF) per stadium per season.
Formula: (RS + RA)_home / (RS + RA)_away  (standardized to 1.00 = neutral).
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

from src.constants import KST
from src.models.game import Game
from src.models.season import KboSeason
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ParkFactorCalculator:
    """ParkFactorCalculator class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    def calculate(self, year: int) -> list[dict]:
        """
        Calculates calculate.

        Args:
            year: Season year.

        Returns:
            List of results.

        """
        games = (
            self.session.query(Game)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(
                KboSeason.season_year == year,
                KboSeason.league_type_name.in_(["정규시즌", "Regular Season"]),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
                Game.home_score.isnot(None),
                Game.away_score.isnot(None),
            )
            .all()
        )

        if not games:
            logger.info("[ParkFactor] %s년 완료 경기 없음.", year)
            return []

        # Standard simplified Park Factor:
        # PF = (runs per game at venue) / (league average runs per game)
        # 1.00 = neutral, >1.00 = hitter-friendly, <1.00 = pitcher-friendly
        venue_games: dict[str, dict] = defaultdict(
            lambda: {
                "home_runs": 0,
                "away_runs": 0,
                "games": 0,
            },
        )

        for g in games:
            stadium = g.stadium or "UNKNOWN"
            vg = venue_games[stadium]
            vg["home_runs"] += g.home_score or 0
            vg["away_runs"] += g.away_score or 0
            vg["games"] += 1

        total_runs = sum(v["home_runs"] + v["away_runs"] for v in venue_games.values())
        total_games = sum(v["games"] for v in venue_games.values())
        league_avg_rpg = total_runs / total_games if total_games > 0 else 10.0

        results = []
        for stadium, vg in sorted(venue_games.items()):
            runs_at_venue = vg["home_runs"] + vg["away_runs"]
            games_at_venue = vg["games"]

            if games_at_venue == 0:
                continue

            runs_per_game = runs_at_venue / games_at_venue

            pf = runs_per_game / league_avg_rpg if league_avg_rpg > 0 else 1.0

            results.append(
                {
                    "season": year,
                    "stadium": stadium,
                    "games": games_at_venue,
                    "home_runs": vg["home_runs"],
                    "away_runs": vg["away_runs"],
                    "total_runs": runs_at_venue,
                    "runs_per_game": round(runs_per_game, 3),
                    "league_avg_rpg": round(league_avg_rpg, 3),
                    "park_factor": round(pf, 3),
                    "park_factor_label": self._label(pf),
                },
            )

        return results

    def _label(self, pf: float) -> str:
        if pf > 1.10:
            return "타자친화"
        if pf > 1.04:
            return "약간 타자친화"
        if pf > 0.96:
            return "중립"
        if pf > 0.90:
            return "약간 투수친화"
        return "투수친화"

    def print_report(self, year: int) -> None:
        """
        Prints print report.

        Args:
            year: Season year.

        """
        results = self.calculate(year)
        if not results:
            return

        logger.info("\n%s", "=" * 80)
        logger.info("  KBO %s년 구장별 파크팩터 (Park Factor)", year)
        logger.info("%s", "=" * 70)
        logger.info("  KBO %s년 구장별 파크팩터 (Park Factor)", year)
        logger.info("%s", "=" * 70)
        logger.info("%-18s %4s %5s %6s  평가", "구장", "경기", "RPG", "PF")
        logger.info("%s", "-" * 70)

        for r in sorted(results, key=lambda x: x["park_factor"], reverse=True):
            pf_label = r["park_factor_label"]
            logger.info(
                "  %-18s %4d %5.1f %5.3f  %s",
                r["stadium"],
                r["games"],
                r["runs_per_game"],
                r["park_factor"],
                pf_label,
            )

        logger.info("%s", "=" * 70)
        logger.info("  리그 평균: %.2f 점/경기", results[0]["league_avg_rpg"])


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    from src.db.engine import SessionLocal

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=datetime.now(KST).year)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    session = SessionLocal()
    try:
        calc = ParkFactorCalculator(session)
        if args.report:
            calc.print_report(args.year)
        else:
            results = calc.calculate(args.year)
            for r in results:
                logger.info(r)
    finally:
        session.close()
