"""
Phase 4f: Historical gap analysis — identify missing data across seasons.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import text

from src.db.engine import SessionLocal

logger = logging.getLogger(__name__)


def analyze_gaps():
    with SessionLocal() as s:
        s.execute(
            text("""
            SELECT season_year, league_type_name FROM kbo_seasons
            WHERE season_year BETWEEN 1982 AND 2000
            ORDER BY season_year
        """)
        ).all()

        # player_season_batting coverage
        batting_q = s.execute(
            text("""
            SELECT season, COUNT(*) as cnt FROM player_season_batting
            WHERE season BETWEEN 1982 AND 2000
            GROUP BY season ORDER BY season
        """)
        ).all()
        batting = {row.season: row.cnt for row in batting_q}

        # player_season_pitching coverage
        pitching_q = s.execute(
            text("""
            SELECT season, COUNT(*) as cnt FROM player_season_pitching
            WHERE season BETWEEN 1982 AND 2000
            GROUP BY season ORDER BY season
        """)
        ).all()
        pitching = {row.season: row.cnt for row in pitching_q}

        # game-level data coverage (only from 2001 onward)
        game_q = s.execute(
            text("""
            SELECT CAST(strftime('%Y', game_date) AS INTEGER) as yr, COUNT(*) as cnt
            FROM game GROUP BY yr ORDER BY yr
        """)
        ).all()
        games_by_year = {row.yr: row.cnt for row in game_q}

        logger.info("=" * 80)
        logger.info("  KBO Historical Data Gap Analysis (1982–2000)")
        logger.info("=" * 80)
        logger.info("%5s %8s %8s %9s %7s  %-30s", "Year", "Season", "Batting", "Pitching", "Games", "Status")
        logger.info("-" * 80)

        all_years = set(range(1982, 2001))
        gaps = defaultdict(list)

        for year in sorted(all_years):
            bat = batting.get(year, 0)
            pit = pitching.get(year, 0)
            gms = games_by_year.get(year, 0)

            if bat > 0 and gms > 0:
                status = "FULL"
            elif bat > 0 and gms == 0:
                status = "NO_GAME_DATA"
                gaps["game_detail"].append(year)
            elif bat == 0 and gms > 0:
                status = "NO_BATTING_DATA"
                gaps["batting"].append(year)
            else:
                status = "NO_DATA"
                gaps["all"].append(year)

            logger.info("  %5s %8s %8s %9s %7s  %-30s", year, str(year) + "시즌", bat, pit, gms, status)

        logger.info("-" * 80)

        if gaps["game_detail"]:
            logger.info(
                "  Game detail gap: %s years (%s–%s)",
                len(gaps["game_detail"]),
                min(gaps["game_detail"]),
                max(gaps["game_detail"]),
            )
            logger.info("  → player_season_batting exists but game table is empty for these years.")
        if gaps["all"]:
            logger.info("  No data at all: %s years: %s", len(gaps["all"]), gaps["all"])

        logger.info("  2000년 이전 게임 데이터는 KBO 웹사이트에서 제공하지 않음.")
        logger.info("  대안: static CSV seeding 또는 KBO 기록실 PDF 수집.")
        logger.info(
            "  Contemporary data (2001+): %s years with games, %s–%s.",
            len(games_by_year),
            min(games_by_year.keys()),
            max(games_by_year.keys()),
        )


if __name__ == "__main__":
    analyze_gaps()
