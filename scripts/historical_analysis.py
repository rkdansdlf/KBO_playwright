"""
Historical analysis of KBO statistics from 1982 to 2026.
Generates Hall of Fame style rankings and historical team records.
"""

from __future__ import annotations

import logging

from sqlalchemy import desc, func

from src.db.engine import SessionLocal
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching

logger = logging.getLogger(__name__)


def analyze_historical_leaders():
    with SessionLocal() as session:
        logger.info("🏆 --- KBO HISTORICAL HALL OF FAME (1982-2026) ---")

        # Define a subquery to get the "best" record for each player-season-league
        # Priority: FINAL_VERIFICATION (1), CRAWLER (2), PROFILE (3)
        # Using a simplified approach: just pick the one with highest games count for each player/season/league

        # Actually, let's just filter by REGULAR league and sum up.
        # But we must avoid duplicates from different sources.

        # Better approach: Create a temporary view or CTE that selects the primary source

        def get_best_source_subquery(model):
            # This is complex in SQLAlchemy. Let's use a simpler filtering:
            # Filter only for REGULAR league and pick only one source if multiple exist.
            # For this analysis, let's just use source='FINAL_VERIFICATION' or 'PROFILE' if it's the only one.
            pass

        # 1. Career Home Run Leaders
        logger.info("🔥 [CAREER HOME RUN LEADERS]")
        # To avoid double counting, we filter by source priority or just use a specific source.
        # Most historical data from my recent backfill is 'PROFILE'.
        # Most recent data is 'FINAL_VERIFICATION' or 'CRAWLER'.

        hr_leaders = (
            session.query(
                PlayerBasic.name,
                func.sum(PlayerSeasonBatting.home_runs).label("total_hr"),
                func.min(PlayerSeasonBatting.season).label("start_year"),
                func.max(PlayerSeasonBatting.season).label("end_year"),
            )
            .join(PlayerBasic, PlayerSeasonBatting.player_id == PlayerBasic.player_id)
            .filter(PlayerSeasonBatting.league == "REGULAR")
            # Filter to avoid double counting if multiple sources exist for same season
            # We'll group by player_id and season first to take the max, then sum.
            .group_by(PlayerBasic.player_id)
            .order_by(desc("total_hr"))
            .limit(10)
            .all()
        )
        # Wait, the above still has the duplicate issue.
        # Let's use a more surgical SQL-like approach using a subquery.

        from sqlalchemy import select

        # CTE to get distinct season records (picking max HR just in case of duplicates)
        batting_cte = (
            select(
                PlayerSeasonBatting.player_id,
                PlayerSeasonBatting.season,
                func.max(PlayerSeasonBatting.home_runs).label("home_runs"),
                func.max(PlayerSeasonBatting.avg).label("avg"),
                func.max(PlayerSeasonBatting.plate_appearances).label("plate_appearances"),
            )
            .where(PlayerSeasonBatting.league == "REGULAR")
            .group_by(PlayerSeasonBatting.player_id, PlayerSeasonBatting.season)
            .cte("batting_cte")
        )

        hr_leaders = (
            session.query(
                PlayerBasic.name,
                func.sum(batting_cte.c.home_runs).label("total_hr"),
                func.min(batting_cte.c.season).label("start_year"),
                func.max(batting_cte.c.season).label("end_year"),
            )
            .join(PlayerBasic, batting_cte.c.player_id == PlayerBasic.player_id)
            .group_by(PlayerBasic.player_id)
            .order_by(desc("total_hr"))
            .limit(15)
            .all()
        )

        for i, (name, total, start, end) in enumerate(hr_leaders, 1):
            if total is None:
                continue
            logger.info("%2d. %-8s | %3d HR | (%s~%s)", i, name, int(total), start, end)

        # 2. Career Win Leaders (Pitchers)
        logger.info("⚾ [CAREER WIN LEADERS (PITCHERS)]")
        pitching_cte = (
            select(
                PlayerSeasonPitching.player_id,
                PlayerSeasonPitching.season,
                func.max(PlayerSeasonPitching.wins).label("wins"),
            )
            .where(PlayerSeasonPitching.league == "REGULAR")
            .where(PlayerSeasonPitching.wins < 35)  # Filter out corrupted data (max wins in KBO is 30)
            .group_by(PlayerSeasonPitching.player_id, PlayerSeasonPitching.season)
            .cte("pitching_cte")
        )

        win_leaders = (
            session.query(
                PlayerBasic.name,
                func.sum(pitching_cte.c.wins).label("total_wins"),
                func.min(pitching_cte.c.season).label("start_year"),
                func.max(pitching_cte.c.season).label("end_year"),
            )
            .join(PlayerBasic, pitching_cte.c.player_id == PlayerBasic.player_id)
            .group_by(PlayerBasic.player_id)
            .order_by(desc("total_wins"))
            .limit(15)
            .all()
        )
        for i, (name, total, start, end) in enumerate(win_leaders, 1):
            if total is None:
                continue
            logger.info("%2d. %-8s | %3d Wins | (%s~%s)", i, name, int(total), start, end)

        # 3. Best Single Season Batting Average (Min 300 PA)
        logger.info("📈 [HIGHEST SINGLE SEASON BATTING AVERAGE (Min 300 PA)]")
        avg_leaders = (
            session.query(PlayerBasic.name, batting_cte.c.season, batting_cte.c.avg)
            .join(PlayerBasic, batting_cte.c.player_id == PlayerBasic.player_id)
            .filter(batting_cte.c.plate_appearances >= 300)
            .order_by(desc(batting_cte.c.avg))
            .limit(10)
            .all()
        )
        for i, (name, year, avg) in enumerate(avg_leaders, 1):
            logger.info("%2d. %-8s (%s) | %.3f AVG", i, name, year, avg)

        # 4. Most Home Runs by a Team in a Single Season
        from src.models.team import Team
        from src.models.team_stats import TeamSeasonBatting

        logger.info("🏟️  [MOST TEAM HOME RUNS IN A SINGLE SEASON]")
        team_hr_leaders = (
            session.query(Team.team_name, TeamSeasonBatting.season, TeamSeasonBatting.home_runs)
            .join(Team, TeamSeasonBatting.team_id == Team.team_id)
            .order_by(desc(TeamSeasonBatting.home_runs))
            .limit(10)
            .all()
        )
        for i, (name, year, hr) in enumerate(team_hr_leaders, 1):
            logger.info("%2d. %-15s (%s) | %3d HR", i, name, year, hr)


if __name__ == "__main__":
    analyze_historical_leaders()
