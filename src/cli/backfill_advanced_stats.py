"""CLI 명령: backfill advanced stats."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
import argparse
from typing import TYPE_CHECKING

from sqlalchemy import func

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.db.engine import SessionLocal
from src.models.game import GameBattingStat, GamePitchingStat
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.repositories.player_stats_repository import (
    PlayerSeasonBaserunningRepository,
    PlayerSeasonFieldingRepository,
)
from src.repositories.safe_batting_repository import save_batting_stats_safe

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any

    from sqlalchemy.orm import Session


def _build_player_team_map(session: Session) -> dict:
    team_map: dict[Any, Any] = {}
    for model in (GameBattingStat, GamePitchingStat):
        team_query = (
            session.query(
                model.player_id,
                model.team_code,
                func.count(model.id).label("cnt"),
            )
            .group_by(model.player_id, model.team_code)
            .all()
        )
        for player_id, team, count in team_query:
            if player_id not in team_map or count > team_map[player_id][1]:
                team_map[player_id] = (team, count)
    return team_map


def _assign_team(stats: list[dict], team_map: dict, *, target_key: str) -> list[dict]:
    for stat in stats:
        stat[target_key] = team_map.get(stat["player_id"], (None, 0))[0]
    return [stat for stat in stats if stat[target_key]]


def _backfill_batting(session: Session, year: int, series: str, team_map: dict) -> None:
    stats = SeasonStatAggregator.aggregate_batting_season_bulk(session, year, series, source="FALLBACK_BACKFILL")
    if not stats:
        return
    valid_stats = _assign_team(stats, team_map, target_key="team_code")
    save_batting_stats_safe(valid_stats)
    logger.info("   ✅ Batting: %s records saved.", len(valid_stats))


def _backfill_pitching(session: Session, year: int, series: str, team_map: dict) -> None:
    stats = SeasonStatAggregator.aggregate_pitching_season_bulk(session, year, series, source="FALLBACK_BACKFILL")
    if not stats:
        return
    valid_stats = _assign_team(stats, team_map, target_key="team_code")
    save_pitching_stats_to_db(valid_stats)
    logger.info("   ✅ Pitching: %s records saved.", len(valid_stats))


def _backfill_baserunning(
    session: Session,
    year: int,
    series: str,
    team_map: dict,
    baserun_repo: PlayerSeasonBaserunningRepository,
) -> None:
    stats = SeasonStatAggregator.aggregate_baserunning_season_bulk(session, year, series, source="FALLBACK_BACKFILL")
    if not stats:
        return
    valid_stats = _assign_team(stats, team_map, target_key="team_id")
    logger.info("   ✅ Baserunning: %s records saved.", baserun_repo.upsert_many(valid_stats))


def _backfill_fielding(
    session: Session,
    year: int,
    series: str,
    team_map: dict,
    fielding_repo: PlayerSeasonFieldingRepository,
) -> None:
    stats = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year, series, source="FALLBACK_BACKFILL")
    if not stats:
        return
    valid_stats = _assign_team(stats, team_map, target_key="team_id")
    logger.info("   ✅ Fielding: %s records saved.", fielding_repo.upsert_many(valid_stats))


def backfill_stats(years: list[int], series: str) -> None:
    """
    Backfills stats.

    Args:
        years: Years.
        series: Series.
        years: Years.
        series: Series.

    """
    fielding_repo = PlayerSeasonFieldingRepository()

    baserun_repo = PlayerSeasonBaserunningRepository()

    with SessionLocal() as session:
        for year in years:
            logger.info("🛠️  Backfilling Advanced Stats for %s %s...", year, series)
            team_map = _build_player_team_map(session)
            _backfill_batting(session, year, series, team_map)
            _backfill_pitching(session, year, series, team_map)
            _backfill_baserunning(session, year, series, team_map, baserun_repo)
            _backfill_fielding(session, year, series, team_map, fielding_repo)


def main(argv: Sequence[str] | None = None) -> int:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Backfill missing advanced stats from transactions.")

    parser.add_argument("--years", type=str, default="2020-2026")
    parser.add_argument("--series", type=str, default="regular")
    args = parser.parse_args(argv)

    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]

    backfill_stats(target_years, args.series)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
