"""Rebuild supported stat_rankings from current season aggregates."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text

from src.aggregators.ranking_aggregator import RankingAggregator
from src.db.engine import SessionLocal
from src.models.player import (
    PlayerBasic,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)
from src.models.rankings import StatRanking

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _dictify_rows(rows: Sequence[object], label_lookup: dict[int, str]) -> list[dict]:
    """Convert ORM rows to dicts and inject player names."""
    result = []
    for row in rows:
        d = row.__dict__.copy()
        # Ensure we don't accidentally pass SQLAlchemy internal state
        d.pop("_sa_instance_state", None)
        # Convert dates/datetimes to ISO strings for JSON serialization
        for k, v in d.items():
            if isinstance(v, (datetime, date)):
                d[k] = v.isoformat()
        d["player_name"] = label_lookup.get(row.player_id, str(row.player_id))
        result.append(d)
    return result


def _games_played_in_season(session: Session, season: int) -> int:
    """Return number of completed game-dates in the given season."""
    row = session.execute(
        text("""
        SELECT COUNT(DISTINCT game_date) AS played
        FROM game
        WHERE CAST(strftime('%Y', game_date) AS INTEGER) = :yr
          AND game_status IN ('COMPLETED', 'DRAW')
        """),
        {"yr": season},
    ).fetchone()
    return int(row[0]) if row and row[0] else 0


_KBO_FULL_SEASON_GAMES = 144  # 정규 시즌 팀당 총 경기 수
_MIN_PA_PER_GAME = 3.1  # KBO 타율왕 자격 기준 PA/경기
_MIN_IP_PER_GAME = 3.0  # KBO 평균자책점 자격 기준 이닝/경기 (1이닝 = 3아웃)
_MIN_PA_FLOOR = 30  # 시즌 초반 최소 보호 기준
_MIN_IP_FLOOR = 90  # 시즌 초반 최소 보호 기준 (90 이닝 아웃 = 30 IP)


def _compute_min_pa(session: Session, season: int) -> int:
    """시즌 진행 경기 수 기반으로 타율 자격 min_pa를 동적으로 계산.

    완료된 시즌(144경기 이상)은 공식 기준 446 PA 적용.
    진행 중인 시즌은 현재까지의 경기 수 기반으로 완화된 기준 적용.
    """
    games_played = _games_played_in_season(session, season)
    if games_played >= _KBO_FULL_SEASON_GAMES:
        return int(_KBO_FULL_SEASON_GAMES * _MIN_PA_PER_GAME)
    return max(int(games_played * _MIN_PA_PER_GAME), _MIN_PA_FLOOR)


def _compute_min_ip_outs(session: Session, season: int) -> int:
    """시즌 진행 경기 수 기반으로 평균자책점 자격 min_ip_outs를 동적으로 계산.

    완료된 시즌(144경기 이상)은 공식 기준 432 이닝아웃 적용.
    진행 중인 시즌은 현재까지의 경기 수 기반으로 완화된 기준 적용.
    """
    games_played = _games_played_in_season(session, season)
    if games_played >= _KBO_FULL_SEASON_GAMES:
        return int(_KBO_FULL_SEASON_GAMES * _MIN_IP_PER_GAME)
    return max(int(games_played * _MIN_IP_PER_GAME), _MIN_IP_FLOOR)


def rebuild_rankings(season: int) -> int:
    """Handles the rebuild rankings operation.

    Args:
        season: Season year.

    Returns:
        Integer result.

    """
    with SessionLocal() as session:
        batting_rows = (
            session.query(PlayerSeasonBatting)
            .filter(
                PlayerSeasonBatting.season == season,
                PlayerSeasonBatting.league == "REGULAR",
            )
            .all()
        )
        pitching_rows = (
            session.query(PlayerSeasonPitching)
            .filter(
                PlayerSeasonPitching.season == season,
                PlayerSeasonPitching.league == "REGULAR",
            )
            .all()
        )
        # Fielding and baserunning use 'year' instead of 'season'
        fielding_rows = (
            session.query(PlayerSeasonFielding)
            .filter(
                PlayerSeasonFielding.year == season,
            )
            .all()
        )
        baserunning_rows = (
            session.query(PlayerSeasonBaserunning)
            .filter(
                PlayerSeasonBaserunning.year == season,
            )
            .all()
        )

        player_ids = {row.player_id for row in batting_rows}
        player_ids.update(row.player_id for row in pitching_rows)
        player_ids.update(row.player_id for row in fielding_rows)
        player_ids.update(row.player_id for row in baserunning_rows)

        label_lookup = (
            {
                row.player_id: row.name
                for row in session.query(PlayerBasic).filter(PlayerBasic.player_id.in_(player_ids)).all()
            }
            if player_ids
            else {}
        )

        batting_dicts = _dictify_rows(batting_rows, label_lookup)
        pitching_dicts = _dictify_rows(pitching_rows, label_lookup)
        fielding_dicts = _dictify_rows(fielding_rows, label_lookup)
        baserunning_dicts = _dictify_rows(baserunning_rows, label_lookup)

        # 시즌 진행 경기 수 기반으로 자격 기준을 동적으로 계산
        # (완료 시즌: 고정 기준 / 진행 중 시즌: 완화된 기준)
        min_pa = _compute_min_pa(session, season)
        min_ip_outs = _compute_min_ip_outs(session, season)
        logger.info("[Rankings] 자격 기준 — min_pa=%s, min_ip_outs=%s (season=%s)", min_pa, min_ip_outs, season)

        # Clear existing rankings for the season before regenerating
        session.query(StatRanking).filter(StatRanking.season == season).delete(synchronize_session=False)
        session.commit()

    aggregator = RankingAggregator()
    rankings = aggregator.generate_rankings(
        season=season,
        fielding_stats=fielding_dicts,
        baserunning_stats=baserunning_dicts,
        batting_stats=batting_dicts,
        pitching_stats=pitching_dicts,
        min_pa=min_pa,
        min_ip_outs=min_ip_outs,
        persist=True,  # Saves to DB inside RankingRepository
    )

    if not rankings:
        logger.info("[Rankings] ℹ️ No season stats available for %s.", season)
        return 0

    logger.info("[Rankings] ✅ Rebuilt %s ranking rows for %s", len(rankings), season)
    return len(rankings)


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="Rebuild supported stat_rankings")
    parser.add_argument("--year", type=int, required=True, help="Season year to rebuild")
    args = parser.parse_args(argv)

    rebuild_rankings(args.year)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
