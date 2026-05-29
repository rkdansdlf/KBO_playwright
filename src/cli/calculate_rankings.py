"""Rebuild supported stat_rankings from current season aggregates."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Sequence

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


def _dictify_rows(rows, label_lookup):
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


def rebuild_rankings(season: int) -> int:
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

        # Assuming 144 games for standard qualifications
        total_games = 144
        min_pa = int(total_games * 3.1)
        min_ip_outs = int(total_games * 3)

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
        print(f"[Rankings] ℹ️ No season stats available for {season}.")
        return 0

    print(f"[Rankings] ✅ Rebuilt {len(rankings)} ranking rows for {season}")
    return len(rankings)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild supported stat_rankings")
    parser.add_argument("--year", type=int, required=True, help="Season year to rebuild")
    args = parser.parse_args(argv)

    rebuild_rankings(args.year)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
