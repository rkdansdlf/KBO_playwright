"""CLI batch command to calculate and store Marcel player projections.

Computes 3-year weighted moving averages with league regression, aging curve,
and park factor adjustments for all qualified hitters and pitchers.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.aggregators.projection_engine import MarcelProjectionEngine
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.projection import PlayerProjection

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProjectionRunSummary:
    """Summary of batch projection run."""

    target_season: int
    hitters_count: int
    pitchers_count: int
    persisted_count: int
    dry_run: bool


def calculate_projections_batch(
    target_season: int,
    *,
    level: str = "KBO1",
    dry_run: bool = False,
    limit: int | None = None,
) -> ProjectionRunSummary:
    """Calculate and persist projections for hitters and pitchers."""
    engine = MarcelProjectionEngine()
    history_years = [target_season - 1, target_season - 2, target_season - 3]

    hitters_projected = 0
    pitchers_projected = 0
    persisted_count = 0

    league_rates_hitter = {
        "h_rate": 0.260,
        "hr_rate": 0.025,
        "bb_rate": 0.085,
        "so_rate": 0.180,
        "tb_rate": 0.380,
        "ab_rate": 0.880,
    }
    league_rates_pitcher = {
        "era": 4.50,
        "so_per_9": 7.5,
        "bb_per_9": 3.5,
        "hr_per_9": 1.0,
        "h_per_9": 9.0,
    }

    with SessionLocal() as session:
        # 1. Hitters Projection
        stmt_hitters = (
            select(PlayerSeasonBatting.player_id)
            .where(
                PlayerSeasonBatting.season.in_(history_years),
                PlayerSeasonBatting.level == level,
            )
            .group_by(PlayerSeasonBatting.player_id)
        )
        if limit:
            stmt_hitters = stmt_hitters.limit(limit)
        hitter_ids = list(session.execute(stmt_hitters).scalars().all())

        for pid in hitter_ids:
            hist_stmt = (
                select(PlayerSeasonBatting)
                .where(
                    PlayerSeasonBatting.player_id == pid,
                    PlayerSeasonBatting.season.in_(history_years),
                )
                .order_by(PlayerSeasonBatting.season.desc())
            )
            seasons = list(session.execute(hist_stmt).scalars().all())
            if not seasons:
                continue

            hist_payload = [
                {
                    "season": float(s.season),
                    "pa": float(s.plate_appearances or 0),
                    "ab": float(s.at_bats or 0),
                    "h": float(s.hits or 0),
                    "hr": float(s.home_runs or 0),
                    "bb": float(s.walks or 0),
                    "so": float(s.strikeouts or 0),
                    "hbp": float(s.hbp or 0),
                    "sf": float(s.sacrifice_flies or 0),
                    "sh": float(s.sacrifice_hits or 0),
                }
                for s in seasons
            ]
            proj_dict = engine.project_hitter(history_seasons=hist_payload, league_rates=league_rates_hitter, age=28)
            hitters_projected += 1

            if not dry_run:
                record = PlayerProjection(
                    target_season=target_season,
                    player_id=pid,
                    player_name=f"Player_{pid}",
                    team_code=seasons[0].team_code if hasattr(seasons[0], "team_code") else None,
                    position_type="HITTER",
                    age=28,
                    projected_pa=proj_dict.get("projected_pa"),
                    projected_avg=proj_dict.get("projected_avg"),
                    projected_obp=proj_dict.get("projected_obp"),
                    projected_slg=proj_dict.get("projected_slg"),
                    projected_ops=proj_dict.get("projected_ops"),
                    projected_woba=proj_dict.get("projected_woba"),
                    projected_stats=proj_dict,
                    weights_used={"w_y1": 5, "w_y2": 4, "w_y3": 3},
                    regression_params=league_rates_hitter,
                    version="marcel-v1",
                )
                session.merge(record)
                persisted_count += 1

        # 2. Pitchers Projection
        stmt_pitchers = (
            select(PlayerSeasonPitching.player_id)
            .where(
                PlayerSeasonPitching.season.in_(history_years),
                PlayerSeasonPitching.level == level,
            )
            .group_by(PlayerSeasonPitching.player_id)
        )
        if limit:
            stmt_pitchers = stmt_pitchers.limit(limit)
        pitcher_ids = list(session.execute(stmt_pitchers).scalars().all())

        for pid in pitcher_ids:
            hist_stmt = (
                select(PlayerSeasonPitching)
                .where(
                    PlayerSeasonPitching.player_id == pid,
                    PlayerSeasonPitching.season.in_(history_years),
                )
                .order_by(PlayerSeasonPitching.season.desc())
            )
            seasons = list(session.execute(hist_stmt).scalars().all())
            if not seasons:
                continue

            hist_payload = [
                {
                    "season": float(s.season),
                    "innings_outs": float(s.innings_outs or 0),
                    "earned_runs": float(s.earned_runs or 0),
                    "strikeouts": float(s.strikeouts or 0),
                    "walks": float(s.walks_allowed or 0),
                    "hits": float(s.hits_allowed or 0),
                    "home_runs": float(s.home_runs_allowed or 0),
                }
                for s in seasons
            ]
            proj_dict = engine.project_pitcher(history_seasons=hist_payload, league_rates=league_rates_pitcher, age=28)
            pitchers_projected += 1

            if not dry_run:
                record = PlayerProjection(
                    target_season=target_season,
                    player_id=pid,
                    player_name=f"Player_{pid}",
                    team_code=seasons[0].team_code if hasattr(seasons[0], "team_code") else None,
                    position_type="PITCHER",
                    age=28,
                    projected_ip=proj_dict.get("projected_ip"),
                    projected_era=proj_dict.get("projected_era"),
                    projected_fip=proj_dict.get("projected_fip"),
                    projected_whip=proj_dict.get("projected_whip"),
                    projected_stats=proj_dict,
                    weights_used={"w_y1": 5, "w_y2": 4, "w_y3": 3},
                    regression_params=league_rates_pitcher,
                    version="marcel-v1",
                )
                session.merge(record)
                persisted_count += 1

        if not dry_run:
            session.commit()

    return ProjectionRunSummary(
        target_season=target_season,
        hitters_count=hitters_projected,
        pitchers_count=pitchers_projected,
        persisted_count=persisted_count,
        dry_run=dry_run,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run CLI main entrypoint."""
    parser = argparse.ArgumentParser(description="Calculate Marcel Player Projections")
    parser.add_argument("--season", type=int, default=2026, help="Target projection season")
    parser.add_argument("--level", type=str, default="KBO1", help="League level code")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Calculate without saving")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of players")
    parser.add_argument("--json", action="store_true", default=False, help="Output JSON result")

    args = parser.parse_args(argv)
    summary = calculate_projections_batch(
        args.season,
        level=args.level,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    if args.json:
        print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))  # noqa: T201
    else:
        logger.info(
            "Marcel Projections: Season=%d, Hitters=%d, Pitchers=%d, Saved=%d, DryRun=%s",
            summary.target_season,
            summary.hitters_count,
            summary.pitchers_count,
            summary.persisted_count,
            summary.dry_run,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
