"""
통합 Game Stat 복구 스크립트.

    --type batting|pitching 으로 선택.

NULL/zero AVG, OBP, ERA, WHIP 등을 계산하여 채웁니다.

"""

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.models.game import GameBattingStat, GamePitchingStat
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def _repair_batting() -> None:
    logger.info("[REPAIR] Starting batting stat repair...")
    with SessionLocal() as session:
        query = session.query(GameBattingStat).filter((GameBattingStat.avg.is_(None)) | (GameBattingStat.avg == 0.0))
        total = query.count()
        if total == 0:
            logger.info("[REPAIR] No missing batting stats found.")
            return

        updated = 0
        for idx, stat in enumerate(query.all(), 1):
            raw = {
                "at_bats": stat.at_bats,
                "hits": stat.hits,
                "walks": stat.walks,
                "hbp": stat.hbp,
                "sacrifice_flies": stat.sacrifice_flies,
                "doubles": stat.doubles,
                "triples": stat.triples,
                "home_runs": stat.home_runs,
                "strikeouts": stat.strikeouts,
                "plate_appearances": stat.plate_appearances,
                "intentional_walks": stat.intentional_walks,
                "stolen_bases": stat.stolen_bases,
                "caught_stealing": stat.caught_stealing,
                "gdp": stat.gdp,
                "sacrifice_hits": stat.sacrifice_hits,
            }
            ratios = BattingStatCalculator.calculate_ratios(raw)
            stat.avg = ratios["avg"]  # type: ignore[assignment]
            stat.obp = ratios["obp"]  # type: ignore[assignment]
            stat.slg = ratios["slg"]  # type: ignore[assignment]
            stat.ops = ratios["ops"]  # type: ignore[assignment]
            stat.iso = ratios["iso"]  # type: ignore[assignment]
            stat.babip = ratios["babip"]  # type: ignore[assignment]
            if stat.extra_stats is None:
                stat.extra_stats = {}
            extras = dict(stat.extra_stats)
            extras["xr"] = ratios["xr"]
            stat.extra_stats = extras  # type: ignore[assignment]
            updated += 1
            if idx % 500 == 0:
                session.commit()
        session.commit()
        logger.info("[REPAIR] Batting: Updated %s rows.", updated)


def _repair_pitching() -> None:
    logger.info("[REPAIR] Starting pitching stat repair...")
    with SessionLocal() as session:
        query = session.query(GamePitchingStat).filter((GamePitchingStat.era.is_(None)) | (GamePitchingStat.era == 0.0))
        total = query.count()
        if total == 0:
            logger.info("[REPAIR] No missing pitching stats found.")
            return

        updated = 0
        for idx, stat in enumerate(query.all(), 1):
            raw = {
                "innings_outs": stat.innings_outs,
                "earned_runs": stat.earned_runs,
                "hits_allowed": stat.hits_allowed,
                "walks_allowed": stat.walks_allowed,
                "strikeouts": stat.strikeouts,
                "home_runs_allowed": stat.home_runs_allowed,
                "hit_batters": stat.hit_batters,
                "batters_faced": stat.batters_faced,
            }
            ratios = PitchingStatCalculator.calculate_ratios(raw)
            stat.era = ratios["era"]  # type: ignore[assignment]
            stat.whip = ratios["whip"]  # type: ignore[assignment]
            stat.k_per_nine = ratios["k_per_nine"]  # type: ignore[assignment]
            stat.bb_per_nine = ratios["bb_per_nine"]  # type: ignore[assignment]
            stat.kbb = ratios["kbb"]  # type: ignore[assignment]
            if hasattr(stat, "fip"):
                stat.fip = ratios["fip"]
            elif hasattr(stat, "extra_stats"):
                if stat.extra_stats is None:
                    stat.extra_stats = {}
                extras = dict(stat.extra_stats)
                extras["fip"] = ratios["fip"]
                stat.extra_stats = extras  # type: ignore[assignment]
            updated += 1
            if idx % 500 == 0:
                session.commit()
        session.commit()
        logger.info("[REPAIR] Pitching: Updated %s rows.", updated)


def main(argv: Sequence[str] | None = None) -> None:
    """
    Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = argparse.ArgumentParser(description="Repair NULL/zero game stats")

    parser.add_argument("--type", choices=["batting", "pitching", "all"], default="all", help="Stat type to repair")
    args = parser.parse_args(argv)
    if args.type in ("batting", "all"):
        _repair_batting()
    if args.type in ("pitching", "all"):
        _repair_pitching()


if __name__ == "__main__":
    main()
