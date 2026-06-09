"""
통합 Game Stat 복구 스크립트. --type batting|pitching 으로 선택.
NULL/zero AVG, OBP, ERA, WHIP 등을 계산하여 채웁니다.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from src.db.engine import SessionLocal
from src.models.game import GameBattingStat, GamePitchingStat
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

logger = logging.getLogger(__name__)


def _repair_batting():
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
            stat.avg = ratios["avg"]
            stat.obp = ratios["obp"]
            stat.slg = ratios["slg"]
            stat.ops = ratios["ops"]
            stat.iso = ratios["iso"]
            stat.babip = ratios["babip"]
            if stat.extra_stats is None:
                stat.extra_stats = {}
            extras = dict(stat.extra_stats)
            extras["xr"] = ratios["xr"]
            stat.extra_stats = extras
            updated += 1
            if idx % 500 == 0:
                session.commit()
        session.commit()
        logger.info(f"[REPAIR] Batting: Updated {updated} rows.")


def _repair_pitching():
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
            stat.era = ratios["era"]
            stat.whip = ratios["whip"]
            stat.k_per_nine = ratios["k_per_nine"]
            stat.bb_per_nine = ratios["bb_per_nine"]
            stat.kbb = ratios["kbb"]
            if hasattr(stat, "fip"):
                stat.fip = ratios["fip"]
            elif hasattr(stat, "extra_stats"):
                if stat.extra_stats is None:
                    stat.extra_stats = {}
                extras = dict(stat.extra_stats)
                extras["fip"] = ratios["fip"]
                stat.extra_stats = extras
            updated += 1
            if idx % 500 == 0:
                session.commit()
        session.commit()
        logger.info(f"[REPAIR] Pitching: Updated {updated} rows.")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Repair NULL/zero game stats")
    parser.add_argument("--type", choices=["batting", "pitching", "all"], default="all", help="Stat type to repair")
    args = parser.parse_args(argv)
    if args.type in ("batting", "all"):
        _repair_batting()
    if args.type in ("pitching", "all"):
        _repair_pitching()


if __name__ == "__main__":
    main()
