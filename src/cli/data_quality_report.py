from __future__ import annotations

import logging
from collections.abc import Sequence

logger = logging.getLogger(__name__)
import argparse
import csv
import json
import os
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.db.engine import SessionLocal, create_engine_for_url
from src.models.player import (
    PlayerBasic,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)


def generate_report(years: list[int], output_format: str, output_dir: str, db_url: str | None = None) -> None:
    report_data = {
        "generated_at": datetime.now().isoformat(),
        "db_target": "LOCAL" if not db_url else "REMOTE",
        "years": {},
    }

    engine = create_engine_for_url(db_url) if db_url else None
    SessionFactory = sessionmaker(bind=engine) if engine else SessionLocal

    with SessionFactory() as session:
        for year in years:
            logger.info("📊 Processing %s...", year)
            year_data = {
                "batting": {"total": 0, "sources": {}, "consistency_rate": 0.0},
                "pitching": {"total": 0, "sources": {}, "consistency_rate": 0.0},
                "fielding": {"total": 0, "sources": {}, "consistency_rate": 0.0},
                "baserunning": {"total": 0, "sources": {}, "consistency_rate": 0.0},
                "discrepancies": [],
            }

            # 1. Batting Summary
            batting_stats = (
                session.query(PlayerSeasonBatting.source, func.count(PlayerSeasonBatting.id))
                .filter(PlayerSeasonBatting.season == year)
                .group_by(PlayerSeasonBatting.source)
                .all()
            )

            total_bat = 0
            for source, count in batting_stats:
                year_data["batting"]["sources"][source or "UNKNOWN"] = count
                total_bat += count
            year_data["batting"]["total"] = total_bat

            # 2. Pitching Summary
            pitching_stats = (
                session.query(PlayerSeasonPitching.source, func.count(PlayerSeasonPitching.id))
                .filter(PlayerSeasonPitching.season == year)
                .group_by(PlayerSeasonPitching.source)
                .all()
            )

            total_pit = 0
            for source, count in pitching_stats:
                year_data["pitching"]["sources"][source or "UNKNOWN"] = count
                total_pit += count
            year_data["pitching"]["total"] = total_pit

            # 3. Fielding Summary
            fielding_stats_query = (
                session.query(PlayerSeasonFielding.source, func.count(PlayerSeasonFielding.id))
                .filter(PlayerSeasonFielding.year == year)
                .group_by(PlayerSeasonFielding.source)
                .all()
            )

            total_fld = 0
            for source, count in fielding_stats_query:
                year_data["fielding"]["sources"][source or "UNKNOWN"] = count
                total_fld += count
            year_data["fielding"]["total"] = total_fld

            # 4. Baserunning Summary
            baserunning_stats_query = (
                session.query(PlayerSeasonBaserunning.source, func.count(PlayerSeasonBaserunning.id))
                .filter(PlayerSeasonBaserunning.year == year)
                .group_by(PlayerSeasonBaserunning.source)
                .all()
            )

            total_br = 0
            for source, count in baserunning_stats_query:
                year_data["baserunning"]["sources"][source or "UNKNOWN"] = count
                total_br += count
            year_data["baserunning"]["total"] = total_br

            # 5. Sample Consistency Check
            logger.info("   🔍 Auditing consistency for %s (sample)...", year)
            sample_players = (
                session.query(PlayerSeasonBatting).filter(PlayerSeasonBatting.season == year).limit(50).all()
            )
            matches = 0
            total_checked = 0
            for off in sample_players:
                # We need local session to aggregate stats from transactional data if they are not on remote
                # but for simplicity in report, we assume transactions ARE on remote too.
                calc = SeasonStatAggregator.aggregate_batting_season(session, off.player_id, year, "regular")
                if not calc:
                    continue
                total_checked += 1
                if off.at_bats == calc.get("at_bats") and off.hits == calc.get("hits"):
                    matches += 1
                else:
                    player = session.query(PlayerBasic).filter_by(player_id=off.player_id).first()
                    year_data["discrepancies"].append(
                        {
                            "player_id": off.player_id,
                            "name": player.name if player else "Unknown",
                            "type": "BATTING",
                            "details": f"AB: {off.at_bats} vs {calc.get('at_bats')}, H: {off.hits} vs {calc.get('hits')}",
                        },
                    )

            if total_checked > 0:
                year_data["batting"]["consistency_rate"] = round(matches / total_checked * 100, 2)

            report_data["years"][year] = year_data

    # Save Output
    os.makedirs(output_dir, exist_ok=True)
    suffix = "remote" if db_url else "local"
    filename = f"data_quality_report_{suffix}_{datetime.now():%Y%m%d_%H%M%S}"

    if output_format == "json":
        path = os.path.join(output_dir, f"{filename}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        logger.info("✅ Report saved to %s", path)
    else:
        path = os.path.join(output_dir, f"{filename}.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Year", "Category", "Total Rows", "Consistency Rate", "Source Breakdown"])
            for yr, data in report_data["years"].items():
                for cat in ["batting", "pitching", "fielding", "baserunning"]:
                    sources = "; ".join([f"{k}:{v}" for k, v in data[cat].get("sources", {}).items()])
                    writer.writerow(
                        [yr, cat.upper(), data[cat]["total"], data[cat].get("consistency_rate", "N/A"), sources],
                    )
        logger.info("✅ Report saved to %s", path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate KBO Data Quality Report")
    parser.add_argument("--years", type=str, default="2020-2026", help="Year range (e.g., 2020-2026)")
    parser.add_argument("--format", choices=["json", "csv"], default="json", help="Output format")
    parser.add_argument("--outdir", default="reports", help="Output directory")
    parser.add_argument("--db-url", help="Database URL (defaults to local SQLite)")
    args = parser.parse_args(argv)

    if "-" in args.years:
        start, end = map(int, args.years.split("-"))
        target_years = list(range(start, end + 1))
    else:
        target_years = [int(args.years)]

    generate_report(target_years, args.format, args.outdir, args.db_url)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
