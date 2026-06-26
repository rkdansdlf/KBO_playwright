"""CLI 명령: data quality report."""

from __future__ import annotations

import logging

from src.constants import KST

logger = logging.getLogger(__name__)
import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.db.engine import SessionLocal, create_engine_for_url
from src.models.player import (
    PlayerBasic,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def _empty_year_data() -> dict:
    return {
        "batting": {"total": 0, "sources": {}, "consistency_rate": 0.0},
        "pitching": {"total": 0, "sources": {}, "consistency_rate": 0.0},
        "fielding": {"total": 0, "sources": {}, "consistency_rate": 0.0},
        "baserunning": {"total": 0, "sources": {}, "consistency_rate": 0.0},
        "discrepancies": [],
    }


def _source_counts(session: Session, model: type[object], year_field: object, year: int) -> tuple[int, dict]:
    rows = session.query(model.source, func.count(model.id)).filter(year_field == year).group_by(model.source).all()
    sources = {source or "UNKNOWN": count for source, count in rows}
    return sum(sources.values()), sources


def _populate_source_summaries(session: Session, year_data: dict, year: int) -> None:
    specs = (
        ("batting", PlayerSeasonBatting, PlayerSeasonBatting.season),
        ("pitching", PlayerSeasonPitching, PlayerSeasonPitching.season),
        ("fielding", PlayerSeasonFielding, PlayerSeasonFielding.year),
        ("baserunning", PlayerSeasonBaserunning, PlayerSeasonBaserunning.year),
    )
    for category, model, year_field in specs:
        total, sources = _source_counts(session, model, year_field, year)
        year_data[category]["total"] = total
        year_data[category]["sources"] = sources


def _audit_batting_consistency(session: Session, year_data: dict, year: int) -> None:
    logger.info("   🔍 Auditing consistency for %s (sample)...", year)
    sample_players = session.query(PlayerSeasonBatting).filter(PlayerSeasonBatting.season == year).limit(50).all()
    matches = 0
    total_checked = 0
    for official in sample_players:
        calc = SeasonStatAggregator.aggregate_batting_season(session, official.player_id, year, "regular")
        if not calc:
            continue
        total_checked += 1
        if official.at_bats == calc.get("at_bats") and official.hits == calc.get("hits"):
            matches += 1
        else:
            _append_batting_discrepancy(session, year_data, official, calc)
    if total_checked > 0:
        year_data["batting"]["consistency_rate"] = round(matches / total_checked * 100, 2)


def _append_batting_discrepancy(session: Session, year_data: dict, official: PlayerSeasonBatting, calc: dict) -> None:
    player = session.query(PlayerBasic).filter_by(player_id=official.player_id).first()
    year_data["discrepancies"].append(
        {
            "player_id": official.player_id,
            "name": player.name if player else "Unknown",
            "type": "BATTING",
            "details": f"AB: {official.at_bats} vs {calc.get('at_bats')}, H: {official.hits} vs {calc.get('hits')}",
        },
    )


def _build_report_data(years: list[int], db_url: str | None) -> dict:
    report_data = {
        "generated_at": datetime.now(KST).isoformat(),
        "db_target": "LOCAL" if not db_url else "REMOTE",
        "years": {},
    }
    engine = create_engine_for_url(db_url) if db_url else None
    session_factory = sessionmaker(bind=engine) if engine else SessionLocal
    with session_factory() as session:
        for year in years:
            logger.info("📊 Processing %s...", year)
            year_data = _empty_year_data()
            _populate_source_summaries(session, year_data, year)
            _audit_batting_consistency(session, year_data, year)
            report_data["years"][year] = year_data
    return report_data


def _report_path(output_dir: str, db_url: str | None, output_format: str) -> str:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    suffix = "remote" if db_url else "local"
    filename = f"data_quality_report_{suffix}_{datetime.now(KST):%Y%m%d_%H%M%S}"
    return str(Path(output_dir, f"{filename}.{output_format}"))


def _write_json_report(report_data: dict, path: str) -> None:
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)


def _write_csv_report(report_data: dict, path: str) -> None:
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Year", "Category", "Total Rows", "Consistency Rate", "Source Breakdown"])
        for year, data in report_data["years"].items():
            for category in ["batting", "pitching", "fielding", "baserunning"]:
                sources = "; ".join([f"{key}:{value}" for key, value in data[category].get("sources", {}).items()])
                writer.writerow(
                    [
                        year,
                        category.upper(),
                        data[category]["total"],
                        data[category].get("consistency_rate", "N/A"),
                        sources,
                    ],
                )


def generate_report(years: list[int], output_format: str, output_dir: str, db_url: str | None = None) -> None:
    """Generates generate report.

    Args:
        years: Years.
        output_format: Output Format.
        output_dir: Output directory path.
        db_url: Db URL.

    """
    report_data = _build_report_data(years, db_url)
    path = _report_path(output_dir, db_url, output_format)
    if output_format == "json":
        _write_json_report(report_data, path)
    else:
        _write_csv_report(report_data, path)
    logger.info("✅ Report saved to %s", path)


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for this CLI command."""
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
