import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))


import logging

from src.db.engine import SessionLocal
from src.models.team import TeamDailyRoster

logger = logging.getLogger(__name__)


def _collect_roster_days(
    year: int, start_date: date, end_date: date
) -> tuple[list[date], list[tuple[date, list[str]]]]:
    expected_team_count = 10
    standard_teams = ["LG", "HH", "SS", "KT", "OB", "LT", "HT", "NC", "SK", "WO"]
    missing_days: list[date] = []
    partial_days: list[tuple[date, list[str]]] = []
    with SessionLocal() as session:
        current_date = start_date
        while current_date <= end_date:
            teams_on_date = (
                session.query(TeamDailyRoster.team_code)
                .filter(TeamDailyRoster.roster_date == current_date)
                .distinct()
                .all()
            )
            team_codes = [t[0] for t in teams_on_date]
            count = len(team_codes)
            if count == 0:
                missing_days.append(current_date)
            elif count < expected_team_count:
                partial_days.append((current_date, [t for t in standard_teams if t not in team_codes]))
            current_date += timedelta(days=1)
    return missing_days, partial_days


def _print_roster_summary(
    year: int, total_days: int, missing_days: list[date], partial_days: list[tuple[date, list[str]]]
) -> None:
    complete_days = total_days - len(missing_days) - len(partial_days)
    logger.info(f"\n{'=' * 40}")
    logger.info(f"SUMMARY FOR {year}")
    logger.info(f"{'=' * 40}")
    logger.info(f"Complete Days: {complete_days}/{total_days}")
    logger.info(f"Missing Days:  {len(missing_days)}")
    logger.info(f"Partial Days:  {len(partial_days)}")
    logger.info(f"{'=' * 40}")
    if partial_days:
        logger.info("Details of Partial Days:")
        for d, missing in partial_days[:10]:
            logger.info(f"  - {d}: Missing teams {missing}")
        if len(partial_days) > 10:
            logger.info(f"  ... and {len(partial_days) - 10} more.")
    if missing_days:
        logger.info("Sample of Missing Days:")
        for d in missing_days[:10]:
            logger.info(f"  - {d}")
        if len(missing_days) > 10:
            logger.info(f"  ... and {len(missing_days) - 10} more.")


def check_integrity(year: int):
    logger.info(f"Auditing Team Daily Roster Integrity for Year: {year}...")
    start_date = date(year, 3, 23)
    end_date = date(year, 10, 31)
    if year == 2026:
        end_date = date(2026, 4, 15)
    missing_days, partial_days = _collect_roster_days(year, start_date, end_date)
    total_days = (end_date - start_date).days + 1
    _print_roster_summary(year, total_days, missing_days, partial_days)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KBO Roster Data Integrity Checker")
    parser.add_argument("--year", type=int, help="Target year to check", default=datetime.now().year)
    args = parser.parse_args()

    check_integrity(args.year)
