"""
Backfill player birthdate ISO date values.
Parses various representations of birth_date into ISO standard birth_date_date column.
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime

logger = logging.getLogger(__name__)

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, update  # noqa: E402

from src.db.engine import SessionLocal  # noqa: E402
from src.models.player import PlayerBasic  # noqa: E402

# Standard formatting options for direct datetime.strptime parsing
_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y.%m.%d",
    "%Y/%m/%d",
    "%Y%m%d",
    "%y-%m-%d",
    "%y.%m.%d",
    "%y/%m/%d",
)


def _parse_birth_date(raw: str | None) -> date | None:
    """
    Parses birthdate string into a datetime.date object.
    Supports standard separator variations, 2-digit years, Korean characters,
    and single-digit month/day fields (e.g. '1990.7.3', '1990년 7월 3일').
    """
    if not raw:
        return None

    # Clean and normalize raw string
    s = raw.strip().replace(" ", "")
    if s in ("", "-", "None", "NULL"):
        return None

    # 1. Try standard formats directly
    for fmt in _FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    # 2. Extract digits if Korean formatting is used, e.g. "1990년7월3일" -> "1990.7.3"
    # Or resolve custom split for patterns like "1990.7.3" which datetime.strptime cannot easily match with %Y.%m.%d directly if not zero-padded
    s_cleaned = s.replace("년", ".").replace("월", ".").replace("일", "").replace("-", ".").replace("/", ".")
    parts = s_cleaned.split(".")

    # Remove any empty parts (e.g. trailing dot from "1990.07.03.")
    parts = [p for p in parts if p]

    if len(parts) == 3:
        try:
            y = int(parts[0])
            m = int(parts[1])
            d = int(parts[2])

            # Handle 2-digit year conversion (Pivot at 30, i.e., 30+ -> 1930+, <30 -> 2000+)
            if y < 100:
                if y >= 30:
                    y += 1900
                else:
                    y += 2000

            if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return datetime(y, m, d).date()
        except Exception:  # noqa: BLE001
            pass

    return None


def backfill(limit: int = 0, dry_run: bool = False, verbose: bool = False) -> int:
    """
    Finds player_basic rows where birth_date exists but birth_date_date is NULL.
    Parses and updates the records.
    """
    updated_count = 0
    parse_fail_count = 0

    with SessionLocal() as session:
        # Select records where birth_date is set but birth_date_date is null
        stmt = select(PlayerBasic).where(
            PlayerBasic.birth_date.isnot(None),
            PlayerBasic.birth_date != "",
            PlayerBasic.birth_date != "-",
            PlayerBasic.birth_date_date.is_(None),
        )

        if limit > 0:
            stmt = stmt.limit(limit)

        players = session.scalars(stmt).all()

        if not players:
            logger.info("✅ No player records found needing birthdate backfill.")
            return 0

        logger.info("⚙️ Found %s player records to process (dry_run=%s)...", len(players), dry_run)

        for idx, p in enumerate(players):
            parsed_date = _parse_birth_date(p.birth_date)

            if parsed_date:
                updated_count += 1
                if verbose or dry_run:
                    logger.info(
                        " [%s/%s] ID: %s | Name: %s | Raw: %s -> Parsed: %s",
                        idx + 1,
                        len(players),
                        p.player_id,
                        p.name,
                        p.birth_date,
                        parsed_date,
                    )

                if not dry_run:
                    session.execute(
                        update(PlayerBasic)
                        .where(PlayerBasic.player_id == p.player_id)
                        .values(birth_date_date=parsed_date)
                    )
            else:
                parse_fail_count += 1
                if verbose or dry_run:
                    logger.warning(
                        " ⚠️ [%s/%s] ID: %s | Name: %s | Raw: %s -> PARSE FAILED",
                        idx + 1,
                        len(players),
                        p.player_id,
                        p.name,
                        p.birth_date,
                    )

        if not dry_run and updated_count > 0:
            session.commit()
            logger.info("💾 Changes committed successfully.")

    logger.info("✨ Process completed!")
    logger.info("   - Successfully parsed/updated: %s", updated_count)
    logger.info("   - Failed to parse:              %s", parse_fail_count)
    return updated_count


def main():
    parser = argparse.ArgumentParser(description="Backfill parsed ISO date to birth_date_date in player_basic.")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of players to update (0 for no limit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and display output without updating database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose details of matches",
    )

    args = parser.parse_args()
    backfill(limit=args.limit, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
