"""Backfill and normalize stadium_code and game_metadata for historical seasons (1982-2000).

Usage:
------
  python3 -m scripts.maintenance.backfill_historical_stadium_codes --all --dry-run
  python3 -m scripts.maintenance.backfill_historical_stadium_codes --all --apply
  python3 -m scripts.maintenance.backfill_historical_stadium_codes --year 1990 --apply
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker


from src.models.game import Game, GameMetadata
from src.utils.stadium_codes import resolve_stadium_code

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# Known dirty stadium string cleanups in raw boxscores
DIRTY_STADIUM_CLEANUP: dict[str, str] = {
    "7월 17일 DH2 무등 야구장": "광주",
    "4월 17일 서울종합운동장 야구장": "잠실",
    "30 숭의야구장": "인천",
}

# Standard full Korean stadium names by code and era
HISTORICAL_STADIUM_NAMES: dict[str, str] = {
    "JAMSIL": "잠실야구장",
    "DAEGU": "대구시민운동장 야구장",
    "SIMIN": "대구시민운동장 야구장",
    "MUDEUNG": "광주 무등경기장 야구장",
    "GWANGJU": "광주-기아 챔피언스 필드",
    "SAJIK": "부산 사직 야구장",
    "MUNHAK": "숭의야구장",
    "HANBAT": "대전 한밭야구장",
    "JEONJU": "전주종합운동장 야구장",
    "DONGDAEMUN": "동대문야구장",
    "CHEONGJU": "청주종합운동장 야구장",
    "SUWON": "수원종합운동장 야구장",
    "MASAN": "마산야구장",
    "GUNSAN": "군산월명종합운동장 야구장",
    "CHUNCHEON": "춘천야구장",
    "JEJU": "제주 오라야구장",
    "POHANG": "포항야구장",
    "ULSAN": "울산문수야구장",
    "MOKDONG": "목동야구장",
    "GOCHEOK": "고척스카이돔",
    "CHANGWON": "창원NC파크",
}

DEFAULT_HOME_STADIUM: dict[str, str] = {
    "SS": "대구",
    "LT": "부산",
    "HT": "광주",
    "OB": "잠실",
    "DB": "잠실",
    "LG": "잠실",
    "MB": "잠실",
    "MBC": "잠실",
    "HH": "대전",
    "BE": "대전",
    "TP": "인천",
    "SM": "인천",
    "CB": "인천",
    "SK": "인천",
    "SL": "전주",
    "HU": "수원",
}


def clean_stadium_name(raw_stadium: str | None, home_team: str | None) -> str:
    """Clean dirty stadium strings or infer from home team if blank."""
    if not raw_stadium or not raw_stadium.strip():
        if home_team and home_team in DEFAULT_HOME_STADIUM:
            return DEFAULT_HOME_STADIUM[home_team]
        return "잠실"

    s = raw_stadium.strip()
    if s in DIRTY_STADIUM_CLEANUP:
        return DIRTY_STADIUM_CLEANUP[s]
    return s


def _process_single_game(
    g: Game,
    year: int,
    session: Any,
    *,
    apply_changes: bool,
) -> tuple[int, int, int]:
    """Process a single game row. Returns (cleaned, created, updated)."""
    raw_stadium = g.stadium
    cleaned_stadium = clean_stadium_name(raw_stadium, g.home_team)
    cleaned = 1 if cleaned_stadium != raw_stadium else 0
    if cleaned and apply_changes:
        g.stadium = cleaned_stadium

    stadium_code = resolve_stadium_code(cleaned_stadium, season_year=year) or "JAMSIL"
    stadium_name = HISTORICAL_STADIUM_NAMES.get(stadium_code, f"{cleaned_stadium}야구장")

    meta = session.execute(select(GameMetadata).where(GameMetadata.game_id == g.game_id)).scalar_one_or_none()

    payload = {
        "source": "historical_boxscore",
        "raw_stadium": raw_stadium,
        "cleaned_stadium": cleaned_stadium,
        "backfilled_at": datetime.now(UTC).isoformat(),
    }

    if meta is None:
        if apply_changes:
            new_meta = GameMetadata(
                game_id=g.game_id,
                stadium_code=stadium_code,
                stadium_name=stadium_name,
                source_payload=payload,
            )
            session.add(new_meta)
        return (cleaned, 1, 0)

    if apply_changes:
        meta.stadium_code = stadium_code
        meta.stadium_name = stadium_name
        if not meta.source_payload:
            meta.source_payload = payload
    return (cleaned, 0, 1)


def backfill_stadiums(
    start_year: int,
    end_year: int,
    *,
    db_url: str | None = None,
    apply_changes: bool = False,
) -> int:
    """Backfill stadium_code and game_metadata for target years."""
    target_url = db_url or os.environ.get("DATABASE_URL") or "sqlite:///./data/kbo_dev.db"
    try:
        engine = create_engine(target_url)
        session_factory = sessionmaker(bind=engine)
        session = session_factory()
    except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
        target_url = "sqlite:///./data/kbo_dev.db"
        engine = create_engine(target_url)
        session_factory = sessionmaker(bind=engine)
        session = session_factory()

    total_processed = 0
    total_cleaned = 0
    total_created = 0
    total_updated = 0

    try:
        for year in range(start_year, end_year + 1):
            date_prefix = f"{year}-%"
            games = (
                session.execute(
                    select(Game).where(Game.game_date.like(date_prefix)).order_by(Game.game_date, Game.game_id)
                )
                .scalars()
                .all()
            )

            for g in games:
                c, cr, up = _process_single_game(g, year, session, apply_changes=apply_changes)
                total_cleaned += c
                total_created += cr
                total_updated += up

            total_processed += len(games)
            print(f"Season {year}: processed {len(games)} games (metadata sync planned)")

        if apply_changes:
            session.commit()
            print(
                f"\nSUCCESS: Backfilled {total_processed} games "
                f"(cleaned stadiums: {total_cleaned}, "
                f"metadata created: {total_created}, "
                f"metadata updated: {total_updated}) to {target_url}"
            )
        else:
            print(
                f"\nDRY-RUN: Would backfill {total_processed} games "
                f"(cleaned stadiums: {total_cleaned}, "
                f"metadata created: {total_created}, "
                f"metadata updated: {total_updated}). No DB changes made."
            )

        return total_processed
    except (SQLAlchemyError, RuntimeError, OSError, ValueError, TypeError):
        session.rollback()
        logger.exception("Error during stadium backfill")
        raise
    finally:
        session.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill stadium_code and game_metadata for historical seasons.")
    parser.add_argument("--year", type=int, default=None, help="Single season year")
    parser.add_argument("--start-year", type=int, default=1982, help="Start season year")
    parser.add_argument("--end-year", type=int, default=2000, help="End season year")
    parser.add_argument("--all", action="store_true", default=False, help="Process all 1982-2000 seasons")
    parser.add_argument("--db-url", type=str, default=None, help="Database connection URL")
    parser.add_argument("--apply", action="store_true", default=False, help="Apply changes to database")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Preview changes without writing")

    args = parser.parse_args(argv)

    if args.year:
        s_year = args.year
        e_year = args.year
    else:
        s_year = args.start_year
        e_year = args.end_year

    backfill_stadiums(
        s_year,
        e_year,
        db_url=args.db_url,
        apply_changes=args.apply and not args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
