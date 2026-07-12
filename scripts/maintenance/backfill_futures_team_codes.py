"""One-time maintenance script to backfill missing team_code in player_season tables."""

from __future__ import annotations

import logging
import re
from sqlalchemy import text
from src.db.engine import SessionLocal
from src.parsers.player_profile_parser import TEAM_CODE_MAP

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Normalize TEAM_CODE_MAP keys and values
NORM_TEAM_MAP = {k.strip(): v for k, v in TEAM_CODE_MAP.items()}
# Add reverse mapping and custom mapping
REVERSE_TEAM_MAP = {v: v for v in TEAM_CODE_MAP.values()}
FULL_TEAM_MAP = {**NORM_TEAM_MAP, **REVERSE_TEAM_MAP}
# Historical and additional names
FULL_TEAM_MAP.update(
    {
        "두산": "DB",
        "삼성": "SS",
        "롯데": "LT",
        "한화": "HH",
        "키움": "KH",
        "넥센": "NX",
        "우리": "WO",
        "현대": "HU",
        "쌍방울": "SL",
        "태평양": "TP",
        "해태": "HT",
        "삼미": "SM",
        "청보": "CB",
        "MBC": "MBC",
        "빙그레": "BE",
        "고양": "OT",
        "상무": "OT",
        "경찰": "OT",
        "경찰청": "OT",
        "울산": "OT",
    }
)


def parse_career_team(career: str, year: int) -> str | None:
    """Extract team code from career path text for a specific year.
    Example career string: "삼성(2010~2015)-한화(2016~2019)-롯데(2020-)"
    """
    if not career:
        return None

    # Split by hyphen or arrow
    parts = re.split(r"\s*[-\u2013\u2014\u2192>]\s*", career)
    for part in parts:
        # Match pattern: Team(StartYear~EndYear) or Team(Year)
        match = re.search(r"([^\(]+)\s*\(([^)]+)\)", part)
        if match:
            team_name = match.group(1).strip()
            period = match.group(2).strip()

            # Parse period
            start_yr, end_yr = None, None
            if "~" in period or "-" in period:
                years = re.split(r"[~-]", period)
                if len(years) >= 1 and years[0].isdigit():
                    start_yr = int(years[0])
                if len(years) >= 2 and years[1].isdigit():
                    end_yr = int(years[1])
                else:
                    # e.g., "2020-"
                    end_yr = 9999
            elif period.isdigit():
                start_yr = int(period)
                end_yr = start_yr

            if start_yr and end_yr:
                if start_yr <= year <= end_yr:
                    # Find team code
                    code = FULL_TEAM_MAP.get(team_name)
                    if not code:
                        for k, v in FULL_TEAM_MAP.items():
                            if k in team_name or team_name in k:
                                code = v
                                break
                    return code
    return None


def backfill_batting_team_codes() -> None:
    logger.info("Starting player_season_batting team_code backfill...")
    session = SessionLocal()
    try:
        # Fetch rows with NULL team_code
        rows = session.execute(
            text("SELECT id, player_id, season FROM player_season_batting WHERE team_code IS NULL")
        ).fetchall()

        logger.info("Found %s player_season_batting rows with NULL team_code", len(rows))
        updated_count = 0

        for row in rows:
            row_id, player_id, season = row
            resolved_code = None

            # Step 1: Look up in player_game_batting for the same season
            game_row = session.execute(
                text(
                    "SELECT team_code FROM player_game_batting WHERE player_id = :pid AND game_id LIKE :pattern LIMIT 1"
                ),
                {"pid": player_id, "pattern": f"{season}%"},
            ).fetchone()
            if game_row and game_row[0]:
                resolved_code = game_row[0]
                logger.debug(
                    "Resolved via player_game_batting for player %s in %s: %s", player_id, season, resolved_code
                )

            # Step 2: Look up in player_season_pitching
            if not resolved_code:
                pitch_row = session.execute(
                    text(
                        "SELECT team_code FROM player_season_pitching WHERE player_id = :pid AND season = :season AND team_code IS NOT NULL LIMIT 1"
                    ),
                    {"pid": player_id, "season": season},
                ).fetchone()
                if pitch_row and pitch_row[0]:
                    resolved_code = pitch_row[0]
                    logger.debug(
                        "Resolved via player_season_pitching for player %s in %s: %s", player_id, season, resolved_code
                    )

            # Step 3: Look up in player_basic current team
            if not resolved_code:
                basic_row = session.execute(
                    text("SELECT team, career FROM player_basic WHERE player_id = :pid"), {"pid": player_id}
                ).fetchone()
                if basic_row:
                    curr_team, career = basic_row
                    # If current team matches and it's a recent season, we might guess it
                    if curr_team:
                        resolved_code = FULL_TEAM_MAP.get(curr_team.strip())
                        if not resolved_code:
                            for k, v in FULL_TEAM_MAP.items():
                                if k in curr_team or curr_team in k:
                                    resolved_code = v
                                    break

                    # Step 4: Parse career text for that season year
                    if not resolved_code and career:
                        resolved_code = parse_career_team(career, season)
                        if not resolved_code:
                            # Fallback: check last part of career path
                            parts = [p.strip() for p in re.split(r"[-\u2013\u2014\u2192>,]", career) if p.strip()]
                            if parts:
                                last_part = parts[-1]
                                resolved_code = FULL_TEAM_MAP.get(last_part)
                                if not resolved_code:
                                    for k, v in FULL_TEAM_MAP.items():
                                        if k in last_part or last_part in k:
                                            resolved_code = v
                                            break

            # Step 5: Look up closest other season with team_code for this player
            if not resolved_code:
                other_season_row = session.execute(
                    text(
                        "SELECT team_code, ABS(season - :season) as diff FROM player_season_batting "
                        "WHERE player_id = :pid AND team_code IS NOT NULL "
                        "ORDER BY diff ASC LIMIT 1"
                    ),
                    {"pid": player_id, "season": season},
                ).fetchone()
                if other_season_row and other_season_row[0]:
                    resolved_code = other_season_row[0]
                    logger.debug(
                        "Resolved via nearest season for player %s in %s: %s", player_id, season, resolved_code
                    )

            if resolved_code:
                # Update team_code and canonical_team_code
                session.execute(
                    text(
                        "UPDATE player_season_batting "
                        "SET team_code = :code, canonical_team_code = :code, updated_at = CURRENT_TIMESTAMP "
                        "WHERE id = :id"
                    ),
                    {"code": resolved_code, "id": row_id},
                )
                updated_count += 1

        session.commit()
        logger.info("Successfully updated %s/%s player_season_batting records.", updated_count, len(rows))

    except Exception:
        session.rollback()
        logger.exception("Failed to backfill batting team codes")
        raise
    finally:
        session.close()


def backfill_pitching_team_codes() -> None:
    logger.info("Starting player_season_pitching team_code backfill...")
    session = SessionLocal()
    try:
        rows = session.execute(
            text("SELECT id, player_id, season FROM player_season_pitching WHERE team_code IS NULL")
        ).fetchall()
        logger.info("Found %s player_season_pitching rows with NULL team_code", len(rows))
        updated_count = 0
        for row in rows:
            row_id, player_id, season = row
            resolved_code = None

            # Look up in player_game_pitching
            game_row = session.execute(
                text(
                    "SELECT team_code FROM player_game_pitching WHERE player_id = :pid AND game_id LIKE :pattern LIMIT 1"
                ),
                {"pid": player_id, "pattern": f"{season}%"},
            ).fetchone()
            if game_row and game_row[0]:
                resolved_code = game_row[0]

            # Look up in player_season_batting
            if not resolved_code:
                bat_row = session.execute(
                    text(
                        "SELECT team_code FROM player_season_batting WHERE player_id = :pid AND season = :season AND team_code IS NOT NULL LIMIT 1"
                    ),
                    {"pid": player_id, "season": season},
                ).fetchone()
                if bat_row and bat_row[0]:
                    resolved_code = bat_row[0]

            # Look up in basic
            if not resolved_code:
                basic_row = session.execute(
                    text("SELECT team, career FROM player_basic WHERE player_id = :pid"), {"pid": player_id}
                ).fetchone()
                if basic_row:
                    curr_team, career = basic_row
                    if curr_team:
                        resolved_code = FULL_TEAM_MAP.get(curr_team.strip())
                    if not resolved_code and career:
                        resolved_code = parse_career_team(career, season)
                        if not resolved_code:
                            parts = [p.strip() for p in re.split(r"[-\u2013\u2014\u2192>,]", career) if p.strip()]
                            if parts:
                                last_part = parts[-1]
                                resolved_code = FULL_TEAM_MAP.get(last_part)
                                if not resolved_code:
                                    for k, v in FULL_TEAM_MAP.items():
                                        if k in last_part or last_part in k:
                                            resolved_code = v
                                            break

            if resolved_code:
                session.execute(
                    text(
                        "UPDATE player_season_pitching "
                        "SET team_code = :code, canonical_team_code = :code, updated_at = CURRENT_TIMESTAMP "
                        "WHERE id = :id"
                    ),
                    {"code": resolved_code, "id": row_id},
                )
                updated_count += 1

        session.commit()
        logger.info("Successfully updated %s/%s player_season_pitching records.", updated_count, len(rows))
    except Exception:
        session.rollback()
        logger.exception("Failed to backfill pitching team codes")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    backfill_batting_team_codes()
    backfill_pitching_team_codes()
