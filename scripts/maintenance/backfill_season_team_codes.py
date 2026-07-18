"""Backfill missing team_code in player_season batting/pitching tables.

Resolves NULL ``team_code`` rows using a conservative, evidence-based cascade:
``player_game_batting`` / ``player_game_pitching`` (same season) ->
``team_daily_roster`` (same year) -> ``player_basic.career`` text.

Only rows with a single, unambiguous team code are resolved; ambiguous or
evidence-less rows are skipped (never invented). Mirrors the resolver
structure of ``backfill_futures_team_codes.py`` but targets the regular
``player_season_*`` tables rather than the Futures/KBO2 scope.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.parsers.player_profile_parser import TEAM_CODE_MAP

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Normalize TEAM_CODE_MAP keys and values
NORM_TEAM_MAP = {k.strip(): v for k, v in TEAM_CODE_MAP.items()}
REVERSE_TEAM_MAP = {v: v for v in TEAM_CODE_MAP.values()}
FULL_TEAM_MAP = {**NORM_TEAM_MAP, **REVERSE_TEAM_MAP}
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
        "화성": "OT",
    }
)


@dataclass(frozen=True, slots=True)
class TeamCodeResolution:
    """Represent the evidence used to resolve a season team code."""

    code: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class BackfillReport:
    """Summarize one season team-code backfill pass."""

    table_name: str
    total: int
    resolved: int
    applied: int
    dry_run: bool
    reason_counts: dict[str, int]


TeamCodeResolver = Callable[[Session, int, int], TeamCodeResolution]

BATTING_MISSING_QUERY = "SELECT id, player_id, season FROM player_season_batting WHERE team_code IS NULL"
PITCHING_MISSING_QUERY = "SELECT id, player_id, season FROM player_season_pitching WHERE team_code IS NULL"
BATTING_UPDATE_QUERY = (
    "UPDATE player_season_batting "
    "SET team_code = :code, canonical_team_code = :code, updated_at = CURRENT_TIMESTAMP "
    "WHERE id = :id"
)
PITCHING_UPDATE_QUERY = (
    "UPDATE player_season_pitching "
    "SET team_code = :code, canonical_team_code = :code, updated_at = CURRENT_TIMESTAMP "
    "WHERE id = :id"
)


def _team_code_for_name(team_name: str | None) -> str | None:
    if not team_name or not (normalized_name := team_name.strip()):
        return None
    if code := FULL_TEAM_MAP.get(normalized_name):
        return code
    return next(
        (code for name, code in FULL_TEAM_MAP.items() if name in normalized_name or normalized_name in name),
        None,
    )


def _parse_career_period(period: str) -> tuple[int, int] | None:
    if period.isdigit():
        year = int(period)
        return year, year
    if "~" not in period and "-" not in period:
        return None
    years = re.split(r"[~-]", period)
    if not years[0].isdigit():
        return None
    start_year = int(years[0])
    end_year = int(years[1]) if len(years) > 1 and years[1].isdigit() else 9999
    return start_year, end_year


def _career_team_parts(career: str) -> list[tuple[str, tuple[int, int]]]:
    team_periods: list[tuple[str, tuple[int, int]]] = []
    for part in re.split(r"\s*(?:[\u2013\u2014\u2192>]|-\s*(?=[가-힣A-Za-z]))\s*", career):
        if match := re.search(r"([^\(]+)\s*\(([^)]+)\)", part):
            period = _parse_career_period(match.group(2).strip())
            if period is not None:
                team_periods.append((match.group(1).strip(), period))
    return team_periods


def _career_codes_for_year(career: str, year: int) -> set[str]:
    return {
        code
        for team_name, (start_year, end_year) in _career_team_parts(career)
        if start_year <= year <= end_year and (code := _team_code_for_name(team_name))
    }


def parse_career_team(career: str, year: int) -> str | None:
    """Extract a team code for a season from a player's career path."""
    codes = _career_codes_for_year(career, year)
    return codes.pop() if len(codes) == 1 else None


def _lookup_team_codes(session: Session, query: str, params: dict[str, int | str]) -> set[str]:
    rows = session.execute(text(query), params).fetchall()
    return {str(row[0]).strip() for row in rows if row and row[0]}


def _resolve_from_player_career(session: Session, player_id: int, season: int) -> TeamCodeResolution:
    basic_row = session.execute(
        text("SELECT career FROM player_basic WHERE player_id = :pid"),
        {"pid": player_id},
    ).fetchone()
    if not basic_row:
        return TeamCodeResolution(None, "missing_player_profile")
    career = basic_row[0]
    if not career:
        return TeamCodeResolution(None, "missing_career_evidence")
    codes = _career_codes_for_year(str(career), season)
    if len(codes) == 1:
        return TeamCodeResolution(codes.pop(), "career_period")
    reason = "ambiguous_career_period" if len(codes) > 1 else "no_matching_career_period"
    return TeamCodeResolution(None, reason)


def _resolve_unique_evidence(codes: set[str], source: str) -> TeamCodeResolution | None:
    if len(codes) == 1:
        return TeamCodeResolution(codes.pop(), source)
    if len(codes) > 1:
        return TeamCodeResolution(None, f"ambiguous_{source}")
    return None


def _resolve_batting_team_code(session: Session, player_id: int, season: int) -> TeamCodeResolution:
    game_evidence = _resolve_unique_evidence(
        _lookup_team_codes(
            session,
            "SELECT DISTINCT team_code FROM player_game_batting "
            "WHERE player_id = :pid AND game_id LIKE :pattern AND team_code IS NOT NULL",
            {"pid": player_id, "pattern": f"{season}%"},
        ),
        "same_season_game",
    )
    if game_evidence is not None:
        return game_evidence
    roster_evidence = _resolve_unique_evidence(
        _lookup_team_codes(
            session,
            "SELECT DISTINCT team_code FROM team_daily_roster "
            "WHERE player_id = :pid AND strftime('%Y', roster_date) = :yr AND team_code IS NOT NULL",
            {"pid": player_id, "yr": str(season)},
        ),
        "same_season_roster",
    )
    return roster_evidence or _resolve_from_player_career(session, player_id, season)


def _resolve_pitching_team_code(session: Session, player_id: int, season: int) -> TeamCodeResolution:
    game_evidence = _resolve_unique_evidence(
        _lookup_team_codes(
            session,
            "SELECT DISTINCT team_code FROM player_game_pitching "
            "WHERE player_id = :pid AND game_id LIKE :pattern AND team_code IS NOT NULL",
            {"pid": player_id, "pattern": f"{season}%"},
        ),
        "same_season_game",
    )
    if game_evidence is not None:
        return game_evidence
    roster_evidence = _resolve_unique_evidence(
        _lookup_team_codes(
            session,
            "SELECT DISTINCT team_code FROM team_daily_roster "
            "WHERE player_id = :pid AND strftime('%Y', roster_date) = :yr AND team_code IS NOT NULL",
            {"pid": player_id, "yr": str(season)},
        ),
        "same_season_roster",
    )
    return roster_evidence or _resolve_from_player_career(session, player_id, season)


def _run_backfill(
    *,
    table_name: str,
    missing_query: str,
    update_query: str,
    resolver: TeamCodeResolver,
    apply: bool = False,
    year: int | None = None,
) -> BackfillReport:
    """Resolve season team codes and update only when explicitly requested."""
    logger.info("Starting %s season team_code backfill (apply=%s, year=%s)...", table_name, apply, year)
    session = SessionLocal()
    try:
        rows = session.execute(text(missing_query)).fetchall()
        if year is not None:
            rows = [r for r in rows if r[2] == year]
        logger.info("Found %s %s rows with NULL team_code%s", len(rows), table_name, f" for {year}" if year else "")
        reason_counts: Counter[str] = Counter()
        resolved_count = 0
        applied_count = 0
        for row_id, player_id, season in rows:
            resolution = resolver(session, player_id, season)
            reason_counts[resolution.reason] += 1
            if resolution.code is None:
                continue
            resolved_count += 1
            if apply:
                session.execute(text(update_query), {"code": resolution.code, "id": row_id})
                applied_count += 1
        if apply:
            session.commit()
        report = BackfillReport(
            table_name=table_name,
            total=len(rows),
            resolved=resolved_count,
            applied=applied_count,
            dry_run=not apply,
            reason_counts=dict(reason_counts),
        )
        logger.info(
            "%s rows: total=%s resolved=%s applied=%s reasons=%s",
            table_name,
            report.total,
            report.resolved,
            report.applied,
            report.reason_counts,
        )
        return report
    except Exception:
        session.rollback()
        logger.exception("Failed to backfill %s team codes", table_name)
        raise
    finally:
        session.close()


def backfill_batting_team_codes(*, apply: bool = False, year: int | None = None) -> BackfillReport:
    """Backfill batting rows with unambiguous season team evidence."""
    return _run_backfill(
        table_name="player_season_batting",
        missing_query=BATTING_MISSING_QUERY,
        update_query=BATTING_UPDATE_QUERY,
        resolver=_resolve_batting_team_code,
        apply=apply,
        year=year,
    )


def backfill_pitching_team_codes(*, apply: bool = False, year: int | None = None) -> BackfillReport:
    """Backfill pitching rows with unambiguous season team evidence."""
    return _run_backfill(
        table_name="player_season_pitching",
        missing_query=PITCHING_MISSING_QUERY,
        update_query=PITCHING_UPDATE_QUERY,
        resolver=_resolve_pitching_team_code,
        apply=apply,
        year=year,
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line options for the season team-code backfill."""
    parser = argparse.ArgumentParser(description="Backfill unambiguous player_season team codes.")
    parser.add_argument("--apply", action="store_true", help="Write resolved team codes. Default is dry-run.")
    parser.add_argument(
        "--table",
        choices=("all", "batting", "pitching"),
        default="all",
        help="Stat table to evaluate.",
    )
    parser.add_argument("--year", type=int, default=None, help="Limit to a single season year.")
    return parser.parse_args()


def main() -> None:
    """Run the conservative season team-code backfill CLI."""
    args = parse_args()
    if args.table in {"all", "batting"}:
        backfill_batting_team_codes(apply=args.apply, year=args.year)
    if args.table in {"all", "pitching"}:
        backfill_pitching_team_codes(apply=args.apply, year=args.year)


if __name__ == "__main__":
    main()
