"""Data integrity checker for post-crawl validation.

Runs after the main daily update pipeline to verify that the collected data
meets quality standards. Checks for:
- Game rows exist for the target date
- All games have terminal status (COMPLETED, DRAW, CANCELLED, POSTPONED)
- Batting and pitching stats exist for completed games
- No unresolved player_id gaps in critical tables

Exits with code 0 on success, code 1 on failure (to fail CI pipeline).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Sequence

from sqlalchemy import func

from src.constants import KST
from src.db.engine import SessionLocal
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import (
    GAME_STATUS_UNRESOLVED,
    is_terminal_status,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PLAYER_ID_TABLES = [
    ("game_batting_stats", "player_id"),
    ("game_pitching_stats", "player_id"),
    ("game_lineups", "player_id"),
]


@dataclass
class CheckResult:
    """CheckResult class."""

    name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class IntegrityReport:
    """IntegrityReport class."""

    target_date: str
    timestamp_kst: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    results: list[CheckResult]
    overall_passed: bool


def _parse_target_date(date_str: str) -> date:
    """Parse YYYYMMDD string to date."""
    try:
        return parse_date_str(date_str)
    except ValueError:
        msg = f"Invalid date format: {date_str}. Expected YYYYMMDD."
        raise ValueError(msg) from None


def check_games_exist(session: Session, target: date) -> CheckResult:
    """Verify that game rows exist for the target date."""
    from src.models.game import Game

    count = session.query(Game).filter(Game.game_date == target).count()
    if count == 0:
        return CheckResult(
            name="games_exist",
            passed=False,
            message=f"No game rows found for {target.isoformat()}",
            details={"count": 0},
        )
    return CheckResult(
        name="games_exist",
        passed=True,
        message=f"Found {count} game(s) for {target.isoformat()}",
        details={"count": count},
    )


def check_all_terminal_status(session: Session, target: date) -> CheckResult:
    """Verify all games for target date have terminal status."""
    from src.models.game import Game

    games = session.query(Game).filter(Game.game_date == target).all()
    if not games:
        return CheckResult(
            name="all_terminal_status",
            passed=True,
            message="No games to check (vacuously true)",
            details={"non_terminal": []},
        )

    non_terminal = []
    for game in games:
        status = game.game_status
        if not is_terminal_status(status) and status != GAME_STATUS_UNRESOLVED:
            non_terminal.append(
                {
                    "game_id": game.game_id,
                    "status": status,
                    "home_team": game.home_team,
                    "away_team": game.away_team,
                },
            )

    if non_terminal:
        return CheckResult(
            name="all_terminal_status",
            passed=False,
            message=f"{len(non_terminal)} game(s) have non-terminal status",
            details={"non_terminal": non_terminal},
        )

    return CheckResult(
        name="all_terminal_status",
        passed=True,
        message=f"All {len(games)} game(s) have terminal status",
        details={"total": len(games)},
    )


def check_child_stats_exist(session: Session, target: date) -> CheckResult:
    """Verify that completed games have batting and pitching stats."""
    from src.models.game import Game, GameBattingStat, GamePitchingStat

    completed_games = (
        session.query(Game)
        .filter(
            Game.game_date == target,
            Game.game_status.in_(["COMPLETED", "DRAW"]),
        )
        .all()
    )

    if not completed_games:
        return CheckResult(
            name="child_stats_exist",
            passed=True,
            message="No completed games to check for child stats",
            details={"completed_games": 0},
        )

    game_ids = [g.game_id for g in completed_games]
    batting_count = (
        session.query(func.count(func.distinct(GameBattingStat.game_id)))
        .filter(GameBattingStat.game_id.in_(game_ids))
        .scalar()
        or 0
    )
    pitching_count = (
        session.query(func.count(func.distinct(GamePitchingStat.game_id)))
        .filter(GamePitchingStat.game_id.in_(game_ids))
        .scalar()
        or 0
    )

    missing_batting = len(game_ids) - batting_count
    missing_pitching = len(game_ids) - pitching_count

    passed = missing_batting == 0 and missing_pitching == 0
    details = {
        "completed_games": len(game_ids),
        "games_with_batting": batting_count,
        "games_with_pitching": pitching_count,
        "missing_batting": missing_batting,
        "missing_pitching": missing_pitching,
    }

    if passed:
        return CheckResult(
            name="child_stats_exist",
            passed=True,
            message=f"All {len(game_ids)} completed games have batting and pitching stats",
            details=details,
        )

    return CheckResult(
        name="child_stats_exist",
        passed=False,
        message=f"Missing stats: {missing_batting} batting, {missing_pitching} pitching",
        details=details,
    )


def check_no_null_player_ids(session: Session, target: date) -> CheckResult:
    """Check for NULL player_ids in critical tables for target date games."""
    from src.models.game import Game

    games = session.query(Game).filter(Game.game_date == target).all()
    if not games:
        return CheckResult(
            name="no_null_player_ids",
            passed=True,
            message="No games to check",
            details={"null_counts": {}},
        )

    game_ids = [g.game_id for g in games]
    null_counts: dict[str, int] = {}

    from src.models.game import GameBattingStat, GameLineup, GamePitchingStat

    table_map = {
        "game_batting_stats": GameBattingStat,
        "game_pitching_stats": GamePitchingStat,
        "game_lineups": GameLineup,
    }

    for table_name, model_class in table_map.items():
        null_count = (
            session.query(func.count())
            .filter(
                model_class.game_id.in_(game_ids),
                model_class.player_id.is_(None),
            )
            .scalar()
            or 0
        )
        if null_count > 0:
            null_counts[table_name] = null_count

    if null_counts:
        total_nulls = sum(null_counts.values())
        return CheckResult(
            name="no_null_player_ids",
            passed=False,
            message=f"Found {total_nulls} NULL player_id rows across tables",
            details={"null_counts": null_counts},
        )

    return CheckResult(
        name="no_null_player_ids",
        passed=True,
        message="No NULL player_id rows found in critical tables",
        details={"null_counts": {}},
    )


def check_game_status_populated(session: Session, target: date) -> CheckResult:
    """Verify all game rows have a non-NULL game_status."""
    from src.models.game import Game

    total = session.query(Game).filter(Game.game_date == target).count()
    null_status = session.query(Game).filter(Game.game_date == target, Game.game_status.is_(None)).count()

    if null_status > 0:
        return CheckResult(
            name="game_status_populated",
            passed=False,
            message=f"{null_status} of {total} games have NULL game_status",
            details={"null_status": null_status, "total": total},
        )

    return CheckResult(
        name="game_status_populated",
        passed=True,
        message=f"All {total} games have game_status populated",
        details={"total": total},
    )


def check_scores_populated(session: Session, target: date) -> CheckResult:
    """Verify completed games have scores populated."""
    from src.models.game import Game

    completed = (
        session.query(Game)
        .filter(
            Game.game_date == target,
            Game.game_status.in_(["COMPLETED", "DRAW"]),
        )
        .all()
    )

    missing_scores = [
        {
            "game_id": game.game_id,
            "home_score": game.home_score,
            "away_score": game.away_score,
        }
        for game in completed
        if game.home_score is None or game.away_score is None
    ]

    if missing_scores:
        return CheckResult(
            name="scores_populated",
            passed=False,
            message=f"{len(missing_scores)} completed games missing scores",
            details={"missing_scores": missing_scores},
        )

    return CheckResult(
        name="scores_populated",
        passed=True,
        message=f"All {len(completed)} completed games have scores",
        details={"completed_games": len(completed)},
    )


def check_winning_team_consistency(session: Session, target: date) -> CheckResult:
    """Verify winning_team matches home_score vs away_score."""
    from src.models.game import Game

    games = (
        session.query(Game)
        .filter(
            Game.game_date == target,
            Game.game_status.in_(["COMPLETED", "DRAW"]),
        )
        .all()
    )

    mismatches = []
    for game in games:
        if game.home_score is None or game.away_score is None:
            continue
        if game.home_score > game.away_score:
            expected = game.home_team
        elif game.away_score > game.home_score:
            expected = game.away_team
        else:
            continue

        if game.winning_team and game.winning_team != expected:
            mismatches.append(
                {
                    "game_id": game.game_id,
                    "winning_team": game.winning_team,
                    "expected": expected,
                    "home_score": game.home_score,
                    "away_score": game.away_score,
                },
            )

    if mismatches:
        return CheckResult(
            name="winning_team_consistency",
            passed=False,
            message=f"{len(mismatches)} games have inconsistent winning_team",
            details={"mismatches": mismatches},
        )

    return CheckResult(
        name="winning_team_consistency",
        passed=True,
        message=f"All {len(games)} completed games have consistent winning_team",
        details={"total": len(games)},
    )


def check_duplicate_games(session: Session, target: date) -> CheckResult:
    """Detect duplicate games (same date + home_team + away_team)."""
    from sqlalchemy import func as sqlfunc

    from src.models.game import Game

    dupes = (
        session.query(
            Game.game_date,
            Game.home_team,
            Game.away_team,
            sqlfunc.count(Game.id).label("cnt"),
        )
        .filter(Game.game_date == target)
        .group_by(Game.game_date, Game.home_team, Game.away_team)
        .having(sqlfunc.count(Game.id) > 1)
        .all()
    )

    if dupes:
        details = [
            {
                "game_date": str(d.game_date),
                "home_team": d.home_team,
                "away_team": d.away_team,
                "count": d.cnt,
            }
            for d in dupes
        ]
        return CheckResult(
            name="duplicate_games",
            passed=False,
            message=f"{len(dupes)} duplicate game combinations found",
            details={"duplicates": details},
        )

    return CheckResult(
        name="duplicate_games",
        passed=True,
        message="No duplicate games found",
        details={"total": 0},
    )


CHECKS = [
    check_games_exist,
    check_game_status_populated,
    check_all_terminal_status,
    check_scores_populated,
    check_child_stats_exist,
    check_no_null_player_ids,
    check_winning_team_consistency,
    check_duplicate_games,
]


def run_integrity_checks(target_date: str) -> IntegrityReport:
    """Run all integrity checks for the given target date."""
    target = _parse_target_date(target_date)
    results: list[CheckResult] = []

    with SessionLocal() as session:
        for check_fn in CHECKS:
            try:
                result = check_fn(session, target)
                results.append(result)
                status_icon = "✅" if result.passed else "❌"
                logger.info(
                    "%s [%s] %s",
                    status_icon,
                    result.name,
                    result.message,
                )
            except Exception as e:
                logger.exception("[ERROR] Check %s failed with exception", check_fn.__name__)
                results.append(
                    CheckResult(
                        name=check_fn.__name__,
                        passed=False,
                        message=f"Exception: {e}",
                    ),
                )

    passed_count = sum(1 for r in results if r.passed)
    failed_count = sum(1 for r in results if not r.passed)

    return IntegrityReport(
        target_date=target_date,
        timestamp_kst=datetime.now(KST).isoformat(),
        total_checks=len(results),
        passed_checks=passed_count,
        failed_checks=failed_count,
        results=results,
        overall_passed=failed_count == 0,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Builds arg parser.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(
        description="Post-crawl data integrity checker",
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Target date in YYYYMMDD format",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output report as JSON",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Main entry point for this CLI command."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    target_date = args.date
    if len(target_date) != 8 or not target_date.isdigit():
        logger.error("Invalid date format: %s. Expected YYYYMMDD.", target_date)
        sys.exit(1)

    report = run_integrity_checks(target_date)

    if args.json:
        output = {
            "target_date": report.target_date,
            "timestamp_kst": report.timestamp_kst,
            "total_checks": report.total_checks,
            "passed_checks": report.passed_checks,
            "failed_checks": report.failed_checks,
            "overall_passed": report.overall_passed,
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "message": r.message,
                    "details": r.details,
                }
                for r in report.results
            ],
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))

    if not report.overall_passed:
        logger.error(
            "❌ INTEGRITY CHECK FAILED: %d of %d checks failed",
            report.failed_checks,
            report.total_checks,
        )
        for r in report.results:
            if not r.passed:
                logger.error("  ❌ %s: %s", r.name, r.message)
        sys.exit(1)

    logger.info(
        "✅ INTEGRITY CHECK PASSED: %d of %d checks passed",
        report.passed_checks,
        report.total_checks,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
