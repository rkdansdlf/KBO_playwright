"""Data integrity checker for post-crawl validation.

Run after the main daily update pipeline to verify that the collected data
meets quality standards. Checks for:
- Game rows exist for the target date
- All games have terminal status (COMPLETED, DRAW, CANCELLED, POSTPONED)
- Batting and pitching stats exist for completed games
- No unresolved player_id gaps in critical tables

Exits with code 0 on success, code 1 on failure (to fail CI pipeline).

"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.sabermetrics_calculator import SabermetricsCalculator

if TYPE_CHECKING:
    from collections.abc import Sequence

from sqlalchemy import func

from src.constants import DATE_STR_LEN, KST
from src.db.engine import SessionLocal
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import (
    GAME_STATUS_UNRESOLVED,
    is_terminal_status,
)
from src.utils.team_codes import normalize_kbo_game_id

FUTURES_BATTING_TOLERANCE = 0.005
FUTURES_PITCHING_TOLERANCE = 0.01
FUTURES_FIP_TOLERANCE = 0.02


if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

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
    """Parse YYYYMMDD string to date.

    Args:
        date_str: Date Str.

    """
    try:
        return parse_date_str(date_str)
    except ValueError:
        msg = f"Invalid date format: {date_str}. Expected YYYYMMDD."
        raise ValueError(msg) from None


def check_games_exist(session: Session, target: date) -> CheckResult:
    """Verify that game rows exist for the target date.

    Args:
        session: Session.
        target: Target.

    """
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
    """Verify all games for target date have terminal status.

    Args:
        session: Session.
        target: Target.

    """
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
        if not is_terminal_status(status) and status != GAME_STATUS_UNRESOLVED:  # type: ignore[arg-type]
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
    """Verify that completed games have batting and pitching stats.

    Args:
        session: Session.
        target: Target.

    """
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
    """Check for NULL player_ids in critical tables for target date games.

    Args:
        session: Session.
        target: Target.

    """
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
                model_class.game_id.in_(game_ids),  # type: ignore[attr-defined]
                model_class.player_id.is_(None),  # type: ignore[attr-defined]
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
    """Verify all game rows have a non-NULL game_status.

    Args:
        session: Session.
        target: Target.

    """
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
    """Verify completed games have scores populated.

    Args:
        session: Session.
        target: Target.

    """
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
    """Verify winning_team matches home_score vs away_score.

    Args:
        session: Session.
        target: Target.

    """
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
    """Detect duplicate canonical game slots without flagging doubleheaders.

    Args:
        session: Session.
        target: Target.

    """
    from src.models.game import Game

    games = session.query(Game).filter(Game.game_date == target).all()
    games_by_slot: dict[str, list[str]] = {}
    for game in games:
        canonical_slot = normalize_kbo_game_id(game.game_id)
        games_by_slot.setdefault(canonical_slot, []).append(game.game_id)
    duplicate_slots = {slot: game_ids for slot, game_ids in games_by_slot.items() if len(game_ids) > 1}

    if duplicate_slots:
        details = [
            {
                "canonical_slot": slot,
                "game_ids": game_ids,
                "count": len(game_ids),
            }
            for slot, game_ids in sorted(duplicate_slots.items())
        ]
        return CheckResult(
            name="duplicate_games",
            passed=False,
            message=f"{len(duplicate_slots)} duplicate canonical game slot(s) found",
            details={"duplicates": details},
        )

    return CheckResult(
        name="duplicate_games",
        passed=True,
        message="No duplicate games found",
        details={"total": 0},
    )


def _check_futures_batting_row(session: Session, bat: PlayerSeasonBatting) -> list[str]:
    pid = bat.player_id
    pa = bat.plate_appearances or 0
    ab = bat.at_bats or 0
    hits = bat.hits or 0
    doubles = bat.doubles or 0
    triples = bat.triples or 0
    hr = bat.home_runs or 0
    so = bat.strikeouts or 0
    walks = bat.walks or 0
    hbp = bat.hbp or 0
    sf = bat.sacrifice_flies or 0
    avg = bat.avg
    obp = bat.obp
    slg = bat.slg

    errors = []
    if ab > pa or hits > ab or doubles + triples + hr > hits or so > pa or walks > pa:
        msg = (
            f"Player {pid} Batting: Impossible stats (PA={pa}, AB={ab}, H={hits}, "
            f"2B+3B+HR={doubles + triples + hr}, SO={so}, BB={walks})"
        )
        errors.append(msg)

    if ab > 0 and avg is not None:
        expected_avg = round(hits / ab, 3)
        if abs(avg - expected_avg) > FUTURES_BATTING_TOLERANCE:
            errors.append(f"Player {pid} Batting: AVG mismatch: recorded={avg}, expected={expected_avg}")

    obp_denom = ab + walks + hbp + sf
    if obp_denom > 0 and obp is not None:
        expected_obp = round((hits + walks + hbp) / obp_denom, 3)
        if abs(obp - expected_obp) > FUTURES_BATTING_TOLERANCE:
            errors.append(f"Player {pid} Batting: OBP mismatch: recorded={obp}, expected={expected_obp}")

    if ab > 0 and slg is not None:
        singles = hits - doubles - triples - hr
        tb = singles + 2 * doubles + 3 * triples + 4 * hr
        expected_slg = round(tb / ab, 3)
        if abs(slg - expected_slg) > FUTURES_BATTING_TOLERANCE:
            errors.append(f"Player {pid} Batting: SLG mismatch: recorded={slg}, expected={expected_slg}")

    if bat.extra_stats and "woba" in bat.extra_stats:
        with contextlib.suppress(SQLAlchemyError, ValueError):
            lg_constants = SabermetricsCalculator.get_league_constants(session, bat.season, level="KBO2")
            metrics = SabermetricsCalculator.calculate_batting_metrics(bat, lg_constants)
            expected_woba = metrics["woba"]
            woba = bat.extra_stats["woba"]
            if abs(woba - expected_woba) > FUTURES_BATTING_TOLERANCE:
                errors.append(f"Player {pid} Batting: wOBA mismatch: recorded={woba}, expected={expected_woba}")

    return errors


def _check_futures_pitching_row(session: Session, pit: PlayerSeasonPitching) -> list[str]:
    pid = pit.player_id
    games = pit.games or 0
    wins = pit.wins or 0
    losses = pit.losses or 0
    saves = pit.saves or 0
    holds = pit.holds or 0
    outs = pit.innings_outs or 0
    er = pit.earned_runs or 0
    r_allowed = pit.runs_allowed or 0
    hits_allowed = pit.hits_allowed or 0
    walks_allowed = pit.walks_allowed or 0
    so = pit.strikeouts or 0
    era = pit.era
    whip = pit.whip

    errors = []
    if er > r_allowed or wins + losses + saves + holds > games or outs < 0 or walks_allowed < 0 or so < 0:
        msg = (
            f"Player {pid} Pitching: Impossible stats (ER={er}, R={r_allowed}, "
            f"W+L+S+H={wins + losses + saves + holds}, IP_outs={outs}, BB={walks_allowed}, SO={so})"
        )
        errors.append(msg)

    if outs > 0 and era is not None:
        expected_era = round((er * 27) / outs, 2)
        if abs(era - expected_era) > FUTURES_PITCHING_TOLERANCE:
            errors.append(f"Player {pid} Pitching: ERA mismatch: recorded={era}, expected={expected_era}")

    if outs > 0 and whip is not None:
        expected_whip = round(((walks_allowed + hits_allowed) * 3) / outs, 2)
        if abs(whip - expected_whip) > FUTURES_PITCHING_TOLERANCE:
            errors.append(f"Player {pid} Pitching: WHIP mismatch: recorded={whip}, expected={expected_whip}")

    if pit.fip is not None:
        with contextlib.suppress(SQLAlchemyError, ValueError):
            lg_constants = SabermetricsCalculator.get_league_constants(session, pit.season, level="KBO2")
            metrics = SabermetricsCalculator.calculate_pitching_metrics(pit, lg_constants)
            expected_fip = metrics["fip_adj"]
            if abs(pit.fip - expected_fip) > FUTURES_FIP_TOLERANCE:
                errors.append(f"Player {pid} Pitching: FIP mismatch: recorded={pit.fip}, expected={expected_fip}")

    return errors


def check_futures_daily_integrity(session: Session, target: date) -> CheckResult:
    """Check that Futures batting/pitching stats updated on target date are consistent.

    Args:
        session: Session.
        target: Target.

    """
    from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

    batting_records = (
        session.query(PlayerSeasonBatting)
        .filter(
            PlayerSeasonBatting.league == "FUTURES",
            func.date(PlayerSeasonBatting.updated_at) == target,
        )
        .all()
    )

    pitching_records = (
        session.query(PlayerSeasonPitching)
        .filter(
            PlayerSeasonPitching.league == "FUTURES",
            func.date(PlayerSeasonPitching.updated_at) == target,
        )
        .all()
    )

    if not batting_records and not pitching_records:
        return CheckResult(
            name="futures_daily_integrity",
            passed=True,
            message=f"No Futures records updated on {target.isoformat()}",
            details={"checked_batting": 0, "checked_pitching": 0},
        )

    errors = []

    for bat in batting_records:
        errors.extend(_check_futures_batting_row(session, bat))

    for pit in pitching_records:
        errors.extend(_check_futures_pitching_row(session, pit))

    if errors:
        return CheckResult(
            name="futures_daily_integrity",
            passed=False,
            message=f"Found {len(errors)} Futures integrity issues",
            details={
                "checked_batting": len(batting_records),
                "checked_pitching": len(pitching_records),
                "errors": errors[:5],
            },
        )

    return CheckResult(
        name="futures_daily_integrity",
        passed=True,
        message=f"Verified {len(batting_records)} batting and {len(pitching_records)} pitching Futures records",
        details={
            "checked_batting": len(batting_records),
            "checked_pitching": len(pitching_records),
        },
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
    check_futures_daily_integrity,
]


def check_season_stat_team_code(session: Session) -> CheckResult:
    """Verify team_code is populated in player_season_batting/pitching."""
    from sqlalchemy import text

    batting_null = session.execute(
        text("SELECT COUNT(*) FROM player_season_batting WHERE team_code IS NULL"),
    ).scalar()
    pitching_null = session.execute(
        text("SELECT COUNT(*) FROM player_season_pitching WHERE team_code IS NULL OR team_code = ''"),
    ).scalar()
    total_null = batting_null + pitching_null  # type: ignore[operator]
    passed = total_null == 0
    return CheckResult(
        name="season_stat_team_code",
        passed=passed,
        message="All season stats have team_code" if passed else f"{total_null} NULL team_code rows",
        details={
            "batting_null": batting_null,
            "pitching_null": pitching_null,
        },
    )


def run_integrity_checks(target_date: str) -> IntegrityReport:
    """Run all integrity checks for the given target date.

    Args:
        target_date: Target date for the operation.

    """
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

    try:
        season_result = check_season_stat_team_code(session)
        results.append(season_result)
        status_icon = "✅" if season_result.passed else "❌"
        logger.info(
            "%s [%s] %s",
            status_icon,
            season_result.name,
            season_result.message,
        )
    except Exception as e:
        logger.exception("[ERROR] Check %s failed with exception", "season_stat_team_code")
        results.append(
            CheckResult(
                name="season_stat_team_code",
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
    """Build arg parser.

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
    """Run the main entry point for this CLI command.

    Args:
        argv: Argv.

    """
    parser = build_arg_parser()

    args = parser.parse_args(argv)

    target_date = args.date
    if len(target_date) != DATE_STR_LEN or not target_date.isdigit():
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
        sys.stdout.write(json.dumps(output, ensure_ascii=False, indent=2) + "\n")

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
