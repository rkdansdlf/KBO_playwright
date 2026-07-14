"""데이터 무결성 및 상태 점검 스크립트.

데이터베이스에 저장된 경기 일정, 선수, 퓨처스리그 데이터 등의 상태를 확인하고,
예상 수치와 비교하여 잠재적인 문제를 경고합니다.

"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.player import Player, PlayerSeasonBatting, PlayerSeasonPitching
from src.services.p0_readiness import (
    P0ReadinessOptions,
    build_p0_readiness,
    format_p0_readiness_summary,
    normalize_yyyymmdd,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

FALSE_ENV_VALUES = {"0", "false", "no", "off"}


def _configure_cli_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def _safe_scalar(session: Session, sql: str, default: int = 0) -> int:
    try:
        return session.execute(text(sql)).scalar() or default
    except SQLAlchemyError:
        return default


def _safe_rows(session: Session, sql: str) -> list:
    try:
        return session.execute(text(sql)).all()  # type: ignore[return-value]
    except SQLAlchemyError:
        return []


def _safe_first(session: Session, sql: str) -> tuple[Any, Any]:
    try:
        return session.execute(text(sql)).first()  # type: ignore[return-value]
    except SQLAlchemyError:
        return None, None


def _operational_game_counts(session: Session) -> tuple[int, int]:
    operational_total = _safe_scalar(session, "SELECT COUNT(*) FROM game")
    operational_scheduled = _safe_scalar(
        session,
        """
        SELECT COUNT(*)

        FROM game
        WHERE UPPER(COALESCE(game_status, '')) = 'SCHEDULED'
        """,
    )
    return operational_total, operational_scheduled


def _log_season_type_counts(session: Session) -> dict[str, int]:
    type_counts = {}
    logger.info("\nBy season type:")
    for season_type, count in _safe_rows(
        session,
        "SELECT season_type, COUNT(*) FROM game_schedules GROUP BY season_type",
    ):
        type_counts[season_type] = count
        logger.info("  %s: %s", season_type, count)
    return type_counts


def _log_schedule_year_counts(session: Session) -> None:
    logger.info("\nBy year:")
    for year, count in _safe_rows(
        session,
        "SELECT season_year, COUNT(*) FROM game_schedules GROUP BY season_year ORDER BY season_year DESC",
    ):
        logger.info("  %s: %s", year, count)


def _log_schedule_date_ranges(session: Session, *, use_operational_fallback: bool) -> None:
    min_date, max_date = _safe_first(session, "SELECT MIN(game_date), MAX(game_date) FROM game_schedules")
    if min_date and max_date:
        logger.info("\nDate range: %s to %s", min_date, max_date)
    if use_operational_fallback:
        game_min_date, game_max_date = _safe_first(session, "SELECT MIN(game_date), MAX(game_date) FROM game")
        if game_min_date and game_max_date:
            logger.info("Operational game date range: %s to %s", game_min_date, game_max_date)


def _validate_schedule_counts(total: int, operational_total: int, type_counts: dict[str, int]) -> list[str]:
    warnings: list[str] = []
    expected = {"preseason": 42, "regular": 720, "postseason": 7}
    logger.info("\nValidation:")
    if total == 0 and operational_total > 0:
        logger.info("  game_schedules: 0 rows [INFO: using game table fallback]")
        logger.info("  game table: %s rows [OK]", operational_total)
        return warnings
    for season_type, expected_count in expected.items():
        actual = type_counts.get(season_type, 0)
        status = "OK" if actual >= expected_count else "WARN"
        logger.info("  %s: %s/%s [%s]", season_type, actual, expected_count, status)
        if actual < expected_count:
            warnings.append(f"{season_type}: {actual} < {expected_count} (missing {expected_count - actual})")
    return warnings


def check_schedules(session: Session) -> dict[str, Any]:
    """`game_schedules` 테이블의 데이터 현황을 점검합니다.

    Args:
        session: Session.

    """
    logger.info("\n=== Game Schedules ===")

    total = _safe_scalar(session, "SELECT COUNT(*) FROM game_schedules")
    logger.info("Total schedules: %s", total)

    operational_total, operational_scheduled = _operational_game_counts(session)
    type_counts = _log_season_type_counts(session)
    _log_schedule_year_counts(session)

    if total == 0 and operational_total > 0:
        logger.info("\nOperational game table fallback:")
        logger.info("  Total game rows: %s", operational_total)
        logger.info("  Scheduled game rows: %s", operational_scheduled)

    _log_schedule_date_ranges(session, use_operational_fallback=total == 0 and operational_total > 0)
    warnings = _validate_schedule_counts(total, operational_total, type_counts)

    effective_total = total if total > 0 else operational_total

    return {
        "total": effective_total,
        "game_schedules_total": total,
        "operational_total": operational_total,
        "operational_scheduled": operational_scheduled,
        "source": "game_schedules" if total > 0 else "game",
        "by_type": type_counts,
        "warnings": warnings,
    }


def check_players(session: Session) -> dict[str, Any]:
    """`players` 테이블의 데이터 현황을 점검합니다.

    Args:
        session: Session.

    """
    logger.info("\n=== Players ===")

    # 전체 선수 수
    total = session.execute(select(func.count(Player.id))).scalar()
    logger.info("Total players: %s", total)

    # 선수 상태(현역, 은퇴 등)별 집계
    stmt = select(Player.status, func.count(Player.id)).group_by(Player.status)

    results = session.execute(stmt).all()
    logger.info("\nBy status:")
    for status, count in results:
        status_label = status or "(null)"
        logger.info("  %s: %s", status_label, count)

    return {"total": total}


def check_futures_data(session: Session) -> dict[str, Any]:
    """퓨처스리그 관련 데이터(타자/투수 기록) 현황을 점검합니다.

    Args:
        session: Session.

    """
    logger.info("\n=== Futures League Data ===")

    # 퓨처스리그 타자 기록 수
    batting_stmt = select(func.count(PlayerSeasonBatting.id)).where(PlayerSeasonBatting.league == "FUTURES")
    batting_count = session.execute(batting_stmt).scalar()
    logger.info("Batting records: %s", batting_count)

    # 시즌별 타자 기록 집계
    stmt = (
        select(PlayerSeasonBatting.season, func.count(PlayerSeasonBatting.id))
        .where(PlayerSeasonBatting.league == "FUTURES")
        .group_by(PlayerSeasonBatting.season)
        .order_by(PlayerSeasonBatting.season.desc())
    )

    results = session.execute(stmt).all()
    if results:
        logger.info("\nBatting by season:")
        for season, count in results:
            logger.info("  %s: %s", season, count)

    # 퓨처스리그 투수 기록 수
    pitching_stmt = select(func.count(PlayerSeasonPitching.id)).where(PlayerSeasonPitching.league == "FUTURES")
    pitching_count = session.execute(pitching_stmt).scalar()
    logger.info("\nPitching records: %s", pitching_count)

    return {"batting": batting_count, "pitching": pitching_count}


def check_game_data(session: Session) -> dict[str, Any]:
    """Check game data.

    Args:
        session: Session.
        session: Session.

    Returns:
        Dictionary result.

    """
    from src.models.game import PlayerGameBatting, PlayerGamePitching

    logger.info("\n=== Game-Level Stats ===")
    batting_count = session.execute(select(func.count(PlayerGameBatting.id))).scalar()
    logger.info("Player game batting records: %s", batting_count)
    pitching_count = session.execute(select(func.count(PlayerGamePitching.id))).scalar()
    logger.info("Player game pitching records: %s", pitching_count)

    # Duplicate check
    dup_b = session.execute(
        text(
            "SELECT COALESCE(COUNT(*), 0) FROM "
            "(SELECT game_id, player_id, COUNT(*) FROM player_game_batting "
            "GROUP BY game_id, player_id HAVING COUNT(*) > 1)",
        ),
    ).scalar()
    dup_p = session.execute(
        text(
            "SELECT COALESCE(COUNT(*), 0) FROM "
            "(SELECT game_id, player_id, COUNT(*) FROM player_game_pitching "
            "GROUP BY game_id, player_id HAVING COUNT(*) > 1)",
        ),
    ).scalar()
    if dup_b or dup_p:
        logger.info("  Duplicates: batting=%s, pitching=%s [WARN]", dup_b, dup_p)
    else:
        logger.info("  Duplicates: none [OK]")

    # NULL field check
    for tbl, label in [("player_game_batting", "batting"), ("player_game_pitching", "pitching")]:
        nid = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE player_id IS NULL")).scalar()  # noqa: S608
        nn = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE player_name IS NULL")).scalar()  # noqa: S608
        ns = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE team_side IS NULL")).scalar()  # noqa: S608
        if nid or nn or ns:
            logger.info("  %s NULLs: player_id=%s, player_name=%s, team_side=%s [WARN]", label, nid, nn, ns)
        else:
            logger.info("  %s NULLs: none [OK]", label)

    # Rate stat anomaly check (avg > obp — documented as expected with SF)
    avg_gt_obp = session.execute(
        text("SELECT COUNT(*) FROM player_game_batting WHERE avg IS NOT NULL AND obp IS NOT NULL AND avg > obp"),
    ).scalar()
    logger.info("  Batting avg > obp: %s (expected when sacrifice flies exist)", avg_gt_obp)

    # Rate stat boundary checks
    for tbl, col, lo, hi, _label in [
        ("player_game_batting", "avg", 0, 1, "avg"),
        ("player_game_batting", "obp", 0, 1, "obp"),
        ("player_game_batting", "slg", 0, 5, "slg"),
        ("player_game_pitching", "era", 0, 200, "era"),
        ("player_game_pitching", "whip", 0, 30, "whip"),
    ]:
        n = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})"),  # noqa: S608
        ).scalar()
        if n:
            logger.info("  %s.%s: %s outside [%s, %s]", tbl, col, n, lo, hi)

    # Coverage: games with PlayerGame vs total COMPLETED/DRAW
    cov = session.execute(
        text("""
        SELECT g.game_status,
               COUNT(DISTINCT g.game_id) as total,
               COUNT(DISTINCT pgb.game_id) as covered
        FROM game g
        LEFT JOIN player_game_batting pgb ON pgb.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
        GROUP BY g.game_status
    """),
    ).fetchall()
    for status, total, covered in cov:
        pct = 100.0 * covered / total if total else 0
        logger.info("  Coverage %-12s: %s/%s (%.1f%%)", status, covered, total, pct)

    # Games missing source stats (COMPLETED/DRAW with no game_batting_stats)
    missing_games = session.execute(
        text("""
        SELECT COUNT(DISTINCT g.game_id)
        FROM game g
        LEFT JOIN game_batting_stats gbs ON gbs.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          AND gbs.game_id IS NULL
    """),
    ).scalar()
    logger.info("  Games without source batting stats: %s", missing_games)

    return {"batting": batting_count, "pitching": pitching_count}


def check_pregame_pitcher_coverage(session: Session, *, verbose: bool = False) -> dict[str, Any]:
    """예정 경기 선발투수 적재율을 점검합니다.

    Args:
        session: Session.
        verbose: Verbose.

    """
    logger.info("\n=== Pregame Starting Pitchers ===")

    scheduled_filter = func.upper(Game.game_status) == "SCHEDULED"
    total = session.query(Game).filter(scheduled_filter).count()
    if total == 0:
        pregame_sync_enabled = _env_enabled("PREGAME_SYNC_TO_OCI")
        oci_url_present = bool(os.getenv("OCI_DB_URL"))
        oci_sync_ready = pregame_sync_enabled and oci_url_present

        logger.info("Scheduled games: 0")
        logger.info("  OCI sync candidates: 0")
        logger.info("  OCI sync candidates with both starters: 0")
        if oci_sync_ready:
            logger.info("  OCI sync config: ready")
        elif not pregame_sync_enabled:
            logger.info("  OCI sync config: disabled by PREGAME_SYNC_TO_OCI")
        else:
            logger.info("  OCI sync config: disabled because OCI_DB_URL is missing")

        return {
            "scheduled_total": 0,
            "away_ok": 0,
            "home_ok": 0,
            "both_ok": 0,
            "both_missing": 0,
            "preview_rows": 0,
            "preview_missing_starters": 0,
            "sync_candidate_games": 0,
            "sync_complete_starters": 0,
            "oci_sync_ready": oci_sync_ready,
            "coverage_pct": 0.0,
        }

    away_ok = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            Game.away_pitcher.is_not(None),
            Game.away_pitcher != "",
        ),
    ).scalar()
    home_ok = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            Game.home_pitcher.is_not(None),
            Game.home_pitcher != "",
        ),
    ).scalar()
    both_ok = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            Game.away_pitcher.is_not(None),
            Game.away_pitcher != "",
            Game.home_pitcher.is_not(None),
            Game.home_pitcher != "",
        ),
    ).scalar()
    both_missing = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            (Game.away_pitcher.is_(None) | (Game.away_pitcher == "")),
            (Game.home_pitcher.is_(None) | (Game.home_pitcher == "")),
        ),
    ).scalar()
    preview_rows = (
        session.execute(
            text(
                """
            SELECT COUNT(DISTINCT g.game_id)

            FROM game g
            JOIN game_summary gs ON gs.game_id = g.game_id
            WHERE UPPER(g.game_status) = 'SCHEDULED'
              AND gs.summary_type = '프리뷰'
            """,
            ),
        ).scalar()
        or 0
    )
    preview_missing_starters = (
        session.execute(
            text(
                """
            SELECT COUNT(DISTINCT g.game_id)

            FROM game g
            JOIN game_summary gs ON gs.game_id = g.game_id
            WHERE UPPER(g.game_status) = 'SCHEDULED'
              AND gs.summary_type = '프리뷰'
              AND (
                g.away_pitcher IS NULL OR g.away_pitcher = ''
                OR g.home_pitcher IS NULL OR g.home_pitcher = ''
              )
            """,
            ),
        ).scalar()
        or 0
    )
    sync_candidate_games = (
        session.execute(
            text(
                """
            SELECT COUNT(DISTINCT g.game_id)

            FROM game g
            LEFT JOIN (
                SELECT DISTINCT game_id
                FROM game_summary
                WHERE summary_type = '프리뷰'
            ) p ON p.game_id = g.game_id
            WHERE UPPER(g.game_status) = 'SCHEDULED'
              AND (
                p.game_id IS NOT NULL
                OR (g.away_pitcher IS NOT NULL AND g.away_pitcher != '')
                OR (g.home_pitcher IS NOT NULL AND g.home_pitcher != '')
              )
            """,
            ),
        ).scalar()
        or 0
    )
    sync_complete_starters = (
        session.execute(
            text(
                """
            SELECT COUNT(DISTINCT g.game_id)

            FROM game g
            LEFT JOIN (
                SELECT DISTINCT game_id
                FROM game_summary
                WHERE summary_type = '프리뷰'
            ) p ON p.game_id = g.game_id
            WHERE UPPER(g.game_status) = 'SCHEDULED'
              AND (
                p.game_id IS NOT NULL
                OR (g.away_pitcher IS NOT NULL AND g.away_pitcher != '')
                OR (g.home_pitcher IS NOT NULL AND g.home_pitcher != '')
              )
              AND g.away_pitcher IS NOT NULL AND g.away_pitcher != ''
              AND g.home_pitcher IS NOT NULL AND g.home_pitcher != ''
            """,
            ),
        ).scalar()
        or 0
    )

    pregame_sync_enabled = _env_enabled("PREGAME_SYNC_TO_OCI")
    oci_url_present = bool(os.getenv("OCI_DB_URL"))
    oci_sync_ready = pregame_sync_enabled and oci_url_present

    coverage_pct = 0.0 if total == 0 else (both_ok / total) * 100  # type: ignore[operator]

    logger.info("Scheduled games: %s", total)
    logger.info("  Both starters present: %s (%.1f%%)", both_ok, coverage_pct)
    logger.info("  Away starters present: %s", away_ok)
    logger.info("  Home starters present: %s", home_ok)
    logger.info("  Both missing: %s", both_missing)
    logger.info("  Preview summaries present: %s", preview_rows)
    logger.info("  Preview summaries missing starters: %s", preview_missing_starters)
    logger.info("  OCI sync candidates: %s", sync_candidate_games)
    logger.info("  OCI sync candidates with both starters: %s", sync_complete_starters)
    if oci_sync_ready:
        logger.info("  OCI sync config: ready")
    elif not pregame_sync_enabled:
        logger.info("  OCI sync config: disabled by PREGAME_SYNC_TO_OCI")
    else:
        logger.info("  OCI sync config: disabled because OCI_DB_URL is missing")

    if verbose:
        rows = session.execute(
            text(
                """
                SELECT

                    g.game_date,
                    COUNT(*) AS total,
                    SUM(
                        CASE
                            WHEN g.away_pitcher IS NOT NULL AND g.away_pitcher != ''
                             AND g.home_pitcher IS NOT NULL AND g.home_pitcher != ''
                            THEN 1 ELSE 0
                        END
                    ) AS both_ok,
                    SUM(CASE WHEN p.game_id IS NOT NULL THEN 1 ELSE 0 END) AS preview_rows
                FROM game g
                LEFT JOIN (
                    SELECT DISTINCT game_id
                    FROM game_summary
                    WHERE summary_type = '프리뷰'
                ) p ON p.game_id = g.game_id
                WHERE UPPER(g.game_status) = 'SCHEDULED'
                GROUP BY g.game_date
                ORDER BY g.game_date
                LIMIT 40
                """,
            ),
        ).all()

        logger.info("\nScheduled pregame by date:")
        for game_date, date_total, date_both_ok, date_preview_rows in rows:
            logger.info(
                "  %s: starters=%s/%s, preview=%s/%s",
                game_date,
                date_both_ok,
                date_total,
                date_preview_rows,
                date_total,
            )

    return {
        "scheduled_total": total,
        "away_ok": away_ok,
        "home_ok": home_ok,
        "both_ok": both_ok,
        "both_missing": both_missing,
        "preview_rows": preview_rows,
        "preview_missing_starters": preview_missing_starters,
        "sync_candidate_games": sync_candidate_games,
        "sync_complete_starters": sync_complete_starters,
        "oci_sync_ready": oci_sync_ready,
        "coverage_pct": coverage_pct,
    }


def _run_p0_readiness_check(args: argparse.Namespace) -> None:
    target_date = normalize_yyyymmdd(args.date)
    with SessionLocal() as session:
        readiness = build_p0_readiness(
            session,
            P0ReadinessOptions(
                target_date=target_date,
                lookback_days=args.lookback_days,
                lookahead_days=args.lookahead_days,
            ),
        )
    if args.json_output:
        logger.info(json.dumps({"p0_readiness": readiness}, ensure_ascii=False, indent=2, default=str))
        return
    _log_p0_readiness(target_date, readiness)


def _log_p0_readiness(target_date: str, readiness: dict[str, Any]) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info(" KBO P0 Readiness Check")
    logger.info(" Target Date: %s", target_date)
    logger.info(" Window: %s..%s", readiness["start_date"], readiness["end_date"])
    logger.info("%s", "=" * 60)
    logger.info(format_p0_readiness_summary(readiness))
    logger.info("\nDataset Summary:")
    for key in ("schedule", "pregame", "live", "postgame", "relay", "roster", "broadcast", "oci"):
        logger.info("  %s: %s", key, readiness[key])
    if readiness["failures"]:
        logger.info("\nFailures:")
        for failure in readiness["failures"]:
            logger.info(
                "  - %s %s %s %s %s",
                failure["severity"],
                failure["dataset"],
                failure.get("game_date") or "-",
                failure.get("game_id") or "-",
                failure["reason"],
            )


def _collect_full_status(
    *,
    verbose: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    with SessionLocal() as session:
        return (
            check_schedules(session),
            check_players(session),
            check_futures_data(session),
            check_game_data(session),
            check_pregame_pitcher_coverage(session, verbose=verbose),
        )


def _log_full_status_summary(
    schedule_stats: dict[str, Any],
    player_stats: dict[str, Any],
    futures_stats: dict[str, Any],
    game_stats: dict[str, Any],
    pregame_pitcher_stats: dict[str, Any],
) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info(" Summary")
    logger.info("%s", "=" * 60)
    logger.info("  Schedules: %s", schedule_stats["total"])
    logger.info("  Players: %s", player_stats["total"])
    logger.info("  Futures batting: %s", futures_stats["batting"])
    logger.info("  Futures pitching: %s", futures_stats["pitching"])
    logger.info("  Game batting: %s", game_stats["batting"])
    logger.info("  Game pitching: %s", game_stats["pitching"])
    logger.info(
        "  Scheduled pregame pitchers: both=%s / %s (%.1f%%)",
        pregame_pitcher_stats["both_ok"],
        pregame_pitcher_stats["scheduled_total"],
        pregame_pitcher_stats["coverage_pct"],
    )
    logger.info(
        "  Scheduled preview summaries: %s (missing starters=%s)",
        pregame_pitcher_stats["preview_rows"],
        pregame_pitcher_stats["preview_missing_starters"],
    )
    logger.info(
        "  Pregame OCI sync: candidates=%s, complete_starters=%s, ready=%s",
        pregame_pitcher_stats["sync_candidate_games"],
        pregame_pitcher_stats["sync_complete_starters"],
        pregame_pitcher_stats["oci_sync_ready"],
    )


def _collect_status_warnings(
    schedule_stats: dict[str, Any],
    futures_stats: dict[str, Any],
    pregame_pitcher_stats: dict[str, Any],
) -> list[str]:
    warnings = []
    if schedule_stats["total"] == 0:
        warnings.append("No schedules found")
    warnings.extend(schedule_stats.get("warnings", []))
    if futures_stats["batting"] == 0:
        warnings.append("No Futures batting data found")
    if pregame_pitcher_stats.get("preview_missing_starters", 0) > 0:
        warnings.append("Scheduled preview summaries exist but pitcher fields are missing")
    if pregame_pitcher_stats.get("sync_candidate_games", 0) > 0 and not pregame_pitcher_stats.get("oci_sync_ready"):
        warnings.append("Pregame sync candidates exist but OCI sync is not ready")
    return warnings


def _log_warnings(warnings: list[str]) -> None:
    if not warnings:
        return
    logger.info("\n%s", "=" * 60)
    logger.info(" WARNINGS")
    logger.info("%s", "=" * 60)
    for warning in warnings:
        logger.info("  - %s", warning)


def _run_full_status_check(*, verbose: bool) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info(" KBO Data Status Check")
    logger.info(" Timestamp: %s", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("%s", "=" * 60)
    schedule_stats, player_stats, futures_stats, game_stats, pregame_pitcher_stats = _collect_full_status(
        verbose=verbose,
    )
    _log_full_status_summary(schedule_stats, player_stats, futures_stats, game_stats, pregame_pitcher_stats)
    _log_warnings(_collect_status_warnings(schedule_stats, futures_stats, pregame_pitcher_stats))
    logger.info("")


def main(argv: Sequence[str] | None = None) -> None:
    """데이터 점검 스크립트의 메인 실행 함수.

    Args:
        argv: Argv.

    """
    _configure_cli_logging()

    load_dotenv()
    parser = argparse.ArgumentParser(description="Check KBO database status and data integrity")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")
    parser.add_argument("--p0", action="store_true", help="Run P0 game-data readiness check")
    parser.add_argument("--date", type=str, default=None, help="Target date for --p0 in YYYYMMDD format")
    parser.add_argument("--lookback-days", type=int, default=7, help="Days before --date to include for --p0")
    parser.add_argument("--lookahead-days", type=int, default=1, help="Days after --date to include for --p0")
    parser.add_argument("--json", action="store_true", dest="json_output", help="Print --p0 result as JSON only")
    args = parser.parse_args(argv)

    if args.p0:
        _run_p0_readiness_check(args)
        return

    _run_full_status_check(verbose=args.verbose)


if __name__ == "__main__":
    main()
