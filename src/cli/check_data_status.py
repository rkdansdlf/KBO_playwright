"""
데이터 무결성 및 상태 점검 스크립트.

데이터베이스에 저장된 경기 일정, 선수, 퓨처스리그 데이터 등의 상태를 확인하고,
예상 수치와 비교하여 잠재적인 문제를 경고합니다.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from typing import Sequence

from dotenv import load_dotenv
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.player import Player, PlayerSeasonBatting, PlayerSeasonPitching
from src.services.p0_readiness import build_p0_readiness, format_p0_readiness_summary, normalize_yyyymmdd
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)

FALSE_ENV_VALUES = {"0", "false", "no", "off"}


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def check_schedules(session) -> dict:
    """`game_schedules` 테이블의 데이터 현황을 점검합니다."""
    logger.info("\n=== Game Schedules ===")

    try:
        # 전체 일정 수
        total = session.execute(text("SELECT COUNT(*) FROM game_schedules")).scalar() or 0
    except SQLAlchemyError:
        total = 0
    logger.exception(f"Total schedules: {total}")

    try:
        operational_total = session.execute(text("SELECT COUNT(*) FROM game")).scalar() or 0
        operational_scheduled = (
            session.execute(
                text(
                    """
                SELECT COUNT(*)
                FROM game
                WHERE UPPER(COALESCE(game_status, '')) = 'SCHEDULED'
                """
                )
            ).scalar()
            or 0
        )
    except SQLAlchemyError:
        operational_total = 0
        operational_scheduled = 0

    # 시즌 유형(정규, 프리시즌 등)별 집계
    type_counts = {}
    results = []
    try:
        stmt = text("SELECT season_type, COUNT(*) FROM game_schedules GROUP BY season_type")
        results = session.execute(stmt).all()
    except SQLAlchemyError:
        results = []

    type_counts = {}
    logger.info("\nBy season type:")
    for season_type, count in results:
        type_counts[season_type] = count
        logger.info(f"  {season_type}: {count}")

    # 연도별 집계
    try:
        stmt = text("SELECT season_year, COUNT(*) FROM game_schedules GROUP BY season_year ORDER BY season_year DESC")
        results = session.execute(stmt).all()
    except SQLAlchemyError:
        results = []
    logger.exception("\nBy year:")
    for year, count in results:
        logger.info(f"  {year}: {count}")

    if total == 0 and operational_total > 0:
        logger.info("\nOperational game table fallback:")
        logger.info(f"  Total game rows: {operational_total}")
        logger.info(f"  Scheduled game rows: {operational_scheduled}")

    # 데이터의 날짜 범위 확인
    try:
        stmt = text("SELECT MIN(game_date), MAX(game_date) FROM game_schedules")
        min_date, max_date = session.execute(stmt).first()
    except SQLAlchemyError:
        min_date, max_date = None, None

    if min_date and max_date:
        logger.info(f"\nDate range: {min_date} to {max_date}")

    if total == 0 and operational_total > 0:
        try:
            game_min_date, game_max_date = session.execute(
                text("SELECT MIN(game_date), MAX(game_date) FROM game")
            ).first()
        except SQLAlchemyError:
            game_min_date, game_max_date = None, None
        if game_min_date and game_max_date:
            logger.info(f"Operational game date range: {game_min_date} to {game_max_date}")

    # 예상 데이터 수와 비교하여 검증 (2025 시즌 기준)
    warnings = []
    expected = {
        "preseason": 42,  # Based on Progress.md
        "regular": 720,  # 10 teams * 144 games / 2
        "postseason": 7,  # Initial fixtures
    }

    logger.info("\nValidation:")
    if total == 0 and operational_total > 0:
        logger.info("  game_schedules: 0 rows [INFO: using game table fallback]")
        logger.info(f"  game table: {operational_total} rows [OK]")
    else:
        for stype, expected_count in expected.items():
            actual = type_counts.get(stype, 0)
            status = "OK" if actual >= expected_count else "WARN"
            logger.info(f"  {stype}: {actual}/{expected_count} [{status}]")
            if actual < expected_count:
                warnings.append(f"{stype}: {actual} < {expected_count} (missing {expected_count - actual})")

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


def check_players(session) -> dict:
    """`players` 테이블의 데이터 현황을 점검합니다."""
    logger.info("\n=== Players ===")

    # 전체 선수 수
    total = session.execute(select(func.count(Player.id))).scalar()
    logger.info(f"Total players: {total}")

    # 선수 상태(현역, 은퇴 등)별 집계
    stmt = select(Player.status, func.count(Player.id)).group_by(Player.status)

    results = session.execute(stmt).all()
    logger.info("\nBy status:")
    for status, count in results:
        status_label = status or "(null)"
        logger.info(f"  {status_label}: {count}")

    return {"total": total}


def check_futures_data(session) -> dict:
    """퓨처스리그 관련 데이터(타자/투수 기록) 현황을 점검합니다."""
    logger.info("\n=== Futures League Data ===")

    # 퓨처스리그 타자 기록 수
    batting_stmt = select(func.count(PlayerSeasonBatting.id)).where(PlayerSeasonBatting.league == "FUTURES")
    batting_count = session.execute(batting_stmt).scalar()
    logger.info(f"Batting records: {batting_count}")

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
            logger.info(f"  {season}: {count}")

    # 퓨처스리그 투수 기록 수
    pitching_stmt = select(func.count(PlayerSeasonPitching.id)).where(PlayerSeasonPitching.league == "FUTURES")
    pitching_count = session.execute(pitching_stmt).scalar()
    logger.info(f"\nPitching records: {pitching_count}")

    return {"batting": batting_count, "pitching": pitching_count}


def check_game_data(session) -> dict:
    from src.models.game import PlayerGameBatting, PlayerGamePitching

    logger.info("\n=== Game-Level Stats ===")
    batting_count = session.execute(select(func.count(PlayerGameBatting.id))).scalar()
    logger.info(f"Player game batting records: {batting_count}")
    pitching_count = session.execute(select(func.count(PlayerGamePitching.id))).scalar()
    logger.info(f"Player game pitching records: {pitching_count}")

    # Duplicate check
    dup_b = session.execute(
        text(
            "SELECT COALESCE(COUNT(*), 0) FROM (SELECT game_id, player_id, COUNT(*) FROM player_game_batting GROUP BY game_id, player_id HAVING COUNT(*) > 1)"
        )
    ).scalar()
    dup_p = session.execute(
        text(
            "SELECT COALESCE(COUNT(*), 0) FROM (SELECT game_id, player_id, COUNT(*) FROM player_game_pitching GROUP BY game_id, player_id HAVING COUNT(*) > 1)"
        )
    ).scalar()
    if dup_b or dup_p:
        logger.info(f"  Duplicates: batting={dup_b}, pitching={dup_p} [WARN]")
    else:
        logger.info("  Duplicates: none [OK]")

    # NULL field check
    for tbl, label in [("player_game_batting", "batting"), ("player_game_pitching", "pitching")]:
        nid = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE player_id IS NULL")).scalar()
        nn = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE player_name IS NULL")).scalar()
        ns = session.execute(text(f"SELECT COUNT(*) FROM {tbl} WHERE team_side IS NULL")).scalar()
        if nid or nn or ns:
            logger.info(f"  {label} NULLs: player_id={nid}, player_name={nn}, team_side={ns} [WARN]")
        else:
            logger.info(f"  {label} NULLs: none [OK]")

    # Rate stat anomaly check (avg > obp — documented as expected with SF)
    avg_gt_obp = session.execute(
        text("SELECT COUNT(*) FROM player_game_batting WHERE avg IS NOT NULL AND obp IS NOT NULL AND avg > obp")
    ).scalar()
    logger.info(f"  Batting avg > obp: {avg_gt_obp} (expected when sacrifice flies exist)")

    # Rate stat boundary checks
    for tbl, col, lo, hi, _label in [
        ("player_game_batting", "avg", 0, 1, "avg"),
        ("player_game_batting", "obp", 0, 1, "obp"),
        ("player_game_batting", "slg", 0, 5, "slg"),
        ("player_game_pitching", "era", 0, 200, "era"),
        ("player_game_pitching", "whip", 0, 30, "whip"),
    ]:
        n = session.execute(
            text(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND ({col} < {lo} OR {col} > {hi})")
        ).scalar()
        if n:
            logger.info(f"  {tbl}.{col}: {n} outside [{lo}, {hi}]")

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
    """)
    ).fetchall()
    for status, total, covered in cov:
        pct = 100.0 * covered / total if total else 0
        logger.info(f"  Coverage {status:<12}: {covered}/{total} ({pct:.1f}%)")

    # Games missing source stats (COMPLETED/DRAW with no game_batting_stats)
    missing_games = session.execute(
        text("""
        SELECT COUNT(DISTINCT g.game_id)
        FROM game g
        LEFT JOIN game_batting_stats gbs ON gbs.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'DRAW')
          AND gbs.game_id IS NULL
    """)
    ).scalar()
    logger.info(f"  Games without source batting stats: {missing_games}")

    return {"batting": batting_count, "pitching": pitching_count}


def check_pregame_pitcher_coverage(session, *, verbose: bool = False) -> dict:
    """예정 경기 선발투수 적재율을 점검합니다."""
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
        )
    ).scalar()
    home_ok = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            Game.home_pitcher.is_not(None),
            Game.home_pitcher != "",
        )
    ).scalar()
    both_ok = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            Game.away_pitcher.is_not(None),
            Game.away_pitcher != "",
            Game.home_pitcher.is_not(None),
            Game.home_pitcher != "",
        )
    ).scalar()
    both_missing = session.execute(
        select(func.count(Game.id)).where(
            scheduled_filter,
            (Game.away_pitcher.is_(None) | (Game.away_pitcher == "")),
            (Game.home_pitcher.is_(None) | (Game.home_pitcher == "")),
        )
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
            """
            )
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
            """
            )
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
            """
            )
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
            """
            )
        ).scalar()
        or 0
    )

    pregame_sync_enabled = _env_enabled("PREGAME_SYNC_TO_OCI")
    oci_url_present = bool(os.getenv("OCI_DB_URL"))
    oci_sync_ready = pregame_sync_enabled and oci_url_present

    coverage_pct = 0.0 if total == 0 else (both_ok / total) * 100

    logger.info(f"Scheduled games: {total}")
    logger.info(f"  Both starters present: {both_ok} ({coverage_pct:.1f}%)")
    logger.info(f"  Away starters present: {away_ok}")
    logger.info(f"  Home starters present: {home_ok}")
    logger.info(f"  Both missing: {both_missing}")
    logger.info(f"  Preview summaries present: {preview_rows}")
    logger.info(f"  Preview summaries missing starters: {preview_missing_starters}")
    logger.info(f"  OCI sync candidates: {sync_candidate_games}")
    logger.info(f"  OCI sync candidates with both starters: {sync_complete_starters}")
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
                """
            )
        ).all()

        logger.info("\nScheduled pregame by date:")
        for game_date, date_total, date_both_ok, date_preview_rows in rows:
            logger.info(
                f"  {game_date}: starters={date_both_ok}/{date_total}, preview={date_preview_rows}/{date_total}"
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


def main(argv: Sequence[str] | None = None) -> None:
    """데이터 점검 스크립트의 메인 실행 함수."""
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
        target_date = normalize_yyyymmdd(args.date)
        with SessionLocal() as session:
            readiness = build_p0_readiness(
                session,
                target_date=target_date,
                lookback_days=args.lookback_days,
                lookahead_days=args.lookahead_days,
            )

        if args.json_output:
            print(json.dumps({"p0_readiness": readiness}, ensure_ascii=False, indent=2, default=str))
            return

        logger.info(f"\n{'=' * 60}")
        logger.info(" KBO P0 Readiness Check")
        logger.info(f" Target Date: {target_date}")
        logger.info(f" Window: {readiness['start_date']}..{readiness['end_date']}")
        logger.info(f"{'=' * 60}")
        logger.info(format_p0_readiness_summary(readiness))
        logger.info("\nDataset Summary:")
        for key in ("schedule", "pregame", "live", "postgame", "relay", "roster", "broadcast", "oci"):
            logger.info(f"  {key}: {readiness[key]}")

        if readiness["failures"]:
            logger.info("\nFailures:")
            for failure in readiness["failures"]:
                print(
                    "  - "
                    f"{failure['severity']} "
                    f"{failure['dataset']} "
                    f"{failure.get('game_date') or '-'} "
                    f"{failure.get('game_id') or '-'} "
                    f"{failure['reason']}"
                )
        return

    logger.info(f"\n{'=' * 60}")
    logger.info(" KBO Data Status Check")
    logger.info(f" Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 60}")

    with SessionLocal() as session:
        schedule_stats = check_schedules(session)
        player_stats = check_players(session)
        futures_stats = check_futures_data(session)
        game_stats = check_game_data(session)
        pregame_pitcher_stats = check_pregame_pitcher_coverage(session, verbose=args.verbose)

    # 최종 요약 출력
    logger.info(f"\n{'=' * 60}")
    logger.info(" Summary")
    logger.info(f"{'=' * 60}")
    logger.info(f"  Schedules: {schedule_stats['total']}")
    logger.info(f"  Players: {player_stats['total']}")
    logger.info(f"  Futures batting: {futures_stats['batting']}")
    logger.info(f"  Futures pitching: {futures_stats['pitching']}")
    logger.info(f"  Game batting: {game_stats['batting']}")
    logger.info(f"  Game pitching: {game_stats['pitching']}")
    print(
        "  Scheduled pregame pitchers: "
        f"both={pregame_pitcher_stats['both_ok']} / "
        f"{pregame_pitcher_stats['scheduled_total']} "
        f"({pregame_pitcher_stats['coverage_pct']:.1f}%)"
    )
    print(
        "  Scheduled preview summaries: "
        f"{pregame_pitcher_stats['preview_rows']} "
        f"(missing starters={pregame_pitcher_stats['preview_missing_starters']})"
    )
    print(
        "  Pregame OCI sync: "
        f"candidates={pregame_pitcher_stats['sync_candidate_games']}, "
        f"complete_starters={pregame_pitcher_stats['sync_complete_starters']}, "
        f"ready={pregame_pitcher_stats['oci_sync_ready']}"
    )

    # 경고 목록 취합 및 출력
    all_warnings = []

    if schedule_stats["total"] == 0:
        all_warnings.append("No schedules found")
    all_warnings.extend(schedule_stats.get("warnings", []))

    if futures_stats["batting"] == 0:
        all_warnings.append("No Futures batting data found")
    if pregame_pitcher_stats.get("preview_missing_starters", 0) > 0:
        all_warnings.append("Scheduled preview summaries exist but pitcher fields are missing")
    if pregame_pitcher_stats.get("sync_candidate_games", 0) > 0 and not pregame_pitcher_stats.get("oci_sync_ready"):
        all_warnings.append("Pregame sync candidates exist but OCI sync is not ready")

    if all_warnings:
        logger.info(f"\n{'=' * 60}")
        logger.info(" WARNINGS")
        logger.info(f"{'=' * 60}")
        for warning in all_warnings:
            logger.info(f"  - {warning}")

    logger.info()


if __name__ == "__main__":
    main()
