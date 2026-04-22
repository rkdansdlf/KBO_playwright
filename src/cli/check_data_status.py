"""
데이터 무결성 및 상태 점검 스크립트.

데이터베이스에 저장된 경기 일정, 선수, 퓨처스리그 데이터 등의 상태를 확인하고,
예상 수치와 비교하여 잠재적인 문제를 경고합니다.
"""
from __future__ import annotations

import argparse
import os
from typing import Sequence
from sqlalchemy import select, func, text
from datetime import datetime

from dotenv import load_dotenv

from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.player import Player, PlayerSeasonBatting, PlayerSeasonPitching
from src.utils.safe_print import safe_print as print


FALSE_ENV_VALUES = {"0", "false", "no", "off"}


def _env_enabled(name: str, default: str = "1") -> bool:
    return os.getenv(name, default).strip().lower() not in FALSE_ENV_VALUES


def check_schedules(session) -> dict:
    """`game_schedules` 테이블의 데이터 현황을 점검합니다."""
    print("\n=== Game Schedules ===")

    try:
        # 전체 일정 수
        total = session.execute(text("SELECT COUNT(*) FROM game_schedules")).scalar() or 0
    except Exception:
        total = 0
    print(f"Total schedules: {total}")

    try:
        operational_total = session.execute(text("SELECT COUNT(*) FROM game")).scalar() or 0
        operational_scheduled = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM game
                WHERE UPPER(COALESCE(game_status, '')) = 'SCHEDULED'
                """
            )
        ).scalar() or 0
    except Exception:
        operational_total = 0
        operational_scheduled = 0

    # 시즌 유형(정규, 프리시즌 등)별 집계
    type_counts = {}
    results = []
    try:
        stmt = text(
            "SELECT season_type, COUNT(*) "
            "FROM game_schedules "
            "GROUP BY season_type"
        )
        results = session.execute(stmt).all()
    except Exception:
        results = []

    type_counts = {}
    print("\nBy season type:")
    for season_type, count in results:
        type_counts[season_type] = count
        print(f"  {season_type}: {count}")

    # 연도별 집계
    try:
        stmt = text(
            "SELECT season_year, COUNT(*) "
            "FROM game_schedules "
            "GROUP BY season_year "
            "ORDER BY season_year DESC"
        )
        results = session.execute(stmt).all()
    except Exception:
        results = []
    print("\nBy year:")
    for year, count in results:
        print(f"  {year}: {count}")

    if total == 0 and operational_total > 0:
        print("\nOperational game table fallback:")
        print(f"  Total game rows: {operational_total}")
        print(f"  Scheduled game rows: {operational_scheduled}")

    # 데이터의 날짜 범위 확인
    try:
        stmt = text(
            "SELECT MIN(game_date), MAX(game_date) "
            "FROM game_schedules"
        )
        min_date, max_date = session.execute(stmt).first()
    except Exception:
        min_date, max_date = None, None

    if min_date and max_date:
        print(f"\nDate range: {min_date} to {max_date}")

    if total == 0 and operational_total > 0:
        try:
            game_min_date, game_max_date = session.execute(
                text("SELECT MIN(game_date), MAX(game_date) FROM game")
            ).first()
        except Exception:
            game_min_date, game_max_date = None, None
        if game_min_date and game_max_date:
            print(f"Operational game date range: {game_min_date} to {game_max_date}")

    # 예상 데이터 수와 비교하여 검증 (2025 시즌 기준)
    warnings = []
    expected = {
        "preseason": 42,    # Based on Progress.md
        "regular": 720,     # 10 teams * 144 games / 2
        "postseason": 7     # Initial fixtures
    }

    print("\nValidation:")
    if total == 0 and operational_total > 0:
        print("  game_schedules: 0 rows [INFO: using game table fallback]")
        print(f"  game table: {operational_total} rows [OK]")
    else:
        for stype, expected_count in expected.items():
            actual = type_counts.get(stype, 0)
            status = "OK" if actual >= expected_count else "WARN"
            print(f"  {stype}: {actual}/{expected_count} [{status}]")
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
        "warnings": warnings
    }


def check_players(session) -> dict:
    """`players` 테이블의 데이터 현황을 점검합니다."""
    print("\n=== Players ===")

    # 전체 선수 수
    total = session.execute(select(func.count(Player.id))).scalar()
    print(f"Total players: {total}")

    # 선수 상태(현역, 은퇴 등)별 집계
    stmt = select(
        Player.status,
        func.count(Player.id)
    ).group_by(Player.status)

    results = session.execute(stmt).all()
    print("\nBy status:")
    for status, count in results:
        status_label = status or "(null)"
        print(f"  {status_label}: {count}")

    return {"total": total}


def check_futures_data(session) -> dict:
    """퓨처스리그 관련 데이터(타자/투수 기록) 현황을 점검합니다."""
    print("\n=== Futures League Data ===")

    # 퓨처스리그 타자 기록 수
    batting_stmt = select(func.count(PlayerSeasonBatting.id)).where(
        PlayerSeasonBatting.league == "FUTURES"
    )
    batting_count = session.execute(batting_stmt).scalar()
    print(f"Batting records: {batting_count}")

    # 시즌별 타자 기록 집계
    stmt = select(
        PlayerSeasonBatting.season,
        func.count(PlayerSeasonBatting.id)
    ).where(
        PlayerSeasonBatting.league == "FUTURES"
    ).group_by(PlayerSeasonBatting.season).order_by(PlayerSeasonBatting.season.desc())

    results = session.execute(stmt).all()
    if results:
        print("\nBatting by season:")
        for season, count in results:
            print(f"  {season}: {count}")

    # 퓨처스리그 투수 기록 수
    pitching_stmt = select(func.count(PlayerSeasonPitching.id)).where(
        PlayerSeasonPitching.league == "FUTURES"
    )
    pitching_count = session.execute(pitching_stmt).scalar()
    print(f"\nPitching records: {pitching_count}")

    return {
        "batting": batting_count,
        "pitching": pitching_count
    }


def check_game_data(session) -> dict:
    """개별 경기 내 선수들의 기록 데이터 현황을 점검합니다."""
    print("\n=== Game-Level Stats ===")

    try:
        from src.models.game import PlayerGameBatting, PlayerGamePitching

        batting_count = session.execute(select(func.count(PlayerGameBatting.id))).scalar()
        print(f"Player game batting records: {batting_count}")

        pitching_count = session.execute(select(func.count(PlayerGamePitching.id))).scalar()
        print(f"Player game pitching records: {pitching_count}")

        return {
            "batting": batting_count,
            "pitching": pitching_count
        }
    except (ImportError, AttributeError):
        print("Player game stats models not found (expected for early development)")
        return {
            "batting": 0,
            "pitching": 0
        }


def check_pregame_pitcher_coverage(session, *, verbose: bool = False) -> dict:
    """예정 경기 선발투수 적재율을 점검합니다."""
    print("\n=== Pregame Starting Pitchers ===")

    scheduled_filter = func.upper(Game.game_status) == "SCHEDULED"
    total = session.query(Game).filter(scheduled_filter).count()
    if total == 0:
        pregame_sync_enabled = _env_enabled("PREGAME_SYNC_TO_OCI")
        oci_url_present = bool(os.getenv("OCI_DB_URL"))
        oci_sync_ready = pregame_sync_enabled and oci_url_present

        print("Scheduled games: 0")
        print("  OCI sync candidates: 0")
        print("  OCI sync candidates with both starters: 0")
        if oci_sync_ready:
            print("  OCI sync config: ready")
        elif not pregame_sync_enabled:
            print("  OCI sync config: disabled by PREGAME_SYNC_TO_OCI")
        else:
            print("  OCI sync config: disabled because OCI_DB_URL is missing")

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
    preview_rows = session.execute(
        text(
            """
            SELECT COUNT(DISTINCT g.game_id)
            FROM game g
            JOIN game_summary gs ON gs.game_id = g.game_id
            WHERE UPPER(g.game_status) = 'SCHEDULED'
              AND gs.summary_type = '프리뷰'
            """
        )
    ).scalar() or 0
    preview_missing_starters = session.execute(
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
    ).scalar() or 0
    sync_candidate_games = session.execute(
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
    ).scalar() or 0
    sync_complete_starters = session.execute(
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
    ).scalar() or 0

    pregame_sync_enabled = _env_enabled("PREGAME_SYNC_TO_OCI")
    oci_url_present = bool(os.getenv("OCI_DB_URL"))
    oci_sync_ready = pregame_sync_enabled and oci_url_present

    coverage_pct = 0.0 if total == 0 else (both_ok / total) * 100

    print(f"Scheduled games: {total}")
    print(f"  Both starters present: {both_ok} ({coverage_pct:.1f}%)")
    print(f"  Away starters present: {away_ok}")
    print(f"  Home starters present: {home_ok}")
    print(f"  Both missing: {both_missing}")
    print(f"  Preview summaries present: {preview_rows}")
    print(f"  Preview summaries missing starters: {preview_missing_starters}")
    print(f"  OCI sync candidates: {sync_candidate_games}")
    print(f"  OCI sync candidates with both starters: {sync_complete_starters}")
    if oci_sync_ready:
        print("  OCI sync config: ready")
    elif not pregame_sync_enabled:
        print("  OCI sync config: disabled by PREGAME_SYNC_TO_OCI")
    else:
        print("  OCI sync config: disabled because OCI_DB_URL is missing")

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

        print("\nScheduled pregame by date:")
        for game_date, date_total, date_both_ok, date_preview_rows in rows:
            print(
                f"  {game_date}: starters={date_both_ok}/{date_total}, "
                f"preview={date_preview_rows}/{date_total}"
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
    parser = argparse.ArgumentParser(
        description="Check KBO database status and data integrity"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed information"
    )
    args = parser.parse_args(argv)

    print(f"\n{'='*60}")
    print(f" KBO Data Status Check")
    print(f" Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    with SessionLocal() as session:
        schedule_stats = check_schedules(session)
        player_stats = check_players(session)
        futures_stats = check_futures_data(session)
        game_stats = check_game_data(session)
        pregame_pitcher_stats = check_pregame_pitcher_coverage(session, verbose=args.verbose)

    # 최종 요약 출력
    print(f"\n{'='*60}")
    print(" Summary")
    print(f"{'='*60}")
    print(f"  Schedules: {schedule_stats['total']}")
    print(f"  Players: {player_stats['total']}")
    print(f"  Futures batting: {futures_stats['batting']}")
    print(f"  Futures pitching: {futures_stats['pitching']}")
    print(f"  Game batting: {game_stats['batting']}")
    print(f"  Game pitching: {game_stats['pitching']}")
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

    if schedule_stats['total'] == 0:
        all_warnings.append("No schedules found")
    all_warnings.extend(schedule_stats.get('warnings', []))

    if futures_stats['batting'] == 0:
        all_warnings.append("No Futures batting data found")
    if pregame_pitcher_stats.get("preview_missing_starters", 0) > 0:
        all_warnings.append(
            "Scheduled preview summaries exist but pitcher fields are missing"
        )
    if pregame_pitcher_stats.get("sync_candidate_games", 0) > 0 and not pregame_pitcher_stats.get("oci_sync_ready"):
        all_warnings.append(
            "Pregame sync candidates exist but OCI sync is not ready"
        )

    if all_warnings:
        print(f"\n{'='*60}")
        print(" WARNINGS")
        print(f"{'='*60}")
        for warning in all_warnings:
            print(f"  - {warning}")

    print()


if __name__ == "__main__":
    main()
