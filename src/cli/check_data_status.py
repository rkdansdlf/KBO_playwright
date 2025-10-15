"""
데이터 무결성 및 상태 점검 스크립트.

데이터베이스에 저장된 경기 일정, 선수, 퓨처스리그 데이터 등의 상태를 확인하고,
예상 수치와 비교하여 잠재적인 문제를 경고합니다.
"""
from __future__ import annotations

import argparse
from typing import Sequence
from sqlalchemy import select, func
from datetime import datetime

from src.db.engine import SessionLocal
from src.models.game import GameSchedule
from src.models.player import Player, PlayerSeasonBatting, PlayerSeasonPitching
from src.utils.safe_print import safe_print as print


def check_schedules(session) -> dict:
    """`game_schedules` 테이블의 데이터 현황을 점검합니다."""
    print("\n=== Game Schedules ===")

    # 전체 일정 수
    total = session.execute(select(func.count(GameSchedule.schedule_id))).scalar()
    print(f"Total schedules: {total}")

    # 시즌 유형(정규, 프리시즌 등)별 집계
    stmt = select(
        GameSchedule.season_type,
        func.count(GameSchedule.schedule_id)
    ).group_by(GameSchedule.season_type)

    results = session.execute(stmt).all()
    type_counts = {}
    print("\nBy season type:")
    for season_type, count in results:
        type_counts[season_type] = count
        print(f"  {season_type}: {count}")

    # 연도별 집계
    stmt = select(
        GameSchedule.season_year,
        func.count(GameSchedule.schedule_id)
    ).group_by(GameSchedule.season_year).order_by(GameSchedule.season_year.desc())

    results = session.execute(stmt).all()
    print("\nBy year:")
    for year, count in results:
        print(f"  {year}: {count}")

    # 데이터의 날짜 범위 확인
    from sqlalchemy import func as sql_func
    stmt = select(
        sql_func.min(GameSchedule.game_date),
        sql_func.max(GameSchedule.game_date)
    )
    min_date, max_date = session.execute(stmt).first()
    if min_date and max_date:
        print(f"\nDate range: {min_date} to {max_date}")

    # 예상 데이터 수와 비교하여 검증 (2025 시즌 기준)
    warnings = []
    expected = {
        "preseason": 42,    # Based on Progress.md
        "regular": 720,     # 10 teams * 144 games / 2
        "postseason": 7     # Initial fixtures
    }

    print("\nValidation:")
    for stype, expected_count in expected.items():
        actual = type_counts.get(stype, 0)
        status = "OK" if actual >= expected_count else "WARN"
        print(f"  {stype}: {actual}/{expected_count} [{status}]")
        if actual < expected_count:
            warnings.append(f"{stype}: {actual} < {expected_count} (missing {expected_count - actual})")

    return {
        "total": total,
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


def main(argv: Sequence[str] | None = None) -> None:
    """데이터 점검 스크립트의 메인 실행 함수."""
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

    # 경고 목록 취합 및 출력
    all_warnings = []

    if schedule_stats['total'] == 0:
        all_warnings.append("No schedules found")
    all_warnings.extend(schedule_stats.get('warnings', []))

    if futures_stats['batting'] == 0:
        all_warnings.append("No Futures batting data found")

    if all_warnings:
        print(f"\n{'='*60}")
        print(" WARNINGS")
        print(f"{'='*60}")
        for warning in all_warnings:
            print(f"  - {warning}")

    print()


if __name__ == "__main__":
    main()
