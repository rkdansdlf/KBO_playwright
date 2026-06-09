#!/usr/bin/env python3
"""
SQLite 데이터베이스 초기화 스크립트
크롤링 시작 전 중복 데이터 방지를 위한 깨끗한 상태로 리셋
"""

import logging

logger = logging.getLogger(__name__)

import argparse

from sqlalchemy import text

from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching


def reset_sqlite_data(tables_to_reset: list = None, confirm: bool = True):
    """
    SQLite 데이터베이스 초기화

    Args:
        tables_to_reset: 초기화할 테이블 목록 (None이면 모든 플레이어 테이블)
        confirm: 사용자 확인 요청 여부
    """
    if tables_to_reset is None:
        tables_to_reset = ["player_season_batting", "player_season_pitching"]

    with SessionLocal() as session:
        # 현재 데이터 현황 확인
        batting_count = session.query(PlayerSeasonBatting).count()
        pitching_count = session.query(PlayerSeasonPitching).count()

        logger.info("🗃️ 현재 SQLite 데이터 현황:")
        logger.info(f"  - player_season_batting: {batting_count:,}건")
        logger.info(f"  - player_season_pitching: {pitching_count:,}건")
        logger.info(f"  - 총합: {batting_count + pitching_count:,}건")

        if batting_count == 0 and pitching_count == 0:
            logger.info("✅ 이미 빈 데이터베이스입니다.")
            return

        if confirm:
            logger.warning("\n⚠️ 다음 테이블들이 초기화됩니다:")
            for table in tables_to_reset:
                logger.info(f"  - {table}")

            response = input("\n정말로 진행하시겠습니까? (y/N): ")
            if response.lower() != "y":
                logger.error("❌ 사용자가 취소했습니다.")
                return

        # 외래키 제약조건 임시 비활성화
        session.execute(text("PRAGMA foreign_keys = OFF"))

        try:
            # 테이블별 초기화
            for table_name in tables_to_reset:
                if table_name == "player_season_batting":
                    deleted = session.query(PlayerSeasonBatting).delete()
                    logger.info(f"🧹 player_season_batting: {deleted:,}건 삭제")
                elif table_name == "player_season_pitching":
                    deleted = session.query(PlayerSeasonPitching).delete()
                    logger.info(f"🧹 player_season_pitching: {deleted:,}건 삭제")
                else:
                    # 다른 테이블은 직접 SQL로 삭제
                    result = session.execute(text(f"DELETE FROM {table_name}"))
                    logger.info(f"🧹 {table_name}: {result.rowcount:,}건 삭제")

            # VACUUM으로 공간 회수
            session.commit()
            session.execute(text("VACUUM"))

            logger.info("✅ SQLite 데이터베이스 초기화 완료")

        except Exception as e:
            session.rollback()
            logger.error(f"❌ 초기화 중 오류 발생: {e}")
            raise
        finally:
            # 외래키 제약조건 복원
            session.execute(text("PRAGMA foreign_keys = ON"))
            session.commit()


def reset_specific_year(year: int, confirm: bool = True):
    """특정 년도 데이터만 삭제"""
    with SessionLocal() as session:
        # 해당 년도 데이터 확인
        batting_count = session.query(PlayerSeasonBatting).filter_by(season=year).count()
        pitching_count = session.query(PlayerSeasonPitching).filter_by(season=year).count()

        logger.info(f"🗃️ {year}년 데이터 현황:")
        logger.info(f"  - 타자: {batting_count:,}건")
        logger.info(f"  - 투수: {pitching_count:,}건")
        logger.info(f"  - 합계: {batting_count + pitching_count:,}건")

        if batting_count == 0 and pitching_count == 0:
            logger.info(f"✅ {year}년 데이터가 없습니다.")
            return

        if confirm:
            response = input(f"\n⚠️ {year}년 데이터를 삭제하시겠습니까? (y/N): ")
            if response.lower() != "y":
                logger.error("❌ 사용자가 취소했습니다.")
                return

        try:
            # 특정 년도 데이터 삭제
            batting_deleted = session.query(PlayerSeasonBatting).filter_by(season=year).delete()
            pitching_deleted = session.query(PlayerSeasonPitching).filter_by(season=year).delete()

            session.commit()

            logger.info(f"🧹 {year}년 데이터 삭제 완료:")
            logger.info(f"  - 타자: {batting_deleted:,}건")
            logger.info(f"  - 투수: {pitching_deleted:,}건")

        except Exception as e:
            session.rollback()
            logger.error(f"❌ 삭제 중 오류 발생: {e}")
            raise


def reset_specific_range(start_year: int, end_year: int, confirm: bool = True):
    """특정 연도 범위 데이터 삭제"""
    with SessionLocal() as session:
        # 해당 범위 데이터 확인
        batting_count = (
            session.query(PlayerSeasonBatting)
            .filter(PlayerSeasonBatting.season >= start_year, PlayerSeasonBatting.season <= end_year)
            .count()
        )

        pitching_count = (
            session.query(PlayerSeasonPitching)
            .filter(PlayerSeasonPitching.season >= start_year, PlayerSeasonPitching.season <= end_year)
            .count()
        )

        logger.info(f"🗃️ {start_year}-{end_year}년 데이터 현황:")
        logger.info(f"  - 타자: {batting_count:,}건")
        logger.info(f"  - 투수: {pitching_count:,}건")
        logger.info(f"  - 합계: {batting_count + pitching_count:,}건")

        if batting_count == 0 and pitching_count == 0:
            logger.info(f"✅ {start_year}-{end_year}년 데이터가 없습니다.")
            return

        if confirm:
            response = input(f"\n⚠️ {start_year}-{end_year}년 데이터를 삭제하시겠습니까? (y/N): ")
            if response.lower() != "y":
                logger.error("❌ 사용자가 취소했습니다.")
                return

        try:
            # 특정 범위 데이터 삭제
            batting_deleted = (
                session.query(PlayerSeasonBatting)
                .filter(PlayerSeasonBatting.season >= start_year, PlayerSeasonBatting.season <= end_year)
                .delete()
            )

            pitching_deleted = (
                session.query(PlayerSeasonPitching)
                .filter(PlayerSeasonPitching.season >= start_year, PlayerSeasonPitching.season <= end_year)
                .delete()
            )

            session.commit()

            logger.info(f"🧹 {start_year}-{end_year}년 데이터 삭제 완료:")
            logger.info(f"  - 타자: {batting_deleted:,}건")
            logger.info(f"  - 투수: {pitching_deleted:,}건")

        except Exception as e:
            session.rollback()
            logger.error(f"❌ 삭제 중 오류 발생: {e}")
            raise


def main():
    parser = argparse.ArgumentParser(description="SQLite 데이터베이스 초기화")
    parser.add_argument("--all", action="store_true", help="모든 플레이어 데이터 삭제")
    parser.add_argument("--year", type=int, help="특정 년도 데이터만 삭제")
    parser.add_argument(
        "--range",
        nargs=2,
        type=int,
        metavar=("START", "END"),
        help="특정 년도 범위 데이터 삭제 (예: --range 2020 2025)",
    )
    parser.add_argument(
        "--tables", nargs="+", choices=["player_season_batting", "player_season_pitching"], help="특정 테이블만 초기화"
    )
    parser.add_argument("--force", action="store_true", help="확인 없이 강제 실행")

    args = parser.parse_args()

    confirm = not args.force

    try:
        if args.all:
            logger.info("🗑️ 전체 플레이어 데이터 초기화")
            reset_sqlite_data(args.tables, confirm)
        elif args.year:
            logger.info(f"🗑️ {args.year}년 데이터 초기화")
            reset_specific_year(args.year, confirm)
        elif args.range:
            start_year, end_year = args.range
            logger.info(f"🗑️ {start_year}-{end_year}년 데이터 초기화")
            reset_specific_range(start_year, end_year, confirm)
        else:
            logger.error("❌ 옵션을 선택해주세요:")
            logger.info("  --all          : 모든 데이터 삭제")
            logger.info("  --year YYYY    : 특정 년도 삭제")
            logger.info("  --range A B    : 특정 범위 삭제")
            logger.info("  --tables T1 T2 : 특정 테이블만")
            logger.info("  --force        : 확인 없이 실행")
            logger.info("\n예시:")
            logger.info("  python3 reset_sqlite.py --all")
            logger.info("  python3 reset_sqlite.py --year 2025")
            logger.info("  python3 reset_sqlite.py --range 2020 2025")
            logger.info("  python3 reset_sqlite.py --all --tables player_season_batting")

    except KeyboardInterrupt:
        logger.error("\n❌ 사용자가 중단했습니다.")
    except Exception as e:  # noqa: BLE001
        logger.error(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()
