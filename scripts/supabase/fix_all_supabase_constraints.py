#!/usr/bin/env python3
"""
Supabase 모든 테이블 제약조건 문제 해결 스크립트
타자/투수 테이블 모두 확인 및 수정
"""


import logging
logger = logging.getLogger(__name__)

import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_supabase_connection():
    """Supabase 연결 생성"""
    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        raise ValueError("SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")

    return psycopg2.connect(supabase_url)


def check_table_constraints(table_name):
    """특정 테이블의 제약조건 확인"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        logger.info(f"🔍 {table_name} 테이블 제약조건 확인 중...")

        # 제약조건 조회
        cursor.execute(
            """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
            WHERE
                tc.table_name = %s
                AND tc.table_schema = 'public'
            GROUP BY tc.constraint_name, tc.constraint_type
            ORDER BY tc.constraint_type, tc.constraint_name;
        """,
            (table_name,),
        )

        constraints = cursor.fetchall()

        logger.info(f"📊 {table_name} 제약조건: {len(constraints)}개")
        for name, ctype, columns in constraints:
            logger.info(f"  - {name} ({ctype}): {columns}")

        return constraints


def check_table_exists(table_name):
    """테이블 존재 확인"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """,
            (table_name,),
        )

        exists = cursor.fetchone()[0]
        logger.info(f"📊 {table_name} 테이블: {'존재' if exists else '존재하지 않음'}")
        return exists


def fix_batting_table_constraints():
    """타자 테이블 제약조건 수정"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        logger.info("\n🔧 player_season_batting 테이블 제약조건 수정 중...")

        try:
            # 1. 기존 유니크 제약조건 확인 및 삭제
            cursor.execute("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = 'player_season_batting'
                AND constraint_type = 'UNIQUE'
                AND constraint_name = 'uq_player_season_batting';
            """)

            existing_constraint = cursor.fetchone()
            if existing_constraint:
                logger.warning(f"   ⚠️ 기존 제약조건 발견: {existing_constraint[0]}")
                cursor.execute(
                    "ALTER TABLE public.player_season_batting DROP CONSTRAINT IF EXISTS uq_player_season_batting;"
                )
                logger.info("   ✅ 기존 제약조건 삭제 완료")
            else:
                logger.info("   ℹ️ 기존 유니크 제약조건 없음")

            # 2. 새 유니크 제약조건 추가
            logger.info("   🔗 새 유니크 제약조건 추가...")
            cursor.execute("""
                ALTER TABLE public.player_season_batting
                ADD CONSTRAINT uq_player_season_batting
                UNIQUE (player_id, season, league, level);
            """)
            logger.info("   ✅ 타자 테이블 유니크 제약조건 추가 완료")

            # 3. 인덱스 추가
            cursor.execute("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'player_season_batting'
                AND indexname = 'idx_player_season_batting_lookup';
            """)

            if not cursor.fetchone():
                cursor.execute("""
                    CREATE INDEX idx_player_season_batting_lookup
                    ON public.player_season_batting (player_id, season, league);
                """)
                logger.info("   ✅ 타자 테이블 조회용 인덱스 추가 완료")
            else:
                logger.info("   ℹ️ 타자 테이블 조회용 인덱스 이미 존재")

        except Exception as e:
            logger.error(f"   ❌ 타자 테이블 제약조건 수정 실패: {e}")
            raise


def fix_pitching_table_constraints():
    """투수 테이블 제약조건 수정"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        logger.info("\n🔧 player_season_pitching 테이블 제약조건 수정 중...")

        try:
            # 1. 기존 유니크 제약조건 확인 및 삭제
            cursor.execute("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = 'player_season_pitching'
                AND constraint_type = 'UNIQUE'
                AND constraint_name = 'uq_player_season_pitching';
            """)

            existing_constraint = cursor.fetchone()
            if existing_constraint:
                logger.warning(f"   ⚠️ 기존 제약조건 발견: {existing_constraint[0]}")
                cursor.execute(
                    "ALTER TABLE public.player_season_pitching DROP CONSTRAINT IF EXISTS uq_player_season_pitching;"
                )
                logger.info("   ✅ 기존 제약조건 삭제 완료")
            else:
                logger.info("   ℹ️ 기존 유니크 제약조건 없음")

            # 2. 새 유니크 제약조건 추가
            logger.info("   🔗 새 유니크 제약조건 추가...")
            cursor.execute("""
                ALTER TABLE public.player_season_pitching
                ADD CONSTRAINT uq_player_season_pitching
                UNIQUE (player_id, season, league, level);
            """)
            logger.info("   ✅ 투수 테이블 유니크 제약조건 추가 완료")

            # 3. 인덱스 추가
            cursor.execute("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'player_season_pitching'
                AND indexname = 'idx_player_season_pitching_lookup';
            """)

            if not cursor.fetchone():
                cursor.execute("""
                    CREATE INDEX idx_player_season_pitching_lookup
                    ON public.player_season_pitching (player_id, season, league);
                """)
                logger.info("   ✅ 투수 테이블 조회용 인덱스 추가 완료")
            else:
                logger.info("   ℹ️ 투수 테이블 조회용 인덱스 이미 존재")

        except Exception as e:
            logger.error(f"   ❌ 투수 테이블 제약조건 수정 실패: {e}")
            raise


def verify_all_constraints():
    """모든 테이블 제약조건 최종 확인"""
    logger.info("\n🔍 최종 제약조건 확인...")

    tables = ["player_season_batting", "player_season_pitching"]
    all_good = True

    for table in tables:
        if not check_table_exists(table):
            logger.error(f"❌ {table} 테이블이 존재하지 않습니다!")
            all_good = False
            continue

        constraints = check_table_constraints(table)

        # 유니크 제약조건 확인
        unique_constraints = [c for c in constraints if c[1] == "UNIQUE"]
        expected_unique = f"uq_{table}"

        found_expected = any(c[0] == expected_unique for c in unique_constraints)

        if found_expected:
            logger.info(f"   ✅ {table}: 유니크 제약조건 정상")
        else:
            logger.error(f"   ❌ {table}: 유니크 제약조건 없음")
            all_good = False

    return all_good


def main():
    try:
        logger.info("🚀 Supabase 모든 테이블 제약조건 문제 해결")
        logger.info("=" * 60)

        # 1. 현재 상태 확인
        tables = ["player_season_batting", "player_season_pitching"]
        for table in tables:
            if check_table_exists(table):
                check_table_constraints(table)
            else:
                logger.error(f"❌ {table} 테이블이 존재하지 않습니다!")
                return

        # 2. 타자 테이블 제약조건 수정
        fix_batting_table_constraints()

        # 3. 투수 테이블 제약조건 수정
        fix_pitching_table_constraints()

        # 4. 최종 확인
        success = verify_all_constraints()

        if success:
            logger.info("\n🎉 모든 테이블 제약조건 문제 해결 완료!")
            logger.info("\n💡 이제 다음 명령으로 데이터 동기화를 시도해보세요:")
            logger.info("   ./venv/bin/python3 -m src.sync.supabase_sync")
        else:
            logger.warning("\n⚠️ 일부 테이블에 문제가 있을 수 있습니다.")

    except Exception as e:
        logger.error(f"\n❌ 오류 발생: {e}")
        logger.info("\n💡 수동 해결 방법:")
        logger.info("   1. Supabase 대시보드에서 SQL 편집기 열기")
        logger.info("   2. 다음 SQL 실행:")
        logger.info("      -- 타자 테이블")
        print(
            "      ALTER TABLE player_season_batting ADD CONSTRAINT uq_player_season_batting UNIQUE (player_id, season, league, level);"
        )
        logger.info("      -- 투수 테이블")
        print(
            "      ALTER TABLE player_season_pitching ADD CONSTRAINT uq_player_season_pitching UNIQUE (player_id, season, league, level);"
        )


if __name__ == "__main__":
    main()
