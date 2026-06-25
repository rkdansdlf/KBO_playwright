#!/usr/bin/env python3
"""Supabase 테이블 제약조건 및 구조 확인 스크립트"""

import logging

logger = logging.getLogger(__name__)

import os

from sqlalchemy import create_engine, text


def check_supabase_structure():
    """Supabase 테이블 구조 및 제약조건 확인"""
    supabase_url = os.getenv("SUPABASE_DB_URL")

    if not supabase_url:
        logger.error("❌ SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")
        return False

    try:
        engine = create_engine(supabase_url)

        with engine.connect() as conn:
            logger.info("✅ Supabase 연결 성공!")
            logger.info("\n%s", "=" * 60)
            logger.info("📊 Supabase 테이블 구조 분석")
            logger.info("=" * 60)

            # 1. 테이블 존재 여부 확인
            tables_query = text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('player_season_batting', 'player_season_pitching')
                ORDER BY table_name
            """)

            tables_result = conn.execute(tables_query)
            existing_tables = [row[0] for row in tables_result]

            logger.info("\n🔍 관련 테이블:")
            for table in ["player_season_batting", "player_season_pitching"]:
                if table in existing_tables:
                    logger.info(f"   ✅ {table}: 존재함")
                else:
                    logger.error(f"   ❌ {table}: 존재하지 않음")

            # 2. 각 테이블의 제약조건 확인
            for table in existing_tables:
                logger.info(f"\n📋 {table} 테이블 제약조건:")

                constraints_query = text("""
                    SELECT
                        conname as constraint_name,
                        contype as constraint_type,
                        pg_get_constraintdef(oid) as constraint_definition
                    FROM pg_constraint
                    WHERE conrelid = (
                        SELECT oid FROM pg_class
                        WHERE relname = :table_name
                        AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
                    )
                    ORDER BY conname
                """)

                constraints_result = conn.execute(constraints_query, {"table_name": table})
                constraints = constraints_result.fetchall()

                if constraints:
                    for constraint in constraints:
                        constraint_type_map = {"p": "PRIMARY KEY", "u": "UNIQUE", "f": "FOREIGN KEY", "c": "CHECK"}
                        type_desc = constraint_type_map.get(constraint[1], constraint[1])
                        logger.info(f"   - {constraint[0]} ({type_desc})")
                        logger.info(f"     정의: {constraint[2]}")
                else:
                    logger.info("   제약조건이 없습니다.")

                # 3. 테이블 컬럼 정보
                columns_query = text("""
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                    ORDER BY ordinal_position
                """)

                columns_result = conn.execute(columns_query, {"table_name": table})
                columns = columns_result.fetchall()

                logger.info(f"\n📋 {table} 테이블 컬럼:")
                for col in columns[:10]:  # 처음 10개만 표시
                    nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                    default = f"DEFAULT {col[3]}" if col[3] else ""
                    logger.info(f"   - {col[0]}: {col[1]} {nullable} {default}")
                if len(columns) > 10:
                    logger.info(f"   ... 총 {len(columns)}개 컬럼")

            # 4. 권장 해결 방법
            logger.info("\n%s", "=" * 60)
            logger.info("💡 UPSERT 해결 방안")
            logger.info("=" * 60)

            for table in existing_tables:
                if table == "player_season_batting":
                    logger.info(f"\n🏏 {table} 테이블:")
                    logger.info("   다음 중 하나의 방법 사용:")
                    logger.info("   1. 유니크 인덱스명 확인 후 ON CONFLICT 수정")
                    logger.info("   2. ON CONFLICT (player_id, season, league, level) 사용")
                    logger.info("   3. INSERT ... ON DUPLICATE KEY UPDATE (MySQL 방식)")

                elif table == "player_season_pitching":
                    logger.info(f"\n⚾ {table} 테이블:")
                    logger.info("   다음 중 하나의 방법 사용:")
                    logger.info("   1. 유니크 인덱스명 확인 후 ON CONFLICT 수정")
                    logger.info("   2. ON CONFLICT (player_id, season, league, level) 사용")

            return True

    except Exception:
        logger.exception("❌ Supabase 연결 또는 쿼리 실패")
        return False


if __name__ == "__main__":
    check_supabase_structure()
