#!/usr/bin/env python3
"""
Supabase 투수 테이블 제약조건 문제 해결 스크립트
"""

import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_supabase_connection():
    """Supabase 연결 생성"""
    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        raise ValueError("SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")

    return psycopg2.connect(supabase_url)


def check_existing_constraints():
    """기존 제약조건 확인"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        print("🔍 player_season_pitching 테이블 제약조건 확인 중...")

        # 제약조건 조회
        cursor.execute("""
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                tc.is_deferrable,
                tc.initially_deferred
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
            WHERE
                tc.table_name = 'player_season_pitching'
                AND tc.table_schema = 'public'
            ORDER BY tc.constraint_name;
        """)

        constraints = cursor.fetchall()

        print(f"📊 발견된 제약조건: {len(constraints)}개")
        for constraint in constraints:
            name, ctype, column, deferrable, deferred = constraint
            print(f"  - {name} ({ctype}): {column}")

        return constraints


def check_table_structure():
    """테이블 구조 확인"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        print("\n🔍 테이블 구조 확인 중...")

        # 테이블 존재 확인
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'player_season_pitching'
            );
        """)

        table_exists = cursor.fetchone()[0]
        print(f"📊 player_season_pitching 테이블: {'존재' if table_exists else '존재하지 않음'}")

        if table_exists:
            # 컬럼 확인
            cursor.execute("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = 'player_season_pitching'
                ORDER BY ordinal_position;
            """)

            columns = cursor.fetchall()
            print(f"📊 테이블 컬럼: {len(columns)}개")
            for col_name, data_type, nullable, default in columns[:10]:  # 처음 10개만
                print(f"  - {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")

            if len(columns) > 10:
                print(f"  ... 및 {len(columns) - 10}개 더")

        return table_exists


def fix_constraint_issue():
    """제약조건 문제 해결"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        print("\n🔧 제약조건 문제 해결 중...")

        try:
            # 1. 기존 유니크 제약조건 삭제 (있다면)
            print("1️⃣ 기존 유니크 제약조건 확인 및 삭제...")

            # uq_player_season_pitching 제약조건 존재 확인
            cursor.execute("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_name = 'player_season_pitching'
                AND constraint_type = 'UNIQUE'
                AND constraint_name = 'uq_player_season_pitching';
            """)

            existing_constraint = cursor.fetchone()
            if existing_constraint:
                print(f"   ⚠️ 기존 제약조건 발견: {existing_constraint[0]}")
                cursor.execute(
                    "ALTER TABLE public.player_season_pitching DROP CONSTRAINT IF EXISTS uq_player_season_pitching;"
                )
                print("   ✅ 기존 제약조건 삭제 완료")
            else:
                print("   ℹ️ 기존 유니크 제약조건 없음")

            # 2. 새 유니크 제약조건 추가
            print("2️⃣ 새 유니크 제약조건 추가...")
            cursor.execute("""
                ALTER TABLE public.player_season_pitching
                ADD CONSTRAINT uq_player_season_pitching
                UNIQUE (player_id, season, league, level);
            """)
            print("   ✅ 새 유니크 제약조건 추가 완료")

            # 3. 인덱스 추가 (성능 향상)
            print("3️⃣ 인덱스 추가...")

            # 기존 인덱스 확인
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
                print("   ✅ 조회용 인덱스 추가 완료")
            else:
                print("   ℹ️ 조회용 인덱스 이미 존재")

            print("\n✅ 모든 제약조건 문제 해결 완료!")

        except Exception as e:
            print(f"❌ 제약조건 수정 중 오류: {e}")
            raise


def verify_final_state():
    """최종 상태 확인"""
    print("\n🔍 최종 상태 확인...")

    constraints = check_existing_constraints()

    # 유니크 제약조건 확인
    unique_constraints = [c for c in constraints if c[1] == "UNIQUE"]
    if unique_constraints:
        print("\n✅ 유니크 제약조건:")
        for constraint in unique_constraints:
            print(f"  - {constraint[0]}: {constraint[2]}")
    else:
        print("\n⚠️ 유니크 제약조건이 없습니다!")

    # 기본키 확인
    pk_constraints = [c for c in constraints if c[1] == "PRIMARY KEY"]
    if pk_constraints:
        print("\n✅ 기본키:")
        for constraint in pk_constraints:
            print(f"  - {constraint[0]}: {constraint[2]}")

    return len(unique_constraints) > 0


def main():
    try:
        print("🚀 Supabase 투수 테이블 제약조건 문제 해결")
        print("=" * 50)

        # 1. 현재 상태 확인
        table_exists = check_table_structure()
        if not table_exists:
            print("❌ player_season_pitching 테이블이 존재하지 않습니다.")
            return

        constraints = check_existing_constraints()

        # 2. 문제 해결
        fix_constraint_issue()

        # 3. 최종 확인
        success = verify_final_state()

        if success:
            print("\n🎉 제약조건 문제 해결 완료!")
            print("\n💡 이제 다음 명령으로 데이터 동기화를 시도해보세요:")
            print("   ./venv/bin/python3 -m src.sync.supabase_sync")
        else:
            print("\n⚠️ 제약조건 설정에 문제가 있을 수 있습니다.")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        print("\n💡 수동 해결 방법:")
        print("   1. Supabase 대시보드에서 SQL 편집기 열기")
        print("   2. 다음 SQL 실행:")
        print("      DROP CONSTRAINT IF EXISTS uq_player_season_pitching;")
        print(
            "      ALTER TABLE player_season_pitching ADD CONSTRAINT uq_player_season_pitching UNIQUE (player_id, season, league, level);"
        )


if __name__ == "__main__":
    main()
