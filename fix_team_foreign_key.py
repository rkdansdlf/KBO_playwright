#!/usr/bin/env python3
"""
team_history 테이블 기반 외래키 제약조건 문제 해결
같은 team_code를 여러 시대가 공유하는 문제 해결
"""
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_supabase_connection():
    """Supabase 연결 생성"""
    supabase_url = os.getenv('SUPABASE_DB_URL')
    if not supabase_url:
        raise ValueError("SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")
    
    return psycopg2.connect(supabase_url)


def analyze_team_history():
    """team_history 테이블 분석"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("🔍 team_history 테이블 분석 중...")
        
        # 테이블 구조 확인
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = 'team_history'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print(f"📊 team_history 테이블 컬럼: {len(columns)}개")
        for col_name, data_type, nullable in columns:
            print(f"  - {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        print()
        
        # 중복 team_code 확인
        cursor.execute("""
            SELECT 
                team_code,
                COUNT(*) as count,
                string_agg(team_name, ' / ' ORDER BY start_season) as teams,
                MIN(start_season) as first_year,
                MAX(COALESCE(end_season, 2025)) as last_year
            FROM team_history
            WHERE team_code IS NOT NULL
            GROUP BY team_code
            HAVING COUNT(*) > 1
            ORDER BY team_code;
        """)
        
        duplicates = cursor.fetchall()
        print(f"🔄 중복 team_code: {len(duplicates)}개")
        for team_code, count, teams, first_year, last_year in duplicates:
            print(f"  - {team_code}: {count}개 팀 ({first_year}-{last_year})")
            print(f"    → {teams}")
        
        print()
        
        # 모든 team_code 목록
        cursor.execute("""
            SELECT DISTINCT team_code, COUNT(*) as count
            FROM team_history
            WHERE team_code IS NOT NULL
            GROUP BY team_code
            ORDER BY team_code;
        """)
        
        all_codes = cursor.fetchall()
        print(f"📋 전체 team_code: {len(all_codes)}개")
        for team_code, count in all_codes:
            status = "🔄" if count > 1 else "✅"
            print(f"  {status} {team_code} ({count}개)")
        
        return all_codes, duplicates


def create_solution_options():
    """해결 방안 제시"""
    print("\n🔧 해결 방안 옵션:")
    print("=" * 50)
    
    print("\n📋 옵션 1: 외래키 제약조건 제거 (빠른 해결)")
    print("장점: 즉시 해결, 기존 데이터 구조 유지")
    print("단점: 데이터 무결성 검증 없음")
    print("SQL:")
    print("""
-- 타자 테이블 외래키 제거
ALTER TABLE public.player_season_batting 
DROP CONSTRAINT IF EXISTS fk_player_season_batting_team;

-- 투수 테이블 외래키 제거  
ALTER TABLE public.player_season_pitching 
DROP CONSTRAINT IF EXISTS fk_player_season_pitching_team;
""")
    
    print("\n📋 옵션 2: 외래키를 team_history.id로 변경")
    print("장점: 정확한 시대별 팀 연결, 데이터 무결성 유지")
    print("단점: 기존 team_code를 team_history_id로 변경 필요")
    print("SQL:")
    print("""
-- 타자 테이블에 team_history_id 컬럼 추가
ALTER TABLE public.player_season_batting 
ADD COLUMN team_history_id INTEGER;

-- 투수 테이블에 team_history_id 컬럼 추가
ALTER TABLE public.player_season_pitching 
ADD COLUMN team_history_id INTEGER;

-- 외래키 제약조건 추가
ALTER TABLE public.player_season_batting 
ADD CONSTRAINT fk_player_season_batting_team_history 
FOREIGN KEY (team_history_id) REFERENCES team_history(id);

ALTER TABLE public.player_season_pitching 
ADD CONSTRAINT fk_player_season_pitching_team_history 
FOREIGN KEY (team_history_id) REFERENCES team_history(id);
""")
    
    print("\n📋 옵션 3: teams 테이블에 모든 team_code 추가")
    print("장점: 기존 구조 유지, 간단한 해결")
    print("단점: 중복 코드 문제 해결 안됨")
    
    print("\n📋 옵션 4: 외래키를 NULL 허용으로 변경")
    print("장점: 일부 데이터는 검증, 문제 데이터는 허용")
    print("단점: 불완전한 해결")


def implement_option1():
    """옵션 1: 외래키 제약조건 제거"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("\n🔧 옵션 1 실행: 외래키 제약조건 제거")
        print("-" * 40)
        
        try:
            # 타자 테이블 외래키 제거
            print("1️⃣ 타자 테이블 외래키 제거...")
            cursor.execute("""
                ALTER TABLE public.player_season_batting 
                DROP CONSTRAINT IF EXISTS fk_player_season_batting_team;
            """)
            print("   ✅ 타자 테이블 외래키 제거 완료")
            
            # 투수 테이블 외래키 제거
            print("2️⃣ 투수 테이블 외래키 제거...")
            cursor.execute("""
                ALTER TABLE public.player_season_pitching 
                DROP CONSTRAINT IF EXISTS fk_player_season_pitching_team;
            """)
            print("   ✅ 투수 테이블 외래키 제거 완료")
            
            print("\n✅ 모든 외래키 제약조건 제거 완료!")
            print("💡 이제 ./venv/bin/python3 -m src.sync.supabase_sync 를 실행해보세요.")
            
        except Exception as e:
            print(f"❌ 외래키 제거 실패: {e}")
            raise


def create_teams_from_history():
    """옵션 3: team_history에서 teams 테이블로 모든 team_code 복사"""
    with get_supabase_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("\n🔧 옵션 3 실행: team_history → teams 복사")
        print("-" * 40)
        
        try:
            # teams 테이블 존재 확인
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'teams'
                );
            """)
            
            teams_exists = cursor.fetchone()[0]
            if not teams_exists:
                print("❌ teams 테이블이 존재하지 않습니다.")
                return
            
            # team_history에서 고유 team_code들을 teams에 삽입
            print("1️⃣ team_history에서 고유 team_code 추출 중...")
            cursor.execute("""
                INSERT INTO public.teams (
                    team_code, team_name, team_name_en, city, 
                    founded_year, is_active, description, 
                    created_at, updated_at
                )
                SELECT DISTINCT ON (team_code)
                    team_code,
                    team_name,
                    team_name || ' (Historical)' as team_name_en,
                    city,
                    start_season,
                    (end_season IS NULL) as is_active,
                    'Imported from team_history',
                    NOW(),
                    NOW()
                FROM team_history
                WHERE team_code IS NOT NULL
                ON CONFLICT (team_code) DO NOTHING;
            """)
            
            inserted_count = cursor.rowcount
            print(f"   ✅ {inserted_count}개 팀 코드 추가 완료")
            
            print("\n✅ teams 테이블 업데이트 완료!")
            print("💡 이제 ./venv/bin/python3 -m src.sync.supabase_sync 를 실행해보세요.")
            
        except Exception as e:
            print(f"❌ teams 테이블 업데이트 실패: {e}")
            raise


def main():
    try:
        print("🚀 team_history 기반 외래키 문제 해결")
        print("=" * 50)
        
        # 1. 현재 상태 분석
        all_codes, duplicates = analyze_team_history()
        
        # 2. 해결 방안 제시
        create_solution_options()
        
        # 3. 사용자 선택
        print(f"\n❓ 어떤 해결 방안을 사용하시겠습니까?")
        print("1: 외래키 제약조건 제거 (빠름)")
        print("2: teams 테이블에 모든 team_code 추가")
        print("3: 수동 SQL 실행 안내")
        
        choice = input("선택 (1/2/3): ").strip()
        
        if choice == "1":
            implement_option1()
        elif choice == "2":
            create_teams_from_history()
        elif choice == "3":
            print("\n📝 수동 SQL 실행 방법:")
            print("Supabase 대시보드 → SQL Editor에서 다음 중 하나 실행:")
            print("\n-- 외래키 제거 (추천)")
            print("ALTER TABLE player_season_batting DROP CONSTRAINT IF EXISTS fk_player_season_batting_team;")
            print("ALTER TABLE player_season_pitching DROP CONSTRAINT IF EXISTS fk_player_season_pitching_team;")
        else:
            print("❌ 잘못된 선택입니다.")
            
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()