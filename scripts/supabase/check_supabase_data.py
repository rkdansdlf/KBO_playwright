#!/usr/bin/env python3
"""
Supabase 데이터 현황 확인 스크립트
기존 데이터 상태를 파악하여 안전한 작업 방향 제시
"""

import os

from sqlalchemy import create_engine, text


def check_supabase_data():
    """Supabase 데이터베이스 현재 상태 확인"""
    supabase_url = os.getenv("SUPABASE_DB_URL")

    if not supabase_url:
        print("❌ SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")
        print("📌 먼저 환경변수를 설정하세요:")
        print("   export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'")
        return False

    try:
        engine = create_engine(supabase_url)

        with engine.connect() as conn:
            print("✅ Supabase 연결 성공!")
            print("\n" + "=" * 60)
            print("📊 Supabase 데이터베이스 현황")
            print("=" * 60)

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

            print("\n🔍 관련 테이블 존재 여부:")
            for table in ["player_season_batting", "player_season_pitching"]:
                if table in existing_tables:
                    print(f"   ✅ {table}: 존재함")
                else:
                    print(f"   ❌ {table}: 존재하지 않음")

            # 2. 각 테이블별 데이터 현황
            for table in existing_tables:
                print(f"\n📋 {table} 테이블 현황:")

                # 총 레코드 수
                count_query = text(f"SELECT COUNT(*) FROM {table}")
                total_count = conn.execute(count_query).scalar()
                print(f"   총 레코드 수: {total_count:,}건")

                if total_count > 0:
                    # 시즌별 분포
                    season_query = text(f"""
                        SELECT season, COUNT(*) as count
                        FROM {table}
                        GROUP BY season
                        ORDER BY season DESC
                        LIMIT 10
                    """)

                    seasons_result = conn.execute(season_query)
                    print("   시즌별 분포:")
                    for season, count in seasons_result:
                        print(f"     {season}년: {count:,}건")

                    # 리그별 분포
                    league_query = text(f"""
                        SELECT league, COUNT(*) as count
                        FROM {table}
                        GROUP BY league
                        ORDER BY count DESC
                    """)

                    leagues_result = conn.execute(league_query)
                    print("   리그별 분포:")
                    for league, count in leagues_result:
                        print(f"     {league}: {count:,}건")

                    # 소스별 분포
                    source_query = text(f"""
                        SELECT source, COUNT(*) as count
                        FROM {table}
                        GROUP BY source
                        ORDER BY count DESC
                    """)

                    sources_result = conn.execute(source_query)
                    print("   소스별 분포:")
                    for source, count in sources_result:
                        print(f"     {source}: {count:,}건")

                    # 샘플 데이터 표시
                    sample_query = text(f"""
                        SELECT player_id, season, league, level, source
                        FROM {table}
                        ORDER BY season DESC, player_id
                        LIMIT 3
                    """)

                    sample_result = conn.execute(sample_query)
                    print("   샘플 데이터:")
                    for row in sample_result:
                        print(
                            f"     player_id={row[0]}, season={row[1]}, league={row[2]}, level={row[3]}, source={row[4]}"
                        )

            # 3. 권장 작업 방향 제시
            print("\n" + "=" * 60)
            print("💡 권장 작업 방향")
            print("=" * 60)

            if "player_season_batting" in existing_tables:
                batting_count = conn.execute(text("SELECT COUNT(*) FROM player_season_batting")).scalar()
                print(f"✅ player_season_batting 테이블 존재 ({batting_count:,}건)")
                print("   → 타자 크롤링 시 UPSERT 방식으로 안전하게 업데이트 가능")
            else:
                print("❌ player_season_batting 테이블 없음")
                print("   → 타자 데이터 신규 생성 필요")

            if "player_season_pitching" in existing_tables:
                pitching_count = conn.execute(text("SELECT COUNT(*) FROM player_season_pitching")).scalar()
                print(f"✅ player_season_pitching 테이블 존재 ({pitching_count:,}건)")
                print("   → 투수 크롤링 시 UPSERT 방식으로 안전하게 업데이트 가능")
            else:
                print("❌ player_season_pitching 테이블 없음")
                print("   → 투수 데이터 신규 생성 필요")

            print("\n📌 다음 단계:")
            print("1. SQLite에서 크롤링 및 검증")
            print("2. 검증된 데이터만 Supabase에 UPSERT")
            print("3. 기존 데이터와 충돌 시 source 필드로 구분")

            return True

    except Exception as e:
        print(f"❌ Supabase 연결 실패: {e}")
        return False


if __name__ == "__main__":
    check_supabase_data()
