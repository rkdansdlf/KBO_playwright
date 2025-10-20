#!/usr/bin/env python3
"""
투수 데이터 Supabase 동기화 테스트
"""
import os
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting

def test_supabase_available():
    """Supabase 연결 가능 여부 확인"""
    supabase_url = os.getenv('SUPABASE_DB_URL')
    
    if not supabase_url:
        print("❌ SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")
        print("📌 Supabase 동기화를 위해서는 다음 명령어로 환경변수를 설정하세요:")
        print("   export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'")
        return False
    
    try:
        from sqlalchemy import create_engine
        engine = create_engine(supabase_url)
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            print("✅ Supabase 연결 성공!")
            return True
    except Exception as e:
        print(f"❌ Supabase 연결 실패: {e}")
        return False

def show_sqlite_pitcher_data():
    """SQLite의 투수 데이터 현황 표시"""
    with SessionLocal() as session:
        pitcher_data = session.query(PlayerSeasonBatting).filter(
            PlayerSeasonBatting.source.like('PITCHER_%')
        ).all()
        
        print(f"\n📊 SQLite에 저장된 투수 데이터: {len(pitcher_data)}건")
        
        if pitcher_data:
            print("투수 데이터 샘플:")
            for i, data in enumerate(pitcher_data[:3]):
                print(f"  {i+1}. player_id: {data.player_id}, season: {data.season}")
                print(f"     게임수: {data.games}, 이닝: {data.plate_appearances}, 삼진: {data.hits}")
                
                # extra_stats에서 투수 데이터 확인
                if data.extra_stats and 'pitcher_data' in data.extra_stats:
                    pitcher_stats = data.extra_stats['pitcher_data']
                    print(f"     승패: {pitcher_stats.get('wins', 'N/A')}-{pitcher_stats.get('losses', 'N/A')}, ERA: {pitcher_stats.get('era', 'N/A')}")
                print()

def manual_sync_instructions():
    """수동 동기화 방법 안내"""
    print("\n📋 Supabase 수동 동기화 방법:")
    print("1. Supabase 환경변수 설정:")
    print("   export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'")
    print("\n2. 기존 동기화 스크립트 실행:")
    print("   ./venv/bin/python3 src/sync/supabase_sync.py")
    print("\n3. 또는 Supabase 대시보드에서 직접 SQL 실행:")
    print("   - 대시보드 → SQL Editor")
    print("   - SQLite 데이터를 CSV로 내보내서 Supabase에 업로드")

if __name__ == "__main__":
    print("🔄 투수 데이터 Supabase 동기화 테스트")
    print("=" * 50)
    
    # SQLite 데이터 확인
    show_sqlite_pitcher_data()
    
    # Supabase 연결 테스트
    if not test_supabase_available():
        manual_sync_instructions()
    else:
        print("\n✅ Supabase 연결이 확인되었습니다!")
        print("📌 이제 src/sync/supabase_sync.py를 실행하여 동기화할 수 있습니다.")