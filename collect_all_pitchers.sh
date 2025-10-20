#!/bin/bash
# 2025년 전체 투수 데이터 안전 수집 스크립트

echo "🚀 2025년 KBO 투수 데이터 전체 수집 시작"
echo "================================================"

# 가상환경 확인
if [ ! -d "venv" ]; then
    echo "❌ venv 디렉토리가 없습니다. 가상환경을 먼저 설정하세요."
    exit 1
fi

# Python 경로 설정
PYTHON="./venv/bin/python3"

# 각 시리즈별로 개별 수집 (안정성을 위해)
series_list=("regular" "exhibition" "wildcard" "semi_playoff" "playoff" "korean_series")

for series in "${series_list[@]}"; do
    echo ""
    echo "📊 ${series} 시리즈 수집 시작..."
    
    # 각 시리즈 개별 실행
    $PYTHON -m src.crawlers.player_pitching_all_series_crawler \
        --year 2025 \
        --series $series \
        --save \
        --headless
    
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ ${series} 시리즈 수집 완료"
    else
        echo "⚠️ ${series} 시리즈 수집 중 오류 발생 (exit code: $exit_code)"
        echo "계속 진행합니다..."
    fi
    
    # 서버 부하 방지를 위한 대기
    echo "⏱️ 3초 대기 중..."
    sleep 3
done

echo ""
echo "================================================"
echo "📈 전체 수집 완료! 결과 확인 중..."

# 최종 결과 확인
$PYTHON -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonPitching
from sqlalchemy import text

with SessionLocal() as session:
    total = session.query(PlayerSeasonPitching).count()
    print(f'\\n📊 SQLite 투수 데이터 총합: {total}건')
    
    # 리그별 분포 확인
    result = session.execute(text('SELECT league, COUNT(*) FROM player_season_pitching GROUP BY league'))
    print('\\n리그별 분포:')
    for league, count in result:
        print(f'  {league}: {count}건')
    
    # 최근 수집된 데이터 확인
    latest = session.query(PlayerSeasonPitching).order_by(PlayerSeasonPitching.id.desc()).limit(3).all()
    print('\\n최근 수집 데이터:')
    for p in latest:
        print(f'  player_id: {p.player_id}, league: {p.league}, wins: {p.wins}, era: {p.era}')
"

echo ""
echo "🎉 모든 투수 데이터 수집이 완료되었습니다!"
echo "다음 단계: Supabase 동기화"
echo "  export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'"
echo "  ./venv/bin/python3 -m src.sync.supabase_sync"