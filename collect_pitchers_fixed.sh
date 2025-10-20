#!/bin/bash
# 2025년 전체 투수 데이터 수집 (페이지네이션 개선 버전)

echo "🚀 2025년 KBO 투수 데이터 전체 수집 시작 (개선 버전)"
echo "================================================"

PYTHON="./venv/bin/python3"

# 각 시리즈별로 개별 수집
series_list=("exhibition" "korean_series")  # 시범경기와 한국시리즈만 추가 수집

for series in "${series_list[@]}"; do
    echo ""
    echo "📊 ${series} 시리즈 수집 시작..."
    
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
    fi
    
    echo "⏱️ 3초 대기 중..."
    sleep 3
done

echo ""
echo "================================================"
echo "📈 최종 결과 확인 중..."

$PYTHON -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonPitching
from sqlalchemy import text

with SessionLocal() as session:
    total = session.query(PlayerSeasonPitching).count()
    print(f'\\n📊 SQLite 투수 데이터 총합: {total}건')
    
    result = session.execute(text('SELECT league, COUNT(*) FROM player_season_pitching GROUP BY league'))
    print('\\n리그별 분포:')
    total_expected = 0
    for league, count in result:
        print(f'  {league}: {count}건')
        total_expected += count
    
    print(f'\\n예상 총합: {total_expected}건')
    if total == total_expected:
        print('✅ 데이터 무결성 확인')
    else:
        print('⚠️ 데이터 불일치 발견')
"

echo ""
echo "🎉 전체 투수 데이터 수집 완료!"