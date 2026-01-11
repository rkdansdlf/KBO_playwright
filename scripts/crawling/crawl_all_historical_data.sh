#!/bin/bash
# KBO 전체 역사적 데이터 크롤링 스크립트
# 타자 + 투수 모든 년도, 모든 시즌 데이터 수집

echo "🚀 KBO 전체 역사적 데이터 크롤링 시작"
echo "========================================================"

# 가상환경 확인
if [ ! -d "venv" ]; then
    echo "❌ venv 디렉토리가 없습니다. 가상환경을 먼저 설정하세요."
    exit 1
fi

# Python 경로 설정
PYTHON="./venv/bin/python3"

# 크롤링 연도 범위 설정 (KBO 출범 1982년부터 현재까지)
START_YEAR=1982
CURRENT_YEAR=$(date +%Y)
END_YEAR=${1:-$CURRENT_YEAR}  # 첫 번째 인자가 있으면 사용, 없으면 현재 년도

echo "📅 크롤링 대상 기간: ${START_YEAR}년 ~ ${END_YEAR}년"
echo "⚡ 예상 소요시간: 약 $((($END_YEAR - $START_YEAR + 1) * 20))분 (년도당 ~20분)"

# 시리즈 목록 (우선순위 순서)
series_list=(
    "regular"        # 정규시즌 (가장 중요)
    "exhibition"     # 시범경기
    "wildcard"       # 와일드카드
    "semi_playoff"   # 준플레이오프
    "playoff"        # 플레이오프
    "korean_series"  # 한국시리즈
)

# 전체 통계 변수
total_batting_count=0
total_pitching_count=0
total_errors=0
failed_years=()

echo ""
echo "🎯 크롤링 전략:"
echo "  1. 타자 데이터 우선 수집 (모든 년도/시리즈)"
echo "  2. 투수 데이터 수집 (모든 년도/시리즈)"
echo "  3. 각 년도별 3초 대기 (서버 부하 방지)"
echo "  4. 에러 발생시 계속 진행 (중단 방지)"
echo "  5. 최종 결과 요약 및 Supabase 동기화 안내"
echo ""

read -p "계속 진행하시겠습니까? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ 사용자가 취소했습니다."
    exit 1
fi

echo ""
echo "🏏" + "=" * 60
echo "🏏 1단계: 타자 데이터 전체 크롤링"
echo "🏏" + "=" * 60

# 타자 데이터 크롤링
for year in $(seq $START_YEAR $END_YEAR); do
    echo ""
    echo "📊 ${year}년 타자 데이터 크롤링 시작..."
    
    year_batting_count=0
    year_errors=0
    
    for series in "${series_list[@]}"; do
        echo "  ▶ ${year}년 ${series} 시리즈 타자 크롤링..."
        
        $PYTHON -m src.crawlers.player_batting_all_series_crawler \
            --year $year \
            --series $series \
            --save \
            --headless 2>/dev/null
        
        exit_code=$?
        
        if [ $exit_code -eq 0 ]; then
            echo "    ✅ ${year}년 ${series} 타자 완료"
            ((year_batting_count++))
        else
            echo "    ⚠️ ${year}년 ${series} 타자 실패 (exit: $exit_code)"
            ((year_errors++))
        fi
        
        # 시리즈 간 짧은 대기
        sleep 1
    done
    
    echo "  📋 ${year}년 타자 결과: 성공 ${year_batting_count}/${#series_list[@]} 시리즈"
    total_batting_count=$((total_batting_count + year_batting_count))
    total_errors=$((total_errors + year_errors))
    
    if [ $year_errors -gt 3 ]; then
        failed_years+=("${year}년 타자")
    fi
    
    # 년도별 대기 (서버 부하 방지)
    echo "  ⏱️ 3초 대기 중..."
    sleep 3
done

echo ""
echo "⚾" + "=" * 60  
echo "⚾ 2단계: 투수 데이터 전체 크롤링"
echo "⚾" + "=" * 60

# 투수 데이터 크롤링
for year in $(seq $START_YEAR $END_YEAR); do
    echo ""
    echo "📊 ${year}년 투수 데이터 크롤링 시작..."
    
    year_pitching_count=0
    year_errors=0
    
    for series in "${series_list[@]}"; do
        echo "  ▶ ${year}년 ${series} 시리즈 투수 크롤링..."
        
        $PYTHON -m src.crawlers.player_pitching_all_series_crawler \
            --year $year \
            --series $series \
            --save \
            --headless 2>/dev/null
        
        exit_code=$?
        
        if [ $exit_code -eq 0 ]; then
            echo "    ✅ ${year}년 ${series} 투수 완료"
            ((year_pitching_count++))
        else
            echo "    ⚠️ ${year}년 ${series} 투수 실패 (exit: $exit_code)"
            ((year_errors++))
        fi
        
        # 시리즈 간 짧은 대기
        sleep 1
    done
    
    echo "  📋 ${year}년 투수 결과: 성공 ${year_pitching_count}/${#series_list[@]} 시리즈"
    total_pitching_count=$((total_pitching_count + year_pitching_count))
    total_errors=$((total_errors + year_errors))
    
    if [ $year_errors -gt 3 ]; then
        failed_years+=("${year}년 투수")
    fi
    
    # 년도별 대기 (서버 부하 방지)
    echo "  ⏱️ 3초 대기 중..."
    sleep 3
done

echo ""
echo "=" * 70
echo "📈 전체 크롤링 완료! 최종 결과 확인"
echo "=" * 70

# 최종 SQLite 데이터 확인
echo ""
echo "🔍 SQLite 데이터베이스 최종 현황:"

$PYTHON -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import text, func

with SessionLocal() as session:
    # 전체 데이터 수
    batting_total = session.query(PlayerSeasonBatting).count()
    pitching_total = session.query(PlayerSeasonPitching).count()
    
    print(f'  📊 총 데이터:')
    print(f'    - 타자 기록: {batting_total:,}건')
    print(f'    - 투수 기록: {pitching_total:,}건')
    print(f'    - 전체 합계: {batting_total + pitching_total:,}건')
    
    # 년도별 분포
    print(f'\\n  📅 년도별 타자 분포:')
    batting_by_year = session.query(
        PlayerSeasonBatting.season, 
        func.count(PlayerSeasonBatting.id)
    ).group_by(PlayerSeasonBatting.season).order_by(PlayerSeasonBatting.season).all()
    
    for year, count in batting_by_year[-10:]:  # 최근 10년만
        print(f'    {year}년: {count:,}건')
    
    print(f'\\n  📅 년도별 투수 분포:')
    pitching_by_year = session.query(
        PlayerSeasonPitching.season, 
        func.count(PlayerSeasonPitching.id)
    ).group_by(PlayerSeasonPitching.season).order_by(PlayerSeasonPitching.season).all()
    
    for year, count in pitching_by_year[-10:]:  # 최근 10년만
        print(f'    {year}년: {count:,}건')
    
    # 리그별 분포
    print(f'\\n  🏆 리그별 타자 분포:')
    batting_by_league = session.query(
        PlayerSeasonBatting.league, 
        func.count(PlayerSeasonBatting.id)
    ).group_by(PlayerSeasonBatting.league).all()
    
    for league, count in batting_by_league:
        print(f'    {league}: {count:,}건')
    
    print(f'\\n  🏆 리그별 투수 분포:')
    pitching_by_league = session.query(
        PlayerSeasonPitching.league, 
        func.count(PlayerSeasonPitching.id)
    ).group_by(PlayerSeasonPitching.league).all()
    
    for league, count in pitching_by_league:
        print(f'    {league}: {count:,}건')
"

echo ""
echo "📊 크롤링 통계 요약:"
echo "  - 타겟 기간: ${START_YEAR}~${END_YEAR}년 ($((END_YEAR - START_YEAR + 1))년간)"
echo "  - 타자 성공 시리즈: ${total_batting_count}개"
echo "  - 투수 성공 시리즈: ${total_pitching_count}개"
echo "  - 총 에러 수: ${total_errors}개"

if [ ${#failed_years[@]} -gt 0 ]; then
    echo ""
    echo "⚠️ 다수 실패 년도:"
    for failed in "${failed_years[@]}"; do
        echo "  - $failed"
    done
fi

echo ""
echo "=" * 70
echo "🎉 전체 역사적 데이터 크롤링 완료!"
echo "=" * 70

echo ""
echo "💡 다음 단계 (Supabase 동기화):"
echo "  1. 환경변수 설정:"
echo "     export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'"
echo ""
echo "  2. Supabase 동기화 실행:"
echo "     ./venv/bin/python3 -m src.sync.supabase_sync"
echo ""
echo "  3. 수동 재크롤링 (실패한 년도가 있는 경우):"
echo "     ./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year YYYY --series regular --save"
echo "     ./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler --year YYYY --series regular --save"

echo ""
echo "📝 로그 확인:"
echo "  - 실시간 로그: tail -f logs/scheduler.log"
echo "  - 데이터 검증: ./venv/bin/python3 verify_data_separation.py"

echo ""
echo "🎯 크롤링이 모두 완료되었습니다!"