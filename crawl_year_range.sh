#!/bin/bash
# KBO 연도 범위 지정 크롤링 스크립트
# 사용법: ./crawl_year_range.sh [시작년도] [끝년도] [모드]

echo "🚀 KBO 연도 범위 지정 크롤링 스크립트"
echo "=========================================="

# 사용법 출력 함수
show_usage() {
    echo "사용법: $0 [시작년도] [끝년도] [모드]"
    echo ""
    echo "예시:"
    echo "  $0                    # 2024-2025년 빠른 모드"
    echo "  $0 2020 2025         # 2020-2025년 빠른 모드"
    echo "  $0 2022 2025 full    # 2022-2025년 완전 모드"
    echo "  $0 1982 2025 full    # 전체 역사 완전 모드"
    echo ""
    echo "모드:"
    echo "  fast (기본값): 정규시즌 + 한국시리즈 + 플레이오프"
    echo "  full: 모든 시리즈 (시범경기 포함)"
    echo ""
    exit 1
}

# 도움말 요청 확인
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_usage
fi

# 가상환경 확인
if [ ! -d "venv" ]; then
    echo "❌ venv 디렉토리가 없습니다. 가상환경을 먼저 설정하세요."
    exit 1
fi

# 인자 파싱
CURRENT_YEAR=$(date +%Y)
START_YEAR=${1:-$((CURRENT_YEAR - 1))}  # 기본값: 작년
END_YEAR=${2:-$CURRENT_YEAR}            # 기본값: 올해
MODE=${3:-"fast"}                       # 기본값: fast

# 연도 유효성 검증
if ! [[ "$START_YEAR" =~ ^[0-9]+$ ]] || ! [[ "$END_YEAR" =~ ^[0-9]+$ ]]; then
    echo "❌ 연도는 숫자여야 합니다."
    show_usage
fi

if [ $START_YEAR -lt 1982 ] || [ $START_YEAR -gt $CURRENT_YEAR ]; then
    echo "❌ 시작년도는 1982-${CURRENT_YEAR} 범위여야 합니다."
    exit 1
fi

if [ $END_YEAR -lt $START_YEAR ] || [ $END_YEAR -gt $CURRENT_YEAR ]; then
    echo "❌ 끝년도는 시작년도-${CURRENT_YEAR} 범위여야 합니다."
    exit 1
fi

# Python 경로 설정
PYTHON="./venv/bin/python3"

# 모드별 시리즈 설정
if [ "$MODE" == "full" ]; then
    series_list=("regular" "exhibition" "wildcard" "semi_playoff" "playoff" "korean_series")
    mode_name="완전 모드"
else
    series_list=("regular" "korean_series" "playoff" "wildcard")
    mode_name="빠른 모드"
fi

# 정보 출력
total_years=$((END_YEAR - START_YEAR + 1))
estimated_time=$((total_years * ${#series_list[@]} * 3))

echo ""
echo "📋 크롤링 설정:"
echo "  🎯 대상 기간: ${START_YEAR}년 ~ ${END_YEAR}년 (${total_years}년간)"
echo "  🚀 실행 모드: ${mode_name}"
echo "  📊 시리즈 목록: ${series_list[*]}"
echo "  ⏱️ 예상 소요시간: 약 ${estimated_time}분"

echo ""
echo "💾 SQLite 데이터베이스 초기화 옵션:"
echo "  1) 기존 데이터 유지 (UPSERT 모드)"
echo "  2) 대상 기간 데이터 초기화 (${START_YEAR}-${END_YEAR}년)"
echo "  3) 전체 데이터 초기화"
echo ""
read -p "선택 (1/2/3, 기본값: 1): " -n 1 -r RESET_MODE
echo ""

case $RESET_MODE in
    "2")
        echo "🗑️ ${START_YEAR}-${END_YEAR}년 데이터 초기화 중..."
        $PYTHON reset_sqlite.py --range $START_YEAR $END_YEAR --force
        if [ $? -eq 0 ]; then
            echo "✅ 대상 기간 데이터 초기화 완료"
        else
            echo "❌ 데이터 초기화 실패"
            exit 1
        fi
        ;;
    "3")
        echo "🗑️ 전체 플레이어 데이터 초기화 중..."
        $PYTHON reset_sqlite.py --all --force
        if [ $? -eq 0 ]; then
            echo "✅ 전체 데이터 초기화 완료"
        else
            echo "❌ 데이터 초기화 실패"
            exit 1
        fi
        ;;
    *)
        echo "📂 기존 데이터 유지 - UPSERT 모드"
        ;;
esac

echo ""
read -p "크롤링을 시작하시겠습니까? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ 사용자가 취소했습니다."
    exit 1
fi

# 크롤링 진행률 추적
total_tasks=$((total_years * ${#series_list[@]} * 2))  # 년도 × 시리즈 × (타자+투수)
completed_tasks=0
success_count=0
error_count=0

# 진행률 표시 함수
show_progress() {
    local percent=$((completed_tasks * 100 / total_tasks))
    local filled=$((percent / 2))
    local empty=$((50 - filled))
    
    printf "\r🔄 진행률: ["
    printf "%*s" $filled | tr ' ' '█'
    printf "%*s" $empty | tr ' ' '░'
    printf "] %d%% (%d/%d)" $percent $completed_tasks $total_tasks
}

echo ""
echo "🚀 크롤링 시작!"
echo "💡 Ctrl+C로 중단할 수 있습니다."
echo ""

# 시작 시간 기록
start_time=$(date +%s)

# 년도별 크롤링
for year in $(seq $START_YEAR $END_YEAR); do
    year_start_time=$(date +%s)
    
    echo ""
    echo "📅 ${year}년 크롤링 중..."
    
    # 시리즈별 크롤링
    for series in "${series_list[@]}"; do
        # 타자 크롤링
        batting_output=$($PYTHON -m src.crawlers.player_batting_all_series_crawler \
            --year $year \
            --series $series \
            --save \
            --headless 2>&1)
        
        batting_exit_code=$?
        # 출력에서 성공 메시지 확인
        if [ $batting_exit_code -eq 0 ] && echo "$batting_output" | grep -q "크롤링 완료"; then
            ((success_count++))
        else
            ((error_count++))
            # 에러시 로그 파일에 기록
            echo "[$year-$series-batting] Exit: $batting_exit_code" >> crawl_errors.log
            echo "$batting_output" | tail -10 >> crawl_errors.log
        fi
        ((completed_tasks++))
        show_progress
        
        # 투수 크롤링
        pitching_output=$($PYTHON -m src.crawlers.player_pitching_all_series_crawler \
            --year $year \
            --series $series \
            --save \
            --headless 2>&1)
        
        pitching_exit_code=$?
        # 출력에서 성공 메시지 확인
        if [ $pitching_exit_code -eq 0 ] && echo "$pitching_output" | grep -q "크롤링 완료"; then
            ((success_count++))
        else
            ((error_count++))
            # 에러시 로그 파일에 기록
            echo "[$year-$series-pitching] Exit: $pitching_exit_code" >> crawl_errors.log
            echo "$pitching_output" | tail -10 >> crawl_errors.log
        fi
        ((completed_tasks++))
        show_progress
        
        sleep 1  # 시리즈 간 대기
    done
    
    year_end_time=$(date +%s)
    year_duration=$((year_end_time - year_start_time))
    
    echo ""
    echo "  ✅ ${year}년 완료 (소요시간: ${year_duration}초)"
    
    # 년도 간 대기 (마지막 년도 제외)
    if [ $year -lt $END_YEAR ]; then
        sleep 3
    fi
done

# 완료
echo ""
echo ""
echo "🎉 모든 크롤링 완료!"

# 총 소요시간 계산
end_time=$(date +%s)
total_duration=$((end_time - start_time))
hours=$((total_duration / 3600))
minutes=$(((total_duration % 3600) / 60))
seconds=$((total_duration % 60))

echo ""
echo "📊 크롤링 결과:"
echo "  🎯 대상 기간: ${START_YEAR}-${END_YEAR}년"
echo "  ✅ 성공: ${success_count}/${total_tasks} 작업"
echo "  ❌ 실패: ${error_count}/${total_tasks} 작업"
printf "  ⏱️ 총 소요시간: "
if [ $hours -gt 0 ]; then
    printf "%d시간 " $hours
fi
if [ $minutes -gt 0 ]; then
    printf "%d분 " $minutes
fi
printf "%d초\n" $seconds

# 최종 데이터 확인
echo ""
echo "🔍 데이터베이스 최종 현황:"

$PYTHON -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import func, and_

with SessionLocal() as session:
    # 전체 및 기간별 통계
    batting_total = session.query(PlayerSeasonBatting).count()
    pitching_total = session.query(PlayerSeasonPitching).count()
    
    batting_range = session.query(PlayerSeasonBatting).filter(
        and_(
            PlayerSeasonBatting.season >= $START_YEAR,
            PlayerSeasonBatting.season <= $END_YEAR
        )
    ).count()
    
    pitching_range = session.query(PlayerSeasonPitching).filter(
        and_(
            PlayerSeasonPitching.season >= $START_YEAR,
            PlayerSeasonPitching.season <= $END_YEAR
        )
    ).count()
    
    print(f'  📊 전체 데이터베이스:')
    print(f'    - 타자: {batting_total:,}건')
    print(f'    - 투수: {pitching_total:,}건')
    print(f'    - 합계: {batting_total + pitching_total:,}건')
    print()
    print(f'  📅 ${START_YEAR}-${END_YEAR}년 크롤링 결과:')
    print(f'    - 타자: {batting_range:,}건')
    print(f'    - 투수: {pitching_range:,}건')
    print(f'    - 합계: {batting_range + pitching_range:,}건')
    
    # 성공률 계산
    success_rate = round((success_count / total_tasks) * 100, 1) if total_tasks > 0 else 0
    print(f'\\n  📈 크롤링 성공률: {success_rate}%')
    
    if batting_range > 0 or pitching_range > 0:
        print(f'\\n  ✅ 크롤링이 성공적으로 완료되었습니다!')
    else:
        print(f'\\n  ⚠️ 데이터가 수집되지 않았습니다. 로그를 확인해보세요.')
" --args $success_count $total_tasks $START_YEAR $END_YEAR

echo ""
echo "📋 다음 단계:"
echo "  1. 데이터 검증:"
echo "     ./venv/bin/python3 verify_data_separation.py"
echo ""
echo "  2. Supabase 동기화:"
echo "     export SUPABASE_DB_URL='postgresql://postgres.xxx:[PASSWORD]@xxx.pooler.supabase.com:5432/postgres'"
echo "     ./venv/bin/python3 -m src.sync.supabase_sync"
echo ""
echo "  3. 실패한 데이터 재크롤링 (필요시):"
echo "     ./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year YYYY --series regular --save"

echo ""
echo "🎯 크롤링 완료! 수고하셨습니다! 🎉"