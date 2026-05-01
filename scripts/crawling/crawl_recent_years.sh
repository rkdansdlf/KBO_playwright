#!/bin/bash
# KBO 최근 년도 데이터 크롤링 스크립트 (실용 버전)
# 기본값: 최근 5년 (2020-2025)

echo "🚀 KBO 최근 년도 데이터 크롤링 시작"
echo "========================================================"

# 가상환경 확인
if [ ! -d "venv" ]; then
    echo "❌ venv 디렉토리가 없습니다. 가상환경을 먼저 설정하세요."
    exit 1
fi

# Python 경로 설정
PYTHON="./venv/bin/python3"

# 기본 설정
CURRENT_YEAR=$(date +%Y)
START_YEAR=${1:-2020}  # 첫 번째 인자가 있으면 사용, 없으면 2020년
END_YEAR=${2:-$CURRENT_YEAR}  # 두 번째 인자가 있으면 사용, 없으면 현재 년도

echo "📅 크롤링 대상 기간: ${START_YEAR}년 ~ ${END_YEAR}년"
echo "⚡ 예상 소요시간: 약 $((($END_YEAR - $START_YEAR + 1) * 15))분"

# 우선순위 시리즈 (실용성 중심)
priority_series=(
    "regular"        # 정규시즌 (필수)
    "korean_series"  # 한국시리즈 (중요)
    "playoff"        # 플레이오프
    "wildcard"       # 와일드카드
)

# 선택적 시리즈 (시간 여유시)
optional_series=(
    "semi_playoff"   # 준플레이오프
    "exhibition"     # 시범경기
)

echo ""
echo "🎯 크롤링 시리즈 우선순위:"
echo "  필수: ${priority_series[*]}"
echo "  선택: ${optional_series[*]}"

# 사용자 옵션 선택
echo ""
echo "크롤링 모드를 선택하세요:"
echo "  1) 빠른 모드 (필수 시리즈만, ~60분)"
echo "  2) 완전 모드 (모든 시리즈, ~90분)"
echo ""
read -p "선택 (1/2, 기본값: 1): " -n 1 -r MODE
echo ""

if [[ $MODE == "2" ]]; then
    all_series=("${priority_series[@]}" "${optional_series[@]}")
    echo "✅ 완전 모드 선택 - 모든 시리즈 크롤링"
else
    all_series=("${priority_series[@]}")
    echo "⚡ 빠른 모드 선택 - 필수 시리즈만 크롤링"
fi

echo ""
read -p "계속 진행하시겠습니까? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ 사용자가 취소했습니다."
    exit 1
fi

# 크롤링 함수
crawl_year_data() {
    local year=$1
    local data_type=$2  # "batting" or "pitching"
    local crawler_module=""
    local emoji=""
    
    if [ "$data_type" == "batting" ]; then
        crawler_module="src.crawlers.player_batting_all_series_crawler"
        emoji="🏏"
    else
        crawler_module="src.crawlers.player_pitching_all_series_crawler"
        emoji="⚾"
    fi
    
    echo ""
    echo "${emoji} ${year}년 ${data_type} 데이터 크롤링..."
    
    local year_success=0
    local year_total=${#all_series[@]}
    
    for series in "${all_series[@]}"; do
        echo "  ▶ ${series} 시리즈..."
        
        $PYTHON -m $crawler_module \
            --year $year \
            --series $series \
            --save \
            --headless > /dev/null 2>&1
        
        if [ $? -eq 0 ]; then
            echo "    ✅ 성공"
            ((year_success++))
        else
            echo "    ⚠️ 실패"
        fi
        
        sleep 1  # 시리즈 간 대기
    done
    
    echo "  📊 ${year}년 ${data_type} 결과: ${year_success}/${year_total} 성공"
    
    return $year_success
}

# 전체 통계
total_years=$((END_YEAR - START_YEAR + 1))
completed_years=0

echo ""
echo "🏃‍♂️ 크롤링 시작!"

# 년도별 크롤링
for year in $(seq $START_YEAR $END_YEAR); do
    echo ""
    echo "=" * 50
    echo "📅 ${year}년 데이터 수집 (${completed_years}/${total_years} 완료)"
    echo "=" * 50
    
    # 타자 데이터
    crawl_year_data $year "batting"
    batting_result=$?
    
    # 투수 데이터  
    crawl_year_data $year "pitching"
    pitching_result=$?
    
    # 년도 완료
    ((completed_years++))
    echo "  ✅ ${year}년 완료 (타자: ${batting_result}, 투수: ${pitching_result})"
    
    # 년도별 대기 (마지막 년도가 아닌 경우)
    if [ $year -lt $END_YEAR ]; then
        echo "  ⏱️ 3초 대기 중..."
        sleep 3
    fi
done

echo ""
echo "=" * 60
echo "🎉 모든 년도 크롤링 완료!"
echo "=" * 60

# 최종 결과 확인
echo ""
echo "🔍 최종 데이터베이스 현황:"

$PYTHON -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import func, and_

with SessionLocal() as session:
    # 전체 데이터
    batting_total = session.query(PlayerSeasonBatting).count()
    pitching_total = session.query(PlayerSeasonPitching).count()
    
    # 크롤링 대상 기간 데이터
    batting_period = session.query(PlayerSeasonBatting).filter(
        and_(
            PlayerSeasonBatting.season >= $START_YEAR,
            PlayerSeasonBatting.season <= $END_YEAR
        )
    ).count()
    
    pitching_period = session.query(PlayerSeasonPitching).filter(
        and_(
            PlayerSeasonPitching.season >= $START_YEAR,
            PlayerSeasonPitching.season <= $END_YEAR
        )
    ).count()
    
    print(f'📊 전체 데이터베이스:')
    print(f'  - 타자: {batting_total:,}건')
    print(f'  - 투수: {pitching_total:,}건')
    print(f'  - 합계: {batting_total + pitching_total:,}건')
    print()
    print(f'📅 ${START_YEAR}-${END_YEAR}년 기간:')
    print(f'  - 타자: {batting_period:,}건')
    print(f'  - 투수: {pitching_period:,}건')
    print(f'  - 합계: {batting_period + pitching_period:,}건')
    
    # 최신 데이터 샘플
    print(f'\\n🔍 최신 수집 데이터 샘플:')
    latest_batting = session.query(PlayerSeasonBatting).filter(
        PlayerSeasonBatting.season == $END_YEAR
    ).limit(3).all()
    
    if latest_batting:
        print(f'  타자 ({END_YEAR}년):')
        for b in latest_batting:
            print(f'    - Player {b.player_id}: {b.league}, AVG {b.avg}, HR {b.home_runs}')
    
    latest_pitching = session.query(PlayerSeasonPitching).filter(
        PlayerSeasonPitching.season == $END_YEAR
    ).limit(3).all()
    
    if latest_pitching:
        print(f'  투수 ({END_YEAR}년):')
        for p in latest_pitching:
            print(f'    - Player {p.player_id}: {p.league}, ERA {p.era}, W-L {p.wins}-{p.losses}')
"

echo ""
echo "🎯 크롤링 완료!"
echo ""
echo "📋 다음 단계:"
echo "  1. 데이터 검증: ./venv/bin/python3 verify_data_separation.py"
echo "  2. OCI 동기화:"
echo "     export OCI_DB_URL='postgresql://user:password@host:5432/bega_backend'"
echo "     ./venv/bin/python3 -m src.cli.sync_oci --game-details --unsynced-only"
echo ""
echo "💡 개별 재크롤링 (필요시):"
echo "  ./venv/bin/python3 -m src.crawlers.player_batting_all_series_crawler --year YYYY --series regular --save"
echo "  ./venv/bin/python3 -m src.crawlers.player_pitching_all_series_crawler --year YYYY --series regular --save"

echo ""
echo "✨ 수고하셨습니다!"
