#!/bin/bash
# 깨끗한 크롤링 + OCI 동기화 원스텝 스크립트

echo "🚀 KBO 데이터 깨끗한 크롤링 + OCI 동기화"
echo "================================================"

# 기본 설정
CURRENT_YEAR=$(date +%Y)
START_YEAR=${1:-$((CURRENT_YEAR - 1))}
END_YEAR=${2:-$CURRENT_YEAR}
MODE=${3:-"fast"}

echo "📋 크롤링 설정:"
echo "  🎯 대상 기간: ${START_YEAR}년 ~ ${END_YEAR}년"
echo "  🚀 실행 모드: ${MODE}"

if [ ! -d "venv" ]; then
    echo "❌ venv 디렉토리가 없습니다."
    exit 1
fi

PYTHON="./venv/bin/python3"

echo ""
echo "💾 데이터베이스 초기화 중..."
$PYTHON reset_sqlite.py --range $START_YEAR $END_YEAR --force

if [ $? -ne 0 ]; then
    echo "❌ SQLite 초기화 실패"
    exit 1
fi

echo ""
echo "🕷️ 크롤링 시작..."
./crawl_year_range.sh $START_YEAR $END_YEAR $MODE

if [ $? -ne 0 ]; then
    echo "❌ 크롤링 실패"
    exit 1
fi

echo ""
echo "☁️ OCI 동기화 확인..."

if [ -z "$OCI_DB_URL" ]; then
    echo "⚠️ OCI_DB_URL 환경변수가 설정되지 않았습니다."
    echo "동기화를 건너뜁니다."
    echo ""
    echo "💡 OCI 동기화 방법:"
    echo "  export OCI_DB_URL='postgresql://user:password@host:5432/bega_backend'"
    echo "  ./venv/bin/python3 -m src.cli.sync_oci --game-details --unsynced-only"
else
    echo "🔄 OCI 동기화 중..."
    $PYTHON -m src.cli.sync_oci --game-details --unsynced-only
    
    if [ $? -eq 0 ]; then
        echo "✅ OCI 동기화 완료"
    else
        echo "⚠️ OCI 동기화 중 문제 발생"
    fi
fi

echo ""
echo "📊 최종 결과 확인..."
$PYTHON -c "
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import func, and_

with SessionLocal() as session:
    # 대상 기간 데이터
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
    
    # 전체 데이터
    batting_total = session.query(PlayerSeasonBatting).count()
    pitching_total = session.query(PlayerSeasonPitching).count()
    
    print(f'📊 크롤링 결과 ({START_YEAR}-{END_YEAR}년):')
    print(f'  - 타자: {batting_range:,}명')
    print(f'  - 투수: {pitching_range:,}명')
    print(f'  - 합계: {batting_range + pitching_range:,}명')
    print(f'')
    print(f'📊 전체 데이터베이스:')
    print(f'  - 타자: {batting_total:,}명')
    print(f'  - 투수: {pitching_total:,}명')
    print(f'  - 합계: {batting_total + pitching_total:,}명')
" --args $START_YEAR $END_YEAR

echo ""
echo "🎉 모든 작업이 완료되었습니다!"
