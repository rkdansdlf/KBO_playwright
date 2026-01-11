#!/usr/bin/env python3
"""
1982-2001년 레거시 데이터 재크롤링 스크립트
기존 데이터를 삭제하고 새로운 레거시 크롤러로 다시 수집
"""
import argparse
import subprocess
import sys
from datetime import datetime

from src.utils.series_validation import get_available_series_by_year


def recrawl_legacy_years(start_year: int = 1982, end_year: int = 2001, 
                        reset_first: bool = True, headless: bool = True):
    """
    레거시 연도 재크롤링
    """
    print("🔄 KBO 레거시 연도 재크롤링")
    print("=" * 50)
    print(f"📅 대상 기간: {start_year}년 ~ {end_year}년")
    print(f"🗑️ 기존 데이터 삭제: {'ON' if reset_first else 'OFF'}")
    print(f"🤖 헤드리스 모드: {'ON' if headless else 'OFF'}")
    
    # 기존 데이터 삭제
    if reset_first:
        print(f"\n🗑️ {start_year}-{end_year}년 기존 데이터 삭제 중...")
        reset_cmd = [
            sys.executable, "reset_sqlite.py", 
            "--range", str(start_year), str(end_year), 
            "--force"
        ]
        try:
            subprocess.run(reset_cmd, check=True)
            print("✅ 기존 데이터 삭제 완료")
        except subprocess.CalledProcessError:
            print("❌ 데이터 삭제 실패")
            return False
    
    # 결과 추적
    results = {
        'total_tasks': 0,
        'success_count': 0,
        'failed_tasks': []
    }
    
    print(f"\n🕷️ 레거시 크롤링 시작...")
    start_time = datetime.now()
    
    # 년도별 크롤링
    for year in range(start_year, end_year + 1):
        print(f"\n📅 {year}년 크롤링 중...")
        
        # 해당 연도에 존재하는 시리즈 확인
        available_series = get_available_series_by_year(year)
        # exhibition 제외 (너무 많은 데이터)
        target_series = [s for s in available_series if s in ['regular', 'korean_series']]
        
        year_success = 0
        year_total = len(target_series) * 2
        
        for series in target_series:
            print(f"  📊 {series} 시리즈:")
            
            # 타자 크롤링
            print(f"    🏏 타자 크롤링...", end=" ")
            batting_cmd = [
                sys.executable, "-m", "src.crawlers.legacy_batting_crawler",
                "--year", str(year),
                "--series", series,
                "--save"
            ]
            if headless:
                batting_cmd.append("--headless")
            
            try:
                result = subprocess.run(batting_cmd, capture_output=True, text=True, timeout=300)
                if result.returncode == 0 and "크롤링 완료" in result.stdout:
                    print("✅")
                    results['success_count'] += 1
                    year_success += 1
                else:
                    print("❌")
                    results['failed_tasks'].append(f"{year}-{series}-batting")
            except subprocess.TimeoutExpired:
                print("❌ (타임아웃)")
                results['failed_tasks'].append(f"{year}-{series}-batting")
            
            # 투수 크롤링
            print(f"    ⚾ 투수 크롤링...", end=" ")
            pitching_cmd = [
                sys.executable, "-m", "src.crawlers.legacy_pitching_crawler",
                "--year", str(year),
                "--series", series,
                "--save"
            ]
            if headless:
                pitching_cmd.append("--headless")
            
            try:
                result = subprocess.run(pitching_cmd, capture_output=True, text=True, timeout=300)
                if result.returncode == 0 and "크롤링 완료" in result.stdout:
                    print("✅")
                    results['success_count'] += 1
                    year_success += 1
                else:
                    print("❌")
                    results['failed_tasks'].append(f"{year}-{series}-pitching")
            except subprocess.TimeoutExpired:
                print("❌ (타임아웃)")
                results['failed_tasks'].append(f"{year}-{series}-pitching")
            
            results['total_tasks'] += 2
        
        # 년도별 결과
        success_rate = (year_success / year_total) * 100 if year_total > 0 else 0
        print(f"  📊 {year}년 결과: {year_success}/{year_total} 성공 ({success_rate:.1f}%)")
    
    # 최종 결과
    duration = (datetime.now() - start_time).total_seconds()
    overall_success_rate = (results['success_count'] / results['total_tasks']) * 100 if results['total_tasks'] > 0 else 0
    
    print("\n" + "=" * 50)
    print("🎉 레거시 재크롤링 완료!")
    print(f"📊 최종 결과:")
    print(f"  ✅ 성공: {results['success_count']}/{results['total_tasks']} ({overall_success_rate:.1f}%)")
    print(f"  ⏱️ 소요시간: {duration:.0f}초")
    
    if results['failed_tasks']:
        print(f"\n❌ 실패한 작업들:")
        for task in results['failed_tasks'][:10]:
            print(f"    - {task}")
        if len(results['failed_tasks']) > 10:
            print(f"    ... 및 {len(results['failed_tasks']) - 10}개 더")
    
    # 최종 데이터 확인
    print(f"\n🔍 최종 데이터베이스 확인:")
    check_cmd = [sys.executable, "-c", f"""
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import and_, func

with SessionLocal() as session:
    batting_stats = session.query(
        PlayerSeasonBatting.league,
        func.count(PlayerSeasonBatting.id)
    ).filter(
        and_(
            PlayerSeasonBatting.season >= {start_year},
            PlayerSeasonBatting.season <= {end_year},
            PlayerSeasonBatting.source == 'LEGACY_CRAWLER'
        )
    ).group_by(PlayerSeasonBatting.league).all()
    
    pitching_stats = session.query(
        PlayerSeasonPitching.league,
        func.count(PlayerSeasonPitching.id)
    ).filter(
        and_(
            PlayerSeasonPitching.season >= {start_year},
            PlayerSeasonPitching.season <= {end_year},
            PlayerSeasonPitching.source == 'LEGACY_CRAWLER'
        )
    ).group_by(PlayerSeasonPitching.league).all()
    
    print("📊 타자 데이터 (시리즈별):")
    for league, count in batting_stats:
        print(f"  - {{league}}: {{count:,}}명")
    
    print("📊 투수 데이터 (시리즈별):")
    for league, count in pitching_stats:
        print(f"  - {{league}}: {{count:,}}명")
"""]
    
    try:
        subprocess.run(check_cmd)
    except:
        print("  ⚠️ 데이터베이스 확인 실패")
    
    return overall_success_rate >= 80


def main():
    parser = argparse.ArgumentParser(description="1982-2001년 레거시 데이터 재크롤링")
    parser.add_argument("--start", type=int, default=1982, help="시작 년도")
    parser.add_argument("--end", type=int, default=2001, help="끝 년도")
    parser.add_argument("--no-reset", action="store_true", help="기존 데이터 삭제 생략")
    parser.add_argument("--no-headless", action="store_true", help="브라우저 UI 표시")
    
    args = parser.parse_args()
    
    try:
        success = recrawl_legacy_years(
            start_year=args.start,
            end_year=args.end,
            reset_first=not args.no_reset,
            headless=not args.no_headless
        )
        
        if success:
            print(f"\n🎉 재크롤링 성공!")
            sys.exit(0)
        else:
            print(f"\n❌ 재크롤링 실패")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n❌ 사용자가 중단했습니다.")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()