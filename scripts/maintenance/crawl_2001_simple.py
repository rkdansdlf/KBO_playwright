"""
Simple 2001 season crawler - tries to work around browser stability issues
"""
import sys
import os
sys.path.insert(0, os.getcwd())

from src.crawlers.player_batting_all_series_crawler import crawl_series_batting_stats
from src.crawlers.player_pitching_all_series_crawler import crawl_pitcher_series

def main():
    print("=" * 60)
    print("2001년 선수 데이터 크롤링 시작")
    print("=" * 60)
    
    # 타자 데이터 수집 (헤드리스 모드 비활성화, 팀별 순회 없음)
    print("\n[1/2] 타자 데이터 수집 중...")
    try:
        batting_data = crawl_series_batting_stats(
            year=2001,
            series_key='regular',
            limit=None,
            save_to_db=True,
            headless=True,  # 일단 headless로 시도
            by_team=False   # 팀별 순회 비활성화
        )
        print(f"✅ 타자 데이터 수집 완료: {len(batting_data)}명")
    except Exception as e:
        print(f"❌ 타자 데이터 수집 실패: {e}")
    
    # 투수 데이터 수집
    print("\n[2/2] 투수 데이터 수집 중...")
    try:
        pitching_data = crawl_pitcher_series(
            year=2001,
            series_key='regular',
            limit=None,
            headless=True,
            save_to_db=True,
            by_team=False
        )
        print(f"✅ 투수 데이터 수집 완료: {len(pitching_data)}명")
    except Exception as e:
        print(f"❌ 투수 데이터 수집 실패: {e}")
    
    print("\n" + "=" * 60)
    print("2001년 크롤링 완료")
    print("=" * 60)

if __name__ == "__main__":
    main()
