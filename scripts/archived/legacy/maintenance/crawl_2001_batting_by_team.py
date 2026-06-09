"""
Targeted 2001 batting crawler by team
"""

import os
import sys

sys.path.insert(0, os.getcwd())

from src.crawlers.player_batting_all_series_crawler import crawl_series_batting_stats


def main():
    print("🚀 2001년 타자 데이터 팀별 수집 시작 (by_team=True)...")
    try:
        data = crawl_series_batting_stats(year=2001, series_key="regular", save_to_db=True, headless=True, by_team=True)
        print(f"✅ 2001년 타자 수집 완료: {len(data)}명")
    except Exception as e:  # noqa: BLE001
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()
