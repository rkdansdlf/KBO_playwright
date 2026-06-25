"""Basic2 헤더 클릭 기능 테스트 - 11개 헤더 모두 검증"""

import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import time

import pytest
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

from src.crawlers.player_batting_all_series_crawler import crawl_basic2_with_headers


@pytest.mark.slow
def test_basic2_headers():
    """11개 Basic2 헤더가 모두 정상적으로 클릭되는지 테스트"""
    if os.getenv("KBO_RUN_LIVE_BASIC2_HEADERS") != "1":
        pytest.skip("set KBO_RUN_LIVE_BASIC2_HEADERS=1 to run live headed Basic2 header test")

    with sync_playwright() as playwright:
        headless = os.getenv("KBO_BASIC2_HEADLESS", "1") != "0"
        browser = playwright.chromium.launch(headless=headless)
        page = browser.new_page()

        try:
            print("🧪 Basic2 헤더 클릭 기능 테스트 시작...")

            # 정규시즌 설정
            year = 2025
            series_info = {"value": "0", "name": "정규시즌"}

            # Basic2 헤더 클릭 기능 테스트
            print(f"📊 {year}년 {series_info['name']} Basic2 헤더 클릭 테스트...")
            result = crawl_basic2_with_headers(page, year, series_info)

            # 결과 확인
            if result:
                print(f"✅ 테스트 성공! {len(result)}명의 플레이어 데이터 수집됨")

                # 첫 번째 플레이어 데이터 샘플 출력
                first_player = next(iter(result.values()))
                print("\n📋 샘플 데이터 (첫 번째 플레이어):")
                for key, value in first_player.items():
                    print(f"   {key}: {value}")

                # Basic2 필드들이 수집되었는지 확인
                basic2_fields = ["BB", "IBB", "HBP", "SO", "GDP", "SLG", "OBP", "OPS", "MH", "RISP", "PH_BA"]
                found_fields = []
                missing_fields = []

                for field in basic2_fields:
                    if field in first_player and first_player[field] is not None:
                        found_fields.append(field)
                    else:
                        missing_fields.append(field)

                print("\n📈 Basic2 필드 수집 결과:")
                print(f"   ✅ 수집된 필드 ({len(found_fields)}/11): {found_fields}")
                if missing_fields:
                    print(f"   ❌ 누락된 필드 ({len(missing_fields)}/11): {missing_fields}")
                else:
                    print("   🎉 모든 Basic2 필드가 성공적으로 수집되었습니다!")

            else:
                print("❌ 테스트 실패! 데이터가 수집되지 않았습니다.")

        except Exception as e:
            logger.exception("테스트 중 오류 발생")
            print(f"❌ 테스트 중 오류 발생: {e}")

        finally:
            if not headless:
                print("\n⏸️  확인을 위해 5초 대기...")
                time.sleep(5)
            browser.close()


if __name__ == "__main__":
    test_basic2_headers()
