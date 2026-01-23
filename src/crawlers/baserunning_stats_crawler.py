"""
선수의 시즌별 주루 기록을 크롤링하고 DB에 저장합니다.
"""
from playwright.sync_api import sync_playwright
import sqlite3
import sys
from pathlib import Path

# config 모듈 임포트
sys.path.append(str(Path(__file__).parent.parent))
from config.browser_config import get_browser_config
import time
from datetime import datetime
from src.utils.playwright_blocking import install_sync_resource_blocking


def crawl_baserunning_stats(year=2025, max_retries=3, timeout=60000):
    """
    전체 선수의 주루 기록을 크롤링합니다.

    Args:
        year: 시즌 연도
        max_retries: 최대 재시도 횟수
        timeout: 페이지 로드 타임아웃 (밀리초)

    Returns:
        list: 주루 기록 리스트
    """
    baserunning_data = []

    with sync_playwright() as playwright:
        browser_config = get_browser_config()
        browser = playwright.chromium.launch(**browser_config)
        page = browser.new_page()
        page.set_default_timeout(timeout)
        install_sync_resource_blocking(page)

        url = 'https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx'

        # 재시도 로직
        for attempt in range(max_retries):
            try:
                page.goto(url, wait_until='load', timeout=timeout)
                page.wait_for_load_state('networkidle', timeout=timeout)
                time.sleep(1)
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"   ⚠️  재시도 {attempt + 1}/{max_retries} (오류: {type(e).__name__}, {wait_time}초 후 재시도)")
                    time.sleep(wait_time)
                else:
                    print(f"   ❌ 최대 재시도 횟수 초과: {e}")
                    browser.close()
                    return baserunning_data

        try:
            # 주루 기록 테이블 찾기
            tables = page.query_selector_all('table')

            if len(tables) > 0:
                tbody = tables[0].query_selector('tbody')
                rows = tbody.query_selector_all('tr') if tbody else []

                print(f"   ✓ {len(rows)}명의 주루 기록 발견")

                for row in rows:
                    cells = row.query_selector_all('td')

                    # [순위(0), 선수명(1), 팀명(2), G(3), SBA(4), SB(5), CS(6), SB%(7), OOB(8), PKO(9)]
                    if len(cells) >= 10:
                        try:
                            # 선수명 셀에서 링크가 있는지 확인하여 player_id 추출 시도
                            player_id = None
                            player_link = cells[1].query_selector('a')
                            if player_link:
                                player_name = player_link.inner_text().strip()
                                href = player_link.get_attribute('href')
                                # href에서 playerId 추출
                                if href and 'playerId=' in href:
                                    player_id = href.split('playerId=')[1].split('&')[0]
                            else:
                                # 링크가 없으면 텍스트만 가져오기
                                player_name = cells[1].inner_text().strip()

                            team_name = cells[2].inner_text().strip()

                            # 팀명을 team_id로 변환
                            team_map = {
                                'LG': 'LG', 'NC': 'NC', 'SSG': 'SK', 'KT': 'KT',
                                '삼성': 'SS', '두산': 'OB', 'KIA': 'HT', '롯데': 'LT',
                                '한화': 'HH', '키움': 'WO'
                            }
                            team_id = team_map.get(team_name, team_name)

                            stats = {
                                'player_id': player_id,  # 링크가 있으면 player_id 포함
                                'player_name': player_name,
                                'team_id': team_id,
                                'year': year,
                                'games': int(cells[3].inner_text().strip().replace(',', '')) if cells[3].inner_text().strip() else 0,
                                'stolen_base_attempts': int(cells[4].inner_text().strip().replace(',', '')) if cells[4].inner_text().strip() else 0,
                                'stolen_bases': int(cells[5].inner_text().strip().replace(',', '')) if cells[5].inner_text().strip() else 0,
                                'caught_stealing': int(cells[6].inner_text().strip().replace(',', '')) if cells[6].inner_text().strip() else 0,
                                'stolen_base_percentage': float(cells[7].inner_text().strip().replace(',', '')) if cells[7].inner_text().strip() else 0.0,
                                'out_on_base': int(cells[8].inner_text().strip().replace(',', '')) if cells[8].inner_text().strip() else 0,
                                'picked_off': int(cells[9].inner_text().strip().replace(',', '')) if cells[9].inner_text().strip() else 0
                            }

                            baserunning_data.append(stats)

                        except (ValueError, AttributeError, IndexError) as e:
                            print(f"   ⚠️  선수 데이터 파싱 오류 ({player_name if 'player_name' in locals() else '알 수 없음'}): {e}")
                            continue

        except Exception as e:
            print(f"⚠️ 주루 기록 크롤링 중 오류: {e}")

        browser.close()

    return baserunning_data


def save_baserunning_stats(player_list, year=2025, db_path='kbo_2025.db'):
    """
    주루 기록을 크롤링하여 DB에 저장합니다.

    Args:
        player_list: 선수 목록 (player_id 매칭용)
        year: 시즌 연도
        db_path: 데이터베이스 파일 경로
    """
    print(f"\n{'='*60}")
    print(f"🏃 {year}년 주루 기록 수집 시작")
    print(f"{'='*60}\n")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 주루 기록 크롤링
    baserunning_data = crawl_baserunning_stats(year)

    if not baserunning_data:
        print("❌ 주루 기록을 가져올 수 없습니다.")
        conn.close()
        return

    # 선수명 -> player_id 매핑 생성
    player_map = {p['player_name']: p['player_id'] for p in player_list}

    success_count = 0
    fail_count = 0

    for idx, stats in enumerate(baserunning_data, 1):
        player_name = stats['player_name']

        # 1. 크롤링 시 추출한 player_id가 있으면 사용
        player_id = stats.get('player_id')

        # 2. player_id가 없으면 player_map에서 찾기
        if not player_id:
            player_id = player_map.get(player_name)

        # 3. 여전히 없으면 DB에서 직접 찾기 (player_season_participation 테이블 사용)
        if not player_id:
            cursor.execute('''
                SELECT player_id FROM player_season_participation
                WHERE player_name = ? AND year = ? AND team_id = ?
            ''', (player_name, year, stats['team_id']))
            row = cursor.fetchone()
            player_id = row[0] if row else None

            # 팀 정보 없이 이름만으로 재시도
            if not player_id:
                cursor.execute('''
                    SELECT player_id FROM player_season_participation
                    WHERE player_name = ? AND year = ?
                    LIMIT 1
                ''', (player_name, year))
                row = cursor.fetchone()
                player_id = row[0] if row else None

        if player_id:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO kbo_season_baserunning_stats
                    (player_id, team_id, year, player_name, games, stolen_base_attempts,
                     stolen_bases, caught_stealing, stolen_base_percentage,
                     out_on_base, picked_off, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    player_id,
                    stats['team_id'],
                    stats['year'],
                    stats['player_name'],
                    stats['games'],
                    stats['stolen_base_attempts'],
                    stats['stolen_bases'],
                    stats['caught_stealing'],
                    stats['stolen_base_percentage'],
                    stats['out_on_base'],
                    stats['picked_off'],
                    datetime.now()
                ))

                conn.commit()
                success_count += 1

                if idx % 10 == 0:
                    print(f"[{idx}/{len(baserunning_data)}] {player_name} 저장 완료")

            except Exception as e:
                fail_count += 1
                print(f"   ❌ {player_name} 저장 실패: {e}")
        else:
            fail_count += 1
            print(f"   ⚠️  {player_name}: player_id를 찾을 수 없음")

    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ 주루 기록 저장 완료!")
    print(f"{'='*60}")
    print(f"  - 성공: {success_count}명")
    print(f"  - 실패: {fail_count}명")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # 테스트용
    from player_list_crawler import crawl_player_list
    players = crawl_player_list(2025)
    save_baserunning_stats(players, 2025, 'data/kbo_2025.db')
