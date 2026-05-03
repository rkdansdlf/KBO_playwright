"""
전체 선수의 수비 기록을 포지션별 랭킹 페이지에서 크롤링하고 DB에 저장합니다.
(2025년 10월 업데이트: KBO 웹사이트에서 개별 선수 수비 페이지가 제거되어 포지션별 랭킹 페이지 사용)
"""
from playwright.sync_api import sync_playwright
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.team_codes import resolve_team_code
from src.utils.request_policy import RequestPolicy


def crawl_all_fielding_stats(year=2025):
    """
    포지션별 수비 랭킹 페이지에서 전체 선수의 수비 기록을 크롤링합니다.

    Args:
        year: 시즌 연도

    Returns:
        list: 수비 기록 딕셔너리 리스트
            - player_id: 선수 ID
            - player_name: 선수명
            - team_name: 팀명
            - position: 포지션 (한글)
            - position_id: 포지션 ID (영문)
            - games: 경기수
            - games_started: 선발 출장
            - innings: 이닝
            - errors: 실책
            - pickoffs: 견제사
            - putouts: 자살
            - assists: 보살
            - double_plays: 병살
            - fielding_pct: 수비율
    """
    fielding_data = []
    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(**policy.build_context_kwargs())
        page = context.new_page()
        install_sync_resource_blocking(page)

        # 포지션별 수비 랭킹 페이지
        url = 'https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx'
        print(f"📊 수비 기록 페이지 접속: {url}")
        page.goto(url, wait_until='networkidle')
        time.sleep(2)

        try:
            # 연도 선택
            year_select = page.query_selector('select#cphContents_cphContents_cphContents_ddlSeason_ddlSeason')
            if year_select:
                page.select_option('select#cphContents_cphContents_cphContents_ddlSeason_ddlSeason', str(year))
                page.wait_for_load_state('networkidle')
                time.sleep(2)
                print(f"✅ {year}년 데이터 선택 완료")

            # 페이지네이션 확인
            pagination = page.query_selector('.paging')
            total_pages = 1
            if pagination:
                page_links = pagination.query_selector_all('a')
                # 숫자 링크만 카운트 (이전/다음 버튼 제외)
                page_numbers = []
                for link in page_links:
                    text = link.inner_text().strip()
                    if text.isdigit():
                        page_numbers.append(int(text))
                if page_numbers:
                    total_pages = max(page_numbers)

            print(f"📄 총 {total_pages}개 페이지 발견")

            # 메인 테이블 크롤링
            table = page.query_selector('table.tData01.tt')

            if not table:
                print("⚠️ 수비 기록 테이블을 찾을 수 없습니다.")
                browser.close()
                return fielding_data

            # 헤더 확인 (첫 페이지에서만)
            headers = table.query_selector_all('thead th')
            header_texts = [h.inner_text().strip() for h in headers]
            print(f"테이블 헤더: {header_texts}")

            # 포지션 한글 → ID 매핑
            position_mapping = {
                '포수': 'C',
                '1루수': '1B',
                '2루수': '2B',
                '3루수': '3B',
                '유격수': 'SS',
                '좌익수': 'LF',
                '중견수': 'CF',
                '우익수': 'RF',
                '외야수': 'OF',
                '내야수': 'IF',
                '지명타자': 'DH'
            }

            # 팀명 한글 → ID 매핑
            team_mapping = {
                'LG': 'LG',
                '한화': 'HH',
                'SSG': 'SK',
                '삼성': 'SS',
                'NC': 'NC',
                'KT': 'KT',
                '롯데': 'LT',
                'KIA': 'HT',
                '두산': 'OB',
                '키움': 'WO'
            }

            # 모든 페이지 순회
            for current_page in range(1, total_pages + 1):
                print(f"\n📄 페이지 {current_page}/{total_pages} 크롤링 중...")

                # 2페이지 이상부터는 페이지 이동
                if current_page > 1:
                    # 페이지 번호 링크 클릭
                    pagination = page.query_selector('.paging')
                    if pagination:
                        page_link = None
                        for link in pagination.query_selector_all('a'):
                            if link.inner_text().strip() == str(current_page):
                                page_link = link
                                break

                        if page_link:
                            page_link.click()
                            page.wait_for_load_state('networkidle')
                            time.sleep(2)
                        else:
                            print(f"⚠️ 페이지 {current_page} 링크를 찾을 수 없습니다.")
                            break

                # 현재 페이지 테이블 가져오기
                table = page.query_selector('table.tData01.tt')
                if not table:
                    print(f"⚠️ 페이지 {current_page}에서 테이블을 찾을 수 없습니다.")
                    continue

                # 데이터 행 추출
                tbody = table.query_selector('tbody')
                if not tbody:
                    print(f"⚠️ 페이지 {current_page}에 tbody가 없습니다.")
                    continue

                rows = tbody.query_selector_all('tr')
                print(f"   ✓ {len(rows)}개의 수비 기록 발견")

                for row in rows:
                    cells = row.query_selector_all('td')

                    if len(cells) >= 13:  # 최소 13개 컬럼 필요 (순위~FPCT)
                        try:
                            # 선수 링크에서 player_id 추출
                            player_link = cells[1].query_selector('a')
                            player_id = None

                            if player_link:
                                href = player_link.get_attribute('href')
                                if href and 'playerId=' in href:
                                    player_id = href.split('playerId=')[1].split('&')[0]

                            # 기본 데이터 추출
                            player_name = cells[1].inner_text().strip()
                            team_name = cells[2].inner_text().strip()
                            position = cells[3].inner_text().strip()

                            # 포지션 ID 변환
                            position_id = position_mapping.get(position, position)

                            # 팀 ID 변환
                            team_id = team_mapping.get(team_name, team_name)

                            def safe_float(text):
                                if not text or text.strip() in ('-', ''): return 0.0
                                try: return float(text.strip().replace(',', ''))
                                except: return 0.0

                            def safe_int(text):
                                if not text or text.strip() in ('-', ''): return 0
                                try: return int(text.strip().replace(',', ''))
                                except: return 0

                            # 이닝 파싱 (예: "1262 1/3" → 1262.333)
                            innings_text = cells[6].inner_text().strip().replace(',', '')
                            innings_value = 0.0

                            if innings_text and innings_text != '-':
                                if ' ' in innings_text:
                                    # 분수 형식 처리 (예: "1262 1/3")
                                    parts = innings_text.split(' ')
                                    innings_value = safe_float(parts[0])

                                    if len(parts) > 1 and '/' in parts[1]:
                                        fraction = parts[1].split('/')
                                        try:
                                            innings_value += float(fraction[0]) / float(fraction[1])
                                        except: pass
                                else:
                                    innings_value = safe_float(innings_text)

                            # 수비 통계 추출 (헤더: 순위, 선수명, 팀명, POS, G, GS, IP, E, PKO, PO, A, DP, FPCT, ...)
                            fielding_record = {
                                'player_id': player_id,
                                'player_name': player_name,
                                'team_name': team_name,
                                'team_id': team_id,
                                'year': year,
                                'position': position,
                                'position_id': position_id,
                                'games': safe_int(cells[4].inner_text()),
                                'games_started': safe_int(cells[5].inner_text()),
                                'innings': innings_value,
                                'errors': safe_int(cells[7].inner_text()),
                                'pickoffs': safe_int(cells[8].inner_text()),
                                'putouts': safe_int(cells[9].inner_text()),
                                'assists': safe_int(cells[10].inner_text()),
                                'double_plays': safe_int(cells[11].inner_text()),
                                'fielding_pct': safe_float(cells[12].inner_text()),
                            }

                            fielding_data.append(fielding_record)

                        except (ValueError, AttributeError) as e:
                            print(f"   ⚠️ 데이터 파싱 오류: {e}")
                            continue

            print(f"\n✅ 총 {len(fielding_data)}개의 수비 기록 수집 완료!")

        except Exception as e:
            print(f"⚠️ 수비 기록 크롤링 중 오류: {e}")
            import traceback
            traceback.print_exc()

        browser.close()

    return fielding_data


def save_fielding_stats(year=2025, db_path='kbo_2025.db'):
    """
    수비 기록을 크롤링하여 DB에 저장합니다.

    Args:
        year: 시즌 연도
        db_path: 데이터베이스 파일 경로
    """
    # 수비 기록 크롤링
    fielding_records = crawl_all_fielding_stats(year)

    if not fielding_records:
        print("⚠️ 수집된 수비 기록이 없습니다.")
        return

    # DB 저장
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    saved_count = 0
    skipped_count = 0

    for record in fielding_records:
        # player_id가 없으면 스킵
        if not record['player_id']:
            print(f"⚠️ player_id 없음: {record['player_name']} ({record['team_name']})")
            skipped_count += 1
            continue

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO kbo_season_fielding_stats
                (player_id, team_id, year, position_id, games, games_started,
                 innings, putouts, assists, errors, double_plays, fielding_pct, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                record['player_id'],
                record['team_id'],
                year,
                record['position_id'],
                record['games'],
                record['games_started'],
                record['innings'],
                record['putouts'],
                record['assists'],
                record['errors'],
                record['double_plays'],
                record['fielding_pct'],
                datetime.now()
            ))

            saved_count += 1

            # participation 테이블 업데이트: has_fielding_record = 1
            cursor.execute('''
                UPDATE player_season_participation
                SET has_fielding_record = 1,
                    updated_at = ?
                WHERE player_id = ? AND year = ? AND team_id = ?
            ''', (datetime.now(), record['player_id'], year, record['team_id']))

        except Exception as e:
            print(f"⚠️ DB 저장 오류: {record['player_name']} - {e}")
            skipped_count += 1
            continue

    conn.commit()
    conn.close()

    print(f"✅ 수비 기록 저장 완료! (저장: {saved_count}건, 스킵: {skipped_count}건)")


if __name__ == "__main__":
    # 테스트용
    print("🧪 수비 기록 크롤링 테스트")
    save_fielding_stats(2025, 'kbo_test.db')
