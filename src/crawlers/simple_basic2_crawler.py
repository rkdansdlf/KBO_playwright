"""
단순화된 Basic2 크롤러 - BB 헤더만 클릭
OCI 동기화 전 SQLite 저장 테스트용
"""

import sys
import os
import time
from typing import Dict, List, Optional, Union
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from playwright.sync_api import sync_playwright, Page
from src.repositories.save_kbo_batting import save_kbo_batting_batch
from src.utils.playwright_blocking import install_sync_resource_blocking

def safe_parse_number(value_str: str, data_type: type) -> Optional[Union[int, float]]:
    """안전한 숫자 파싱 (0값 보존)"""
    if not value_str:
        return None
    value_str = value_str.strip()
    if not value_str or value_str in ['-', 'N/A', '']:
        return None
    try:
        return data_type(value_str)
    except (ValueError, TypeError):
        return None

def parse_player_id_from_link(link_href: str) -> Optional[int]:
    """링크에서 player_id 추출"""
    try:
        if 'playerId=' in link_href:
            player_id_str = link_href.split('playerId=')[1].split('&')[0]
            return int(player_id_str)
    except (ValueError, IndexError):
        pass
    return None

def crawl_bb_basic2_data(page: Page, year: int) -> Dict[int, Dict]:
    """
    BB 헤더만 클릭하는 단순화된 Basic2 크롤링
    """
    print(f"📊 {year}년 정규시즌 BB 헤더 Basic2 크롤링 시작...")
    
    try:
        # 1. Basic1 페이지로 이동
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        print(f"   🔍 Basic1 페이지로 이동: {url}")
        page.goto(url, wait_until='load', timeout=30000)
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(2)
        
        # 2. 연도 선택
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        page.select_option(season_selector, str(year))
        print(f"   ✅ {year}년 연도 선택")
        time.sleep(1)
        
        # 3. 정규시즌 선택
        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        page.select_option(series_selector, value="0")  # 정규시즌
        print(f"   ✅ 정규시즌 선택")
        time.sleep(2)
        
        # 4. "다음" 링크로 Basic2 접근
        next_link = page.query_selector('a.next[href*="Basic2.aspx"]')
        if not next_link:
            print(f"   ❌ Basic2 '다음' 링크를 찾을 수 없습니다.")
            return {}
        
        print(f"   🔗 'Basic2' 다음 링크 클릭...")
        next_link.click()
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(2)
        
        current_url = page.url
        print(f"   ✅ Basic2 페이지 접속: {current_url}")
        
        # 5. BB 헤더 클릭
        print(f"   📊 BB(볼넷) 헤더 클릭...")
        
        bb_link = page.query_selector('a[href*="sort(\'BB_CN\')"]')
        if not bb_link:
            print(f"   ❌ BB 헤더를 찾을 수 없습니다.")
            return {}
        
        bb_link.click()
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(1)
        
        print(f"   ✅ BB 헤더 클릭 완료")
        
        # 6. 테이블 헤더 확인
        table = page.query_selector("table")
        if not table:
            print(f"   ❌ 테이블을 찾을 수 없습니다.")
            return {}
        
        thead = table.query_selector("thead")
        if thead:
            header_cells = thead.query_selector_all("th")
            headers = [cell.inner_text().strip() for cell in header_cells]
            print(f"   📋 테이블 헤더: {headers}")
        
        # 7. 모든 페이지 데이터 수집
        all_player_data = {}
        page_num = 1
        
        while True:
            print(f"      📄 페이지 {page_num} 처리 중...")
            
            # 현재 페이지 데이터 수집
            page_data = collect_current_page_bb_data(page)
            if not page_data:
                print(f"      ⚠️ 페이지 {page_num}에 데이터가 없습니다.")
                break
            
            # 데이터 병합
            for player_id, data in page_data.items():
                if player_id not in all_player_data:
                    all_player_data[player_id] = data
                else:
                    all_player_data[player_id].update(data)
            
            print(f"         ✅ {len(page_data)}명 데이터 수집, 총 {len(all_player_data)}명")
            
            # 다음 페이지로 이동
            if not goto_next_page(page):
                break
            
            page_num += 1
            time.sleep(1)
        
        print(f"   ✅ BB 헤더 기준 데이터 수집 완료: {len(all_player_data)}명")
        return all_player_data
        
    except Exception as e:
        print(f"   ❌ Basic2 BB 데이터 수집 중 오류: {e}")
        return {}

def collect_current_page_bb_data(page: Page) -> Dict[int, Dict]:
    """현재 페이지의 BB 기준 선수 데이터 수집"""
    page_data = {}
    
    try:
        table = page.query_selector("table")
        if not table:
            return page_data
        
        tbody = table.query_selector("tbody")
        if tbody:
            rows = tbody.query_selector_all("tr")
        else:
            rows = table.query_selector_all("tr")[1:]  # 첫 번째 행(헤더) 제외
        
        if not rows:
            return page_data
        
        for row in rows:
            cells = row.query_selector_all("td")
            if len(cells) < 5:
                continue
            
            # player_id 추출
            name_cell = cells[1] if len(cells) > 1 else None
            if not name_cell:
                continue
            
            player_link = name_cell.query_selector("a")
            if not player_link:
                continue
            
            player_id = parse_player_id_from_link(player_link.get_attribute("href"))
            if not player_id:
                continue
            
            player_name = player_link.inner_text().strip()
            team_code = cells[2].inner_text().strip() if len(cells) > 2 else None
            
            # 기본 정보
            player_data = {
                'player_id': player_id,
                'player_name': player_name,
                'team_code': team_code,
                'year': 2025,  # 하드코딩 (테스트용)
                'league': 'KBO',
                'source': 'PROFILE',
                'level': 'KBO1'
            }
            
            # BB 기준 테이블에서 스탯 추출
            # 예상 헤더: ['순위', '선수명', '팀명', 'AVG', 'BB', 'IBB', 'HBP', 'SO', 'GDP', 'SLG', 'OBP', 'OPS', 'MH', 'RISP', 'PH-BA']
            try:
                if len(cells) >= 15:
                    player_data.update({
                        'avg': safe_parse_number(cells[3].inner_text().strip(), float),
                        'walks': safe_parse_number(cells[4].inner_text().strip(), int),
                        'intentional_walks': safe_parse_number(cells[5].inner_text().strip(), int),
                        'hit_by_pitch': safe_parse_number(cells[6].inner_text().strip(), int),
                        'strikeouts': safe_parse_number(cells[7].inner_text().strip(), int),
                        'gdp': safe_parse_number(cells[8].inner_text().strip(), int),
                        'slg': safe_parse_number(cells[9].inner_text().strip(), float),
                        'obp': safe_parse_number(cells[10].inner_text().strip(), float),
                        'ops': safe_parse_number(cells[11].inner_text().strip(), float),
                    })
                    
                    # 확장 스탯 (JSON)
                    extra_stats = {}
                    if len(cells) > 12:
                        extra_stats['multi_hits'] = safe_parse_number(cells[12].inner_text().strip(), int)
                    if len(cells) > 13:
                        extra_stats['risp_avg'] = safe_parse_number(cells[13].inner_text().strip(), float)
                    if len(cells) > 14:
                        extra_stats['pinch_hit_avg'] = safe_parse_number(cells[14].inner_text().strip(), float)
                    
                    player_data['extra_stats'] = extra_stats
                    
            except Exception as e:
                print(f"         ⚠️ {player_name} 스탯 파싱 오류: {e}")
            
            page_data[player_id] = player_data
    
    except Exception as e:
        print(f"         ⚠️ 페이지 데이터 수집 중 오류: {e}")
    
    return page_data

def goto_next_page(page: Page) -> bool:
    """다음 페이지로 이동"""
    try:
        # 페이지네이션 확인
        pagination = page.query_selector(".paging")
        if not pagination:
            return False
        
        # "다음" 링크 찾기
        next_links = pagination.query_selector_all("a")
        for link in next_links:
            text = link.inner_text().strip()
            if "다음" in text or ">" in text:
                href = link.get_attribute("href")
                if href and "javascript:" not in href:
                    link.click()
                    page.wait_for_load_state('networkidle', timeout=30000)
                    time.sleep(1)
                    return True
        
        return False
        
    except Exception as e:
        print(f"      ⚠️ 페이지 이동 중 오류: {e}")
        return False

def main():
    """메인 실행 함수"""
    YEAR = 2025
    
    print(f"🚀 KBO {YEAR}년 BB 헤더 Basic2 크롤링 테스트 시작")
    
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        install_sync_resource_blocking(page)
        
        try:
            # BB 헤더 Basic2 데이터 수집
            bb_data = crawl_bb_basic2_data(page, YEAR)
            
            if bb_data:
                print(f"\n📊 수집 결과: {len(bb_data)}명")
                
                # 샘플 데이터 출력
                if bb_data:
                    first_player = next(iter(bb_data.values()))
                    print(f"\n📋 샘플 데이터:")
                    for key, value in first_player.items():
                        print(f"   {key}: {value}")
                
                # SQLite 저장
                print(f"\n💾 SQLite 저장 시작...")
                saved_count = save_kbo_batting_batch(bb_data, "정규시즌 BB 테스트")
                
                print(f"\n🎉 완료!")
                print(f"   📊 수집: {len(bb_data)}명")
                print(f"   💾 저장: {saved_count}명")
                print(f"   📅 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
            else:
                print(f"❌ 데이터를 수집하지 못했습니다.")
            
        except Exception as e:
            print(f"❌ 크롤링 중 오류 발생: {e}")
        
        finally:
            browser.close()

if __name__ == "__main__":
    main()
