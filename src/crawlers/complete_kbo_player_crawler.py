"""
KBO 선수 타자 기록 완전 크롤러
Docs/schema/player_season_data.md 스키마를 기반으로 구현

정규시즌: Basic1 + Basic2 (헤더 클릭) 데이터 수집
기타시리즈: Basic1 기본 데이터만 수집
"""

import sys
import os
import time
from typing import Dict, List, Optional, Union
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from playwright.sync_api import sync_playwright, Page
from src.repositories.save_futures_batting import save_futures_batting_stats
from src.db.engine import create_engine_for_url

def safe_parse_number(value_str: str, data_type: type, allow_zero: bool = True) -> Optional[Union[int, float]]:
    """안전한 숫자 파싱 (0값 보존)"""
    if not value_str:
        return None
    value_str = value_str.strip()
    if not value_str or value_str in ['-', 'N/A', '']:
        return None
    try:
        parsed_value = data_type(value_str)
        return parsed_value
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

def crawl_regular_season_data(page: Page, year: int) -> Dict[int, Dict]:
    """
    정규시즌 데이터 크롤링 (Basic1 + Basic2)
    컬럼: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF + BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA
    """
    print(f"📊 {year}년 정규시즌 데이터 크롤링 시작...")
    
    # 1. Basic1 데이터 수집
    basic1_data = crawl_basic1_data(page, year, {'value': '0', 'name': '정규시즌'})
    print(f"   ✅ Basic1 데이터: {len(basic1_data)}명")
    
    # 2. Basic2 데이터 수집 (헤더 클릭)
    basic2_data = crawl_basic2_with_headers(page, year, {'value': '0', 'name': '정규시즌'})
    print(f"   ✅ Basic2 데이터: {len(basic2_data)}명")
    
    # 3. 데이터 병합
    merged_data = {}
    for player_id in basic1_data.keys():
        merged_data[player_id] = {
            **basic1_data[player_id],
            **basic2_data.get(player_id, {})
        }
    
    # Basic2에만 있는 선수들도 추가
    for player_id in basic2_data.keys():
        if player_id not in merged_data:
            merged_data[player_id] = basic2_data[player_id]
    
    print(f"   ✅ 병합 완료: {len(merged_data)}명")
    return merged_data

def crawl_basic1_data(page: Page, year: int, series_info: Dict) -> Dict[int, Dict]:
    """
    Basic1 페이지 데이터 크롤링
    컬럼: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF (정규시즌)
    컬럼: AVG,G,PA,AB,H,2B,3B,HR,RBI,SB,CS,BB,HBP,SO,GDP,E (기타시리즈)
    """
    print(f"   🔍 Basic1 데이터 수집: {series_info['name']}")
    
    try:
        # Basic1 페이지로 이동
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        page.goto(url, wait_until='load', timeout=30000)
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(2)
        
        # 연도 선택
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        page.select_option(season_selector, str(year))
        time.sleep(1)
        
        # 시리즈 선택
        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        page.select_option(series_selector, value=series_info['value'])
        time.sleep(2)
        
        # 페이지별 데이터 수집
        all_player_data = {}
        page_num = 1
        
        while True:
            print(f"      📄 페이지 {page_num} 처리 중...")
            
            # 테이블 찾기
            table = page.query_selector("table")
            if not table:
                print(f"      ⚠️ 페이지 {page_num}에서 테이블을 찾을 수 없습니다.")
                break
            
            # 헤더 확인
            thead = table.query_selector("thead")
            if thead:
                header_cells = thead.query_selector_all("th")
                headers = [cell.inner_text().strip() for cell in header_cells]
                print(f"         📋 헤더: {headers}")
            
            # 데이터 행 처리
            tbody = table.query_selector("tbody")
            if tbody:
                rows = tbody.query_selector_all("tr")
            else:
                rows = table.query_selector_all("tr")[1:]  # 첫 번째 행(헤더) 제외
            
            if not rows:
                print(f"      ⚠️ 페이지 {page_num}에 데이터가 없습니다.")
                break
            
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 5:  # 최소 데이터 확인
                    continue
                
                # 선수 링크에서 player_id 추출
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
                    'year': year,
                    'league': 'KBO',
                    'source': 'profile',
                    'series_name': series_info['name'],
                    'series_value': series_info['value']
                }
                
                # 시리즈별 컬럼 매핑
                if series_info['value'] == '0':  # 정규시즌
                    player_data.update(parse_regular_season_basic1_stats(cells))
                else:  # 기타 시리즈
                    player_data.update(parse_other_series_stats(cells))
                
                all_player_data[player_id] = player_data
            
            print(f"         ✅ {len(rows)}개 행 처리, 총 {len(all_player_data)}명")
            
            # 다음 페이지 확인
            if not goto_next_page(page):
                break
            
            page_num += 1
            time.sleep(1)
        
        return all_player_data
        
    except Exception as e:
        print(f"   ❌ Basic1 데이터 수집 중 오류: {e}")
        return {}

def parse_regular_season_basic1_stats(cells: List) -> Dict:
    """정규시즌 Basic1 통계 파싱: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF"""
    stats = {}
    try:
        if len(cells) >= 16:  # 예상 컬럼 수
            stats.update({
                'avg': safe_parse_number(cells[3].inner_text().strip(), float),
                'games': safe_parse_number(cells[4].inner_text().strip(), int),
                'plate_appearances': safe_parse_number(cells[5].inner_text().strip(), int),
                'at_bats': safe_parse_number(cells[6].inner_text().strip(), int),
                'runs': safe_parse_number(cells[7].inner_text().strip(), int),
                'hits': safe_parse_number(cells[8].inner_text().strip(), int),
                'doubles': safe_parse_number(cells[9].inner_text().strip(), int),
                'triples': safe_parse_number(cells[10].inner_text().strip(), int),
                'home_runs': safe_parse_number(cells[11].inner_text().strip(), int),
                'total_bases': safe_parse_number(cells[12].inner_text().strip(), int),
                'rbis': safe_parse_number(cells[13].inner_text().strip(), int),
                'sacrifice_bunts': safe_parse_number(cells[14].inner_text().strip(), int),
                'sacrifice_flies': safe_parse_number(cells[15].inner_text().strip(), int)
            })
    except Exception as e:
        print(f"      ⚠️ 정규시즌 Basic1 통계 파싱 오류: {e}")
    
    return stats

def parse_other_series_stats(cells: List) -> Dict:
    """기타시리즈 통계 파싱: AVG,G,PA,AB,H,2B,3B,HR,RBI,SB,CS,BB,HBP,SO,GDP,E"""
    stats = {}
    try:
        if len(cells) >= 18:  # 예상 컬럼 수
            stats.update({
                'avg': safe_parse_number(cells[3].inner_text().strip(), float),
                'games': safe_parse_number(cells[4].inner_text().strip(), int),
                'plate_appearances': safe_parse_number(cells[5].inner_text().strip(), int),
                'at_bats': safe_parse_number(cells[6].inner_text().strip(), int),
                'hits': safe_parse_number(cells[7].inner_text().strip(), int),
                'doubles': safe_parse_number(cells[8].inner_text().strip(), int),
                'triples': safe_parse_number(cells[9].inner_text().strip(), int),
                'home_runs': safe_parse_number(cells[10].inner_text().strip(), int),
                'rbis': safe_parse_number(cells[11].inner_text().strip(), int),
                'stolen_bases': safe_parse_number(cells[12].inner_text().strip(), int),
                'caught_stealing': safe_parse_number(cells[13].inner_text().strip(), int),
                'walks': safe_parse_number(cells[14].inner_text().strip(), int),
                'hit_by_pitch': safe_parse_number(cells[15].inner_text().strip(), int),
                'strikeouts': safe_parse_number(cells[16].inner_text().strip(), int),
                'gdp': safe_parse_number(cells[17].inner_text().strip(), int),
                'errors': safe_parse_number(cells[18].inner_text().strip(), int) if len(cells) > 18 else None
            })
    except Exception as e:
        print(f"      ⚠️ 기타시리즈 통계 파싱 오류: {e}")
    
    return stats

def crawl_basic2_with_headers(page: Page, year: int, series_info: Dict) -> Dict[int, Dict]:
    """
    Basic2 헤더 클릭으로 추가 데이터 수집
    컬럼: BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA
    """
    print(f"   🔍 Basic2 헤더 클릭 데이터 수집: {series_info['name']}")
    
    headers_to_click = [
        ('BB', 'BB_CN', '볼넷'),
        ('IBB', 'IB_CN', '고의사구'),
        ('HBP', 'HP_CN', '사구'),
        ('SO', 'KK_CN', '삼진'),
        ('GDP', 'GD_CN', '병살타'),
        ('SLG', 'SLG_RT', '장타율'),
        ('OBP', 'OBP_RT', '출루율'),
        ('OPS', 'OPS_RT', 'OPS'),
        ('MH', 'MH_HITTER_CN', '멀티히트'),
        ('RISP', 'SP_HRA_RT', '득점권타율'),
        ('PH-BA', 'PH_HRA_RT', '대타타율')
    ]
    
    all_player_data = {}
    
    try:
        # Basic1에서 시작하여 Basic2로 이동
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        page.goto(url, wait_until='load', timeout=30000)
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(2)
        
        # 연도 및 시리즈 선택
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        page.select_option(season_selector, str(year))
        time.sleep(1)
        
        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        page.select_option(series_selector, value=series_info['value'])
        time.sleep(2)
        
        # "다음" 링크로 Basic2 접근
        next_link = page.query_selector('a.next[href*="Basic2.aspx"]')
        if not next_link:
            print(f"      ⚠️ Basic2 '다음' 링크를 찾을 수 없습니다.")
            return {}
        
        next_link.click()
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(2)
        
        # 각 헤더별 데이터 수집
        for i, (header_name, sort_code, description) in enumerate(headers_to_click):
            print(f"      📊 {description}({header_name}) 헤더 클릭... ({i+1}/11)")
            
            try:
                # 헤더 클릭
                header_link = page.query_selector(f'a[href*="sort(\'{sort_code}\')"]')
                if not header_link:
                    print(f"         ⚠️ {header_name} 헤더를 찾을 수 없습니다.")
                    continue
                
                header_link.click()
                page.wait_for_load_state('networkidle', timeout=30000)
                time.sleep(1)
                
                # 현재 정렬 기준으로 데이터 수집
                page_data = collect_current_page_data(page, header_name)
                
                # 데이터 병합
                for player_id, data in page_data.items():
                    if player_id not in all_player_data:
                        all_player_data[player_id] = data
                    else:
                        all_player_data[player_id].update(data)
                
                print(f"         ✅ {len(page_data)}명 데이터 수집")
                
            except Exception as e:
                print(f"         ❌ {header_name} 헤더 처리 중 오류: {e}")
                continue
        
        print(f"   ✅ Basic2 헤더별 데이터 수집 완료: {len(all_player_data)}명")
        return all_player_data
        
    except Exception as e:
        print(f"   ❌ Basic2 데이터 수집 중 오류: {e}")
        return {}

def collect_current_page_data(page: Page, sort_field: str) -> Dict[int, Dict]:
    """현재 페이지의 모든 선수 데이터 수집"""
    page_data = {}
    
    try:
        # 모든 페이지 순회
        page_num = 1
        while True:
            # 테이블 데이터 파싱
            table = page.query_selector("table")
            if not table:
                break
            
            tbody = table.query_selector("tbody")
            if tbody:
                rows = tbody.query_selector_all("tr")
            else:
                rows = table.query_selector_all("tr")[1:]
            
            if not rows:
                break
            
            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 5:
                    continue
                
                # player_id 추출
                name_cell = cells[1]
                player_link = name_cell.query_selector("a")
                if not player_link:
                    continue
                
                player_id = parse_player_id_from_link(player_link.get_attribute("href"))
                if not player_id:
                    continue
                
                # 기본 정보
                player_data = {
                    'player_id': player_id,
                    'player_name': player_link.inner_text().strip(),
                    'team_code': cells[2].inner_text().strip()
                }
                
                # sort_field에 따른 데이터 추출
                player_data.update(extract_basic2_stats(cells, sort_field))
                
                page_data[player_id] = player_data
            
            # 다음 페이지로 이동
            if not goto_next_page(page):
                break
            
            page_num += 1
            time.sleep(1)
    
    except Exception as e:
        print(f"         ⚠️ 페이지 데이터 수집 중 오류: {e}")
    
    return page_data

def extract_basic2_stats(cells: List, sort_field: str) -> Dict:
    """Basic2 통계 추출"""
    stats = {}
    try:
        # 헤더 위치 매핑 (추정값, 실제 사이트 구조에 따라 조정 필요)
        field_positions = {
            'BB': 4, 'IBB': 5, 'HBP': 6, 'SO': 7, 'GDP': 8,
            'SLG': 9, 'OBP': 10, 'OPS': 11, 'MH': 12, 'RISP': 13, 'PH-BA': 14
        }
        
        field_mapping = {
            'BB': 'walks',
            'IBB': 'intentional_walks', 
            'HBP': 'hit_by_pitch',
            'SO': 'strikeouts',
            'GDP': 'gdp',
            'SLG': 'slg',
            'OBP': 'obp', 
            'OPS': 'ops',
            'MH': 'multi_hits',
            'RISP': 'risp_avg',
            'PH-BA': 'pinch_hit_avg'
        }
        
        if sort_field in field_positions and sort_field in field_mapping:
            pos = field_positions[sort_field]
            field_name = field_mapping[sort_field]
            
            if len(cells) > pos:
                value_str = cells[pos].inner_text().strip()
                
                # 데이터 타입 결정
                if sort_field in ['SLG', 'OBP', 'OPS', 'RISP', 'PH-BA']:
                    data_type = float
                else:
                    data_type = int
                
                parsed_value = safe_parse_number(value_str, data_type)
                if parsed_value is not None:
                    stats[field_name] = parsed_value
                
    except Exception as e:
        print(f"         ⚠️ Basic2 통계 추출 오류: {e}")
    
    return stats

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
            if "다음" in link.inner_text() or ">" in link.inner_text():
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

def crawl_other_series_data(page: Page, year: int, series_list: List[Dict]) -> Dict[str, Dict[int, Dict]]:
    """기타 시리즈 데이터 크롤링 (기본 데이터만)"""
    all_series_data = {}
    
    for series_info in series_list:
        print(f"📊 {year}년 {series_info['name']} 데이터 크롤링...")
        
        series_data = crawl_basic1_data(page, year, series_info)
        if series_data:
            all_series_data[series_info['name']] = series_data
            print(f"   ✅ {series_info['name']}: {len(series_data)}명")
        else:
            print(f"   ⚠️ {series_info['name']}: 데이터 없음")
    
    return all_series_data

def save_to_database(player_data: Dict[int, Dict], series_name: str):
    """데이터베이스에 저장"""
    try:
        print(f"💾 {series_name} 데이터 저장 중...")
        
        saved_count = 0
        for player_id, data in player_data.items():
            try:
                # 저장용 데이터 형식 변환
                save_data = {
                    'player_id': data['player_id'],
                    'year': data['year'],
                    'league': data['league'],
                    'source': data['source'],
                    'series_name': data['series_name'],
                    'team_code': data['team_code'],
                    'games': data.get('games'),
                    'plate_appearances': data.get('plate_appearances'),
                    'at_bats': data.get('at_bats'),
                    'runs': data.get('runs'),
                    'hits': data.get('hits'),
                    'doubles': data.get('doubles'),
                    'triples': data.get('triples'),
                    'home_runs': data.get('home_runs'),
                    'rbis': data.get('rbis'),
                    'walks': data.get('walks'),
                    'strikeouts': data.get('strikeouts'),
                    'avg': data.get('avg'),
                    'obp': data.get('obp'),
                    'slg': data.get('slg'),
                    'ops': data.get('ops'),
                    'stolen_bases': data.get('stolen_bases'),
                    'caught_stealing': data.get('caught_stealing'),
                    'hit_by_pitch': data.get('hit_by_pitch'),
                    'intentional_walks': data.get('intentional_walks'),
                    'sacrifice_bunts': data.get('sacrifice_bunts'),
                    'sacrifice_flies': data.get('sacrifice_flies'),
                    'gdp': data.get('gdp'),
                    'errors': data.get('errors'),
                    'total_bases': data.get('total_bases'),
                    'extra_stats': {
                        'multi_hits': data.get('multi_hits'),
                        'risp_avg': data.get('risp_avg'),
                        'pinch_hit_avg': data.get('pinch_hit_avg')
                    }
                }
                
                save_futures_batting_stats(save_data)
                saved_count += 1
                
            except Exception as e:
                print(f"   ⚠️ {data['player_name']} 저장 실패: {e}")
                continue
        
        print(f"   ✅ {saved_count}/{len(player_data)}명 저장 완료")
        return saved_count
        
    except Exception as e:
        print(f"   ❌ 데이터베이스 저장 중 오류: {e}")
        return 0

def main():
    """메인 실행 함수"""
    # 크롤링 대상 설정
    YEAR = 2025
    
    # 시리즈 정의
    SERIES_LIST = [
        {'value': '1', 'name': 'KBO 시범경기'},
        {'value': '4', 'name': 'KBO 와일드카드'},
        {'value': '3', 'name': 'KBO 준플레이오프'},
        {'value': '5', 'name': 'KBO 플레이오프'},
        {'value': '7', 'name': 'KBO 한국시리즈'}
    ]
    
    print(f"🚀 KBO {YEAR}년 선수 타자 기록 완전 크롤링 시작")
    print(f"📋 대상: 정규시즌(Enhanced) + {len(SERIES_LIST)}개 시리즈(Basic)")
    
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            total_saved = 0
            
            # 1. 정규시즌 데이터 수집 (Basic1 + Basic2)
            print(f"\n{'='*50}")
            print(f"📊 1단계: 정규시즌 데이터 수집 (Enhanced)")
            print(f"{'='*50}")
            
            regular_season_data = crawl_regular_season_data(page, YEAR)
            if regular_season_data:
                saved = save_to_database(regular_season_data, "정규시즌")
                total_saved += saved
            
            # 2. 기타 시리즈 데이터 수집 (Basic1만)
            print(f"\n{'='*50}")
            print(f"📊 2단계: 기타 시리즈 데이터 수집 (Basic)")
            print(f"{'='*50}")
            
            other_series_data = crawl_other_series_data(page, YEAR, SERIES_LIST)
            for series_name, series_data in other_series_data.items():
                if series_data:
                    saved = save_to_database(series_data, series_name)
                    total_saved += saved
            
            # 3. 최종 결과
            print(f"\n{'='*50}")
            print(f"🎉 크롤링 완료")
            print(f"{'='*50}")
            print(f"📊 총 저장된 레코드: {total_saved}개")
            print(f"📅 크롤링 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
        except Exception as e:
            print(f"❌ 크롤링 중 오류 발생: {e}")
        
        finally:
            browser.close()

if __name__ == "__main__":
    main()