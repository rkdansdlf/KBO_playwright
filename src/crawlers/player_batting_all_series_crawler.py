"""
KBO 전체 시리즈 타자 기록 크롤러
- 정규시즌, 시범경기, 와일드카드, 준플레이오프, 플레이오프, 한국시리즈

Usage:
    # 2025년 모든 시리즈 크롤링
    python -m src.crawlers.player_batting_all_series_crawler --year 2025 --save

    # 특정 시리즈만
    python -m src.crawlers.player_batting_all_series_crawler --year 2025 --series regular --save
    python -m src.crawlers.player_batting_all_series_crawler --year 2025 --series exhibition --save
"""
import argparse
import time
from datetime import datetime
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright, Page

from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.utils.team_mapping import get_team_code, get_team_mapping_for_year


def get_team_code_mapping() -> Dict[str, str]:
    """팀명 → 팀 코드 매핑 (하위 호환성을 위해 유지)"""
    return {
        'LG': 'LG',
        'NC': 'NC', 
        'KT': 'KT',
        '삼성': 'SS',
        '롯데': 'LT',
        '두산': 'OB',
        'KIA': 'HT',
        '한화': 'HH',
        '키움': 'WO',
        'SSG': 'SK'
    }


def get_series_mapping() -> Dict[str, Dict[str, str]]:
    """시리즈 이름과 선택 값 매핑 (실제 페이지에서 확인된 값)"""
    return {
        'regular': {
            'name': 'KBO 정규시즌',
            'value': '0',
            'league': 'REGULAR'
        },
        'exhibition': {
            'name': 'KBO 시범경기',
            'value': '1',
            'league': 'EXHIBITION'
        },
        'wildcard': {
            'name': 'KBO 와일드카드',
            'value': '4',
            'league': 'WILDCARD'
        },
        'semi_playoff': {
            'name': 'KBO 준플레이오프',
            'value': '3',
            'league': 'SEMI_PLAYOFF'
        },
        'playoff': {
            'name': 'KBO 플레이오프',
            'value': '5',
            'league': 'PLAYOFF'
        },
        'korean_series': {
            'name': 'KBO 한국시리즈',
            'value': '7',
            'league': 'KOREAN_SERIES'
        }
    }


def safe_parse_number(value_str: str, data_type: type, allow_zero: bool = True) -> Optional[int | float]:
    """
    안전하게 숫자를 파싱하는 함수
    
    Args:
        value_str: 파싱할 문자열
        data_type: 변환할 데이터 타입 (int 또는 float)
        allow_zero: 0 값을 허용할지 여부
    
    Returns:
        파싱된 숫자 또는 None
    """
    if not value_str:
        return None
    
    value_str = value_str.strip()
    
    # 빈 문자열, "-", "N/A" 등은 None으로 처리
    if not value_str or value_str in ['-', 'N/A', '']:
        return None
    
    try:
        parsed_value = data_type(value_str)
        # 0은 실제 값이므로 0으로 저장
        return parsed_value
    except (ValueError, TypeError):
        return None


def parse_batting_stats_table(page: Page, series_key: str, year: int = 2025) -> List[Dict]:
    """
    현재 페이지의 타자 기록 테이블 파싱
    
    Args:
        page: Playwright Page 객체
        series_key: 시리즈 키 (regular, exhibition, etc.)
        year: 크롤링 대상 년도 (팀 매핑용)
    
    Returns:
        선수별 타격 기록 리스트
    """
    players_data = []
    # 동적 팀 매핑 사용 (년도별 역대 팀 고려)
    team_mapping = get_team_mapping_for_year(year)

    try:
        # 테이블 찾기
        table = page.query_selector("table")
        if not table:
            print("⚠️ 기록 테이블을 찾을 수 없습니다.")
            return players_data

        # 테이블 구조 확인
        tbody = table.query_selector("tbody")
        if tbody:
            rows = tbody.query_selector_all("tr")
        else:
            rows = table.query_selector_all("tr")
        
        if len(rows) == 0:
            print("⚠️ 테이블에 데이터 행이 없습니다.")
            return players_data

        print(f"🔍 {len(rows)}개 행 발견")

        # 테이블 헤더 구조 확인 (디버깅)
        thead = table.query_selector("thead")
        table_type = "Basic1"
        if thead and series_key == 'regular':
            header_cells = thead.query_selector_all("th")
            headers = [cell.inner_text().strip() for cell in header_cells]
            print(f"🔍 테이블 헤더: {headers}")
            # Basic2 특징적인 헤더들이 있는지 확인
            basic2_indicators = ['BB', '볼넷', 'IBB', 'HBP', 'SLG', 'OBP', 'OPS']
            is_basic2_page = any(indicator in ''.join(headers) for indicator in basic2_indicators)
            table_type = "Basic2" if is_basic2_page else "Basic1"
            print(f"🔍 페이지 타입: {table_type}")

        # 첫 번째 행의 컬럼 구조 확인 (디버깅)
        if len(rows) > 0:
            first_row_cells = rows[0].query_selector_all("td")
            print(f"🔍 컬럼 수: {len(first_row_cells)}개")
            print("🔍 첫 번째 행 각 셀 내용:")
            for i, cell in enumerate(first_row_cells):
                content = cell.inner_text().strip()
                print(f"   [{i}]: '{content}'")

        for row_idx, row in enumerate(rows):
            cells = row.query_selector_all("td")
            
            if len(cells) < 10:  # 최소 필드 수 확인
                continue

            try:
                # 선수명과 선수 ID 추출
                name_cell = cells[1]  # 선수명
                name_link = name_cell.query_selector("a")
                
                if not name_link:
                    continue
                
                player_name = name_link.inner_text().strip()
                href = name_link.get_attribute("href")
                
                # href에서 playerId 추출
                import re
                player_id_match = re.search(r'playerId=(\d+)', href)
                if not player_id_match:
                    continue
                
                player_id = int(player_id_match.group(1))
                
                # 팀명 추출 및 동적 매핑
                team_name = cells[2].inner_text().strip()
                team_code = get_team_code(team_name, year)
                if not team_code:
                    # 정적 매핑 폴백
                    team_code = team_mapping.get(team_name, team_name)
                    print(f"⚠️ {year}년 '{team_name}' 팀 매핑 실패, 폴백: {team_code}")

                # 시리즈별 컬럼 구조에 따른 데이터 추출
                if series_key == 'regular':
                    # 정규시즌: 헤더 분석하여 Basic1 vs Basic2 구분
                    # Basic1 실제 구조 (순위,선수명,팀명,AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF)
                    # Basic2 실제 구조 (순위,선수명,팀명,AVG,BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA)
                    
                    # 테이블 헤더로 Basic1/Basic2 구분
                    thead = table.query_selector("thead")
                    is_basic2 = False
                    if thead:
                        header_cells = thead.query_selector_all("th")
                        headers = [cell.inner_text().strip() for cell in header_cells]
                        # Basic2 특징적인 헤더들이 있는지 확인
                        basic2_indicators = ['BB', '볼넷', 'IBB', 'HBP', 'SLG', 'OBP', 'OPS']
                        is_basic2 = any(indicator in ''.join(headers) for indicator in basic2_indicators)
                    
                    if is_basic2:
                        # Basic2 구조 처리 (순위,선수명,팀명,AVG,BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA)
                        batting_data = {
                            'player_id': player_id,
                            'player_name': player_name,
                            'team_code': team_code,
                            'avg': safe_parse_number(cells[3].inner_text(), float),  # [3]: AVG
                            'walks': safe_parse_number(cells[4].inner_text(), int),  # [4]: BB
                            'intentional_walks': safe_parse_number(cells[5].inner_text(), int),  # [5]: IBB
                            'hbp': safe_parse_number(cells[6].inner_text(), int),  # [6]: HBP
                            'strikeouts': safe_parse_number(cells[7].inner_text(), int),  # [7]: SO
                            'gdp': safe_parse_number(cells[8].inner_text(), int),  # [8]: GDP
                            'slg': safe_parse_number(cells[9].inner_text(), float),  # [9]: SLG
                            'obp': safe_parse_number(cells[10].inner_text(), float),  # [10]: OBP
                            'ops': safe_parse_number(cells[11].inner_text(), float),  # [11]: OPS
                            'extra_stats': {
                                'multi_hits': safe_parse_number(cells[12].inner_text(), int) if len(cells) > 12 else None,  # [12]: MH
                                'risp_avg': safe_parse_number(cells[13].inner_text(), float) if len(cells) > 13 else None,  # [13]: RISP
                                'pinch_hit_avg': safe_parse_number(cells[14].inner_text(), float) if len(cells) > 14 else None  # [14]: PH-BA
                            }
                        }
                    else:
                        # Basic1 구조 처리 (순위,선수명,팀명,AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF)
                        batting_data = {
                            'player_id': player_id,
                            'player_name': player_name,
                            'team_code': team_code,
                            'avg': safe_parse_number(cells[3].inner_text(), float),  # [3]: AVG
                            'games': safe_parse_number(cells[4].inner_text(), int),  # [4]: G
                            'plate_appearances': safe_parse_number(cells[5].inner_text(), int),  # [5]: PA
                            'at_bats': safe_parse_number(cells[6].inner_text(), int),  # [6]: AB
                            'runs': safe_parse_number(cells[7].inner_text(), int),  # [7]: R
                            'hits': safe_parse_number(cells[8].inner_text(), int),  # [8]: H
                            'doubles': safe_parse_number(cells[9].inner_text(), int),  # [9]: 2B
                            'triples': safe_parse_number(cells[10].inner_text(), int),  # [10]: 3B
                            'home_runs': safe_parse_number(cells[11].inner_text(), int),  # [11]: HR
                            'total_bases': safe_parse_number(cells[12].inner_text(), int),  # [12]: TB
                            'rbi': safe_parse_number(cells[13].inner_text(), int),  # [13]: RBI
                            'sacrifice_hits': safe_parse_number(cells[14].inner_text(), int),  # [14]: SAC
                            'sacrifice_flies': safe_parse_number(cells[15].inner_text(), int),  # [15]: SF
                        }
                else:
                    # 기타 시리즈: 실제 구조 (순위,선수명,팀명,AVG,G,PA,AB,H,2B,3B,HR,RBI,SB,CS,BB,HBP,SO,GDP,E)
                    batting_data = {
                        'player_id': player_id,
                        'player_name': player_name,
                        'team_code': team_code,
                        'avg': safe_parse_number(cells[3].inner_text(), float),  # [3]: AVG
                        'games': safe_parse_number(cells[4].inner_text(), int),  # [4]: G
                        'plate_appearances': safe_parse_number(cells[5].inner_text(), int),  # [5]: PA
                        'at_bats': safe_parse_number(cells[6].inner_text(), int),  # [6]: AB
                        'hits': safe_parse_number(cells[7].inner_text(), int),  # [7]: H
                        'doubles': safe_parse_number(cells[8].inner_text(), int),  # [8]: 2B
                        'triples': safe_parse_number(cells[9].inner_text(), int),  # [9]: 3B
                        'home_runs': safe_parse_number(cells[10].inner_text(), int),  # [10]: HR
                        'rbi': safe_parse_number(cells[11].inner_text(), int),  # [11]: RBI
                        'stolen_bases': safe_parse_number(cells[12].inner_text(), int),  # [12]: SB
                        'caught_stealing': safe_parse_number(cells[13].inner_text(), int),  # [13]: CS
                        'walks': safe_parse_number(cells[14].inner_text(), int),  # [14]: BB
                        'hbp': safe_parse_number(cells[15].inner_text(), int),  # [15]: HBP
                        'strikeouts': safe_parse_number(cells[16].inner_text(), int),  # [16]: SO
                        'gdp': safe_parse_number(cells[17].inner_text(), int),  # [17]: GDP
                        # [18]: E(실책) - extra_stats에 저장
                        'extra_stats': {
                            'errors': safe_parse_number(cells[18].inner_text(), int) if len(cells) > 18 else None
                        }
                    }

                players_data.append(batting_data)
                
                if row_idx < 3:  # 처음 3개 행만 출력 (디버깅)
                    if series_key == 'regular':
                        page_type = "Basic2" if is_basic2 else "Basic1"
                        key_stat = batting_data.get('walks', batting_data.get('home_runs', 'N/A'))
                        print(f"   ✅ {player_name} ({team_name}) - [{page_type}] AVG: {batting_data['avg']}, Key: {key_stat}")
                    else:
                        print(f"   ✅ {player_name} ({team_name}) - AVG: {batting_data['avg']}, HR: {batting_data.get('home_runs', 'N/A')}")
                
            except (ValueError, AttributeError) as e:
                print(f"⚠️ 행 파싱 오류: {e}")
                continue

    except Exception as e:
        print(f"❌ 테이블 파싱 오류: {e}")

    return players_data


def go_to_next_page(page: Page, current_page_num: int) -> bool:
    """
    다음 페이지로 이동 (1→2,3,4,5→다음→6,7,8,9,10→다음 반복)
    
    Args:
        page: Playwright Page 객체
        current_page_num: 현재 페이지 번호
    
    Returns:
        성공 여부 (마지막 페이지이면 False)
    """
    try:
        # 1→2,3,4,5→다음→6,7,8,9,10→다음 패턴
        if current_page_num % 5 == 0:  # 5페이지마다 "다음" 버튼 클릭
            # 다음 버튼 찾기 (실제 페이지 구조에 맞는 셀렉터)
            next_button_selector = 'a[href*="btnNext"]'
            next_button = page.query_selector(next_button_selector)
            
            if not next_button:
                print("📄 다음 페이지 버튼을 찾을 수 없습니다.")
                return False
            
            # 버튼이 비활성화되어 있는지 확인
            if next_button.get_attribute("disabled") or "disabled" in (next_button.get_attribute("class") or ""):
                print("📄 마지막 페이지에 도달했습니다.")
                return False
            
            next_button.click()
            page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)
            print(f"➡️ 다음 버튼 클릭 ({current_page_num}페이지 후)")
            
        else:  # 개별 페이지 번호 클릭 (1,2,3,4,5 범위 내)
            next_page_num = current_page_num + 1
            relative_page_num = ((next_page_num - 1) % 5) + 1  # 1~5 범위로 변환
            
            # 실제 페이지 구조에 맞는 셀렉터 사용
            page_button_selector = f'a[href*="btnNo{relative_page_num}"]'
            page_button = page.query_selector(page_button_selector)
            
            if page_button:
                page_button.click()
                page.wait_for_load_state('networkidle', timeout=30000)
                time.sleep(1)
                print(f"➡️ {next_page_num}페이지로 이동 (btnNo{relative_page_num})")
            else:
                print(f"📄 페이지 {next_page_num} 버튼(btnNo{relative_page_num})을 찾을 수 없습니다.")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ 페이지 이동 실패: {e}")
        return False


def crawl_basic2_with_headers(page: Page, year: int, series_info: dict) -> Dict[int, Dict]:
    """
    정규시즌용 Basic2 페이지에서 각 헤더를 클릭하여 고급 통계 데이터 수집
    스키마 기준: BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA
    
    접근 순서: 타자 -> 정규시즌 선택 -> 연도 선택 -> "다음" 링크 클릭하여 Basic2 접근
    """
    # 클릭할 헤더들과 정렬 코드 정의 (실제 페이지에서 확인된 코드)
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
        # 올바른 접근 순서: Basic1에서 시작하여 "다음" 링크로 Basic2 접근
        print(f"   🔍 Basic2 접근을 위해 Basic1에서 시작...")
        
        # 1. Basic1 페이지로 이동
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        page.goto(url, wait_until='load', timeout=30000)
        page.wait_for_load_state('networkidle', timeout=30000)
        time.sleep(2)
        
        # 2. 연도 선택
        try:
            season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
            page.select_option(season_selector, str(year))
            print(f"   ✅ {year}년 연도 선택")
            time.sleep(1)
        except Exception as e:
            print(f"   ⚠️ 연도 선택 중 오류: {e}")
            return {}

        # 3. 정규시즌 선택
        try:
            series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
            page.select_option(series_selector, value=series_info['value'])
            print(f"   ✅ {series_info['name']} 선택")
            time.sleep(2)
        except Exception as e:
            print(f"   ⚠️ 시리즈 선택 중 오류: {e}")
            return {}
        
        # 4. "다음" 링크 클릭하여 Basic2로 이동
        try:
            next_link_selector = 'a[href="/Record/Player/HitterBasic/Basic2.aspx"]'
            next_link = page.query_selector(next_link_selector)
            
            if not next_link:
                # 다른 가능한 셀렉터들 시도
                possible_selectors = [
                    'a.next',
                    'a[class*="next"]',
                    'a[href*="Basic2"]',
                    'a:has-text("다음")'
                ]
                
                for selector in possible_selectors:
                    next_link = page.query_selector(selector)
                    if next_link:
                        print(f"   🔍 다음 링크 발견: {selector}")
                        break
            
            if next_link:
                print(f"   🔗 'Basic2' 다음 링크 클릭...")
                next_link.click()
                page.wait_for_load_state('networkidle', timeout=30000)
                time.sleep(3)
                
                current_url = page.url
                print(f"   ✅ Basic2 페이지 접속: {current_url}")
                
                # Basic2 페이지 확인
                if "Basic2" not in current_url:
                    print(f"   ⚠️ Basic2 접근 실패, 현재 URL: {current_url}")
                    return {}
                    
            else:
                print(f"   ❌ Basic2로 이동하는 '다음' 링크를 찾을 수 없습니다.")
                # 사용 가능한 링크들 디버깅
                all_links = page.query_selector_all("a")
                print(f"   🔍 사용 가능한 링크들:")
                for i, link in enumerate(all_links[:20]):  # 처음 20개만
                    href = link.get_attribute("href") or ""
                    text = link.inner_text().strip()
                    class_name = link.get_attribute("class") or ""
                    if "Basic" in href or "다음" in text or "next" in class_name:
                        print(f"      [{i}] href: '{href}', text: '{text}', class: '{class_name}'")
                return {}
                
        except Exception as e:
            print(f"   ⚠️ Basic2 접근 중 오류: {e}")
            return {}
        
        print(f"   🔍 Basic2 헤더별 데이터 수집 시작...")
        
        # 각 헤더별로 데이터 수집
        for header_name, sort_code, description in headers_to_click:
            print(f"   📊 {description}({header_name}) 헤더 클릭...")
            
            try:
                # 헤더 클릭 (정렬 변경)
                header_link = f'a[href="javascript:sort(\'{sort_code}\');"]'
                header_element = page.query_selector(header_link)
                
                if header_element:
                    header_element.click()
                    page.wait_for_load_state('networkidle', timeout=30000)
                    time.sleep(2)
                    
                    # 현재 정렬 기준으로 데이터 파싱 (첫 페이지만)
                    page_data = parse_basic2_header_data(page, header_name, description, year)
                    
                    # 데이터 병합
                    for player_id, player_data in page_data.items():
                        if player_id not in all_player_data:
                            all_player_data[player_id] = player_data
                        else:
                            # 기존 데이터 업데이트 (None이 아닌 값만)
                            for key, value in player_data.items():
                                if value is not None and key not in ['player_id', 'player_name', 'team_code']:
                                    if key == 'extra_stats':
                                        # extra_stats 딕셔너리 병합
                                        if 'extra_stats' not in all_player_data[player_id]:
                                            all_player_data[player_id]['extra_stats'] = {}
                                        for stat_key, stat_value in value.items():
                                            if stat_value is not None:
                                                all_player_data[player_id]['extra_stats'][stat_key] = stat_value
                                    else:
                                        all_player_data[player_id][key] = value
                    
                    print(f"      ✅ {description} 기준 {len(page_data)}명 데이터 수집")
                    
                else:
                    print(f"      ⚠️ {header_name} 헤더 버튼을 찾을 수 없습니다.")
                    
                    # 사용 가능한 정렬 링크들 디버깅
                    print(f"      🔍 사용 가능한 정렬 링크들:")
                    sort_links = page.query_selector_all('a[href*="javascript:sort"]')
                    for i, link in enumerate(sort_links[:15]):  # 처음 15개만
                        href = link.get_attribute("href") or ""
                        text = link.inner_text().strip()
                        print(f"         [{i}] '{text}' -> '{href}'")
                    
            except Exception as e:
                print(f"      ❌ {header_name} 헤더 처리 중 오류: {e}")
            
            # 서버 부하 방지
            time.sleep(1)
        
        print(f"   ✅ Basic2 헤더별 데이터 수집 완료: {len(all_player_data)}명")
        
    except Exception as e:
        print(f"   ❌ Basic2 크롤링 중 오류: {e}")
    
    return all_player_data


def parse_basic2_header_data(page: Page, current_header: str, description: str, year: int = 2025) -> Dict[int, Dict]:
    """
    Basic2 페이지에서 특정 헤더 클릭 후 데이터 파싱
    각 헤더 클릭시 해당 기준으로 정렬된 선수 데이터를 수집
    """
    players_data = {}
    team_mapping = get_team_mapping_for_year(year)

    try:
        table = page.query_selector("table")
        if not table:
            return players_data

        tbody = table.query_selector("tbody")
        if tbody:
            rows = tbody.query_selector_all("tr")
        else:
            rows = table.query_selector_all("tr")
        
        if len(rows) == 0:
            return players_data

        # 테이블 헤더 구조 확인 (디버깅용)
        thead = page.query_selector("thead")
        if thead:
            header_cells = thead.query_selector_all("th")
            headers = [cell.inner_text().strip() for cell in header_cells]
            print(f"      🔍 {description} 기준 테이블 헤더: {headers}")

        # 첫 번째 행 샘플 확인 (디버깅용)
        if len(rows) > 0:
            first_row_cells = rows[0].query_selector_all("td")
            print(f"      🔍 {description} 기준 첫 행 데이터 ({len(first_row_cells)}개 컬럼):")
            for i, cell in enumerate(first_row_cells[:10]):  # 처음 10개만
                content = cell.inner_text().strip()
                print(f"         [{i}]: '{content}'")

        for row_idx, row in enumerate(rows):
            cells = row.query_selector_all("td")
            
            if len(cells) < 5:  # 최소 필드 수 확인
                continue

            try:
                # 선수명과 ID 추출
                name_cell = cells[1]  # 선수명
                name_link = name_cell.query_selector("a")
                
                if not name_link:
                    continue
                
                player_name = name_link.inner_text().strip()
                href = name_link.get_attribute("href")
                
                import re
                player_id_match = re.search(r'playerId=(\d+)', href)
                if not player_id_match:
                    continue
                
                player_id = int(player_id_match.group(1))
                
                # 팀명 추출 및 동적 매핑
                team_name = cells[2].inner_text().strip()
                team_code = get_team_code(team_name, year)
                if not team_code:
                    # 정적 매핑 폴백
                    team_code = team_mapping.get(team_name, team_name)
                    print(f"⚠️ {year}년 '{team_name}' 팀 매핑 실패, 폴백: {team_code}")

                # 헤더별로 해당 데이터만 추출
                batting_data = {
                    'player_id': player_id,
                    'player_name': player_name,
                    'team_code': team_code,
                }
                
                # Basic2 테이블의 실제 구조에 맞게 데이터 추출
                # 헤더: ['순위', '선수명', '팀명', 'AVG', 'BB', 'IBB', 'HBP', 'SO', 'GDP', 'SLG', 'OBP', 'OPS', 'MH', 'RISP', 'PH-BA']
                #       [0]    [1]    [2]    [3]   [4]   [5]   [6]    [7]   [8]    [9]    [10]   [11]   [12]   [13]    [14]
                
                # 헤더에 따른 정확한 컬럼 위치에서 데이터 추출
                if current_header == 'BB' and len(cells) > 4:
                    batting_data['walks'] = safe_parse_number(cells[4].inner_text().strip(), int)
                elif current_header == 'IBB' and len(cells) > 5:
                    batting_data['intentional_walks'] = safe_parse_number(cells[5].inner_text().strip(), int)
                elif current_header == 'HBP' and len(cells) > 6:
                    batting_data['hbp'] = safe_parse_number(cells[6].inner_text().strip(), int)
                elif current_header == 'SO' and len(cells) > 7:
                    batting_data['strikeouts'] = safe_parse_number(cells[7].inner_text().strip(), int)
                elif current_header == 'GDP' and len(cells) > 8:
                    batting_data['gdp'] = safe_parse_number(cells[8].inner_text().strip(), int)
                elif current_header == 'SLG' and len(cells) > 9:
                    batting_data['slg'] = safe_parse_number(cells[9].inner_text().strip(), float)
                elif current_header == 'OBP' and len(cells) > 10:
                    batting_data['obp'] = safe_parse_number(cells[10].inner_text().strip(), float)
                elif current_header == 'OPS' and len(cells) > 11:
                    batting_data['ops'] = safe_parse_number(cells[11].inner_text().strip(), float)
                elif current_header == 'MH' and len(cells) > 12:
                    if 'extra_stats' not in batting_data:
                        batting_data['extra_stats'] = {}
                    batting_data['extra_stats']['multi_hits'] = safe_parse_number(cells[12].inner_text().strip(), int)
                elif current_header == 'RISP' and len(cells) > 13:
                    if 'extra_stats' not in batting_data:
                        batting_data['extra_stats'] = {}
                    batting_data['extra_stats']['risp_avg'] = safe_parse_number(cells[13].inner_text().strip(), float)
                elif current_header == 'PH-BA' and len(cells) > 14:
                    if 'extra_stats' not in batting_data:
                        batting_data['extra_stats'] = {}
                    batting_data['extra_stats']['pinch_hit_avg'] = safe_parse_number(cells[14].inner_text().strip(), float)

                players_data[player_id] = batting_data
                
                if row_idx < 3:  # 첫 3개 행만 출력
                    sort_value = "N/A"
                    if current_header in ['BB', 'IBB', 'HBP', 'SO', 'GDP']:
                        sort_value = batting_data.get(current_header.lower(), "N/A")
                    elif current_header in ['SLG', 'OBP', 'OPS']:
                        sort_value = batting_data.get(current_header.lower(), "N/A")
                    elif current_header in ['MH', 'RISP', 'PH-BA']:
                        sort_value = batting_data.get('extra_stats', {}).get(current_header.lower().replace('-', '_'), "N/A")
                    
                    print(f"      ✅ {player_name} ({team_name}) - {current_header}: {sort_value}")
                
            except (ValueError, AttributeError) as e:
                print(f"      ⚠️ {description} 행 파싱 오류: {e}")
                continue

    except Exception as e:
        print(f"      ❌ {description} 테이블 파싱 오류: {e}")

    return players_data




def crawl_series_batting_stats(year: int = 2025, series_key: str = 'regular', 
                             limit: int = None, save_to_db: bool = False, 
                             headless: bool = False) -> List[Dict]:
    """
    특정 시리즈의 타자 기록을 크롤링
    
    Args:
        year: 시즌 연도
        series_key: 시리즈 키 (regular, exhibition, wildcard, etc.)
        limit: 수집할 선수 수 제한
        save_to_db: DB에 저장할지 여부
    
    Returns:
        수집된 타자 기록 리스트
    """
    series_mapping = get_series_mapping()
    
    if series_key not in series_mapping:
        print(f"❌ 지원하지 않는 시리즈: {series_key}")
        return []
    
    series_info = series_mapping[series_key]
    all_players_data = []
    
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_default_timeout(30000)

        try:
            print(f"\n📊 {year}년 {series_info['name']} 타자 기록 수집 시작")
            print("-" * 60)

            # 페이지로 이동 (Basic1 사용)
            url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
            page.goto(url, wait_until='load', timeout=30000)
            page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)

            # 시즌과 시리즈 설정
            try:
                # 시즌 연도 선택
                season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
                page.select_option(season_selector, str(year))
                print(f"✅ {year}년 시즌 선택")
                time.sleep(1)

                # 시리즈 선택
                series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
                
                # 시리즈 옵션들 확인 (디버깅)
                series_options = page.query_selector_all(f'{series_selector} option')
                print(f"🔍 사용 가능한 시리즈 옵션:")
                for option in series_options:
                    value = option.get_attribute('value')
                    text = option.inner_text().strip()
                    print(f"   값: '{value}' - 텍스트: '{text}'")
                
                page.select_option(series_selector, value=series_info['value'])
                print(f"✅ {series_info['name']} 선택")
                time.sleep(1)

                # 타석(PA) 기준 정렬
                pa_sort_link = 'a[href="javascript:sort(\'PA_CN\');"]'
                if page.query_selector(pa_sort_link):
                    page.click(pa_sort_link)
                    print(f"✅ 타석(PA) 기준 정렬 적용")
                    page.wait_for_load_state('networkidle', timeout=30000)
                    time.sleep(2)
                else:
                    print("⚠️ 타석 정렬 버튼을 찾을 수 없습니다.")

            except Exception as e:
                print(f"⚠️ 페이지 설정 중 오류: {e}")

            # 페이징 처리하여 모든 데이터 수집
            page_num = 1
            total_collected = 0
            
            while True:
                print(f"📄 {page_num}페이지 수집 중...")
                
                # 현재 페이지 데이터 파싱
                page_data = parse_batting_stats_table(page, series_key, year)
                
                if not page_data:
                    if page_num == 1:
                        print(f"⚠️ {series_info['name']}에서 데이터를 찾을 수 없습니다.")
                    else:
                        print(f"📄 {page_num}페이지에서 데이터 없음. 페이징 종료.")
                    break
                
                # 시즌 정보 추가
                for player_data in page_data:
                    player_data.update({
                        'season': year,
                        'league': series_info['league'],
                        'level': 'KBO1',
                        'source': 'CRAWLER'
                    })
                
                all_players_data.extend(page_data)
                total_collected += len(page_data)
                
                print(f"   ✅ {page_num}페이지에서 {len(page_data)}명 수집 (누적: {total_collected}명)")
                
                # 제한 수 확인
                if limit and total_collected >= limit:
                    print(f"🎯 목표 수({limit}명) 달성. 수집 중단.")
                    all_players_data = all_players_data[:limit]
                    break
                
                # 페이징 구조 디버깅 (첫 번째 페이지에서만)
                if page_num == 1:
                    print("🔍 페이징 구조 디버깅:")
                    # 페이징 관련 요소들 찾기
                    pager_elements = page.query_selector_all("*[class*='pag'], *[id*='pag'], *[class*='Pag'], a[href*='Page'], a[onclick*='Page']")
                    for i, elem in enumerate(pager_elements[:10]):  # 처음 10개만
                        try:
                            tag_name = elem.evaluate("el => el.tagName")
                            class_name = elem.get_attribute("class") or ""
                            href = elem.get_attribute("href") or ""
                            onclick = elem.get_attribute("onclick") or ""
                            text = elem.inner_text().strip() or ""
                            print(f"   [{i}] {tag_name}: class='{class_name}', href='{href}', onclick='{onclick}', text='{text}'")
                        except:
                            pass
                
                # 다음 페이지로 이동
                if not go_to_next_page(page, page_num):
                    print(f"📄 마지막 페이지에 도달했습니다.")
                    break
                
                page_num += 1
                time.sleep(1)  # 서버 부하 방지

            # 정규시즌인 경우 Basic2 페이지에서 추가 데이터 수집
            if series_key == 'regular' and all_players_data:
                print(f"\n🔍 정규시즌 Basic2 추가 데이터 수집 시작...")
                basic2_data = crawl_basic2_with_headers(page, year, series_info)
                
                # Basic1과 Basic2 데이터 병합
                if basic2_data:
                    basic1_dict = {p['player_id']: p for p in all_players_data}
                    
                    for player_id, basic2_player in basic2_data.items():
                        if player_id in basic1_dict:
                            # Basic1 데이터에 Basic2 데이터 병합
                            for key, value in basic2_player.items():
                                if value is not None and key not in ['player_id', 'player_name', 'team_code', 'season', 'league', 'level', 'source']:
                                    basic1_dict[player_id][key] = value
                    
                    # 리스트로 다시 변환
                    all_players_data = list(basic1_dict.values())
                    print(f"✅ Basic1 + Basic2 데이터 병합 완료")
                else:
                    print(f"⚠️ Basic2 데이터 수집 실패, Basic1 데이터만 사용")
            
            print(f"✅ {series_info['name']} 데이터 수집 완료")

        except Exception as e:
            print(f"❌ 크롤링 중 오류: {e}")
        
        finally:
            browser.close()

    print("-" * 60)
    print(f"✅ {series_info['name']} 크롤링 완료! 총 {len(all_players_data)}명 수집")

    # DB 저장 (안전한 외래키 제약조건 우회)
    if save_to_db and all_players_data:
        print(f"\n💾 타자 데이터 DB 저장 시작 (외래키 제약조건 임시 비활성화)...")
        try:
            saved_count = save_batting_stats_safe(all_players_data)
            print(f"✅ 타자 데이터 저장 완료: {saved_count}명")
        except Exception as e:
            print(f"❌ 타자 데이터 저장 실패: {e}")

    return all_players_data


def crawl_all_series(year: int = 2025, limit: int = None, save_to_db: bool = False, headless: bool = False) -> Dict[str, List[Dict]]:
    """
    모든 시리즈의 타자 기록을 크롤링
    
    Returns:
        시리즈별 수집된 데이터 딕셔너리
    """
    series_mapping = get_series_mapping()
    all_series_data = {}
    
    for series_key, series_info in series_mapping.items():
        print(f"\n🚀 {series_info['name']} 시작...")
        series_data = crawl_series_batting_stats(year, series_key, limit, save_to_db, headless)
        all_series_data[series_key] = series_data
        
        # 시리즈 간 대기
        time.sleep(3)
    
    return all_series_data


def main():
    parser = argparse.ArgumentParser(description="KBO 전체 시리즈 타자 기록 크롤러")
    
    parser.add_argument("--year", type=int, default=2025, help="시즌 연도 (기본값: 2025)")
    parser.add_argument("--series", type=str, help="특정 시리즈만 크롤링 (regular, exhibition, wildcard, etc.)")
    parser.add_argument("--limit", type=int, help="수집할 선수 수 제한")
    parser.add_argument("--save", action="store_true", help="DB에 저장")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드로 실행")
    
    args = parser.parse_args()

    if args.series:
        # 특정 시리즈만 크롤링
        crawl_series_batting_stats(args.year, args.series, args.limit, args.save, args.headless)
    else:
        # 모든 시리즈 크롤링
        all_data = crawl_all_series(args.year, args.limit, args.save, args.headless)
        
        # 전체 요약
        print(f"\n" + "=" * 60)
        print(f"📈 전체 수집 요약 ({args.year}년)")
        print("=" * 60)
        for series_key, data in all_data.items():
            series_name = get_series_mapping()[series_key]['name']
            print(f"  {series_name}: {len(data)}명")
        
        total_players = sum(len(data) for data in all_data.values())
        print(f"\n총 수집 선수: {total_players}명")


if __name__ == "__main__":
    main()