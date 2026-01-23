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
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright, Page

from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.utils.team_mapping import get_team_code, get_team_mapping_for_year
from src.utils.playwright_blocking import install_sync_resource_blocking


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


def _extract_rows_fast(page: Page, table_selector: str = "table") -> Optional[List[Dict[str, object]]]:
    try:
        payload = page.evaluate(
            """
            (selector) => {
                const table = document.querySelector(selector);
                if (!table) return null;
                const body = table.tBodies && table.tBodies.length ? table.tBodies[0] : table;
                const rows = Array.from(body.querySelectorAll('tr'));
                return rows.map((row) => {
                    const cells = Array.from(row.querySelectorAll('td')).map(td => (td.innerText || '').trim());
                    const link = row.querySelector('td:nth-child(2) a');
                    return {
                        cells,
                        linkText: link ? (link.innerText || '').trim() : null,
                        linkHref: link ? link.getAttribute('href') : null,
                    };
                });
            }
            """,
            table_selector,
        )
        return payload or []
    except Exception:
        return None


def _extract_player_id_from_href(href: Optional[str]) -> Optional[int]:
    if not href:
        return None
    match = re.search(r"playerId=(\d+)", href)
    return int(match.group(1)) if match else None


def _is_basic2_headers(headers: List[str]) -> bool:
    basic2_indicators = ["BB", "볼넷", "IBB", "HBP", "SLG", "OBP", "OPS"]
    combined = "".join(headers)
    return any(indicator in combined for indicator in basic2_indicators)


def _build_batting_data(
    cells: List[str],
    player_id: int,
    player_name: str,
    team_code: str,
    series_key: str,
    is_basic2: bool,
) -> Dict:
    def cell(idx: int) -> Optional[str]:
        return cells[idx] if len(cells) > idx else None

    if series_key == "regular":
        if is_basic2:
            return {
                "player_id": player_id,
                "player_name": player_name,
                "team_code": team_code,
                "avg": safe_parse_number(cell(3), float),
                "walks": safe_parse_number(cell(4), int),
                "intentional_walks": safe_parse_number(cell(5), int),
                "hbp": safe_parse_number(cell(6), int),
                "strikeouts": safe_parse_number(cell(7), int),
                "gdp": safe_parse_number(cell(8), int),
                "slg": safe_parse_number(cell(9), float),
                "obp": safe_parse_number(cell(10), float),
                "ops": safe_parse_number(cell(11), float),
                "extra_stats": {
                    "multi_hits": safe_parse_number(cell(12), int),
                    "risp_avg": safe_parse_number(cell(13), float),
                    "pinch_hit_avg": safe_parse_number(cell(14), float),
                },
            }
        return {
            "player_id": player_id,
            "player_name": player_name,
            "team_code": team_code,
            "avg": safe_parse_number(cell(3), float),
            "games": safe_parse_number(cell(4), int),
            "plate_appearances": safe_parse_number(cell(5), int),
            "at_bats": safe_parse_number(cell(6), int),
            "runs": safe_parse_number(cell(7), int),
            "hits": safe_parse_number(cell(8), int),
            "doubles": safe_parse_number(cell(9), int),
            "triples": safe_parse_number(cell(10), int),
            "home_runs": safe_parse_number(cell(11), int),
            "total_bases": safe_parse_number(cell(12), int),
            "rbi": safe_parse_number(cell(13), int),
            "sacrifice_hits": safe_parse_number(cell(14), int),
            "sacrifice_flies": safe_parse_number(cell(15), int),
        }

    return {
        "player_id": player_id,
        "player_name": player_name,
        "team_code": team_code,
        "avg": safe_parse_number(cell(3), float),
        "games": safe_parse_number(cell(4), int),
        "plate_appearances": safe_parse_number(cell(5), int),
        "at_bats": safe_parse_number(cell(6), int),
        "hits": safe_parse_number(cell(7), int),
        "doubles": safe_parse_number(cell(8), int),
        "triples": safe_parse_number(cell(9), int),
        "home_runs": safe_parse_number(cell(10), int),
        "rbi": safe_parse_number(cell(11), int),
        "stolen_bases": safe_parse_number(cell(12), int),
        "caught_stealing": safe_parse_number(cell(13), int),
        "walks": safe_parse_number(cell(14), int),
        "hbp": safe_parse_number(cell(15), int),
        "strikeouts": safe_parse_number(cell(16), int),
        "gdp": safe_parse_number(cell(17), int),
        "extra_stats": {"errors": safe_parse_number(cell(18), int)},
    }


def _parse_batting_stats_table_fast(page: Page, series_key: str, year: int = 2025) -> List[Dict]:
    """
    Parse batting table using JS extraction for reduced RPC.
    """
    team_mapping = get_team_mapping_for_year(year)

    extraction_script = """
    () => {
        const rows = document.querySelectorAll('table tbody tr');
        if (rows.length === 0) return null;

        const headers = Array.from(document.querySelectorAll('table thead th')).map(th => th.innerText.trim());
        const basic2_indicators = ['BB', '볼넷', 'IBB', 'HBP', 'SLG', 'OBP', 'OPS'];
        const is_basic2 = basic2_indicators.some(ind => headers.join('').includes(ind));

        const results = [];
        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            if (cells.length < 10) return;

            const nameLink = cells[1].querySelector('a');
            if (!nameLink) return;

            const playerName = nameLink.innerText.trim();
            const href = nameLink.getAttribute('href');
            const idMatch = href ? href.match(/playerId=(\d+)/) : null;
            if (!idMatch) return;
            const playerId = parseInt(idMatch[1], 10);

            const teamName = cells[2].innerText.trim();
            results.push({
                player_id: playerId,
                player_name: playerName,
                team_name: teamName,
                raw_cells: cells.map(c => c.innerText.trim()),
            });
        });
        return { is_basic2, results };
    }
    """

    try:
        js_result = page.evaluate(extraction_script)
        if not js_result or not isinstance(js_result, dict) or not js_result.get("results"):
            return []

        extracted_rows = js_result["results"]
        is_basic2 = js_result.get("is_basic2", False)

        players_data = []
        for row in extracted_rows:
            player_id = row["player_id"]
            player_name = row["player_name"]
            team_name = row["team_name"]
            cells = row["raw_cells"]

            team_code = get_team_code(team_name, year)
            if not team_code:
                team_code = team_mapping.get(team_name, team_name)

            batting_data = _build_batting_data(
                cells=cells,
                player_id=player_id,
                player_name=player_name,
                team_code=team_code,
                series_key=series_key,
                is_basic2=is_basic2,
            )
            players_data.append(batting_data)

        return players_data
    except Exception as exc:
        print(f"❌ 테이블 파싱 오류 (JS): {exc}")
        return []


def _parse_batting_stats_table_legacy(page: Page, series_key: str, year: int = 2025) -> List[Dict]:
    team_mapping = get_team_mapping_for_year(year)
    try:
        table = page.query_selector("table")
        if not table:
            return []

        thead = table.query_selector("thead")
        headers = []
        if thead:
            header_cells = thead.query_selector_all("th")
            headers = [cell.inner_text().strip() for cell in header_cells]
        is_basic2 = _is_basic2_headers(headers) if headers else False

        tbody = table.query_selector("tbody")
        rows = tbody.query_selector_all("tr") if tbody else table.query_selector_all("tr")
        if not rows:
            return []

        players_data = []
        for row in rows:
            cell_nodes = row.query_selector_all("td")
            if len(cell_nodes) < 10:
                continue

            cells = [cell.inner_text().strip() for cell in cell_nodes]
            name_link = cell_nodes[1].query_selector("a")
            href = name_link.get_attribute("href") if name_link else None
            player_id = _extract_player_id_from_href(href)
            if not player_id:
                continue

            player_name = name_link.inner_text().strip() if name_link else (cells[1] if len(cells) > 1 else "")
            team_name = cells[2] if len(cells) > 2 else ""
            team_code = get_team_code(team_name, year)
            if not team_code:
                team_code = team_mapping.get(team_name, team_name)

            batting_data = _build_batting_data(
                cells=cells,
                player_id=player_id,
                player_name=player_name,
                team_code=team_code,
                series_key=series_key,
                is_basic2=is_basic2,
            )
            players_data.append(batting_data)

        return players_data
    except Exception as exc:
        print(f"❌ 테이블 파싱 오류 (Legacy): {exc}")
        return []


def parse_batting_stats_table(
    page: Page,
    series_key: str,
    year: int = 2025,
    use_fast: Optional[bool] = None,
) -> List[Dict]:
    if use_fast is None:
        use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"
    if use_fast:
        return _parse_batting_stats_table_fast(page, series_key, year)
    return _parse_batting_stats_table_legacy(page, series_key, year)


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
    # Refactored: 헤더별 클릭 대신 전체 페이지 순회로 변경
    all_player_data = {}
    
    try:
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
            
            # fallback find
            if not next_link:
                possible_selectors = ['a.next', 'a[href*="Basic2"]', 'a:has-text("다음")']
                for sel in possible_selectors:
                    next_link = page.query_selector(sel)
                    if next_link: break
            
            if next_link:
                print(f"   🔗 'Basic2' 다음 링크 클릭...")
                next_link.click()
                page.wait_for_load_state('networkidle', timeout=30000)
                time.sleep(3)
                
                if "Basic2" not in page.url:
                    print(f"   ⚠️ Basic2 접근 실패, 현재 URL: {page.url}")
                    return {}
            else:
                print(f"   ❌ Basic2로 이동하는 '다음' 링크를 찾을 수 없습니다.")
                return {}
                
        except Exception as e:
            print(f"   ⚠️ Basic2 접근 중 오류: {e}")
            return {}
        
        print(f"   🔍 Basic2 전체 페이지 순회 시작...")
        
        # 5. Basic2 페이지 전체 순회
        page_num = 1
        while True:
            # 현재 페이지 데이터 파싱 using parse_batting_stats_table
            # This function automatically detects Basic2 headers via is_basic2 logic
            current_page_data = parse_batting_stats_table(page, "regular", year)
            
            for player_stat in current_page_data:
                pid = player_stat['player_id']
                # Basic2 data keys mapping
                # parse_batting_stats_table returns structured dict with keys like 'walks', 'ops' etc.
                if pid not in all_player_data:
                    all_player_data[pid] = player_stat
                else:
                    # Merge if needed, but usually we just want the Basic2 specific fields
                    # However, parse_batting_stats_table returns full record for that view.
                    # Since we are iterating full list, we just take it.
                    # Note: The caller might merge this with Basic1 data.
                    all_player_data[pid].update(player_stat)

            print(f"      ✅ {page_num}페이지: {len(current_page_data)}명 수집")

            # 다음 페이지 이동
            if not go_to_next_page(page, page_num):
                break
            page_num += 1
            time.sleep(1)

        print(f"   ✅ Basic2 전체 수집 완료: {len(all_player_data)}명")
        
    except Exception as e:
        print(f"   ❌ Basic2 크롤링 중 오류: {e}")
    
    return all_player_data


def _parse_basic2_header_data_legacy(
    page: Page,
    current_header: str,
    description: str,
    year: int = 2025,
) -> Dict[int, Dict]:
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
                player_id = _extract_player_id_from_href(href)
                if not player_id:
                    continue
                
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


def _parse_basic2_header_data_fast(
    page: Page,
    current_header: str,
    description: str,
    year: int = 2025,
) -> Dict[int, Dict]:
    players_data: Dict[int, Dict] = {}
    team_mapping = get_team_mapping_for_year(year)

    rows_data = _extract_rows_fast(page)
    if not rows_data:
        return players_data

    thead = page.query_selector("thead")
    if thead:
        header_cells = thead.query_selector_all("th")
        headers = [cell.inner_text().strip() for cell in header_cells]
        print(f"      🔍 {description} 기준 테이블 헤더: {headers}")

    if rows_data:
        first_row = rows_data[0]
        cells = first_row.get("cells") or []
        print(f"      🔍 {description} 기준 첫 행 데이터 ({len(cells)}개 컬럼):")
        for idx, value in enumerate(cells[:10]):
            print(f"         [{idx}]: '{value}'")

    for row in rows_data:
        cells = row.get("cells") or []
        if len(cells) < 5:
            continue

        href = row.get("linkHref")
        player_id = _extract_player_id_from_href(href)
        if not player_id:
            continue

        player_name = (row.get("linkText") or (cells[1] if len(cells) > 1 else "")).strip()
        team_name = cells[2] if len(cells) > 2 else ""
        team_code = get_team_code(team_name, year)
        if not team_code:
            team_code = team_mapping.get(team_name, team_name)

        batting_data = {
            "player_id": player_id,
            "player_name": player_name,
            "team_code": team_code,
        }

        if current_header == "BB" and len(cells) > 4:
            batting_data["walks"] = safe_parse_number(cells[4].strip(), int)
        elif current_header == "IBB" and len(cells) > 5:
            batting_data["intentional_walks"] = safe_parse_number(cells[5].strip(), int)
        elif current_header == "HBP" and len(cells) > 6:
            batting_data["hbp"] = safe_parse_number(cells[6].strip(), int)
        elif current_header == "SO" and len(cells) > 7:
            batting_data["strikeouts"] = safe_parse_number(cells[7].strip(), int)
        elif current_header == "GDP" and len(cells) > 8:
            batting_data["gdp"] = safe_parse_number(cells[8].strip(), int)
        elif current_header == "SLG" and len(cells) > 9:
            batting_data["slg"] = safe_parse_number(cells[9].strip(), float)
        elif current_header == "OBP" and len(cells) > 10:
            batting_data["obp"] = safe_parse_number(cells[10].strip(), float)
        elif current_header == "OPS" and len(cells) > 11:
            batting_data["ops"] = safe_parse_number(cells[11].strip(), float)
        elif current_header == "MH" and len(cells) > 12:
            batting_data.setdefault("extra_stats", {})
            batting_data["extra_stats"]["multi_hits"] = safe_parse_number(cells[12].strip(), int)
        elif current_header == "RISP" and len(cells) > 13:
            batting_data.setdefault("extra_stats", {})
            batting_data["extra_stats"]["risp_avg"] = safe_parse_number(cells[13].strip(), float)
        elif current_header == "PH-BA" and len(cells) > 14:
            batting_data.setdefault("extra_stats", {})
            batting_data["extra_stats"]["pinch_hit_avg"] = safe_parse_number(cells[14].strip(), float)

        players_data[player_id] = batting_data

    return players_data


def parse_basic2_header_data(
    page: Page,
    current_header: str,
    description: str,
    year: int = 2025,
    use_fast: Optional[bool] = None,
) -> Dict[int, Dict]:
    if use_fast is None:
        use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"
    if use_fast:
        return _parse_basic2_header_data_fast(page, current_header, description, year)
    return _parse_basic2_header_data_legacy(page, current_header, description, year)




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
        install_sync_resource_blocking(page)

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
