"""
KBO 2001년 이전 투수 기록 크롤러 (레거시 버전)
2001년까지는 단순 컬럼 구조로 크롤링
투수 기본 컬럼: 순위, 선수명, 팀명, ERA, G, GS, W, L, SV, HLD, IP, H, HR, BB, SO, R, ER
"""
import argparse
import time
from datetime import datetime
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright, Page

from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.utils.team_mapping import get_team_code, get_team_mapping_for_year
from src.utils.playwright_blocking import install_sync_resource_blocking


def get_series_mapping() -> Dict[str, Dict[str, str]]:
    """시리즈 이름과 선택 값 매핑 (2001년 이전용)"""
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
        'korean_series': {
            'name': 'KBO 한국시리즈',
            'value': '7',
            'league': 'KOREAN_SERIES'
        }
    }


def safe_parse_number(value_str: str, data_type: type = int) -> Optional[int | float]:
    """안전하게 숫자를 파싱하는 함수"""
    if not value_str:
        return None
    
    value_str = value_str.strip()
    
    if not value_str or value_str in ['-', 'N/A', '']:
        return None
    
    try:
        return data_type(value_str)
    except (ValueError, TypeError):
        return None


def parse_innings(value: Optional[str]) -> Optional[float]:
    """이닝 파싱 (180 2/3 형태)"""
    if not value:
        return None
    
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None

    try:
        main_part = cleaned
        fraction_part = ""
        if " " in cleaned:
            main_part, fraction_part = cleaned.split()
        elif "/" in cleaned:
            main_part, fraction_part = "0", cleaned

        # main innings
        main_int = int(float(main_part))
        
        frac_value = 0.0
        if fraction_part and "/" in fraction_part:
            num, den = fraction_part.split("/")
            frac_value = int(num) / int(den)
        
        return round(main_int + frac_value, 2)
    except (ValueError, ZeroDivisionError):
        return None


def parse_legacy_pitching_table(page: Page, year: int, series_key: str = 'regular') -> List[Dict]:
    """
    2001년 이전 단순 투수 테이블 구조 파싱
    기본 컬럼: 순위, 선수명, 팀명, ERA, G, GS, W, L, SV, HLD, IP, H, HR, BB, SO, R, ER
    """
    players_data = []
    team_mapping = get_team_mapping_for_year(year)
    
    # 시리즈별 league 매핑
    series_to_league = {
        'regular': 'REGULAR',
        'exhibition': 'EXHIBITION', 
        'korean_series': 'KOREAN_SERIES'
    }

    try:
        table = page.query_selector("table")
        if not table:
            print("⚠️ 기록 테이블을 찾을 수 없습니다.")
            return players_data

        tbody = table.query_selector("tbody")
        if tbody:
            rows = tbody.query_selector_all("tr")
        else:
            rows = table.query_selector_all("tr")
        
        if len(rows) == 0:
            print("⚠️ 테이블에 데이터 행이 없습니다.")
            return players_data

        print(f"🔍 {len(rows)}개 행 발견")

        # 테이블 헤더 구조 확인
        thead = table.query_selector("thead")
        if thead:
            header_cells = thead.query_selector_all("th")
            headers = [cell.inner_text().strip() for cell in header_cells]
            print(f"🔍 테이블 헤더: {headers}")

        for row in rows:
            cells = row.query_selector_all("td")
            
            if len(cells) < 5:  # 최소 필드 수 확인
                continue

            try:
                # 컬럼 인덱스 확인 (순위, 선수명, 팀명, ERA, G, GS, W, L, SV, HLD, IP, H, HR, BB, SO, R, ER)
                if len(cells) < 16:  # 최소 16개 컬럼 필요
                    continue

                # 선수명과 ID 추출
                name_cell = cells[1]  # 선수명
                link = name_cell.query_selector("a")
                
                if not link:
                    continue
                
                player_name = link.inner_text().strip()
                href = link.get_attribute("href")
                
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
                    team_code = team_mapping.get(team_name, team_name)
                    print(f"⚠️ {year}년 '{team_name}' 팀 매핑 실패, 폴백: {team_code}")

                # 레거시 구조에 맞는 투수 데이터 추출
                player_data = {
                    'player_id': player_id,
                    'season': year,
                    'league': series_to_league.get(series_key, 'REGULAR'),
                    'level': 'KBO1',
                    'source': 'LEGACY_CRAWLER',
                    'team_code': team_code,
                    
                    # 레거시 투수 컬럼 매핑
                    'era': safe_parse_number(cells[3].inner_text(), float),
                    'games': safe_parse_number(cells[4].inner_text()),
                    'games_started': safe_parse_number(cells[5].inner_text()),
                    'wins': safe_parse_number(cells[6].inner_text()),
                    'losses': safe_parse_number(cells[7].inner_text()),
                    'saves': safe_parse_number(cells[8].inner_text()),
                    'holds': safe_parse_number(cells[9].inner_text()),
                    'innings_pitched': parse_innings(cells[10].inner_text()),
                    'hits_allowed': safe_parse_number(cells[11].inner_text()),
                    'home_runs_allowed': safe_parse_number(cells[12].inner_text()),
                    'walks_allowed': safe_parse_number(cells[13].inner_text()),
                    'strikeouts': safe_parse_number(cells[14].inner_text()),
                    'runs_allowed': safe_parse_number(cells[15].inner_text()),
                    'earned_runs': safe_parse_number(cells[16].inner_text()) if len(cells) > 16 else None,
                    
                    # 기본값들
                    'intentional_walks': None,
                    'hit_batters': None,
                    'wild_pitches': None,
                    'balks': None,
                    'whip': None,
                    'fip': None,
                    'k_per_nine': None,
                    'bb_per_nine': None,
                    'kbb': None,
                    
                    # 확장 통계
                    'complete_games': None,
                    'shutouts': None,
                    'quality_starts': None,
                    'blown_saves': None,
                    'tbf': None,
                    'np': None,
                    'avg_against': None,
                    'doubles_allowed': None,
                    'triples_allowed': None,
                    'sacrifices_allowed': None,
                    'sacrifice_flies_allowed': None,
                    
                    # 추가 정보
                    'extra_stats': {
                        'legacy_mode': True,
                        'rank': safe_parse_number(cells[0].inner_text()) if len(cells) > 0 else None,
                        'rankings': {}
                    }
                }

                players_data.append(player_data)
                
                print(f"   ✅ {player_name} ({team_name}) - ERA: {player_data['era']}, W-L: {player_data['wins']}-{player_data['losses']}")

            except Exception as e:
                print(f"   ⚠️ 행 파싱 오류: {e}")
                continue

    except Exception as e:
        print(f"❌ 테이블 파싱 실패: {e}")

    return players_data


def crawl_legacy_pitching_stats(year: int = 2000, series_key: str = 'regular', 
                               limit: int = None, save_to_db: bool = False, 
                               headless: bool = False) -> List[Dict]:
    """
    2001년 이전 레거시 투수 기록 크롤링
    """
    series_mapping = get_series_mapping()
    
    if series_key not in series_mapping:
        print(f"❌ 지원하지 않는 시리즈: {series_key}")
        return []
    
    series_info = series_mapping[series_key]
    all_players_data = []
    
    print(f"📊 {year}년 {series_info['name']} 레거시 투수 기록 수집 시작")
    print("-" * 60)
    
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_default_timeout(30000)
        install_sync_resource_blocking(page)

        try:
            # 투수 기록 페이지로 이동
            url = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx"
            page.goto(url, wait_until='load')
            page.wait_for_load_state('networkidle')
            time.sleep(2)

            # 1. 시즌 선택
            try:
                season_selector = 'select[name*="Season"]'
                page.select_option(season_selector, str(year))
                page.wait_for_timeout(500)
                print(f"✅ {year}년 시즌 선택")
            except Exception as e:
                print(f"❌ 시즌 선택 실패: {e}")
                return []

            # 2. 시리즈 선택
            try:
                series_selector = 'select[name*="Series"]'
                page.select_option(series_selector, value=series_info['value'])
                page.wait_for_load_state('networkidle')
                page.wait_for_timeout(1000)
                print(f"✅ {series_info['name']} 선택")
            except Exception as e:
                print(f"❌ 시리즈 선택 실패: {e}")
                return []

            # 3. 데이터 수집 (페이지네이션)
            page_num = 1
            while True:
                print(f"📄 {page_num}페이지 수집 중...")
                
                # 현재 페이지 데이터 파싱
                page_data = parse_legacy_pitching_table(page, year, series_key)
                
                if not page_data:
                    if page_num == 1:
                        print(f"⚠️ {series_info['name']}에서 데이터를 찾을 수 없습니다.")
                    break

                all_players_data.extend(page_data)
                print(f"   ✅ {page_num}페이지에서 {len(page_data)}명 수집 (누적: {len(all_players_data)}명)")

                if limit and len(all_players_data) >= limit:
                    all_players_data = all_players_data[:limit]
                    print(f"   🎯 수집 제한에 도달했습니다.")
                    break

                # 다음 페이지로 이동
                try:
                    # 단순 페이지네이션 (2001년 이전은 복잡한 페이지네이션이 없을 가능성)
                    next_page = page_num + 1
                    if next_page <= 5:  # 5페이지 내
                        next_button = page.query_selector(f'a[href*="btnNo{next_page}"]')
                    else:
                        next_button = page.query_selector('a[href*="btnNext"]')
                    
                    if not next_button:
                        print(f"   📄 마지막 페이지에 도달했습니다.")
                        break
                    
                    next_button.click()
                    page.wait_for_load_state('networkidle')
                    page.wait_for_timeout(1000)
                    page_num += 1
                    
                except Exception as e:
                    print(f"   📄 페이지 이동 실패: {e}")
                    break

        except Exception as e:
            print(f"❌ 크롤링 중 오류: {e}")
        finally:
            browser.close()

    print(f"✅ {series_info['name']} 데이터 수집 완료")
    print("-" * 60)
    print(f"✅ {series_info['name']} 크롤링 완료! 총 {len(all_players_data)}명 수집")

    # 데이터베이스 저장
    if save_to_db and all_players_data:
        print(f"\n💾 레거시 투수 데이터 DB 저장 시작...")
        try:
            saved_count = save_pitching_stats_to_db(all_players_data)
            print(f"✅ 레거시 투수 데이터 저장 완료: {saved_count}명")
        except Exception as e:
            print(f"❌ 레거시 투수 데이터 저장 실패: {e}")

    return all_players_data


def main():
    parser = argparse.ArgumentParser(description="KBO 2001년 이전 레거시 투수 기록 크롤러")
    
    parser.add_argument("--year", type=int, default=2000, help="시즌 연도 (기본값: 2000)")
    parser.add_argument("--series", type=str, default='regular', 
                       choices=['regular', 'exhibition', 'korean_series'],
                       help="크롤링할 시리즈")
    parser.add_argument("--limit", type=int, help="수집할 선수 수 제한")
    parser.add_argument("--save", action="store_true", help="DB에 저장")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드로 실행")
    
    args = parser.parse_args()

    crawl_legacy_pitching_stats(
        year=args.year,
        series_key=args.series,
        limit=args.limit,
        save_to_db=args.save,
        headless=args.headless
    )


if __name__ == "__main__":
    main()
