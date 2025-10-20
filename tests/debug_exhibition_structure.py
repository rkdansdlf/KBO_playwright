"""
시범경기 페이지의 실제 컬럼 구조를 확인하는 디버깅 스크립트
"""
from playwright.sync_api import sync_playwright
import time

def debug_exhibition_structure():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print("📊 시범경기 페이지 컬럼 구조 분석 시작...")
            
            # Basic1 페이지로 이동
            url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
            print(f"🔍 Basic1 페이지로 이동: {url}")
            page.goto(url, wait_until='load', timeout=30000)
            page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(3)
            
            # 2025년 시범경기 설정
            season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
            page.select_option(season_selector, "2025")
            time.sleep(1)
            
            series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
            page.select_option(series_selector, value="1")  # 시범경기
            time.sleep(2)
            
            # 테이블 헤더 확인
            print("\n🔍 시범경기 테이블 헤더:")
            thead = page.query_selector("thead")
            if thead:
                header_cells = thead.query_selector_all("th")
                headers = []
                for i, cell in enumerate(header_cells):
                    text = cell.inner_text().strip()
                    headers.append(text)
                    print(f"   [{i}] '{text}'")
                
                print(f"\n총 컬럼 수: {len(headers)}")
                print(f"헤더 리스트: {headers}")
            else:
                print("   thead를 찾을 수 없습니다.")
            
            # 첫 번째 데이터 행 샘플 확인
            print("\n🔍 첫 번째 데이터 행 샘플:")
            table = page.query_selector("table")
            if table:
                tbody = table.query_selector("tbody")
                if tbody:
                    rows = tbody.query_selector_all("tr")
                else:
                    rows = table.query_selector_all("tr")
                
                if len(rows) > 0:
                    first_row = rows[0]
                    cells = first_row.query_selector_all("td")
                    print(f"   컬럼 수: {len(cells)}개")
                    for i, cell in enumerate(cells):
                        content = cell.inner_text().strip()
                        print(f"   [{i}]: '{content}'")
            
            print("\n⏸️  확인을 위해 10초 대기...")
            time.sleep(10)
            
        except Exception as e:
            print(f"❌ 오류: {e}")
        
        finally:
            browser.close()

if __name__ == "__main__":
    debug_exhibition_structure()