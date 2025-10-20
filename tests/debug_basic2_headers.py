"""
Basic2 페이지의 실제 헤더 구조를 확인하는 디버깅 스크립트
"""
from playwright.sync_api import sync_playwright
import time

def debug_basic2_headers():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print("📊 Basic2 페이지 헤더 구조 분석 시작...")
            
            # 먼저 기록실 메인 페이지로 이동하여 네비게이션 확인
            print("🔍 기록실 페이지 구조 확인...")
            main_url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
            page.goto(main_url, wait_until='load', timeout=30000)
            page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(2)
            
            # 탭 구조 확인
            print("\n🔍 탭 구조 확인:")
            tabs = page.query_selector_all('.tab a, .nav a, a[href*="Basic"]')
            for i, tab in enumerate(tabs):
                href = tab.get_attribute("href") or ""
                text = tab.inner_text().strip()
                if "Basic" in href or "Hitter" in href:
                    print(f"   [{i}] TAB: '{text}' -> '{href}'")
            
            # Basic2 페이지로 이동
            url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx"
            print(f"\n🔍 Basic2 페이지로 이동: {url}")
            page.goto(url, wait_until='load', timeout=30000)
            page.wait_for_load_state('networkidle', timeout=30000)
            time.sleep(3)
            
            # URL 확인
            current_url = page.url
            print(f"🔍 현재 URL: {current_url}")
            
            # 2025년 시범경기 설정
            season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
            page.select_option(season_selector, "2025")
            time.sleep(1)
            
            series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
            page.select_option(series_selector, value="1")
            time.sleep(2)
            
            # 모든 정렬 가능한 링크 찾기
            print("\n🔍 모든 정렬 링크 찾기:")
            sort_links = page.query_selector_all('a[href*="javascript:sort"]')
            
            for i, link in enumerate(sort_links):
                href = link.get_attribute("href")
                text = link.inner_text().strip()
                print(f"   [{i}] href: '{href}' - text: '{text}'")
            
            # 테이블 헤더 구조 확인
            print("\n🔍 테이블 헤더 구조 확인:")
            thead = page.query_selector("thead")
            if thead:
                header_cells = thead.query_selector_all("th")
                for i, cell in enumerate(header_cells):
                    text = cell.inner_text().strip()
                    links = cell.query_selector_all("a")
                    link_info = []
                    for link in links:
                        href = link.get_attribute("href")
                        link_text = link.inner_text().strip()
                        link_info.append(f"link: '{link_text}' -> '{href}'")
                    
                    print(f"   [{i}] '{text}' - {link_info if link_info else 'no links'}")
            else:
                print("   thead를 찾을 수 없습니다.")
            
            # 첫 번째 테이블의 모든 <a> 태그 찾기
            print("\n🔍 테이블 내 모든 <a> 태그 확인:")
            table = page.query_selector("table")
            if table:
                all_links = table.query_selector_all("a")
                for i, link in enumerate(all_links[:20]):  # 처음 20개만
                    href = link.get_attribute("href") or ""
                    text = link.inner_text().strip()
                    if "javascript:sort" in href:
                        print(f"   [{i}] SORT LINK: '{text}' -> '{href}'")
            
            # 잠시 대기하여 수동으로 페이지 확인 가능
            print("\n⏸️  페이지 확인을 위해 10초 대기...")
            time.sleep(10)
            
        except Exception as e:
            print(f"❌ 디버깅 중 오류: {e}")
        
        finally:
            browser.close()

if __name__ == "__main__":
    debug_basic2_headers()