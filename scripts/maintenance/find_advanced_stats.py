"""
Basic2나 다른 페이지에서 고급 통계를 찾는 스크립트
"""
from playwright.sync_api import sync_playwright
import time

def find_advanced_stats():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print("📊 고급 통계 페이지 찾기 시작...")
            
            # 다양한 URL 시도
            urls_to_try = [
                "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx",
                "https://www.koreabaseball.com/Record/Player/HitterBasic/BasicOld.aspx",
                "https://www.koreabaseball.com/Record/Player/HitterDetail/Basic.aspx"
            ]
            
            for url in urls_to_try:
                print(f"\n🔍 {url} 시도...")
                page.goto(url, wait_until='load', timeout=30000)
                page.wait_for_load_state('networkidle', timeout=30000)
                time.sleep(2)
                
                print(f"   실제 URL: {page.url}")
                
                # 2025년 정규시즌 설정
                try:
                    season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
                    if page.query_selector(season_selector):
                        page.select_option(season_selector, "2025")
                        time.sleep(1)
                    
                    series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
                    if page.query_selector(series_selector):
                        page.select_option(series_selector, value="0")
                        time.sleep(2)
                except:
                    pass
                
                # 테이블 헤더 확인
                thead = page.query_selector("thead")
                if thead:
                    header_cells = thead.query_selector_all("th")
                    headers = [cell.inner_text().strip() for cell in header_cells]
                    print(f"   헤더: {headers}")
                    
                    # BB, SLG, OBP, OPS 등이 있는지 확인
                    advanced_stats = ['BB', 'IBB', 'SLG', 'OBP', 'OPS', 'MH', 'RISP', 'PH-BA']
                    found_stats = [stat for stat in advanced_stats if stat in headers]
                    if found_stats:
                        print(f"   ✅ 고급 통계 발견: {found_stats}")
                    else:
                        print(f"   ❌ 고급 통계 없음")
                
                # 탭이나 링크 확인
                print("   사용 가능한 탭/링크:")
                tabs = page.query_selector_all('a[href*="Basic"], .tab a, .nav a')
                for i, tab in enumerate(tabs[:10]):
                    href = tab.get_attribute("href") or ""
                    text = tab.inner_text().strip()
                    if "Basic" in href:
                        print(f"      [{i}] '{text}' -> '{href}'")
            
            print("\n⏸️  수동 확인을 위해 15초 대기...")
            time.sleep(15)
            
        except Exception as e:
            print(f"❌ 오류: {e}")
        
        finally:
            browser.close()

if __name__ == "__main__":
    find_advanced_stats()