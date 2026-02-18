
from playwright.sync_api import sync_playwright
import time

def investigate_2009_game_detail():
    game_id = "20090404HHSK0"
    game_date = "20090404"
    # Try REVIEW section first as it was the default link
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    
    print(f"📡 Navigating to: {url}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, wait_until="networkidle")
            time.sleep(5) 
            
            # Check for generic container
            container = page.query_selector('.contents')
            if not container:
                print("❌ '.contents' container not found. Page might be empty.")
            
            # Check for Tabs
            tabs = page.query_selector_all('.tab-type1 li')
            print(f"\nFound {len(tabs)} tabs:")
            for t in tabs:
                print(f"  - {t.inner_text()}")

            # Check for current section content
            # 2009 might use different classes. Let's dump all tables again.
            tables = page.query_selector_all('table')
            print(f"\nFound {len(tables)} tables:")
            for i, tbl in enumerate(tables):
                cls = tbl.get_attribute('class') or "No Class"
                summary = tbl.get_attribute('summary') or "No Summary"
                print(f"  Table {i+1}: Class='{cls}', Summary='{summary}'")
                
                # Check for Lineup specific keywords
                text = tbl.inner_text()
                if "투수" in text and "타자" in text:
                     print("    -> Potential Lineup/Boxscore Table")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    investigate_2009_game_detail()
