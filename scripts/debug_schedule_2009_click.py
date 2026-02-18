
from playwright.sync_api import sync_playwright
import time

def debug_schedule_2009_click():
    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        
        # Select 2009, 04, Regular Season
        page.select_option('#ddlYear', '2009')
        time.sleep(1)
        page.select_option('#ddlMonth', '04')
        time.sleep(1)
        page.select_option('#ddlSeries', '0,9,6') # Regular Season
        time.sleep(2)
        
        # Find table
        tbl = page.query_selector('.tbl-type06')
        link = tbl.query_selector('tbody a')
        
        if link:
            with page.expect_navigation():
                link.click()
            time.sleep(3)
            
            tables = page.query_selector_all('table')
            print(f"   Found {len(tables)} tables:")
            for i, tbl in enumerate(tables):
                tid = tbl.get_attribute('id') or "No ID"
                cls = tbl.get_attribute('class') or "No Class"
                
                # Get headers
                headers = [th.inner_text().strip() for th in tbl.query_selector_all('thead th')]
                
                print(f"     Table {i+1}: ID='{tid}', Class='{cls}'")
                print(f"       Headers: {headers}")
                # print(f"       Sample: {tbl.inner_text()[:50].replace(chr(10), ' ')}")

        browser.close()

if __name__ == "__main__":
    debug_schedule_2009_click()
