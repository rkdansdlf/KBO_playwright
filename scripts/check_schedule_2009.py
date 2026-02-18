
from playwright.sync_api import sync_playwright
import time

def check_schedule_link_2009():
    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    
    print(f"📡 Navigating to: {url}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, wait_until="networkidle")
            
            # Select 2009 Year
            page.select_option('#ddlYear', '2009')
            time.sleep(1)
            
            # Select April
            page.select_option('#ddlMonth', '04')
            time.sleep(2)
            
            # Print the HTML of the schedule table
            tbl = page.query_selector('.tbl-type06')
            if tbl:
                print("\n--- Schedule Table HTML (First 1000 chars) ---")
                print(tbl.inner_html()[:1000])
                
                # Check for any 'a' tags in the table
                links = tbl.query_selector_all('a')
                print(f"\nFound {len(links)} links in the table.")
                for i, link in enumerate(links[:5]):
                    print(f"Link {i+1}: Text='{link.inner_text()}', Href='{link.get_attribute('href')}'")
            else:
                print("❌ '.tbl-type06' table not found.")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    check_schedule_link_2009()
