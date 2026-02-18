
from playwright.sync_api import sync_playwright
import time

def debug_schedule_2009():
    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        
        year = '2009'
        print(f"📡 Checking Year {year}...")
        
        # Select Year
        page.select_option('#ddlYear', year)
        time.sleep(1)
        
        # Check Series Options
        series_options = page.eval_on_selector_all('#ddlSeries option', 'options => options.map(o => ({text: o.innerText, value: o.value}))')
        print(f"   Series Options: {series_options}")
        
        # Select Month (April)
        page.select_option('#ddlMonth', '04')
        time.sleep(2)
        
        # Try each series
        for opt in series_options:
            val = opt['value']
            txt = opt['text']
            if not val: continue
            
            print(f"   👉 Selecting Series: {txt} ({val})")
            page.select_option('#ddlSeries', val)
            time.sleep(2)
            
            tbl = page.query_selector('.tbl-type06')
            if tbl and '데이터가 없습니다' not in tbl.inner_text():
                print(f"      ✅ Data Found for Series {val}!")
                # Print first game link
                link = tbl.query_selector('a')
                if link:
                    href = link.get_attribute('href')
                    print(f"      Sample Link: {href}")
            else:
                 print(f"      ❌ No Data for Series {val}")

        browser.close()

if __name__ == "__main__":
    debug_schedule_2009()
