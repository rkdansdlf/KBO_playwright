from playwright.sync_api import sync_playwright

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print("Navigating to KBO Pitcher Basic page...")
        page.goto("https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx", wait_until="networkidle")
        
        # Select 2026
        print("Selecting 2026...")
        page.select_option('select[name*="ddlSeason"]', "2026")
        page.wait_for_load_state("networkidle")
        
        # Capture table HTML
        table = page.query_selector("table.tData01")
        if table:
            print("Table found!")
            print(f"Table classes: {table.get_attribute('class')}")
            headers = page.evaluate("() => Array.from(document.querySelectorAll('table.tData01 thead th')).map(th => th.innerText.trim())")
            print(f"Headers: {headers}")
        else:
            print("Table NOT found!")
            print(f"Body content (snippet): {page.content()[:1000]}")
            
        browser.close()

if __name__ == "__main__":
    run()
