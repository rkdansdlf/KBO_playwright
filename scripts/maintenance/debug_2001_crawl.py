
from playwright.sync_api import sync_playwright
import time
import sys
import os

sys.path.append(os.getcwd())
from src.crawlers.player_batting_all_series_crawler import parse_batting_stats_table, go_to_next_page, get_series_mapping

def debug_2001():
    year = 2001
    series_info = get_series_mapping()['regular']
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx"
        print(f"Go to {url}")
        page.goto(url)
        time.sleep(2)
        
        # Select Season
        page.select_option('select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]', str(year))
        print(f"Selected {year}")
        time.sleep(2)
        
        # Select Regular
        page.select_option('select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]', series_info['value'])
        print(f"Selected Series {series_info['name']}")
        time.sleep(2)
        
        # Parse Page 1
        print("Parsing Page 1...")
        data = parse_batting_stats_table(page, "regular", year, use_fast=False) # Try legacy parser
        print(f"Found {len(data)} rows on Page 1")
        if data:
            print(f"Sample: {data[0]}")
            
        # Try Next Page
        print("Attempting to go to Page 2...")
        success = go_to_next_page(page, 1)
        print(f"Next Page Success: {success}")
        
        if success:
             time.sleep(2)
             data2 = parse_batting_stats_table(page, "regular", year, use_fast=False)
             print(f"Found {len(data2)} rows on Page 2")
             
        browser.close()

if __name__ == "__main__":
    debug_2001()
