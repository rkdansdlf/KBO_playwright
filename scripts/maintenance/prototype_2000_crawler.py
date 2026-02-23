
import sys
import os
from playwright.sync_api import sync_playwright

sys.path.insert(0, os.getcwd())

from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler
from src.utils.team_history import resolve_team_code_for_season

def run_prototype():
    # Example game from 2000 season Opening Day (April 5, 2000)
    # Samsung (SS) vs Lotte (LT) at Busan
    game_id = "20000405SSLT0"
    game_date = "2000-04-05"
    
    # KBO Box Score URL pattern
    url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameId={game_id}"
    
    print(f"🚀 Testing extraction for Game: {game_id}")
    print(f"🔗 URL: {url}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, wait_until="networkidle")
            print(f"✅ Page loaded. Current URL: {page.url}")
            
            # Save screenshot for debugging
            page.screenshot(path="debug_2000_main.png")
            print("📸 Screenshot saved to debug_2000_main.png")
            
            # Try to find 'Box Score' or '리뷰' tab
            try:
                page.wait_for_selector("#liBoxScore", timeout=5000)
                page.click("#liBoxScore")
                print("✅ Clicked #liBoxScore")
            except:
                print("⚠️ #liBoxScore not found. Trying direct BoxScore URL...")
                direct_url = f"https://www.koreabaseball.com/Schedule/GameCenter/BoxScore.aspx?gameId={game_id}"
                page.goto(direct_url, wait_until="networkidle")
                print(f"🔗 Navigated to direct URL: {page.url}")
                page.screenshot(path="debug_2000_boxscore.png")
            
            # Wait for stats tables to load
            print("⏳ Waiting for statistics tables...")
            page.wait_for_selector("#tblAwayHitter1", timeout=10000)
            
            crawler = LegacyGameDetailCrawler()
            details = crawler.extract_game_details(page, game_id, game_date)
            
            print("\n" + "="*50)
            print(f"📊 Extracted Details for {game_id}")
            print(f"Stadium: {details['metadata'].get('stadium')}")
            print(f"Away Team: {details['away_team_code']}")
            print(f"Home Team: {details['home_team_code']}")
            
            print("\n🏏 Away Hitters (Top 3):")
            for h in details['hitters']['away'][:3]:
                print(f"  - {h['player_name']} ({h['position']}): {h['stats']}")
                
            print("\n🏏 Home Hitters (Top 3):")
            for h in details['hitters']['home'][:3]:
                print(f"  - {h['player_name']} ({h['position']}): {h['stats']}")
                
            print("\n⚾ Away Pitchers:")
            for p_stat in details['pitchers']['away']:
                print(f"  - {p_stat['player_name']}: {p_stat['stats']}")
                
            print("\n⚾ Home Pitchers:")
            for p_stat in details['pitchers']['home']:
                print(f"  - {p_stat['player_name']}: {p_stat['stats']}")
                
            print("="*50)
            
        except Exception as e:
            print(f"❌ Error during prototype run: {e}")
            import traceback
            traceback.print_exc()
        finally:
            browser.close()

if __name__ == "__main__":
    run_prototype()
