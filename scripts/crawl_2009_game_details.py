
import os
import sys
import time
import json
import urllib.parse
from playwright.sync_api import sync_playwright

sys.path.insert(0, os.getcwd())
from src.crawlers.legacy_game_detail_crawler import LegacyGameDetailCrawler
from src.services.player_id_resolver import PlayerIdResolver
from src.repositories.game_repository import save_game_detail
from src.db.engine import SessionLocal

def crawl_2009_details():
    # DB Session
    session = SessionLocal()
    resolver = PlayerIdResolver(session)
    resolver.preload_season_index(2009)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # 1. Navigate to 2009 Schedule
        url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
        print(f"📡 Navigating to Schedule: {url}")
        page.goto(url, wait_until="networkidle")
        
        # Select 2009, 04, Regular
        print("   Selecting Year 2009...")
        page.select_option('#ddlYear', '2009')
        time.sleep(1)
        print("   Selecting Month 04...")
        page.select_option('#ddlMonth', '04')
        time.sleep(1)
        print("   Selecting Series...")
        sys.stdout.flush()
        try:
            page.select_option('#ddlSeries', '0,9,6')
        except Exception as e:
            print(f"⚠️ Error selecting series: {e}")
        time.sleep(2)
        print("   Series selected.")
        sys.stdout.flush()
        
        print("   Locating table...")
        tbl = page.query_selector('.tbl-type06')
        if not tbl:
            print("❌ Table not found!")
            return

        print("   Finding links...")
        links = tbl.query_selector_all('tbody a') 
        review_links = [l for l in links if "리뷰" in l.inner_text()]
        
        print(f"Found {len(review_links)} review links.")
        
        for i, link in enumerate(review_links[:1]):
            try:
                print(f"   Processing Game {i+1}...")
                href = link.get_attribute('href')
                print(f"   Link: {href}")
                
                 # Navigation
                try:
                    print("   [Driver] Navigating to URL directly...")
                    full_url = f"https://www.koreabaseball.com{href}"
                    page.goto(full_url, wait_until="networkidle", timeout=30000)
                    print("   [Driver] Navigation done.")
                except Exception as e:
                    print(f"⚠️ [Driver] Navigation failed: {e}")
                    
                # Extract Data
                print(f"   [Driver] Instantiating Crawler...")
                crawler = LegacyGameDetailCrawler(resolver=resolver) 
                
                # Derive Game ID from href
                parsed = urllib.parse.urlparse(href)
                qs = urllib.parse.parse_qs(parsed.query)
                # gameId=20090404HHSK0
                game_id = qs.get('gameId', [f"20090404_TEST_{i}"])[0]
                game_date = qs.get('gameDate', ["20090404"])[0]

                print(f"   [Driver] Extracting details for {game_id}...")
                try:
                    data = crawler.extract_game_details(page, game_id, game_date)
                    print(f"   [Driver] Extraction done.")
                except Exception as e:
                    print(f"🔥 [Driver] Extraction CRASHED: {e}")
                    import traceback
                    traceback.print_exc()
                    raise e
                
                # Save to DB
                print(f"   [Driver] Saving to DB...")
                saved = save_game_detail(data)
                if saved:
                    print(f"   ✅ Game {game_id} saved successfully!")
                else:
                    print(f"   ❌ Failed to save game {game_id}.")
                
                print("\n📊 Extracted Data Structure:")
                print(f"  Game ID: {data['game_id']}")
                print(f"  Teams: {data['teams']}")
                
                # Go back for next game
                print("   [Driver] Going back...")
                page.go_back()
                time.sleep(2)
            
            except Exception as e:
                print(f"🔥 [CRITICAL] Loop iteration failed: {e}")
                import traceback
                traceback.print_exc()
            
            print("   [Driver] End of loop iteration.")
            
        browser.close()
    session.close()

if __name__ == "__main__":
    crawl_2009_details()
