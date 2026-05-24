"""
Crawler for real-time issue text: Naver Sports baseball news headlines and MLBPark popular threads.
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Any
import httpx
from bs4 import BeautifulSoup

from src.utils.safe_print import safe_print as print

class RealtimeIssueCrawler:
    """
    Scrapes real-time baseball topics, headlines, and forum discussions.
    """

    def __init__(self, timeout: int = 15):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def fetch_naver_news_headlines(self) -> List[Dict[str, Any]]:
        """
        Fetches latest baseball news headlines from Naver Sports GW API (JSON)
        with fallback to web scraping if API is down.
        """
        api_url = "https://api-gw.sports.naver.com/news/list?category=kbaseball&page=1&pageSize=20"
        print(f"📰 Fetching Naver news from API: {api_url}")
        
        articles = []
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                res = client.get(api_url)
                if res.status_code == 200:
                    data = res.json()
                    # Naver Sports news JSON structures vary; typically result or list
                    result_data = data.get("result", {})
                    news_list = result_data.get("list", [])
                    
                    for item in news_list:
                        title = item.get("title", "")
                        oid = item.get("oid", "")
                        aid = item.get("aid", "")
                        office_name = item.get("officeName", "")
                        dt_str = item.get("datetime", "")
                        
                        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={aid}"
                        
                        articles.append({
                            "title": title,
                            "content": f"[{office_name}] {title}", # Placeholder or short summary
                            "meta": {
                                "source": url,
                                "office_name": office_name,
                                "published_at": dt_str,
                                "crawled_at": datetime.now().isoformat(),
                                "category": "naver_news"
                            }
                        })
                    print(f"   Fetched {len(articles)} headlines from JSON API.")
                    return articles
        except Exception as e:
            print(f"⚠️ Naver news API failed ({e}). Falling back to HTML scraping...")

        # HTML Scraping Fallback
        fallback_url = "https://sports.news.naver.com/kbaseball/news/index"
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                res = client.get(fallback_url)
                if res.status_code == 200:
                    soup = BeautifulSoup(res.text, "lxml")
                    # Naver News elements list
                    links = soup.select(".news_list a")
                    for a in links[:15]:
                        title = a.get("title") or a.text.strip()
                        href = a.get("href")
                        if not href:
                            continue
                        if href.startswith("/"):
                            href = "https://sports.news.naver.com" + href
                        
                        articles.append({
                            "title": title,
                            "content": title,
                            "meta": {
                                "source": href,
                                "crawled_at": datetime.now().isoformat(),
                                "category": "naver_news"
                            }
                        })
            print(f"   Fetched {len(articles)} headlines from HTML fallback.")
        except Exception as ex:
            print(f"⚠️ Naver News HTML fallback also failed: {ex}")
            
        return articles

    def fetch_mlbpark_bullpen_posts(self) -> List[Dict[str, Any]]:
        """
        Crawls popular titles and post preview contents from MLBPark Bullpen forum.
        """
        url = "https://mlbpark.donga.com/mp/b.php?b=bullpen"
        print(f"💬 Fetching posts from MLBPark Bullpen: {url}")
        
        posts = []
        try:
            with httpx.Client(headers=self.headers, timeout=self.timeout) as client:
                res = client.get(url)
                if res.status_code == 200:
                    soup = BeautifulSoup(res.text, "lxml")
                    
                    # MLBPark bullpen list items
                    # Typical elements are tr, inside td class 'tit'
                    rows = soup.select("table.tbl_type01 tbody tr")
                    
                    for r in rows:
                        # Skip notice rows
                        if r.select_one(".notice"):
                            continue
                            
                        tit_el = r.select_one("td.tit a")
                        if not tit_el:
                            continue
                            
                        title = tit_el.text.strip()
                        href = tit_el.get("href", "")
                        
                        # Filter to baseball-related or generic popular posts
                        # (Many posts are non-baseball, but bullpen contains baseball issues during season)
                        # We capture them as general community trends.
                        
                        posts.append({
                            "title": title,
                            "content": f"MLBPark Bullpen Post: {title}",
                            "meta": {
                                "source": href,
                                "crawled_at": datetime.now().isoformat(),
                                "category": "mlbpark"
                            }
                        })
            print(f"   Fetched {len(posts)} posts from MLBPark.")
        except Exception as e:
            print(f"⚠️ Error fetching MLBPark bullpen posts: {e}")
            
        return posts
