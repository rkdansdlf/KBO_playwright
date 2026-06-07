import argparse
import contextlib
import logging
import re
from datetime import datetime, timedelta

import httpx
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.foreign_player_repository import ForeignPlayerRepository
from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)

NAVER_API_URL = (
    "https://api-gw.sports.naver.com/news/articles/kbaseball?sort=latest&date={date}&page=1&pageSize=30&isPhoto=N"
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://sports.news.naver.com/kbaseball/news/index",
    "Origin": "https://sports.news.naver.com",
}

FOREIGN_KEYWORDS = ["외국인", "대체", "교체", "방출", "영입", "재계약", "웨이버", "퇴출"]


class ForeignPlayerCrawler:
    async def run(self, save: bool = False):
        data = await self._fetch_news()
        print(f"Found {len(data)} foreign player change entries.")
        if save:
            self._save_to_db(data)
        else:
            for d in data[:10]:
                print(d)

    async def _fetch_news(self) -> list[dict]:
        results = []
        today = datetime.now()
        client = httpx.Client(headers=HEADERS, timeout=15)

        for days_ago in range(7):
            date_str = (today - timedelta(days=days_ago)).strftime("%Y%m%d")
            url = NAVER_API_URL.format(date=date_str)
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    continue
                news_list = resp.json().get("result", {}).get("newsList", [])
                for article in news_list:
                    title = article.get("title", "")
                    if not any(kw in title for kw in FOREIGN_KEYWORDS):
                        continue
                    parsed = self._parse_article(article)
                    if parsed:
                        results.append(parsed)
            except Exception as e:
                logger.warning(f"Foreign player news fetch failed: {e}")

        client.close()
        return results

    def _parse_article(self, article: dict) -> dict | None:
        title = article.get("title", "")
        content = article.get("subContent", "")
        text = title + " " + content
        team_map = {
            "LG": "LG",
            "KT": "KT",
            "NC": "NC",
            "두산": "DB",
            "롯데": "LT",
            "삼성": "SS",
            "키움": "KH",
            "한화": "HH",
            "KIA": "KIA",
            "SSG": "SSG",
        }

        # Try matching foreign player name near change keywords
        fp_match = re.search(
            r"([가-힣]{2,5}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*(?:교체|대체|방출|영입|재계약|웨이버)", text
        )
        if fp_match:
            player_name = fp_match.group(1)
        else:
            names = re.findall(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*", text)
            player_name = None
            for name in names:
                if len(name) > 3 and name.lower() not in ("Kbo", "Naver", "Sports", "Http", "Https", "Mlb"):
                    player_name = name
                    break
            if not player_name:
                korean_name = re.search(r"([가-힣]{2,4})(?:선수|투수|타자)", text)
                if korean_name:
                    player_name = korean_name.group(1)
        if not player_name:
            return None

        team_id = None
        for kr, code in team_map.items():
            if kr in text:
                team_id = code
                break

        change_type = "SIGNED"
        if "방출" in text or "퇴출" in text or "웨이버" in text:
            change_type = "RELEASED"
        elif "교체" in text or "대체" in text:
            change_type = "REPLACED"
        elif "재계약" in text:
            change_type = "RENEWED"

        reason = None
        if "부상" in text:
            reason = "INJURY"
        elif "성적" in text or "부진" in text:
            reason = "PERFORMANCE"

        pub_date = article.get("dateTime", "")
        announcement_date = None
        if pub_date:
            with contextlib.suppress(ValueError, AttributeError):
                announcement_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00")).date()

        season = announcement_date.year if announcement_date else datetime.now().year
        oid = article.get("oid", "")
        aid = article.get("aid", "")
        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={aid}" if oid and aid else ""

        return {
            "player_name": player_name,
            "team_id": team_id or "UNKNOWN",
            "season": season,
            "change_type": change_type,
            "announcement_date": announcement_date,
            "replacement_reason": reason,
            "note": title[:500],
            "source_url": url,
        }

    def _save_to_db(self, data: list[dict]):
        session = SessionLocal()
        repo = ForeignPlayerRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_change(item)
                    count += 1
                except SQLAlchemyError as e:
                    logger.warning(f"Foreign player save failed: {e}")
            session.commit()
            print(f"Saved {count} foreign player change records.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error saving foreign players: {e}", exc_info=True)
            print(f"Error: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(ForeignPlayerCrawler().run(save=args.save))
