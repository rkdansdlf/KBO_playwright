import argparse
import contextlib
import logging
import re
from datetime import datetime, timedelta

import httpx
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.injury_repository import InjuryRepository

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

INJURY_KEYWORDS = ["부상", "IL", "전력이탈", "이탈", "재활", "복귀"]


class InjuryCrawler:
    async def run(self, save: bool = False):
        data = await self._fetch_news()
        logger.info(f"Found {len(data)} injury entries.")
        if save:
            self._save_to_db(data)
        else:
            for d in data[:10]:
                logger.info(d)

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
                    if not any(kw in title for kw in INJURY_KEYWORDS):
                        continue
                    parsed = self._parse_article(article)
                    if parsed:
                        results.append(parsed)
            except Exception as e:
                logger.warning(f"Injury news fetch failed: {e}")

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

        player_match = re.search(r"([가-힣]{2,4})(?:선수|투수|타자|내야수|외야수|포수)", text)
        if not player_match:
            player_match = re.search(r"(?:부상|이탈)[:：\s]*([가-힣]{2,4})", text)
        if not player_match:
            player_match = re.search(r"([가-힣]{2,4})\s*(?:부상|이탈|복귀)", text)
        if not player_match:
            return None

        player_name = player_match.group(1)
        team_id = None
        for kr, code in team_map.items():
            if kr in text:
                team_id = code
                break

        body_parts = [
            "어깨",
            "팔꿈치",
            "손목",
            "허리",
            "무릎",
            "발목",
            "햄스트링",
            "종아리",
            "옆구리",
            "등",
            "발",
            "사두",
        ]
        body_part = None
        for bp in body_parts:
            if bp in text:
                body_part = bp
                break

        pub_date = article.get("dateTime", "")
        injury_date = None
        if pub_date:
            with contextlib.suppress(ValueError, AttributeError):
                injury_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00")).date()

        oid = article.get("oid", "")
        aid = article.get("aid", "")
        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={aid}" if oid and aid else ""

        return {
            "player_name": player_name,
            "team_id": team_id or "UNKNOWN",
            "body_part": body_part,
            "injury_type": title[:100],
            "injury_date": injury_date,
            "status": "ACTIVE",
            "note": title[:300],
            "source_url": url,
        }

    def _save_to_db(self, data: list[dict]):
        session = SessionLocal()
        repo = InjuryRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_injury(item)
                    count += 1
                except SQLAlchemyError as e:
                    logger.warning(f"Injury save failed: {e}")
            session.commit()
            logger.info(f"Saved {count} injury records.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error saving injury records: {e}", exc_info=True)
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(InjuryCrawler().run(save=args.save))
