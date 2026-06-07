import argparse
import contextlib
import logging
import re
from datetime import datetime, timedelta

import httpx

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.manager_change_repository import ManagerChangeRepository
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

MANAGER_KEYWORDS = ["감독", "선임", "경질", "사임", "대행", "사퇴"]


class ManagerChangeCrawler:
    async def run(self, save: bool = False):
        data = await self._fetch_news()
        print(f"Found {len(data)} manager change entries.")
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
                    if not any(kw in title for kw in MANAGER_KEYWORDS):
                        continue
                    parsed = self._parse_article(article)
                    if parsed:
                        results.append(parsed)
            except Exception as e:
                logger.warning(f"Manager change news fetch failed: {e}")

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

        if not any(kw in text for kw in MANAGER_KEYWORDS):
            return None

        team_id = None
        for kr, code in team_map.items():
            if kr in text:
                team_id = code
                break

        name_match = re.search(r"([가-힣]{2,4})\s*(?:감독|대행)", text)
        if not name_match:
            name_match = re.search(r"(?:감독|새\s*감독)[:：\s]*([가-힣]{2,4})", text)
        if not name_match:
            return None

        new_manager = name_match.group(1)
        # Filter out non-manager words
        exclude_names = {
            "타이거즈",
            "자이언츠",
            "베어스",
            "트윈스",
            "위즈",
            "랜더스",
            "다이노스",
            "이글스",
            "라이온즈",
            "히어로즈",
        }
        if new_manager in exclude_names:
            return None

        reason = None
        if "경질" in text:
            reason = "FIRED"
        elif "사임" in text or "사퇴" in text:
            reason = "RESIGN"
        elif "대행" in text:
            reason = "INTERIM"

        pub_date = article.get("dateTime", "")
        change_date = None
        if pub_date:
            with contextlib.suppress(ValueError, AttributeError):
                change_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00")).date()

        season = change_date.year if change_date else datetime.now().year
        oid = article.get("oid", "")
        aid = article.get("aid", "")
        url = f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={aid}" if oid and aid else ""

        return {
            "team_id": team_id or "UNKNOWN",
            "season": season,
            "new_manager": new_manager,
            "change_date": change_date,
            "change_reason": reason,
            "note": title[:500],
            "source_url": url,
        }

    def _save_to_db(self, data: list[dict]):
        session = SessionLocal()
        repo = ManagerChangeRepository(session)
        count = 0
        seen = set()
        try:
            for item in data:
                key = (item["team_id"], item["season"], item["new_manager"])
                if key in seen:
                    continue
                seen.add(key)
                try:
                    repo.save_change(item)
                    session.flush()
                    count += 1
                except SQLAlchemyError as e:
                    logger.warning(f"Manager change save failed: {e}")
                    session.rollback()
            session.commit()
            print(f"Saved {count} manager change records.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error saving manager changes: {e}", exc_info=True)
            print(f"Error: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(ManagerChangeCrawler().run(save=args.save))
