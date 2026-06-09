import argparse
import logging
import re
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal
from src.repositories.game_mvp_repository import GameMvpRepository

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


class GameMvpCrawler:
    async def run(self, game_ids: list[str] = None, save: bool = False):
        if game_ids:
            results = []
            for gid in game_ids:
                data = await self._search_mvp_for_game(gid)
                if data:
                    results.append(data)
            if save and results:
                self._save_to_db(results)
            else:
                for d in results:
                    logger.info(d)
        else:
            data = await self._fetch_recent_mvp_news()
            logger.info(f"Found {len(data)} MVP entries.")
            if save:
                self._save_to_db(data)
            else:
                for d in data[:10]:
                    logger.info(d)

    async def _search_mvp_for_game(self, game_id: str) -> dict[str, Any] | None:
        date_str = game_id[:8]
        url = NAVER_API_URL.format(date=date_str)
        client = httpx.Client(headers=HEADERS, timeout=15)
        try:
            resp = client.get(url)
            if resp.status_code != 200:
                return None
            news_list = resp.json().get("result", {}).get("newsList", [])
            for article in news_list:
                title = article.get("title", "")
                text = title + " " + article.get("subContent", "")
                if "MVP" not in text:
                    continue
                player_name = self._parse_mvp_player(text)
                if player_name:
                    return {
                        "game_id": game_id,
                        "player_name": player_name,
                        "team_id": self._parse_mvp_team(text),
                        "mvp_type": "GAME",
                        "reason": title[:300],
                        "award_source": "NAVER",
                    }
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Error searching MVP for game {game_id}: {e}", exc_info=True)
        finally:
            client.close()
        return None

    async def _fetch_recent_mvp_news(self) -> list[dict]:
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
                    if "MVP" not in title:
                        continue
                    player_name = self._parse_mvp_player(title)
                    if not player_name:
                        continue
                    game_id_match = re.search(r"(\d{8})", title)
                    game_id = game_id_match.group(1) if game_id_match else date_str + "0000"
                    results.append(
                        {
                            "game_id": game_id,
                            "player_name": player_name,
                            "team_id": self._parse_mvp_team(title),
                            "mvp_type": "GAME",
                            "reason": title[:300],
                            "award_source": "NAVER",
                        }
                    )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Game MVP news fetch failed: {e}")

        client.close()
        return results

    def _parse_mvp_player(self, text: str) -> str | None:
        patterns = [
            r"([가-힣]{2,4})\s*선수.*MVP",
            r"MVP[:\s]*([가-힣]{2,4})",
            r"([가-힣]{2,4})\s*,\s*MVP",
            r"([가-힣]{2,4})\s*MVP",
        ]
        for p in patterns:
            m = re.search(p, text)
            if m:
                return m.group(1)
        return None

    def _parse_mvp_team(self, text: str) -> str | None:
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
        for kr, code in team_map.items():
            if kr in text:
                return code
        return None

    def _save_to_db(self, data: list[dict]):
        session = SessionLocal()
        repo = GameMvpRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_mvp(item)
                    count += 1
                except SQLAlchemyError as e:
                    logger.warning(f"Game MVP save failed: {e}")
            session.commit()
            logger.info(f"Saved {count} MVP records.")
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error saving MVP records: {e}", exc_info=True)
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--game-ids", nargs="+")
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(GameMvpCrawler().run(game_ids=args.game_ids, save=args.save))
