from __future__ import annotations

import argparse
import contextlib
import logging
import re
from datetime import datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.base_naver_crawler import NaverNewsCrawlerBase
from src.db.engine import SessionLocal
from src.repositories.injury_repository import InjuryRepository

logger = logging.getLogger(__name__)


class InjuryCrawler(NaverNewsCrawlerBase):
    KEYWORDS = ["부상", "IL", "전력이탈", "이탈", "재활", "복귀"]
    LABEL = "injury"

    def _parse_article(self, article: dict) -> dict[str, Any] | None:
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

    def _save_to_db(self, data: list[dict]) -> None:
        session = SessionLocal()
        repo = InjuryRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_injury(item)
                    count += 1
                except SQLAlchemyError as e:
                    logger.warning("Injury save failed: %s", e)
            session.commit()
            logger.info("Saved %s injury records.", count)
        except SQLAlchemyError as e:
            session.rollback()
            logger.exception("Database error saving injury records: %s", e)
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(InjuryCrawler().run(save=args.save))
