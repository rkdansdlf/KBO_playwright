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
from src.repositories.foreign_player_repository import ForeignPlayerRepository

logger = logging.getLogger(__name__)


class ForeignPlayerCrawler(NaverNewsCrawlerBase):
    KEYWORDS = ["외국인", "대체", "교체", "방출", "영입", "재계약", "웨이버", "퇴출"]
    LABEL = "foreign player change"

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

        # Try matching foreign player name near change keywords
        fp_match = re.search(
            r"([가-힣]{2,5}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*(?:교체|대체|방출|영입|재계약|웨이버)",
            text,
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

    def _save_to_db(self, data: list[dict]) -> None:
        session = SessionLocal()
        repo = ForeignPlayerRepository(session)
        count = 0
        try:
            for item in data:
                try:
                    repo.save_change(item)
                    count += 1
                except SQLAlchemyError as e:
                    logger.warning("Foreign player save failed: %s", e)
            session.commit()
            logger.info("Saved %s foreign player change records.", count)
        except SQLAlchemyError:
            session.rollback()
            logger.exception("Database error saving foreign players")
        finally:
            session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(ForeignPlayerCrawler().run(save=args.save))
