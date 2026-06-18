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

    TEAM_MAP: dict[str, str] = {
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

    def _parse_article(self, article: dict) -> dict[str, Any] | None:
        text = (article.get("title", "") or "") + " " + (article.get("subContent", "") or "")
        player_name = self._extract_foreign_player_name(text)
        if not player_name:
            return None
        team_id = self._extract_team_id(text)
        change_type = self._detect_change_type(text)
        reason = self._detect_reason(text)
        announcement_date = self._parse_date(article.get("dateTime", ""))
        season = announcement_date.year if announcement_date else datetime.now().year
        url = self._build_naver_url(article.get("oid", ""), article.get("aid", ""))
        return {
            "player_name": player_name,
            "team_id": team_id or "UNKNOWN",
            "season": season,
            "change_type": change_type,
            "announcement_date": announcement_date,
            "replacement_reason": reason,
            "note": (article.get("title", "") or "")[:500],
            "source_url": url,
        }

    @staticmethod
    def _extract_foreign_player_name(text: str) -> str | None:
        fp_match = re.search(
            r"([가-힣]{2,5}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*(?:교체|대체|방출|영입|재계약|웨이버)",
            text,
        )
        if fp_match:
            return fp_match.group(1)
        names = re.findall(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*", text)
        for name in names:
            if len(name) > 3 and name.lower() not in ("Kbo", "Naver", "Sports", "Http", "Https", "Mlb"):
                return name
        korean_name = re.search(r"([가-힣]{2,4})(?:선수|투수|타자)", text)
        return korean_name.group(1) if korean_name else None

    def _extract_team_id(self, text: str) -> str | None:
        for kr, code in self.TEAM_MAP.items():
            if kr in text:
                return code
        return None

    @staticmethod
    def _detect_change_type(text: str) -> str:
        if any(t in text for t in ("방출", "퇴출", "웨이버")):
            return "RELEASED"
        if any(t in text for t in ("교체", "대체")):
            return "REPLACED"
        if "재계약" in text:
            return "RENEWED"
        return "SIGNED"

    @staticmethod
    def _detect_reason(text: str) -> str | None:
        if "부상" in text:
            return "INJURY"
        if "성적" in text or "부진" in text:
            return "PERFORMANCE"
        return None

    @staticmethod
    def _parse_date(pub_date: str) -> object:
        if not pub_date:
            return None
        with contextlib.suppress(ValueError, AttributeError):
            return datetime.fromisoformat(pub_date.replace("Z", "+00:00")).date()
        return None

    @staticmethod
    def _build_naver_url(oid: str, aid: str) -> str:
        if oid and aid:
            return f"https://sports.news.naver.com/kbaseball/news/read?oid={oid}&aid={aid}"
        return ""

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
