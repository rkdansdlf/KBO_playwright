"""KBO manager change crawler 크롤러."""

from __future__ import annotations

import argparse
import logging
import re
from datetime import date, datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.base_naver_crawler import NaverNewsCrawlerBase
from src.db.engine import SessionLocal
from src.repositories.manager_change_repository import ManagerChangeRepository
from src.utils.naver_helpers import NAVER_TEAM_MAP, build_naver_sports_url, parse_iso_date

logger = logging.getLogger(__name__)


class ManagerChangeCrawler(NaverNewsCrawlerBase):
    """ManagerChangeCrawler class."""

    KEYWORDS = ["감독", "선임", "경질", "사임", "대행", "사퇴"]
    LABEL = "manager change"

    EXCLUDE_MANAGER_NAMES: set[str] = {
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

    def _parse_article(self, article: dict) -> dict[str, Any] | None:
        text = (article.get("title", "") or "") + " " + (article.get("subContent", "") or "")
        if not any(kw in text for kw in self.KEYWORDS):
            return None
        team_id = self._extract_team_id(text)
        new_manager = self._extract_manager_name(text)
        if not new_manager:
            return None
        reason = self._detect_reason(text)
        change_date = self._parse_date(article.get("dateTime", ""))
        season = change_date.year if change_date else datetime.now(KST).year
        url = self._build_naver_url(article.get("oid", ""), article.get("aid", ""))
        return {
            "team_id": team_id or "UNKNOWN",
            "season": season,
            "new_manager": new_manager,
            "change_date": change_date,
            "change_reason": reason,
            "note": (article.get("title", "") or "")[:500],
            "source_url": url,
        }

    def _extract_team_id(self, text: str) -> str | None:
        for kr, code in NAVER_TEAM_MAP.items():
            if kr in text:
                return code
        return None

    def _extract_manager_name(self, text: str) -> str | None:
        name_match = re.search(r"([가-힣]{2,4})\s*(?:감독|대행)", text)
        if not name_match:
            name_match = re.search(r"(?:감독|새\s*감독)[:\uff1a\s]*([가-힣]{2,4})", text)
        if not name_match:
            return None
        name = name_match.group(1)
        return None if name in self.EXCLUDE_MANAGER_NAMES else name

    @staticmethod
    def _detect_reason(text: str) -> str | None:
        if "경질" in text:
            return "FIRED"
        if "사임" in text or "사퇴" in text:
            return "RESIGN"
        if "대행" in text:
            return "INTERIM"
        return None

    @staticmethod
    def _parse_date(pub_date: str) -> date | None:
        return parse_iso_date(pub_date)

    @staticmethod
    def _build_naver_url(oid: str, aid: str) -> str:
        return build_naver_sports_url(oid, aid)

    def _save_to_db(self, data: list[dict]) -> None:
        session = SessionLocal()
        repo = ManagerChangeRepository(session)
        count = 0
        seen = set()
        for item in data:
            key = (item["team_id"], item["season"], item["new_manager"])
            if key in seen:
                continue
            seen.add(key)
            try:
                repo.save_change(item)
                session.commit()
                count += 1
            except SQLAlchemyError as e:
                logger.warning("Manager change save failed: %s", e)
                session.rollback()
        logger.info("Saved %s manager change records.", count)
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(ManagerChangeCrawler().run(save=args.save))
