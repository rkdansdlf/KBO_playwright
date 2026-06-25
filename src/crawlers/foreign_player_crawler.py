from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime
from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.crawlers.base_naver_crawler import NaverNewsCrawlerBase
from src.db.engine import SessionLocal
from src.repositories.foreign_player_repository import ForeignPlayerRepository
from src.utils.naver_helpers import NAVER_TEAM_MAP, build_naver_sports_url, parse_iso_date

logger = logging.getLogger(__name__)


class ForeignPlayerCrawler(NaverNewsCrawlerBase):
    KEYWORDS = ["외국인", "대체", "교체", "방출", "영입", "재계약", "웨이버", "퇴출"]
    LABEL = "foreign player change"

    def _parse_article(self, article: dict) -> dict[str, Any] | None:
        text = (article.get("title", "") or "") + " " + (article.get("subContent", "") or "")
        player_name = self._extract_foreign_player_name(text)
        if not player_name:
            return None
        team_id = self._extract_team_id(text)
        change_type = self._detect_change_type(text)
        reason = self._detect_reason(text)
        announcement_date = self._parse_date(article.get("dateTime", ""))
        season = announcement_date.year if announcement_date else datetime.now(KST).year
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
        invalid_names = {
            "부상",
            "대체",
            "교체",
            "영입",
            "방출",
            "퇴출",
            "투수",
            "타자",
            "선수",
            "외인",
            "외국인",
            "구단",
            "시즌",
            "리그",
            "올해",
            "최근",
            "경기",
            "감독",
            "코치",
            "대표",
            "단장",
            "구단주",
            "야구",
            "한국",
            "미국",
            "웨이버",
        }
        role_name = re.search(
            r"(?:새\s*)?외국인\s*(?:투수|타자|선수)\s+([가-힣]{2,5}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*)(?:와|과)?\s*(?:계약|영입|합류|입단)",
            text,
        )
        if role_name:
            name = role_name.group(1).strip()
            if name not in invalid_names:
                return name
        for m in re.finditer(
            r"([가-힣]{2,5}|[A-Z][a-z]+(?:\s[A-Z][a-z]+)*)\s*(?:교체|대체|방출|영입|재계약|웨이버)",
            text,
        ):
            name = m.group(1).strip()
            if name not in invalid_names:
                return name
        names = re.findall(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*", text)
        for name in names:
            if len(name) > 3 and name.lower() not in ("kbo", "naver", "sports", "http", "https", "mlb"):
                return name
        korean_name = re.search(r"([가-힣]{2,4})(?:선수|투수|타자)", text)
        if korean_name:
            name = korean_name.group(1).strip()
            if name not in invalid_names:
                return name
        return None

    def _extract_team_id(self, text: str) -> str | None:
        for kr, code in NAVER_TEAM_MAP.items():
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
    def _parse_date(pub_date: str) -> date:
        return parse_iso_date(pub_date)

    @staticmethod
    def _build_naver_url(oid: str, aid: str) -> str:
        return build_naver_sports_url(oid, aid)

    def _save_to_db(self, data: list[dict]) -> None:
        session = SessionLocal()
        repo = ForeignPlayerRepository(session)
        count = 0
        for item in data:
            try:
                repo.save_change(item)
                session.commit()
                count += 1
            except SQLAlchemyError as e:
                session.rollback()
                logger.warning("Foreign player save failed: %s", e)
        logger.info("Saved %s foreign player change records.", count)
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    args = parser.parse_args()
    import asyncio

    asyncio.run(ForeignPlayerCrawler().run(save=args.save))
