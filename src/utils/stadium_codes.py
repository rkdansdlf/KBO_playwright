"""Utility helpers for mapping KBO stadium names to canonical stadium codes."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

STADIUM_SHORT_NAME_MAP: dict[str, str] = {
    "잠실": "JAMSIL",
    "문학": "MUNHAK",
    "사직": "SAJIK",
    "대구": "DAEGU",
    "한밭": "HANBAT",
    "수원": "SUWON",
    "광주": "GWANGJU",
    "고척": "GOCHEOK",
    "창원": "CHANGWON",
    "목동": "MOKDONG",
    "마산": "MASAN",
    "무등": "MUDEUNG",
    "대전": "HANBAT",
    "시민": "SIMIN",
    "인천": "MUNHAK",
    "포항": "POHANG",
    "청주": "CHEONGJU",
    "울산": "ULSAN",
    "군산": "GUNSAN",
    "제주": "JEJU",
    "이천(두산)": "ICHUN_DOSAN",
    "이천(LG)": "ICHUN_LG",
    "상동": "SANGDONG",
    "파나메리카노": "PANAMERICANO",
    "도쿄돔": "TOKYO_DOME",
    "타이페이돔": "TAIPEI_DOME",
    "티엔무": "TIANMU",
    "콜로소델파시피코": "COLOSO",
    "반테린돔나고야": "VANTELIN_NAGOYA",
    "춘천": "CHUNCHEON",
}

STADIUM_KR_TO_CODE: dict[str, str] = {
    "잠실야구장": "JAMSIL",
    "잠실": "JAMSIL",
    "인천문학야구장": "MUNHAK",
    "문학": "MUNHAK",
    "부산 사직 야구장": "SAJIK",
    "사직": "SAJIK",
    "대구 삼성 라이온즈 파크": "DAEGU",
    "대구": "DAEGU",
    "대전 한화생명 이글스 파크": "HANBAT",
    "한밭": "HANBAT",
    "수원 kt wiz 파크": "SUWON",
    "수원": "SUWON",
    "광주-기아 챔피언스 필드": "GWANGJU",
    "광주": "GWANGJU",
    "고척스카이돔": "GOCHEOK",
    "고척": "GOCHEOK",
    "창원NC파크": "CHANGWON",
    "창원": "CHANGWON",
    "목동야구장": "MOKDONG",
    "목동": "MOKDONG",
    "마산야구장": "MASAN",
    "마산": "MASAN",
    "광주 무등경기장 야구장": "MUDEUNG",
    "무등": "MUDEUNG",
    "대전시민운동장 야구장": "SIMIN",
    "시민": "SIMIN",
    "대전한밭야구장": "HANBAT",
    "대구시민운동장 야구장": "DAEGU",
    "청주종합운동장 야구장": "CHEONGJU",
    "인천공설운동장 야구장": "MUNHAK",
    "군산월명종합운동장 야구장": "GUNSAN",
    "제주 오라 CC 야구장": "JEJU",
    "인천SSG랜더스필드": "MUNHAK",
    "인천": "MUNHAK",
    "포항야구장": "POHANG",
    "포항": "POHANG",
    "청주야구장": "CHEONGJU",
    "청주": "CHEONGJU",
    "울산야구장": "ULSAN",
    "울산": "ULSAN",
    "군산야구장": "GUNSAN",
    "군산": "GUNSAN",
    "제주야구장": "JEJU",
    "제주": "JEJU",
    "이천두산야구장": "ICHUN_DOSAN",
    "이천(두산)": "ICHUN_DOSAN",
    "이천LG야구장": "ICHUN_LG",
    "이천(LG)": "ICHUN_LG",
    "상동야구장": "SANGDONG",
    "상동": "SANGDONG",
    "파나메리카노": "PANAMERICANO",
    "도쿄돔": "TOKYO_DOME",
    "타이페이돔": "TAIPEI_DOME",
    "티엔무": "TIANMU",
    "콜로소델파시피코": "COLOSO",
    "반테린돔나고야": "VANTELIN_NAGOYA",
    "춘천야구장": "CHUNCHEON",
    "춘천": "CHUNCHEON",
}


@lru_cache(maxsize=256)
def resolve_stadium_code(stadium_name: str | None) -> str | None:
    """
    Resolve a stadium name (Korean short or full) to canonical stadium_code.

    Args:
        stadium_name: Stadium Name.
        stadium_name: Stadium Name.
        stadium_name: Korean stadium name (short like '잠실' or full like '잠실야구장')

    Returns:
        Canonical stadium_code (e.g. 'JAMSIL') or None if not resolvable

    """
    if not stadium_name:
        return None

    name = stadium_name.strip()

    code = STADIUM_SHORT_NAME_MAP.get(name)
    if code:
        return code

    code = STADIUM_KR_TO_CODE.get(name)
    if code:
        return code

    logger.debug("Unresolved stadium name: %s", stadium_name)
    return None


def resolve_stadium_code_from_db(
    session: Session,
    stadium_name: str | None,
) -> str | None:
    """
    Resolve stadium code using the database stadium_short_name_map table.

    Falls back to the static mapping if DB lookup fails.

    Args:
        session: Session.
        stadium_name: Stadium Name.
        session: Session.
        stadium_name: Stadium Name.
        session: SQLAlchemy session
        stadium_name: Korean stadium name

    Returns:
        Canonical stadium_code or None

    """
    if not stadium_name:
        return None

    code = resolve_stadium_code(stadium_name)
    if code:
        return code

    try:
        from sqlalchemy import text

        row = session.execute(
            text("SELECT stadium_code FROM stadium_short_name_map WHERE short_name = :name"),
            {"name": stadium_name.strip()},
        ).one_or_none()
        if row is not None:
            return row[0]
    except SQLAlchemyError:
        logger.debug("DB stadium lookup failed for: %s", stadium_name)

    return None
