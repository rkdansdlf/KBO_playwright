"""Utility helpers for mapping KBO stadium names to canonical stadium codes."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import TYPE_CHECKING, cast

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
    "동대문": "DONGDAEMUN",
    "전주": "JEONJU",
    "부산": "SAJIK",
    "구덕": "SAJIK",
    "숭의": "MUNHAK",
    "도원": "MUNHAK",
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
    "서울종합운동장 야구장": "JAMSIL",
    "인천문학야구장": "MUNHAK",
    "인천SSG랜더스필드": "MUNHAK",
    "문학": "MUNHAK",
    "인천": "MUNHAK",
    "인천공설운동장 야구장": "MUNHAK",
    "숭의야구장": "MUNHAK",
    "숭의 야구장": "MUNHAK",
    "숭의": "MUNHAK",
    "도원야구장": "MUNHAK",
    "도원": "MUNHAK",
    "부산 사직 야구장": "SAJIK",
    "사직": "SAJIK",
    "부산": "SAJIK",
    "구덕야구장": "SAJIK",
    "구덕": "SAJIK",
    "대구 삼성 라이온즈 파크": "DAEGU",
    "대구": "DAEGU",
    "대구시민운동장 야구장": "DAEGU",
    "대전 한화생명 이글스 파크": "HANBAT",
    "대전한밭야구장": "HANBAT",
    "한밭야구장": "HANBAT",
    "한밭": "HANBAT",
    "대전": "HANBAT",
    "대전시민운동장 야구장": "SIMIN",
    "시민": "SIMIN",
    "수원 kt wiz 파크": "SUWON",
    "수원야구장": "SUWON",
    "수원": "SUWON",
    "광주-기아 챔피언스 필드": "GWANGJU",
    "광주 무등경기장 야구장": "MUDEUNG",
    "무등": "MUDEUNG",
    "광주": "GWANGJU",
    "고척스카이돔": "GOCHEOK",
    "고척": "GOCHEOK",
    "창원NC파크": "CHANGWON",
    "창원": "CHANGWON",
    "목동야구장": "MOKDONG",
    "목동": "MOKDONG",
    "마산야구장": "MASAN",
    "마산": "MASAN",
    "전주종합운동장 야구장": "JEONJU",
    "전주종합경기장 야구장": "JEONJU",
    "전주야구장": "JEONJU",
    "전주": "JEONJU",
    "동대문야구장": "DONGDAEMUN",
    "동대문": "DONGDAEMUN",
    "청주종합운동장 야구장": "CHEONGJU",
    "청주야구장": "CHEONGJU",
    "청주": "CHEONGJU",
    "군산월명종합운동장 야구장": "GUNSAN",
    "월명종합경기장 야구장": "GUNSAN",
    "군산야구장": "GUNSAN",
    "군산": "GUNSAN",
    "제주 오라 CC 야구장": "JEJU",
    "제주 오라야구장": "JEJU",
    "제주야구장": "JEJU",
    "제주": "JEJU",
    "춘천야구장": "CHUNCHEON",
    "춘천": "CHUNCHEON",
    "포항야구장": "POHANG",
    "포항": "POHANG",
    "울산야구장": "ULSAN",
    "울산": "ULSAN",
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
}


HISTORICAL_MUDEUNG_LAST_SEASON = 2013


NOISY_STADIUM_SUBSTRINGS: tuple[tuple[str, str], ...] = (
    ("무등", "MUDEUNG"),
    ("서울종합운동장", "JAMSIL"),
    ("잠실", "JAMSIL"),
    ("숭의", "MUNHAK"),
    ("도원", "MUNHAK"),
    ("동대문", "DONGDAEMUN"),
    ("전주", "JEONJU"),
    ("월명", "GUNSAN"),
    ("군산", "GUNSAN"),
)


def _clean_noisy_stadium_pattern(name: str) -> str | None:
    for sub, code in NOISY_STADIUM_SUBSTRINGS:
        if sub in name:
            return code
    return None


@lru_cache(maxsize=256)
def resolve_stadium_code(stadium_name: str | None, season_year: int | None = None) -> str | None:
    """Resolve a stadium name (Korean short or full) to canonical stadium_code.

    Args:
        stadium_name: Korean stadium name (short like '잠실' or full like '잠실야구장')
        season_year: Optional season year for historical disambiguation (e.g. 광주 무등 vs 챔피언스필드)

    Returns:
        Canonical stadium_code (e.g. 'JAMSIL') or None if not resolvable

    """
    if not stadium_name:
        return None

    name = stadium_name.strip()
    pattern_code = _clean_noisy_stadium_pattern(name)
    if pattern_code:
        return pattern_code

    code = STADIUM_SHORT_NAME_MAP.get(name) or STADIUM_KR_TO_CODE.get(name)
    if code == "GWANGJU" and season_year is not None and season_year <= HISTORICAL_MUDEUNG_LAST_SEASON:
        return "MUDEUNG"

    if code:
        return code

    logger.debug("Unresolved stadium name: %s", stadium_name)
    return None


def resolve_stadium_code_from_db(
    session: Session,
    stadium_name: str | None,
) -> str | None:
    """Resolve stadium code using the database stadium_short_name_map table.

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
            return cast("str", row[0])
    except SQLAlchemyError:
        logger.debug("DB stadium lookup failed for: %s", stadium_name)

    return None
