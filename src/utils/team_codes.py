"""Utility helpers for mapping KBO team names to canonical short codes."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from sqlalchemy import func, or_

from src.constants import DATE_STR_LEN, GAME_ID_FULL_LEN, GAME_ID_MIN_LEN
from src.models.franchise import Franchise
from src.utils.team_history import franchise_id_for_team_code, resolve_team_code_for_season

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def franchise_ids_for_code(session: Session, code: str) -> tuple[int, ...]:
    """Return every franchise a team code can name, across eras.

    A stored team code is not a stable identifier. ``OB`` and ``DB`` are the
    same franchise, ``WO`` and ``KH`` are the same franchise, and ``SK`` and
    ``SSG`` are the same franchise -- while ``SS`` is a *different* one, because
    Samsung Lions and SSG Landers are different clubs that inherited the same
    slot in 2025. ``team_franchises`` records both ends of that history
    (``original_code`` and ``current_code``) and is the only table that states
    it; matching on the string column cannot tell those two apart.

    Args:
        session: Open session, used to read ``team_franchises``.
        code: A team code as a user or a caller would type it.

    Returns:
        Franchise ids for the code, or an empty tuple when no franchise claims
        it -- which is the caller's cue to fall back to a literal string match
        rather than to return nothing.

    """
    if not code or not str(code).strip():
        return ()
    normalized = str(code).strip().upper()
    rows = session.query(Franchise).filter(
        or_(
            func.upper(Franchise.current_code) == normalized,
            func.upper(Franchise.original_code) == normalized,
        ),
    )
    return tuple(row.id for row in rows)


# Canonical KBO short codes (aligned with modern franchise IDs)
TEAM_NAME_TO_CODE = {
    # Active franchises
    "삼성": "SS",
    "삼성 라이온즈": "SS",
    "롯데": "LT",
    "롯데 자이언츠": "LT",
    "두산": "DB",
    "두산 베어스": "DB",
    "OB": "OB",  # Keep historical name mapping to historical code
    "OB베어스": "OB",
    "OB 베어스": "OB",
    "LG": "LG",
    "LG 트윈스": "LG",
    "KIA": "KIA",
    "KIA 타이거즈": "KIA",
    "HT": "HT",
    "WO": "WO",
    "기아": "KIA",
    "기아 타이거즈": "KIA",
    "한화": "HH",
    "한화 이글스": "HH",
    "KT": "KT",
    "KT 위즈": "KT",
    "NC": "NC",
    "NC 다이노스": "NC",
    "키움": "KH",
    "키움 히어로즈": "KH",
    "SSG": "SSG",
    "SSG 랜더스": "SSG",
    "SK": "SK",
    "SK 와이번스": "SK",
    # Historical brands
    "넥센": "NX",
    "넥센 히어로즈": "NX",
    "히어로즈": "WO",  # Usually refers to Woori/Heroes era
    "우리": "WO",
    "우리 히어로즈": "WO",
    "HD": "HU",
    "현대": "HU",
    "현대 유니콘스": "HU",
    "태평양": "TP",
    "태평양 돌핀스": "TP",
    "청보": "CB",
    "청보 핀토스": "CB",
    "삼미": "SM",
    "삼미 슈퍼스타즈": "SM",
    # Historical names
    "해태": "HT",
    "해태 타이거즈": "HT",
    "MBC": "MBC",
    "MBC 청룡": "MBC",
    "빙그레": "BE",
    "빙그레 이글스": "BE",
    # Dissolved
    "쌍방울": "SL",
    "쌍방울 레이더스": "SL",
    "나눔": "EA",
    "드림": "WE",
    "대한민국": "KR",
    "한국": "KR",
    "일본": "JP",
    "대만": "TW",
    "쿠바": "CU",
    "호주": "AU",
    "도미니카": "DOM",
    "파나마": "PA",
    "네덜란드": "NL",
    "미국": "US",
    "베네수엘라": "VE",
    "멕시코": "MX",
    "푸에르토리코": "PR",
    "중국": "CN",
    "캐나다": "CA",
    "이탈리아": "IT",
    "체코": "CZ",
    "상무": "OT",
    "경찰": "OT",
    "경찰청": "OT",
    "고양": "OT",
    "울산": "OT",
    "화성": "OT",
}

KBO_LEGACY_TECHNICAL_CODE = {
    "SSG": "SK",
    "KH": "WO",
    "DB": "OB",
    "KIA": "HT",
}
LEGACY_GAME_ID_NORMALIZATION_START_YEAR = 2024


def resolve_team_code(name: str | None, season_year: int | None = None) -> str | None:
    """Resolve team code.

    Args:
        name: Name.
        season_year: Season Year.
        name: Name.
        season_year: Season Year.
        name: Name.
        season_year: Season Year.

    Returns:
        The result of the operation.

    """
    if not name:
        return None
    key = " ".join(name.replace("\n", " ").split())
    raw_code = TEAM_NAME_TO_CODE.get(key)
    if not raw_code:
        raw_code = TEAM_NAME_TO_CODE.get(key.upper())

    if raw_code and season_year:
        # If we have a year, try to resolve the specific brand code for that franchise
        from src.utils.team_history import resolve_team_code_for_season

        resolved = resolve_team_code_for_season(raw_code, season_year)
        if resolved:
            return resolved

    return raw_code


def resolve_kbo_legacy_team_code(name: str | None, season_year: int | None = None) -> str | None:
    """Resolve kbo legacy team code.

    Args:
        name: Name.
        season_year: Season Year.
        name: Name.
        season_year: Season Year.
        name: Name.
        season_year: Season Year.

    Returns:
        The result of the operation.

    """
    code = resolve_team_code(name, season_year)

    return KBO_LEGACY_TECHNICAL_CODE.get(code or "", code)


def kbo_game_id_team_code(team_code: str | None, season_year: int | None = None) -> str | None:
    """Return the KBO GameCenter team-code token for a team code.

    Args:
        team_code: Team Code.
        season_year: Season Year.
        team_code: Team Code.
        season_year: Season Year.

    """
    if not team_code:
        return None

    raw_code = str(team_code).strip().upper()
    if not raw_code:
        return None

    normalized_code = team_code_from_game_id_segment(raw_code, season_year) or raw_code
    if season_year:
        normalized_code = resolve_team_code_for_season(normalized_code, season_year) or normalized_code

    return KBO_LEGACY_TECHNICAL_CODE.get(normalized_code, normalized_code)


def build_kbo_game_id(
    game_date: str | None,
    away_team_code: str | None,
    home_team_code: str | None,
    *,
    doubleheader_no: object | None = 0,
    season_year: int | None = None,
) -> str | None:
    """Build a canonical KBO legacy GameCenter ID from explicit game fields.

    Args:
        game_date: Game Date.
        away_team_code: Away Team Code.
        home_team_code: Home Team Code.
        doubleheader_no: Doubleheader No.
        season_year: Season Year.
        game_date: Game Date.
        away_team_code: Away Team Code.
        home_team_code: Home Team Code.
        doubleheader_no: Doubleheader No.
        season_year: Season Year.

    """
    if not game_date:
        return None

    date_part = str(game_date).replace("-", "").strip()
    if len(date_part) != DATE_STR_LEN or not date_part.isdigit():
        return None

    year = season_year
    if year is None:
        try:
            year = int(date_part[:4])
        except ValueError:
            year = None

    away_code = kbo_game_id_team_code(away_team_code, year)
    home_code = kbo_game_id_team_code(home_team_code, year)
    if not away_code or not home_code:
        return None

    dh = str(doubleheader_no if doubleheader_no is not None else 0).strip()
    dh = "0" if not dh or not dh[-1].isdigit() else dh[-1]

    return f"{date_part}{away_code}{home_code}{dh}"


GAME_ID_SEGMENT_TO_CODE = {
    "LG": "LG",
    "KT": "KT",
    "SS": "SS",
    "NC": "NC",
    "OB": "DB",  # KBO uses OB for Doosan franchise, we use DB for current
    "DO": "DB",
    "HH": "HH",
    "LT": "LT",
    "SK": "SSG",  # KBO uses SK for SSG franchise, we use SSG for current
    "SSG": "SSG",
    "WO": "KH",  # KBO uses WO (Woori) for Kiwoom franchise, we use KH for current
    "NX": "KH",  # KBO uses NX (Nexen) for Kiwoom franchise
    "KI": "KH",
    "KH": "KH",
    "HD": "HU",
    "HU": "HU",
    "DB": "DB",
    "HT": "KIA",  # KBO uses HT (Haitai) for KIA franchise, we use KIA for current
    "KIA": "KIA",
    "SA": "SS",
    "AN": "HH",
    "EA": "EA",
    "WE": "WE",
}


def team_code_from_game_id_segment(segment: str | None, season_year: int | None = None) -> str | None:
    """Handle the team code from game id segment operation.

    Args:
        segment: Segment.
        season_year: Season Year.
        segment: Segment.
        season_year: Season Year.
        segment: Segment.
        season_year: Season Year.

    Returns:
        The result of the operation.

    """
    if not segment:
        return None
    segment = segment.upper()

    # 1. Map to modern canonical code if it's a known KBO segment
    mapped = GAME_ID_SEGMENT_TO_CODE.get(segment, segment)

    # 2. If year is provided, use history to find the EXACT brand code for that year
    if season_year:
        resolved = resolve_team_code_for_season(mapped, season_year)
        if resolved:
            return resolved

    return mapped


#: KBO GameCenter technical tokens that differ from a modern season brand.
KBO_TECHNICAL_TEAM_CODES = frozenset(KBO_LEGACY_TECHNICAL_CODE.values())


def season_team_code(team_code: object, season_year: int | None) -> str | None:
    """Return the code to store for a team token, mapping KBO technical tokens.

    KBO GameCenter ids and some page fallbacks still carry the technical codes
    ``HT``/``OB``/``SK``/``WO`` for franchises we store under their season brand
    (``KIA``/``DB``/``SSG``/``KH`` in modern seasons). Writing the raw token splits
    one franchise into two codes, so every consumer that groups by the string
    (standings, the rankings snapshot) loses games (issue #39).

    Only those technical tokens are rewritten, and only to the brand that
    franchise used in ``season_year`` -- ``HT`` stays ``HT`` in 1995. Any other code
    is returned stripped and upper-cased but otherwise untouched, so historical
    rows that use a modern brand are not rewritten here.

    Args:
        team_code: Raw code as received (``None``/blank is returned as ``None``).
        season_year: Season year used for brand resolution.

    Returns:
        The code to store.

    """
    if team_code is None:
        return None
    raw = str(team_code).strip().upper()
    if not raw:
        return None
    if raw not in KBO_TECHNICAL_TEAM_CODES or season_year is None:
        return raw
    resolved = team_code_from_game_id_segment(raw, season_year)
    if not resolved:
        return raw
    # Never cross franchises (e.g. the SSG/SL slot reuse in the 1990s).
    if franchise_id_for_team_code(resolved, season_year) != franchise_id_for_team_code(GAME_ID_SEGMENT_TO_CODE[raw]):
        return raw
    return resolved


def normalize_kbo_game_id(game_id: str) -> str:
    """Normalize KBO game IDs to always use legacy franchise codes (SK, WO, OB, HT).

    Some KBO internal APIs (like GetKboGameList) have started returning modern codes
    (SSG, KH, DB, KIA) in the G_ID field for 2026, while the public GameCenter
    HTML links and boxscore parameters still expect legacy codes.

    Format: YYYYMMDD + AWAY(2-3) + HOME(2-3) + DH(0-2)
    Example: 20260418SSGNC0 -> 20260418SKNC0

    Args:
        game_id: Game ID.
        game_id: Game ID.

    """
    if not game_id or len(game_id) < GAME_ID_FULL_LEN:
        return game_id

    raw = str(game_id).strip().upper()
    match = re.match(r"^(\d{8})([A-Z]+)(\d)$", raw)
    if not match:
        return game_id

    date_part, team_part, dh = match.groups()

    # IMPORTANT: Only normalize for 2024 and later.
    # Legacy data (2001-2023) in the database often uses modern or mixed codes
    # and shouldn't be forcefully normalized to legacy codes which might not
    # match existing records.
    try:
        year = int(date_part[:4])
        if year < LEGACY_GAME_ID_NORMALIZATION_START_YEAR:
            return game_id
    except ValueError:
        return game_id

    away_segment, home_segment = _split_game_id_team_part(team_part)
    if not away_segment or not home_segment:
        return game_id

    away_legacy = KBO_LEGACY_TECHNICAL_CODE.get(away_segment, away_segment)
    home_legacy = KBO_LEGACY_TECHNICAL_CODE.get(home_segment, home_segment)

    return f"{date_part}{away_legacy}{home_legacy}{dh}"


KBO_GAME_ID_TEAM_CODES = tuple(
    sorted(
        {
            *GAME_ID_SEGMENT_TO_CODE.keys(),
            *GAME_ID_SEGMENT_TO_CODE.values(),
            "SSG",
            "KIA",
            "KH",
            "DB",
            "SK",
            "WO",
            "OB",
            "HT",
            "NX",
            "OT",
        },
        key=lambda code: (-len(code), code),
    ),
)


def _split_game_id_team_part(team_part: str) -> tuple[str | None, str | None]:
    """Split AWAY+HOME team code suffix using known KBO game-id code tokens.

    Args:
        team_part: Team Part.
        team_part: Team Part.

    """
    for away_code in KBO_GAME_ID_TEAM_CODES:
        if not team_part.startswith(away_code):
            continue
        home_code = team_part[len(away_code) :]
        if home_code in KBO_GAME_ID_TEAM_CODES:
            return away_code, home_code
    return None, None


KBO_SITE_GAME_ID_MAPPINGS: dict[str, str] = {
    "NX": "WO",
    "KH": "WO",
    "HU": "HD",
    "SSG": "SK",
    "DB": "OB",
    "KIA": "HT",
}


def to_kbo_site_game_id(game_id: str) -> str:
    """Map any game_id (with brand or modern codes) to the KBO site's GameCenter URL gameId."""
    if not game_id or len(game_id) < GAME_ID_MIN_LEN:
        return game_id
    raw = str(game_id).strip().upper()
    match = re.match(r"^(\d{8})([A-Z]+)(\d)$", raw)
    if not match:
        return game_id
    date_part, team_part, dh = match.groups()
    away_code, home_code = _split_game_id_team_part(team_part)
    if not away_code or not home_code:
        return game_id
    away_site = KBO_SITE_GAME_ID_MAPPINGS.get(away_code, away_code)
    home_site = KBO_SITE_GAME_ID_MAPPINGS.get(home_code, home_code)
    return f"{date_part}{away_site}{home_site}{dh}"


# Standard codes for the 10 current franchises
STANDARD_TEAM_CODES = {"HH", "KIA", "KT", "LG", "LT", "NC", "DB", "SSG", "SS", "KH"}
# All valid historical and modern team codes
VALID_TEAM_CODES = STANDARD_TEAM_CODES | set(KBO_GAME_ID_TEAM_CODES) | set(TEAM_NAME_TO_CODE.values())

__all__ = [
    "KBO_SITE_GAME_ID_MAPPINGS",
    "KBO_TECHNICAL_TEAM_CODES",
    "STANDARD_TEAM_CODES",
    "TEAM_NAME_TO_CODE",
    "VALID_TEAM_CODES",
    "build_kbo_game_id",
    "kbo_game_id_team_code",
    "normalize_kbo_game_id",
    "resolve_kbo_legacy_team_code",
    "resolve_team_code",
    "season_team_code",
    "team_code_from_game_id_segment",
    "to_kbo_site_game_id",
]
