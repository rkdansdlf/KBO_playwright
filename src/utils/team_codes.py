"""
Utility helpers for mapping KBO team names to canonical short codes.
"""
from __future__ import annotations

import re
from typing import Optional

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
    "히어로즈": "WO", # Usually refers to Woori/Heroes era
    "우리": "WO",
    "우리 히어로즈": "WO",
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
    
    # Special / National (same)
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
}

KBO_LEGACY_TECHNICAL_CODE = {
    "SSG": "SK",
    "KH": "WO",
    "DB": "OB",
    "KIA": "HT",
}

def resolve_team_code(name: Optional[str], season_year: Optional[int] = None) -> Optional[str]:
    if not name:
        return None
    key = " ".join(name.replace("\n", " ").split())
    raw_code = TEAM_NAME_TO_CODE.get(key)
    
    if raw_code and season_year:
        # If we have a year, try to resolve the specific brand code for that franchise
        from src.utils.team_history import resolve_team_code_for_season
        resolved = resolve_team_code_for_season(raw_code, season_year)
        if resolved:
            return resolved
            
    return raw_code


def resolve_kbo_legacy_team_code(name: Optional[str], season_year: Optional[int] = None) -> Optional[str]:
    code = resolve_team_code(name, season_year)
    return KBO_LEGACY_TECHNICAL_CODE.get(code or "", code)


def kbo_game_id_team_code(team_code: Optional[str], season_year: Optional[int] = None) -> Optional[str]:
    """Return the KBO GameCenter team-code token for a team code."""
    if not team_code:
        return None

    raw_code = str(team_code).strip().upper()
    if not raw_code:
        return None

    normalized_code = team_code_from_game_id_segment(raw_code, season_year) or raw_code
    if season_year:
        from src.utils.team_history import resolve_team_code_for_season

        normalized_code = resolve_team_code_for_season(normalized_code, season_year) or normalized_code

    return KBO_LEGACY_TECHNICAL_CODE.get(normalized_code, normalized_code)


def build_kbo_game_id(
    game_date: Optional[str],
    away_team_code: Optional[str],
    home_team_code: Optional[str],
    *,
    doubleheader_no: Optional[object] = 0,
    season_year: Optional[int] = None,
) -> Optional[str]:
    """Build a canonical KBO legacy GameCenter ID from explicit game fields."""
    if not game_date:
        return None

    date_part = str(game_date).replace("-", "").strip()
    if len(date_part) != 8 or not date_part.isdigit():
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
    if not dh or not dh[-1].isdigit():
        dh = "0"
    else:
        dh = dh[-1]

    return f"{date_part}{away_code}{home_code}{dh}"


GAME_ID_SEGMENT_TO_CODE = {
    "LG": "LG",
    "KT": "KT",
    "SS": "SS",
    "NC": "NC",
    "OB": "DB", # KBO uses OB for Doosan franchise, we use DB for current
    "DO": "DB",
    "HH": "HH",
    "LT": "LT",
    "SK": "SSG", # KBO uses SK for SSG franchise, we use SSG for current
    "SSG": "SSG",
    "WO": "KH", # KBO uses WO (Woori) for Kiwoom franchise, we use KH for current
    "KI": "KH",
    "KH": "KH",
    "DB": "DB",
    "HT": "KIA", # KBO uses HT (Haitai) for KIA franchise, we use KIA for current
    "KIA": "KIA",
    "SA": "SS",
    "AN": "HH",
    "EA": "EA",
    "WE": "WE",
}

def team_code_from_game_id_segment(segment: Optional[str], season_year: Optional[int] = None) -> Optional[str]:
    if not segment:
        return None
    segment = segment.upper()
    
    # 1. Map to modern canonical code if it's a known KBO segment
    mapped = GAME_ID_SEGMENT_TO_CODE.get(segment, segment)
    
    # 2. If year is provided, use history to find the EXACT brand code for that year
    if season_year:
        from src.utils.team_history import resolve_team_code_for_season
        resolved = resolve_team_code_for_season(mapped, season_year)
        if resolved:
            return resolved
            
    return mapped

def normalize_kbo_game_id(game_id: str) -> str:
    """
    Normalize KBO game IDs to always use legacy franchise codes (SK, WO, OB, HT).
    
    Some KBO internal APIs (like GetKboGameList) have started returning modern codes
    (SSG, KH, DB, KIA) in the G_ID field for 2026, while the public GameCenter 
    HTML links and boxscore parameters still expect legacy codes.
    
    Format: YYYYMMDD + AWAY(2-3) + HOME(2-3) + DH(0-2)
    Example: 20260418SSGNC0 -> 20260418SKNC0
    """
    if not game_id or len(game_id) < 12:
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
        if year < 2024:
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
        },
        key=lambda code: (-len(code), code),
    )
)


def _split_game_id_team_part(team_part: str) -> tuple[Optional[str], Optional[str]]:
    """Split AWAY+HOME team code suffix using known KBO game-id code tokens."""
    for away_code in KBO_GAME_ID_TEAM_CODES:
        if not team_part.startswith(away_code):
            continue
        home_code = team_part[len(away_code):]
        if home_code in KBO_GAME_ID_TEAM_CODES:
            return away_code, home_code
    return None, None

# Standard codes for the 10 current franchises
STANDARD_TEAM_CODES = {"HH", "KIA", "KT", "LG", "LT", "NC", "DB", "SSG", "SS", "KH"}

__all__ = [
    "build_kbo_game_id",
    "kbo_game_id_team_code",
    "resolve_team_code",
    "team_code_from_game_id_segment",
    "resolve_kbo_legacy_team_code",
    "TEAM_NAME_TO_CODE",
    "STANDARD_TEAM_CODES",
    "normalize_kbo_game_id",
]
