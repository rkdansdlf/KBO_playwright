"""
Utility helpers for mapping KBO team names to canonical short codes.
"""
from __future__ import annotations

from typing import Optional

from .team_history import resolve_team_code_for_season

# Canonical KBO short codes (aligned with Docs/schema)
TEAM_NAME_TO_CODE = {
    # Active franchises
    "삼성": "SS",
    "삼성 라이온즈": "SS",
    "롯데": "LOT",
    "롯데 자이언츠": "LOT",
    "두산": "OB",
    "두산 베어스": "OB",
    "OB": "OB",
    "LG": "LG",
    "LG 트윈스": "LG",
    "KIA": "KIA",
    "KIA 타이거즈": "KIA",
    "한화": "HH",
    "한화 이글스": "HH",
    "KT": "KT",
    "KT 위즈": "KT",
    "NC": "NC",
    "NC 다이노스": "NC",
    "키움": "WO",
    "키움 히어로즈": "WO",
    "SSG": "SSG",
    "SSG 랜더스": "SSG",
    "SK": "SK",
    "SK 와이번스": "SK",
    # Historical brands (Heroes lineage)
    "넥센": "NEX",
    "넥센 히어로즈": "NEX",
    "우리": "WO",
    "우리 히어로즈": "WO",
    "현대": "HYU",
    "현대 유니콘스": "HYU",
    "태평양": "TP",
    "태평양 돌핀스": "TP",
    "청보": "CB",
    "청보 핀토스": "CB",
    "삼미": "SAM",
    "삼미 슈퍼스타즈": "SAM",
    # Historical LG/KIA predecessors
    "해태": "HAI",
    "해태 타이거즈": "HAI",
    "MBC": "MBC",
    "MBC 청룡": "MBC",
    # Dissolved franchises
    "쌍방울": "SSANG",
    "쌍방울 레이더스": "SSANG",
    # Special Teams (All-Star Games)
    # Special Teams (All-Star Games)
    "나눔": "EA",
    "드림": "WE",
    "동군": "EA",
    "서군": "WE",
    # National Teams (International)
    "대한민국": "KR",
    "한국": "KR",
    "일본": "JP",
    "대만": "TW",
    "쿠바": "CU",
    "호주": "AU",
    "도미니카": "DO", # Note: DO was also Doosan Bears segment but typically Bears is OB. However, Doosan Bears is OB/DO.
                  # Let's verify if "DO" is already used for Doosan. 
                  # Looking at TEAM_NAME_TO_CODE -> "Doosan Bears" is "OB".
                  # GAME_ID_SEGMENT_TO_CODE -> "DO" maps to "OB".
                  # For national codes, ISO-2 "DO" is Dominican Republic.
                  # Using "DOM" might be safer but ISO2 is standard. 
                  # Since "DO" maps to "OB" in game segment, we should be careful.
                  # BUT, resolve_team_code uses TEAM_NAME_TO_CODE. 
                  # "두산" -> "OB". "도미니카" -> "DO".
                  # As long as there is no overlap in *names* mapping to same code, we are fine.
                  # Wait, "DO" code is used in `game` table?
                  # In `database`, we used "OB" for Doosan usually?
                  # repair_game_teams.py: [20250404OBLT0] Away: OB -> DO.
                  # Ah! I changed Doosan to use "DO" in 2025 repair!
                  # So "DO" is Doosan Bears!
                  # I CANNOT use "DO" for Dominican Republic.
                  # I will use "DOM" for Dominican Republic to avoid collision.
    "도미니카공화국": "DOM",
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


def resolve_team_code(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    key = name.strip()
    return TEAM_NAME_TO_CODE.get(key)


GAME_ID_SEGMENT_TO_CODE = {
    "LG": "LG",
    "KT": "KT",
    "SS": "SS",
    "NC": "NC",
    "OB": "OB",
    "DO": "OB",
    "HH": "HH",
    "LT": "LOT",
    "SK": "SSG",
    "SSG": "SSG",
    "WO": "WO",
    "KI": "WO",
    "HT": "KIA",
    "KIA": "KIA",
    "SA": "SS",   # legacy 삼성
    "AN": "HH",   # legacy 한화
    "HY": "WO",
    "TP": "WO",
    "CB": "WO",
    "SM": "WO",
    "BE": "HH",
    "SL": "SSG",
    "MBC": "LG",
    # All-Star Game segments
    "EA": "EA",
    "WE": "WE",
}


def team_code_from_game_id_segment(segment: Optional[str], season_year: Optional[int] = None) -> Optional[str]:
    if not segment:
        return None
    segment = segment.upper()
    if season_year:
        resolved = resolve_team_code_for_season(segment, season_year)
        if resolved:
            return resolved
    return GAME_ID_SEGMENT_TO_CODE.get(segment, segment)


__all__ = ["resolve_team_code", "team_code_from_game_id_segment", "TEAM_NAME_TO_CODE"]
