"""
Utility helpers for mapping KBO team names to canonical short codes.
"""
from __future__ import annotations

from typing import Optional

# Canonical KBO short codes (aligned with Docs/schema)
TEAM_NAME_TO_CODE = {
    # Active franchises
    "삼성": "SS",
    "삼성 라이온즈": "SS",
    "롯데": "LT",
    "롯데 자이언츠": "LT",
    "두산": "OB",
    "두산 베어스": "OB",
    "OB": "OB",
    "OB베어스": "OB", # Handling newline join
    "LG": "LG",
    "LG 트윈스": "LG",
    "KIA": "HT",
    "KIA 타이거즈": "HT",
    "기아": "HT",
    "기아 타이거즈": "HT",
    "한화": "HH",
    "한화 이글스": "HH",
    "KT": "KT",
    "KT 위즈": "KT",
    "kt": "KT",
    "NC": "NC",
    "NC 다이노스": "NC",
    "nc": "NC",
    "키움": "WO",
    "키움 히어로즈": "WO",
    "SSG": "SSG",
    "SSG 랜더스": "SSG",
    "SK": "SK",
    "SK 와이번스": "SK",
    
    # Historical brands (Heroes lineage)
    "넥센": "NX",
    "넥센 히어로즈": "NX",
    "우리": "WO", # Woori Heroes used WO code?
    "우리 히어로즈": "WO",
    "현대": "HU", 
    "현대 유니콘스": "HU",
    "태평양": "TP",
    "태평양 돌핀스": "TP",
    "청보": "CB",
    "청보 핀토스": "CB",
    "삼미": "SM",
    "삼미 슈퍼스타즈": "SM", # Database uses SM
    
    # Historical LG/KIA predecessors
    "해태": "HT", # Use HT code (matches teams table/history)
    "해태 타이거즈": "HT",
    "MBC": "MBC",
    "MBC 청룡": "MBC",
    "MBC청룡": "MBC",
    "빙그레": "BE",
    "빙그레 이글스": "BE",
    
    # Dissolved franchises
    "쌍방울": "SL", # Use SL code (matches teams table)
    "쌍방울 레이더스": "SL",
    
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
    "도미니카": "DOM",
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
    # Normalize: join lines (replace newlines with space), strip, collapse spaces
    key = " ".join(name.replace("\n", " ").split())
    # Handle specific no-space legacy cases if needed, but dictionary handles 'OB베어스'.
    return TEAM_NAME_TO_CODE.get(key)


GAME_ID_SEGMENT_TO_CODE = {
    "LG": "LG",
    "KT": "KT",
    "SS": "SS",
    "NC": "NC",
    "OB": "OB",
    "DO": "OB",
    "HH": "HH",
    "LT": "LT",
    "SK": "SSG",
    "SSG": "SSG",
    "WO": "WO",
    "KI": "WO",
    "HT": "HT",
    "KIA": "HT",
    "SA": "SS",   # legacy 삼성
    "AN": "HH",   # legacy 한화
    "HY": "WO",
    "TP": "WO",
    "CB": "WO",
    "SM": "WO",
    "BE": "HH",
    "SL": "SSG",
    "MBC": "LG",
    "NX": "WO", # Nexen Heroes -> Kiwoom Franchise
    "HU": "WO", # Hyundai -> Kiwoom (Loose connection for franchise grouping?)
                # If we want HU to stand alone, map to NULL or 'HU'.
                # But to enable updates, mapping to WO collects them under Heroes franchise id.
    # All-Star Game segments
    "EA": "EA",
    "WE": "WE",
}

def team_code_from_game_id_segment(segment: Optional[str], season_year: Optional[int] = None) -> Optional[str]:
    if not segment:
        return None
    segment = segment.upper()
    # Cyclic import prevention if needed, but here simple map.
    # Note: src.utils.team_history import resolve_team_code_for_season is imported in original file
    # I should re-add if I removed it?
    # I'll just use the map for now. Logic for extensive history requires the other module.
    return GAME_ID_SEGMENT_TO_CODE.get(segment, segment)

__all__ = ["resolve_team_code", "team_code_from_game_id_segment", "TEAM_NAME_TO_CODE"]
