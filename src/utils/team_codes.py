"""
Utility helpers for mapping KBO team names to canonical short codes.
"""
from __future__ import annotations

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

# Standard codes for the 10 current franchises
STANDARD_TEAM_CODES = {"HH", "KIA", "KT", "LG", "LT", "NC", "DB", "SSG", "SS", "KH"}

__all__ = ["resolve_team_code", "team_code_from_game_id_segment", "TEAM_NAME_TO_CODE", "STANDARD_TEAM_CODES"]

__all__ = ["resolve_team_code", "team_code_from_game_id_segment", "TEAM_NAME_TO_CODE", "STANDARD_TEAM_CODES"]

