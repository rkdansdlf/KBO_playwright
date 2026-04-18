"""
Stadium names normalization utility.
Maps common shorthand or regional names to official KBO stadium names.
"""
from typing import Dict, Optional

STADIUM_MAP: Dict[str, str] = {
    # Current Major Stadiums
    "잠실": "잠실야구장",
    "문학": "인천SSG랜더스필드",
    "인천SSG랜더스필드": "인천SSG랜더스필드",
    "인천문학야구장": "인천SSG랜더스필드",
    "고척": "고척스카이돔",
    "수원": "수원 kt wiz 파크",
    "사직": "부산 사직 야구장",
    "대구": "대구 삼성 라이온즈 파크",
    "창원": "창원NC파크",
    "광주": "광주-기아 챔피언스 필드",
    "한밭": "대전 한화생명 이글스 파크",
    "대전": "대전 한화생명 이글스 파크",

    # Regional / Second Stadiums
    "울산": "울산 문수 야구장",
    "포항": "포항야구장",
    "청주": "청주종합운동장 야구장",
    "군산": "군산월명종합운동장 야구장",
    "마산": "마산야구장",
    "사직": "부산 사직 야구장",
    "제주": "제주 오라 CC 야구장",
    "목동": "목동야구장",
    
    # Historical / Legacy
    "시민": "대구시민운동장 야구장",
    "무등": "광주 무등경기장 야구장",
    "인천": "인천공설운동장 야구장",
}

STADIUM_CODE_MAP: Dict[str, str] = {
    "잠실야구장": "JAMSIL",
    "인천SSG랜더스필드": "MUNHAK",
    "고척스카이돔": "GOCHEOK",
    "수원 kt wiz 파크": "SUWON",
    "부산 사직 야구장": "SAJIK",
    "대구 삼성 라이온즈 파크": "DAEGU",
    "창원NC파크": "CHANGWON",
    "광주-기아 챔피언스 필드": "GWANGJU",
    "대전 한화생명 이글스 파크": "HANBAT",
}

def normalize_stadium_name(name: str) -> str:
    """Normalize a shorthand or legacy stadium name to its canonical version."""
    if not name:
        return name
        
    stripped = name.strip()
    return STADIUM_MAP.get(stripped, stripped)

def get_stadium_code(normalized_name: str) -> Optional[str]:
    """Get internal stadium code for a normalized name."""
    return STADIUM_CODE_MAP.get(normalized_name)
