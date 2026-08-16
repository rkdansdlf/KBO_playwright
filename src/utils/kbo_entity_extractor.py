"""KBO Domain Entity and Filter Extractor from Natural Language Queries."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

MIN_PLAYER_NAME_LEN = 2
MAX_PLAYER_NAME_LEN = 4
MIN_PLAYER_SUFFIX_STRIP_LEN = 4

BASEBALL_STOPWORDS = {
    "타율",
    "홈런",
    "기록",
    "안타",
    "삼진",
    "볼넷",
    "평자",
    "방어율",
    "순위",
    "일정",
    "결과",
    "승리",
    "패배",
    "경기",
    "야구",
    "프로야구",
    "시즌",
    "선수",
    "구단",
    "팀별",
    "오늘",
    "어제",
    "내일",
    "중계",
    "문자중계",
    "라인업",
    "하이라이트",
    "영상",
    "뉴스",
    "보기",
    "정보",
    "분석",
    "예측",
    "추천",
    "안내",
    "소식",
    "기사",
    "스코어",
    "대진",
    "상황",
    "선발",
    "투수",
    "타자",
    "포수",
    "내야수",
    "외야수",
    "주차",
    "주차장",
    "요금",
    "먹거리",
    "음식",
    "식당",
    "좌석",
    "예매",
    "티켓",
    "구장",
    "야구장",
    "시설",
    "편의시설",
    "및",
    "팁",
    "성적",
    "스탯",
    "알려줘",
    "해줘",
    "대해",
    "어떻게",
    "몇",
    "규정",
    "규칙",
    "abs",
    "판정",
    "허용",
    "금지",
    "스트라이크존",
    "이유",
    "원인",
    "설명",
    "최종",
    "정규시즌",
    "최종순위",
    "우승",
    "역대",
    "초창기",
    "중요한",
    "동명이인",
    "사건",
    "변천",
    "고의사구",
    "콜드게임",
    "승률",
    "올스타전",
    "올스타",
    "역사",
    "오른",
    "주자",
    "베이스",
    "투구",
    "떠나",
    "되나요",
    "되나",
}

TEAM_SYNONYM_MAP: dict[str, str] = {
    "기아": "KIA",
    "kia": "KIA",
    "타이거즈": "KIA",
    "두산": "DB",
    "베어스": "DB",
    "db": "DB",
    "ob": "DB",
    "엘지": "LG",
    "lg": "LG",
    "트윈스": "LG",
    "키움": "KH",
    "히어로즈": "KH",
    "kh": "KH",
    "넥센": "KH",
    "ssg": "SSG",
    "랜더스": "SSG",
    "쓱": "SSG",
    "sk": "SSG",
    "와이번스": "SSG",
    "kt": "KT",
    "케이티": "KT",
    "위즈": "KT",
    "위즈파크": "KT",
    "엔씨": "NC",
    "nc": "NC",
    "다이노스": "NC",
    "삼성": "SS",
    "라이온즈": "SS",
    "ss": "SS",
    "롯데": "LT",
    "자이언츠": "LT",
    "lt": "LT",
    "한화": "HH",
    "이글스": "HH",
    "hh": "HH",
    "빙그레": "HH",
    "해태": "KIA",
}

STADIUM_MAP: dict[str, str] = {
    "잠실": "잠실",
    "고척": "고척",
    "스카이돔": "고척",
    "문학": "문학",
    "랜더스필드": "문학",
    "수원": "수원",
    "위즈파크": "수원",
    "대전": "대전",
    "이글스파크": "대전",
    "대구": "대구",
    "라이온즈파크": "대구",
    "라팍": "대구",
    "광주": "광주",
    "챔피언스필드": "광주",
    "챔필": "광주",
    "사직": "사직",
    "창원": "창원",
    "NC파크": "창원",
    "엔팍": "창원",
    "포항": "포항",
    "울산": "울산",
    "청주": "청주",
}

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "press_release": ["보도자료", "뉴스", "발표", "공지", "기사", "선정", "시상"],
    "milestone": ["달성", "기록", "마일스톤", "통산", "호투", "홈런", "첫", "역대"],
    "futures_schedule": ["퓨처스", "2군", "상무", "경찰"],
    "player_splits": ["스플릿", "상성", "좌우", "득점권", "상대전적", "맞대결"],
    "stadium_facility": ["주차", "주차장", "먹거리", "음식", "식당", "좌석", "예매", "티켓", "구장"],
}


@dataclass
class ExtractedKboEntities:
    """Extracted entities and metadata filters from a natural query."""

    team_id: str | None = None
    season_year: int | None = None
    stadium: str | None = None
    category: str | None = None
    player_name: str | None = None
    remaining_query: str = ""
    extra_filters: dict[str, Any] = field(default_factory=dict)

    def to_filters(self) -> dict[str, Any]:
        """Convert extracted entities to RAG / Vector search filter dict."""
        f: dict[str, Any] = {}
        if self.team_id:
            f["team_id"] = self.team_id
        if self.season_year:
            f["season_year"] = self.season_year
        if self.category:
            f["document_type"] = self.category
        if self.stadium:
            f["stadium"] = self.stadium
        if self.player_name:
            f["player_name"] = self.player_name
        if self.extra_filters:
            f.update(self.extra_filters)
        return f


def _extract_team(query_lower: str) -> str | None:
    for synonym, team_code in TEAM_SYNONYM_MAP.items():
        pattern = r"(?:^|\s|\b)" + re.escape(synonym) + r"(?:$|\s|\b|의|는|은|이|가|와|과|도)"
        if re.search(pattern, query_lower):
            return team_code
    return None


def _extract_stadium(query_lower: str) -> str | None:
    for st_name, st_canonical in STADIUM_MAP.items():
        if st_name.lower() in query_lower:
            return st_canonical
    return None


def _extract_category(query_lower: str) -> str | None:
    for cat, kws in CATEGORY_KEYWORDS.items():
        if any(kw in query_lower for kw in kws):
            return cat
    return None


def _extract_player_candidate(query: str) -> str | None:
    for w in query.split():
        clean_w = re.sub(r"[^가-힣]", "", w)
        clean_w = re.sub(r"(해줘|알려줘|으로|에서|에게|께서)$", "", clean_w)
        stripped = re.sub(r"(의|은|는|이|가|을|를|와|과|도|에)$", "", clean_w)
        if len(clean_w) >= MIN_PLAYER_SUFFIX_STRIP_LEN or stripped in BASEBALL_STOPWORDS:
            clean_w = stripped
        if (
            MIN_PLAYER_NAME_LEN <= len(clean_w) <= MAX_PLAYER_NAME_LEN
            and clean_w.lower() not in TEAM_SYNONYM_MAP
            and clean_w not in STADIUM_MAP
            and clean_w not in BASEBALL_STOPWORDS
        ):
            return clean_w
    return None


def extract_kbo_entities(query: str, *, extract_player: bool = True) -> ExtractedKboEntities:
    """Extract KBO-specific domain entities from user queries."""
    query_lower = query.lower().strip()
    result = ExtractedKboEntities()

    year_match = re.search(r"(?:^|\D)(198\d|199\d|20[0-2]\d|2030)(?:\D|$)", query)
    if year_match:
        result.season_year = int(year_match.group(1))

    result.team_id = _extract_team(query_lower)
    result.stadium = _extract_stadium(query_lower)
    result.category = _extract_category(query_lower)
    if extract_player:
        result.player_name = _extract_player_candidate(query)
    result.remaining_query = query

    return result
