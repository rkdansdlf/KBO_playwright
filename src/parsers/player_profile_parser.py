"""KBO Player Profile Parser.

Parse raw KBO profile texts and original string fields into structured attributes.

"""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel


class PlayerProfileParsed(BaseModel):
    """PlayerProfileParsed class."""

    player_id: int | None = None
    player_name: str | None = None
    back_number: int | None = None
    birth_date: str | None = None  # 'YYYY-MM-DD'
    position: str | None = None  # 'P','C','IF','OF','DH'...
    throwing_hand: str | None = None  # 'R','L','S'
    batting_hand: str | None = None  # 'R','L','S'
    height_cm: int | None = None
    weight_kg: int | None = None
    education_or_career_path: list[str] = []
    # contracts
    signing_bonus_amount: int | None = None
    signing_bonus_currency: str | None = None  # 'KRW' or 'USD'
    signing_bonus_original: str | None = None
    salary_amount: int | None = None
    salary_currency: str | None = None
    salary_original: str | None = None
    # draft
    draft_year: int | None = None
    draft_team_code: str | None = None
    draft_round: int | None = None
    draft_pick_overall: int | None = None
    draft_type: str | None = None  # '자유선발','1차','2차'...
    # entry
    entry_year: int | None = None
    entry_team_code: str | None = None
    # flags
    is_active: bool | None = None
    is_foreign: bool | None = None  # 소스 맥락/페이지 플래그로 세팅
    photo_url: str | None = None
    team: str | None = None

    @property
    def education_path(self) -> list[str]:
        """Handle the education path operation.

        Returns:
            List of results.

        """
        return self.education_or_career_path

    def __getitem__(self, item: str) -> object:
        """Return the item at the given key.

        Args:
            item: Item.
            item: Item.

        """
        if item == "education_path":
            return self.education_path
        if hasattr(self, item):
            return getattr(self, item)
        raise KeyError(item)

    def get(self, item: str, default: object = None) -> object:
        """Get get.

        Args:
            item: Item.
            default: Default.
            item: Item.
            default: Default.
            item: Item.
            default: Default.

        Returns:
            object instance.

        """
        try:
            return self[item]
        except KeyError:
            return default


# Labels list for tokenization
LABELS = "선수명|등번호|생년월일|포지션|신장/체중|경력|출신교|입단 계약금|연봉|지명순위|입단년도"
LABEL_REGEX = re.compile(
    rf"(?P<key>{LABELS})\s*:\s*(?P<val>.*?)(?=(?:{LABELS})\s*:|$)",
    re.DOTALL,
)

# Standard KBO Team name to Team ID mapping
TEAM_CODE_MAP = {
    "삼성": "SS",
    "두산": "OB",
    "LG": "LG",
    "SSG": "SSG",
    "KIA": "KIA",
    "한화": "HH",
    "롯데": "LT",
    "KT": "KT",
    "NC": "NC",
    "키움": "WO",
    "넥센": "WO",
    "현대": "HD",
    "쌍방울": "SB",
    "태평양": "TP",
    "해태": "HT",
    "삼미": "SM",
    "청보": "CB",
    "MBC": "MB",
    "빙그레": "BG",
}

POS_MAP = {
    "투수": "P",
    "포수": "C",
    "내야수": "IF",
    "외야수": "OF",
    "지명타자": "DH",
}


def _clean(s: str | None) -> str:
    """Handle the clean operation.

    Args:
        s: S.
        s: S.
        s: S.

    Returns:
        String result.

    """
    if not s:
        return ""
    # Normalize whitespaces
    return re.sub(r"\s+", " ", s).strip()


def _to_year(two_digits: int, cutoff: int = 50) -> int:
    """00~cutoff-1 -> 2000s, cutoff~99 -> 1900s (e.g. 06 -> 2006, 98 -> 1998).

    Args:
        two_digits: Two Digits.
        cutoff: Cutoff.
        two_digits: Two Digits.
        cutoff: Cutoff.

    """
    return (2000 + two_digits) if two_digits < cutoff else (1900 + two_digits)


def tokenize_profile(raw: str) -> dict[str, str]:
    """Tokenize raw text by matching defined KBO labels.

    Args:
        raw: Raw.
        raw: Raw.

    """
    raw = _clean(raw)

    out: dict[str, str] = {}
    for m in LABEL_REGEX.finditer(raw):
        key = _clean(m.group("key"))
        val = _clean(m.group("val"))
        out[key] = val
    return out


def parse_back_number(s: str) -> int | None:
    """Extract back number from values like 'No.25' or '25'.

    Args:
        s: S.
        s: S.

    """
    if not s:
        return None
    m = re.search(r"(?:No\.\s*)?(\d+)", _clean(s))
    return int(m.group(1)) if m else None


def parse_birth_date(s: str) -> str | None:
    """Convert '1987년 06월 05일' to '1987-06-05'.

    Args:
        s: S.
        s: S.

    """
    if not s:
        return None
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", _clean(s))
    if not m:
        # Fallback to standard YYYY-MM-DD check if already formatted
        m_alt = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", _clean(s))
        if m_alt:
            return f"{m_alt.group(1)}-{int(m_alt.group(2)):02d}-{int(m_alt.group(3)):02d}"
        return None
    y, mm, dd = m.group(1), int(m.group(2)), int(m.group(3))
    return f"{y}-{mm:02d}-{dd:02d}"


def parse_position_and_hands(s: str) -> dict[str, str | None]:
    """Parse position and throwing/batting hands: '포수(우투우타)' -> C, R, R.

    Args:
        s: S.
        s: S.

    """
    s = _clean(s)

    if not s:
        return {"position": None, "throwing_hand": None, "batting_hand": None}

    # Extract position prefix before '('
    pos_txt = s.split("(")[0].strip()
    pos_code = POS_MAP.get(pos_txt)

    # Extract hands info inside parenthesis
    throw, bat = None, None
    m = re.search(r"\((.?)[\s]*투(.?)[\s]*타\)", s)
    conv = {"우": "R", "좌": "L", "양": "S"}
    if m:
        throw = conv.get(m.group(1).strip())
        bat = conv.get(m.group(2).strip())

    return {"position": pos_code, "throwing_hand": throw, "batting_hand": bat}


def parse_height_weight(s: str) -> dict[str, int | None]:
    """Parse height/weight from '180cm/95kg' format.

    Args:
        s: S.
        s: S.

    """
    if not s:
        return {"height_cm": None, "weight_kg": None}
    m = re.search(r"(\d+)\s*cm\s*/\s*(\d+)\s*kg", _clean(s), re.IGNORECASE)
    return {"height_cm": int(m.group(1)), "weight_kg": int(m.group(2))} if m else {"height_cm": None, "weight_kg": None}


def parse_path(s: str) -> list[str]:
    """Parse education or career path string: '송정동초-무등중-진흥고' -> list.

    Args:
        s: S.
        s: S.

    """
    if not s:
        return []
    # Support hyphens, arrows, slashes, or commas as dividers
    parts = re.split(r"\s*[-\u2013\u2014\u2192,>]\s*", s.strip())
    return [p for p in (i.strip() for i in parts) if p]


def parse_money(s: str) -> dict[str, Any | None]:
    """Parse currency amounts.

    - '200000달러' -> amount=200000, currency='USD'
    - '160000만원' -> amount=1600000000, currency='KRW'.

    Args:
        s: S.
        s: S.

    """
    if not s:
        return {"amount": None, "currency": None, "original": None}

    original = _clean(s)
    if original in ("-", "", "None", "NULL"):
        return {"amount": None, "currency": None, "original": None}

    # Extract all digits
    num_str = re.sub(r"[^\d]", "", original)
    if not num_str:
        return {"amount": None, "currency": None, "original": original}

    num = int(num_str)

    if "달러" in original or "USD" in original or "$" in original:
        return {"amount": num, "currency": "USD", "original": original}
    if "만원" in original or "원" in original or "KRW" in original:
        # KBO salaries are usually in '만원' (10,000 KRW)
        multiplier = 10000 if "만원" in original else 1
        return {"amount": num * multiplier, "currency": "KRW", "original": original}

    # Fallback to KRW if no currency symbol but default
    return {"amount": num, "currency": "KRW", "original": original}


def parse_draft(s: str) -> dict[str, Any | None]:
    """Parse draft info like '06 두산 2차 8라운드 59순위', '25 삼성 자유선발', or '98 삼성 1차'.

    Args:
        s: S.
        s: S.

    """
    default_res = {
        "draft_year": None,
        "draft_team_code": None,
        "draft_round": None,
        "draft_pick_overall": None,
        "draft_type": None,
    }

    if not s:
        return default_res

    s = _clean(s)
    if s in ("-", "", "None"):
        return default_res

    # Try matching full draft pick pattern: yy team [type] [round] [pick]
    m = re.search(
        r"(?P<yy>\d{2})\s*(?P<team>\S+)\s*(?P<dtype>1차|2차|자유선발|특별지명)?"
        r"(?:\s*(?P<round>\d+)\s*(?:라운드|R|차))?(?:\s*(?P<pick>\d+)\s*순위)?",
        s,
    )
    if not m:
        return default_res

    try:
        yy = int(m.group("yy"))
        team = m.group("team")
        dtype = m.group("dtype")
        rnd = m.group("round")
        pick = m.group("pick")

        # Map team name to code
        team_code = TEAM_CODE_MAP.get(team)
        if not team_code:
            # Try partial matching for team names
            for k, v in TEAM_CODE_MAP.items():
                if k in team or team in k:
                    team_code = v
                    break

        return {
            "draft_year": _to_year(yy),
            "draft_team_code": team_code,
            "draft_round": int(rnd) if rnd else None,
            "draft_pick_overall": int(pick) if pick else None,
            "draft_type": dtype or ("자유선발" if "자유" in s else None),
        }
    except (ValueError, TypeError):
        return default_res


def parse_entry_year_team(s: str) -> dict[str, Any | None]:
    """Parse entrant details like '06두산' or '25 삼성'.

    Args:
        s: S.
        s: S.

    """
    if not s:
        return {"entry_year": None, "entry_team_code": None}

    m = re.search(r"(?P<yy>\d{2})\s*(?P<team>\S+)", _clean(s))
    if not m:
        return {"entry_year": None, "entry_team_code": None}

    try:
        yy = int(m.group("yy"))
        team = m.group("team").strip()
        team_code = TEAM_CODE_MAP.get(team)
        if not team_code:
            for k, v in TEAM_CODE_MAP.items():
                if k in team or team in k:
                    team_code = v
                    break

        return {"entry_year": _to_year(yy), "entry_team_code": team_code}
    except (ValueError, TypeError):
        return {"entry_year": None, "entry_team_code": None}


def parse_profile(
    raw_text: str,
    *,
    is_active: bool | None = None,
    is_foreign: bool | None = None,
    team: str | None = None,
) -> PlayerProfileParsed:
    """Tokenize raw profile text raw profile text and returns a structured dictionary.

    of all parsed values.

    Args:
        raw_text: Raw Text.
        is_active: Is Active.
        is_foreign: Is Foreign.
        team: Team.

    """
    tokens = tokenize_profile(raw_text)

    # Initialize standard payload
    out: dict[str, Any] = {
        "player_name": tokens.get("선수명") or None,
        "back_number": parse_back_number(tokens.get("등번호", "")),
        "birth_date": parse_birth_date(tokens.get("생년월일", "")),
        "education_path": [],
        "signing_bonus_amount": None,
        "signing_bonus_currency": None,
        "signing_bonus_original": None,
        "salary_amount": None,
        "salary_currency": None,
        "salary_original": None,
        "draft_year": None,
        "draft_team_code": None,
        "draft_round": None,
        "draft_pick_overall": None,
        "draft_type": None,
        "entry_year": None,
        "entry_team_code": None,
        "position": None,
        "throwing_hand": None,
        "batting_hand": None,
        "height_cm": None,
        "weight_kg": None,
        "is_active": is_active,
        "is_foreign": is_foreign,
    }

    # Position & Hand
    pos_pack = parse_position_and_hands(tokens.get("포지션", ""))
    out.update(pos_pack)

    # Height & Weight
    hw = parse_height_weight(tokens.get("신장/체중", ""))
    out.update(hw)

    # Education / Career history path
    path = []
    if tokens.get("경력"):
        path += parse_path(tokens["경력"])
    if tokens.get("출신교"):
        path += parse_path(tokens["출신교"])
    out["education_path"] = path

    # Contracts: Signing bonus and Salary
    sb = parse_money(tokens.get("입단 계약금", ""))
    sal = parse_money(tokens.get("연봉", ""))
    out.update(
        {
            "signing_bonus_amount": sb["amount"],
            "signing_bonus_currency": sb["currency"],
            "signing_bonus_original": sb["original"],
            "salary_amount": sal["amount"],
            "salary_currency": sal["currency"],
            "salary_original": sal["original"],
        },
    )

    # Draft & Entry Year details
    out.update(parse_draft(tokens.get("지명순위", "")))
    out.update(parse_entry_year_team(tokens.get("입단년도", "")))

    # Fallbacks for names & other fields if still missing
    if not out["player_name"] and "선수명" in tokens:
        out["player_name"] = _clean(tokens["선수명"])

    return PlayerProfileParsed(
        player_name=out.get("player_name"),
        back_number=out.get("back_number"),
        birth_date=out.get("birth_date"),
        position=out.get("position"),
        throwing_hand=out.get("throwing_hand"),
        batting_hand=out.get("batting_hand"),
        height_cm=out.get("height_cm"),
        weight_kg=out.get("weight_kg"),
        education_or_career_path=out.get("education_path") or [],
        signing_bonus_amount=out.get("signing_bonus_amount"),
        signing_bonus_currency=out.get("signing_bonus_currency"),
        signing_bonus_original=out.get("signing_bonus_original"),
        salary_amount=out.get("salary_amount"),
        salary_currency=out.get("salary_currency"),
        salary_original=out.get("salary_original"),
        draft_year=out.get("draft_year"),
        draft_team_code=out.get("draft_team_code"),
        draft_round=out.get("draft_round"),
        draft_pick_overall=out.get("draft_pick_overall"),
        draft_type=out.get("draft_type"),
        entry_year=out.get("entry_year"),
        entry_team_code=out.get("entry_team_code"),
        is_active=is_active,
        is_foreign=is_foreign,
        team=team,
    )
