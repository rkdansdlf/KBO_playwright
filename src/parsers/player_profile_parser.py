"""
Player profile parsing utilities.
Implements the strategy described in Docs/Player_Profile_Parsing_ Plan.md
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from src.utils.team_codes import resolve_kbo_legacy_team_code

LABELS = "선수명|등번호|생년월일|포지션|신장/체중|경력|출신교|입단 계약금|연봉|지명순위|입단년도"
LABEL_REGEX = re.compile(
    rf"(?P<key>{LABELS})\s*:\s*(?P<val>.*?)(?=(?:{LABELS})\s*:|$)",
    re.S,
)

POS_MAP = {
    "투수": "P",
    "포수": "C",
    "내야수": "IF",
    "외야수": "OF",
    "지명타자": "DH",
}

HAND_MAP = {"우": "R", "좌": "L", "양": "S"}


def _clean(text: str | None) -> str:
    """Normalize whitespace in text segments."""
    return re.sub(r"\s+", " ", text or "").strip()


def _to_year(two_digits: int, cutoff: int = 50) -> int:
    """
    Convert YY to YYYY using cutoff rule.
    00~cutoff-1 -> 2000 series, cutoff~99 -> 1900 series.
    """
    return (2000 + two_digits) if two_digits < cutoff else (1900 + two_digits)


def tokenize_profile(raw: str) -> dict[str, str]:
    """Extract key:value tokens using the shared label dictionary."""
    raw = _clean(raw)
    tokens: dict[str, str] = {}
    for match in LABEL_REGEX.finditer(raw):
        key = _clean(match.group("key"))
        val = _clean(match.group("val"))
        tokens[key] = val
    return tokens


def parse_back_number(value: str) -> int | None:
    match = re.search(r"(?:No\.\s*)?(\d+)", value or "")
    return int(match.group(1)) if match else None


def parse_birth_date(value: str) -> str | None:
    match = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", value or "")
    if not match:
        return None
    year, month, day = match.group(1), int(match.group(2)), int(match.group(3))
    return f"{year}-{month:02d}-{day:02d}"


def parse_position_and_hands(value: str) -> dict[str, str | None]:
    text = _clean(value)
    position_txt = text.split("(")[0].strip()
    pos_code = POS_MAP.get(position_txt)

    throwing, batting = None, None
    match = re.search(r"\((.?)[\s]*투(.?)[\s]*타\)", text)
    if match:
        throwing = HAND_MAP.get(match.group(1))
        batting = HAND_MAP.get(match.group(2))
    return {"position": pos_code, "throwing_hand": throwing, "batting_hand": batting}


def parse_height_weight(value: str) -> dict[str, int | None]:
    match = re.search(r"(\d+)\s*cm\s*/\s*(\d+)\s*kg", value or "", re.I)
    if not match:
        return {"height_cm": None, "weight_kg": None}
    return {"height_cm": int(match.group(1)), "weight_kg": int(match.group(2))}


def parse_path(value: str) -> list[str]:
    if not value:
        return []
    parts = re.split(r"\s*[-–—→,]\s*", value.strip())
    return [segment for segment in (p.strip() for p in parts) if segment]


def parse_money(value: str) -> dict[str, Any | None]:
    if not value:
        return {"amount": None, "currency": None, "original": None}
    original = value.strip()
    normalized = re.sub(r"[^\d]", "", original)
    amount = int(normalized) if normalized else None
    if amount is None:
        return {"amount": None, "currency": None, "original": original}

    if "달러" in original:
        return {"amount": amount, "currency": "USD", "original": original}
    if "만원" in original:
        return {"amount": amount * 10_000, "currency": "KRW", "original": original}
    return {"amount": amount, "currency": None, "original": original}


def parse_draft(value: str) -> dict[str, Any | None]:
    if not value:
        return {
            "draft_year": None,
            "draft_team_code": None,
            "draft_round": None,
            "draft_pick_overall": None,
            "draft_type": None,
        }

    text = _clean(value)
    match = re.search(
        r"(?P<yy>\d{2})\s*(?P<team>\S+)\s*(?P<dtype>1차|2차|자유선발)?"
        r"(?:\s*(?P<round>\d+)라운드)?(?:\s*(?P<pick>\d+)순위)?",
        text,
    )
    if not match:
        return {
            "draft_year": None,
            "draft_team_code": None,
            "draft_round": None,
            "draft_pick_overall": None,
            "draft_type": None,
        }

    yy = int(match.group("yy"))
    team = match.group("team")
    draft_type = match.group("dtype")
    draft_round = match.group("round")
    draft_pick = match.group("pick")

    return {
        "draft_year": _to_year(yy),
        "draft_team_code": resolve_kbo_legacy_team_code(team, _to_year(yy)),
        "draft_round": int(draft_round) if draft_round else None,
        "draft_pick_overall": int(draft_pick) if draft_pick else None,
        "draft_type": draft_type,
    }


def parse_entry_year_team(value: str) -> dict[str, Any | None]:
    if not value:
        return {"entry_year": None, "entry_team_code": None}
    match = re.search(r"(?P<yy>\d{2})\s*(?P<team>\S+)", _clean(value))
    if not match:
        return {"entry_year": None, "entry_team_code": None}
    yy = int(match.group("yy"))
    team = match.group("team")
    return {
        "entry_year": _to_year(yy),
        "entry_team_code": resolve_kbo_legacy_team_code(team, _to_year(yy)),
    }


@dataclass
class PlayerProfileParsed:
    player_id: int | None = None
    player_name: str | None = None
    photo_url: str | None = None
    back_number: int | None = None
    birth_date: str | None = None
    position: str | None = None
    throwing_hand: str | None = None
    batting_hand: str | None = None
    height_cm: int | None = None
    weight_kg: int | None = None
    education_or_career_path: list[str] = field(default_factory=list)
    signing_bonus_amount: int | None = None
    signing_bonus_currency: str | None = None
    signing_bonus_original: str | None = None
    salary_amount: int | None = None
    salary_currency: str | None = None
    salary_original: str | None = None
    draft_year: int | None = None
    draft_team_code: str | None = None
    draft_round: int | None = None
    draft_pick_overall: int | None = None
    draft_type: str | None = None
    entry_year: int | None = None
    entry_team_code: str | None = None
    is_active: bool | None = None
    is_foreign: bool | None = None


def parse_profile(
    raw_text: str, *, is_active: bool | None = None, is_foreign: bool | None = None
) -> PlayerProfileParsed:
    """Parse raw profile text into structured fields."""
    tokens = tokenize_profile(raw_text)

    parsed = PlayerProfileParsed(
        player_name=tokens.get("선수명"),
        back_number=parse_back_number(tokens.get("등번호", "")),
        birth_date=parse_birth_date(tokens.get("생년월일", "")),
        is_active=is_active,
        is_foreign=is_foreign,
    )

    parsed.__dict__.update(parse_position_and_hands(tokens.get("포지션", "")))
    parsed.__dict__.update(parse_height_weight(tokens.get("신장/체중", "")))

    career_path: list[str] = []
    if tokens.get("경력"):
        career_path += parse_path(tokens["경력"])
    if tokens.get("출신교"):
        career_path += parse_path(tokens["출신교"])
    parsed.education_or_career_path = career_path

    signing_bonus = parse_money(tokens.get("입단 계약금", ""))
    salary = parse_money(tokens.get("연봉", ""))
    parsed.signing_bonus_amount = signing_bonus["amount"]
    parsed.signing_bonus_currency = signing_bonus["currency"]
    parsed.signing_bonus_original = signing_bonus["original"]
    parsed.salary_amount = salary["amount"]
    parsed.salary_currency = salary["currency"]
    parsed.salary_original = salary["original"]

    parsed.__dict__.update(parse_draft(tokens.get("지명순위", "")))
    parsed.__dict__.update(parse_entry_year_team(tokens.get("입단년도", "")))

    return parsed


__all__ = [
    "parse_profile",
    "tokenize_profile",
    "parse_back_number",
    "parse_birth_date",
    "parse_position_and_hands",
    "parse_height_weight",
    "parse_path",
    "parse_money",
    "parse_draft",
    "parse_entry_year_team",
    "PlayerProfileParsed",
]
