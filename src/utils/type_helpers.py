"""Shared type conversion helpers for crawlers."""

from __future__ import annotations

import re

_EMPTY_SENTINELS = frozenset({"", "-", "—", "–", "null"})


def to_int(val: object, default: int = 0) -> int:
    """Convert value to int, returning default on failure."""
    try:
        return int(str(val).strip().replace(",", ""))
    except (ValueError, TypeError):
        return default


def safe_int(value: object) -> int:
    """Convert cell text to int, returning 0 on failure."""
    try:
        return int(str(value).strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


def safe_int_or_none(value: object) -> int | None:
    """Convert cell text to int, returning None for empty/invalid values."""
    if value is None:
        return None
    cleaned = str(value).replace(",", "").strip()
    if cleaned in _EMPTY_SENTINELS:
        return None
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        return None


def safe_float(value: object) -> float:
    """Convert cell text to float, returning 0.0 on failure."""
    try:
        return float(str(value).strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def safe_float_or_none(value: object) -> float | None:
    """Convert cell text to float, returning None for empty/invalid values."""
    if value is None:
        return None
    cleaned = str(value).replace(",", "").strip()
    if cleaned in _EMPTY_SENTINELS:
        return None
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_innings(value: str | None) -> float:
    """Parse inning string like '112 1/3' to float 112.333..."""
    if not value:
        return 0.0
    txt = value.strip().replace(",", "")
    if not txt or txt == "-":
        return 0.0
    if " " in txt:
        parts = txt.split(" ")
        val = float(parts[0])
        if len(parts) > 1 and "/" in parts[1]:
            frac = parts[1].split("/")
            val += float(frac[0]) / float(frac[1])
        return val
    if "/" in txt:
        frac = txt.split("/")
        return float(frac[0]) / float(frac[1])
    return float(txt)


def _parse_fraction_innings(cleaned: str) -> int | None:
    frac_match = re.match(r"^(\d+)\s+(\d+)/(\d+)$", cleaned)
    if frac_match:
        whole = int(frac_match.group(1))
        num = int(frac_match.group(2))
        den = int(frac_match.group(3))
        return whole * 3 + round(num * 3 / den)

    frac_only = re.match(r"^(\d+)/(\d+)$", cleaned)
    if frac_only:
        return round(int(frac_only.group(1)) * 3 / int(frac_only.group(2)))

    return None


def _parse_decimal_innings(cleaned: str) -> int | None:
    if "." not in cleaned:
        return None
    try:
        parts = cleaned.split(".", 1)
        whole = int(parts[0].strip()) if parts[0].strip() else 0
        frac_digit = int(parts[1].strip()[:1])
        return whole * 3 + frac_digit
    except (ValueError, IndexError):
        return None


def parse_innings_to_outs(text: str | None) -> int | None:
    """
    Convert innings string to total outs.

    Supports:
      - 'X Y/3'   (e.g. '5 1/3' -> 16)
      - 'X/Y'     (e.g. '2/3' -> 2)
      - Unicode fractions
      - Decimal   (e.g. '5.1' -> 16, '0.2' -> 2)
      - 'X:Y'     (e.g. '5:1' -> 16)
      - Plain int (e.g. '5' -> 15)
    """
    cleaned = _clean_innings_text(text)
    if cleaned is None:
        return None

    if ":" in cleaned:
        return _parse_colon_innings(cleaned)
    result = _parse_fraction_innings(cleaned)
    if result is not None:
        return result
    result = _parse_decimal_innings(cleaned)
    if result is not None:
        return result
    try:
        return int(cleaned) * 3
    except ValueError:
        return None


def _clean_innings_text(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = str(text).strip().replace("⅓", " 1/3").replace("⅔", " 2/3").strip()
    if cleaned in _EMPTY_SENTINELS:
        return None
    return cleaned


def _parse_colon_innings(cleaned: str) -> int:
    parts = cleaned.split(":")
    innings = int(parts[0])
    remainder = int(parts[1]) if len(parts) > 1 else 0
    return innings * 3 + remainder
