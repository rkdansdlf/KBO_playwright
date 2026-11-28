"""Shared type conversion helpers for crawlers."""

from __future__ import annotations

import math
import re

_EMPTY_SENTINELS = frozenset(
    {
        "",
        "-",
        "\u2014",
        "\u2013",
        "null",
        "none",
        "nan",
        "inf",
        "-inf",
        "+inf",
        "infinity",
        "-infinity",
        "+infinity",
        "무한대",
        "\u00a0",
    }
)


def to_int(val: object, default: int = 0) -> int:
    """Convert value to int, returning default on failure."""
    if val is None:
        return default
    cleaned = str(val).replace(",", "").strip()
    if cleaned.lower() in _EMPTY_SENTINELS:
        return default
    try:
        return int(cleaned)
    except ValueError, TypeError:
        return default


def safe_int(value: object) -> int:
    """Convert cell text to int, returning 0 on failure."""
    if value is None:
        return 0
    cleaned = str(value).replace(",", "").strip()
    if cleaned.lower() in _EMPTY_SENTINELS:
        return 0
    try:
        return int(cleaned)
    except ValueError, TypeError:
        return 0


def safe_int_or_none(value: object) -> int | None:
    """Convert cell text to int, returning None for empty/invalid values."""
    if value is None:
        return None
    cleaned = str(value).replace(",", "").strip()
    if cleaned.lower() in _EMPTY_SENTINELS:
        return None
    try:
        return int(cleaned)
    except ValueError, TypeError:
        return None


def safe_float(value: object) -> float:
    """Convert cell text to float, returning 0.0 on failure."""
    if value is None:
        return 0.0
    cleaned = str(value).replace(",", "").strip()
    if cleaned.lower() in _EMPTY_SENTINELS:
        return 0.0
    try:
        val = float(cleaned)
    except ValueError, TypeError:
        return 0.0
    else:
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return val


def safe_float_or_none(value: object) -> float | None:
    """Convert cell text to float, returning None for empty/invalid values."""
    if value is None:
        return None
    cleaned = str(value).replace(",", "").strip()
    if cleaned.lower() in _EMPTY_SENTINELS:
        return None
    try:
        val = float(cleaned)
    except ValueError, TypeError:
        return None
    else:
        if math.isnan(val) or math.isinf(val):
            return None
        return val


def parse_innings(value: str | None) -> float:
    """Parse inning string like '112 1/3', '5.1', or '⅓' to float."""
    cleaned = _clean_innings_text(value)
    if not cleaned:
        return 0.0
    try:
        if " " in cleaned:
            parts = cleaned.split(" ")
            val = float(parts[0])
            if len(parts) > 1 and "/" in parts[1]:
                frac = parts[1].split("/")
                den = float(frac[1])
                if den != 0:
                    val += float(frac[0]) / den
            return round(val, 3)
        if "/" in cleaned:
            frac = cleaned.split("/")
            den = float(frac[1])
            if den != 0:
                return round(float(frac[0]) / den, 3)
            return 0.0
        return float(cleaned)
    except ValueError, TypeError, ZeroDivisionError:
        return 0.0


def _parse_fraction_innings(cleaned: str) -> int | None:
    """Parse fraction innings into total outs."""
    frac_match = re.match(r"^(\d+)\s+(\d+)/(\d+)$", cleaned)
    if frac_match:
        whole = int(frac_match.group(1))
        num = int(frac_match.group(2))
        den = int(frac_match.group(3))
        if den == 0:
            return whole * 3
        return whole * 3 + round(num * 3 / den)

    frac_only = re.match(r"^(\d+)/(\d+)$", cleaned)
    if frac_only:
        num = int(frac_only.group(1))
        den = int(frac_only.group(2))
        if den == 0:
            return 0
        return round(num * 3 / den)

    return None


def _parse_decimal_innings(cleaned: str) -> int | None:
    """Parse decimal innings into total outs."""
    if "." not in cleaned:
        return None
    try:
        parts = cleaned.split(".", 1)
        whole = int(parts[0].strip()) if parts[0].strip() else 0
        frac_str = parts[1].strip()
        if not frac_str:
            return whole * 3
        # In baseball notation, .1 is 1 out and .2 is 2 outs
        if len(frac_str) == 1 and frac_str in ("1", "2"):
            return whole * 3 + int(frac_str)
        # Decimal representations e.g. .333 or .667
        return round(float(cleaned) * 3)
    except ValueError, IndexError:
        return None


def parse_innings_to_outs(text: str | None) -> int | None:
    """Convert innings string to total outs.

    Supports:
      - 'X Y/3'   (e.g. '5 1/3' -> 16)
      - 'X/Y'     (e.g. '2/3' -> 2)
      - Unicode fractions ('5 ⅓', '⅓', '⅔')
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
    """Normalize innings string removing extra whitespace and converting Unicode fractions."""
    if not text:
        return None
    cleaned = str(text).strip().replace("⅓", " 1/3").replace("⅔", " 2/3").replace(",", "").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if cleaned.lower() in _EMPTY_SENTINELS:
        return None
    return cleaned


def _parse_colon_innings(cleaned: str) -> int:
    """Parse colon innings (e.g. '5:1' -> 16 outs)."""
    parts = cleaned.split(":")
    innings = int(parts[0])
    remainder = int(parts[1]) if len(parts) > 1 else 0
    return innings * 3 + remainder
