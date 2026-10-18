"""Season-type classification for KBO games (regular, exhibition, postseason).

The season type of a game decides which ``kbo_seasons`` row it belongs to, and
that row decides whether the game counts toward the 144-game regular season
(standings) or is shown as an exhibition or postseason game. Getting it wrong is
visible: exhibition games inflate team game counts, and regular-season games
that land in a postseason row are rendered as "한국시리즈 1차전".

Classification order, strongest evidence first:

1. **The source itself.** Naver marks postseason games with a repeated-digit
   game-id prefix (``4444`` wildcard, ``3333`` 준PO, ``5555`` PO, ``7777`` KS;
   the digits are the KBO series ids) and its game detail carries a
   ``roundCode`` (``kbo_e``, ``kbo_r``, ``kbo_ps_*``). The KBO schedule page is
   crawled per series option (``1`` exhibition, ``0,9,6`` regular,
   ``3,4,5,7`` postseason).
2. **The season calendar** (:data:`SEASON_DATE_RULES`), for the exhibition
   window and for the postseason stage when the source only says "postseason".
3. Otherwise the game is a regular-season game.

A source-confirmed type always wins over the calendar: the calendar is a
fallback, and a guessed calendar overriding real source data is exactly how
regular-season games ended up tagged as wildcard/준PO/KS.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

REGULAR = "regular"
EXHIBITION = "exhibition"
WILDCARD = "wildcard"
SEMI_PLAYOFF = "semi_playoff"
PLAYOFF = "playoff"
KOREAN_SERIES = "korean_series"
ALLSTAR = "allstar"
#: The source said "postseason" without naming the stage (KBO page ``3,4,5,7``).
POSTSEASON = "postseason"

POSTSEASON_STAGES: tuple[str, ...] = (WILDCARD, SEMI_PLAYOFF, PLAYOFF, KOREAN_SERIES)

#: Canonical season type -> ``kbo_seasons.league_type_code``.
SEASON_TYPE_TO_LEAGUE_CODE: dict[str, int] = {
    REGULAR: 0,
    EXHIBITION: 1,
    WILDCARD: 2,
    SEMI_PLAYOFF: 3,
    PLAYOFF: 4,
    KOREAN_SERIES: 5,
}
LEAGUE_CODE_TO_SEASON_TYPE: dict[int, str] = {code: key for key, code in SEASON_TYPE_TO_LEAGUE_CODE.items()}

#: ``kbo_seasons.league_type_name`` used when a row has to be created.
SEASON_TYPE_TO_LEAGUE_NAME: dict[str, str] = {
    REGULAR: "정규시즌",
    EXHIBITION: "시범경기",
    WILDCARD: "와일드카드",
    SEMI_PLAYOFF: "준플레이오프",
    PLAYOFF: "플레이오프",
    KOREAN_SERIES: "한국시리즈",
    ALLSTAR: "올스타전",
}
LEAGUE_NAME_TO_SEASON_TYPE: dict[str, str] = {name: key for key, name in SEASON_TYPE_TO_LEAGUE_NAME.items()}

_SEASON_TYPE_ALIASES: dict[str, str] = {
    "regular": REGULAR,
    "regular_season": REGULAR,
    "정규시즌": REGULAR,
    "exhibition": EXHIBITION,
    "preseason": EXHIBITION,
    "시범경기": EXHIBITION,
    "wildcard": WILDCARD,
    "wild_card": WILDCARD,
    "와일드카드": WILDCARD,
    "semi_playoff": SEMI_PLAYOFF,
    "semi-playoff": SEMI_PLAYOFF,
    "semiplayoff": SEMI_PLAYOFF,
    "준플레이오프": SEMI_PLAYOFF,
    "playoff": PLAYOFF,
    "플레이오프": PLAYOFF,
    "korean_series": KOREAN_SERIES,
    "korean-series": KOREAN_SERIES,
    "한국시리즈": KOREAN_SERIES,
    "allstar": ALLSTAR,
    "all_star": ALLSTAR,
    "올스타전": ALLSTAR,
    "postseason": POSTSEASON,
    "post_season": POSTSEASON,
    "포스트시즌": POSTSEASON,
}

#: Naver postseason game ids replace the date's year with the KBO series id
#: repeated four times, e.g. ``33331017LGHT02026`` is 준PO game 1 on 10/17.
NAVER_GAME_ID_PREFIX_TO_SEASON_TYPE: dict[str, str] = {
    "4444": WILDCARD,
    "3333": SEMI_PLAYOFF,
    "5555": PLAYOFF,
    "7777": KOREAN_SERIES,
    "9999": ALLSTAR,
}

#: Naver game detail ``roundCode``.
NAVER_ROUND_CODE_TO_SEASON_TYPE: dict[str, str] = {
    "kbo_e": EXHIBITION,
    "kbo_r": REGULAR,
    "kbo_ps_wd": WILDCARD,
    "kbo_ps_sp": SEMI_PLAYOFF,
    "kbo_ps_po": PLAYOFF,
    "kbo_ps_ks": KOREAN_SERIES,
    "kbo_as": ALLSTAR,
}

#: KBO schedule page ``#ddlSeries`` option values. The live page offers only the
#: three grouped options; the single ids are kept for older page versions.
KBO_SERIES_VALUE_TO_SEASON_TYPE: dict[str, str] = {
    "1": EXHIBITION,
    "0,9,6": REGULAR,
    "0": REGULAR,
    "3,4,5,7": POSTSEASON,
    "4": WILDCARD,
    "3": SEMI_PLAYOFF,
    "5": PLAYOFF,
    "7": KOREAN_SERIES,
}

#: Known non-regular windows per season, ``(start, end, kbo_seasons name)``.
#: A date outside every window is a regular-season date. Postseason end dates
#: are the last *scheduled* day of each series (rain-outs may shift them).
#: 2026 source: Naver schedule (exhibition 3/12-3/24, WC 10/14, 준PO from 10/17)
#: and the KBO press release of 2026-10-07 (WC 10/14-15, 준PO 10/17, PO 10/25,
#: KS 11/2; 준PO/PO best of five, KS best of seven).
SEASON_DATE_RULES: dict[int, list[tuple[str, str, str]]] = {
    2023: [
        ("2023-03-13", "2023-03-28", "시범경기"),
        ("2023-10-19", "2023-10-19", "와일드카드"),
        ("2023-10-22", "2023-10-25", "준플레이오프"),
        ("2023-10-30", "2023-11-05", "플레이오프"),
        ("2023-11-07", "2023-11-13", "한국시리즈"),
    ],
    2024: [
        ("2024-03-09", "2024-03-19", "시범경기"),
        ("2024-07-06", "2024-07-06", "올스타전"),
        ("2024-10-02", "2024-10-03", "와일드카드"),
        ("2024-10-05", "2024-10-11", "준플레이오프"),
        ("2024-10-13", "2024-10-19", "플레이오프"),
        ("2024-10-21", "2024-10-30", "한국시리즈"),
    ],
    2025: [
        ("2025-03-08", "2025-03-18", "시범경기"),
        ("2025-10-06", "2025-10-07", "와일드카드"),
        ("2025-10-09", "2025-10-14", "준플레이오프"),
        ("2025-10-18", "2025-10-24", "플레이오프"),
        ("2025-10-26", "2025-11-01", "한국시리즈"),
    ],
    2026: [
        ("2026-03-12", "2026-03-24", "시범경기"),
        ("2026-10-14", "2026-10-15", "와일드카드"),
        ("2026-10-17", "2026-10-23", "준플레이오프"),
        ("2026-10-25", "2026-10-31", "플레이오프"),
        ("2026-11-02", "2026-11-10", "한국시리즈"),
    ],
}

#: Regular-season span per season, used to write ``kbo_seasons`` start/end.
REGULAR_SEASON_SPANS: dict[int, tuple[str, str]] = {
    2026: ("2026-03-28", "2026-10-12"),
}


def normalize_season_type(value: object) -> str | None:
    """Return the canonical season type for a key, Korean name or league code.

    Args:
        value: A season type key (``"korean_series"``), a ``kbo_seasons`` name
            (``"한국시리즈"``) or a league type code (``5`` / ``"5"``).

    Returns:
        The canonical key, or None when the value is empty or unknown.

    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return LEAGUE_CODE_TO_SEASON_TYPE.get(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return LEAGUE_CODE_TO_SEASON_TYPE.get(int(text))
    return _SEASON_TYPE_ALIASES.get(text.lower()) or _SEASON_TYPE_ALIASES.get(text)


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        digits = value.replace("-", "").replace("/", "").strip()[:8]
        if len(digits) == 8 and digits.isdigit():  # noqa: PLR2004
            try:
                return date(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
            except ValueError:
                return None
    return None


def calendar_season_type(game_date: object) -> str | None:
    """Return the non-regular season type the calendar assigns to a date.

    Args:
        game_date: A date, datetime or ``YYYYMMDD`` / ``YYYY-MM-DD`` string.

    Returns:
        ``exhibition``, a postseason stage or ``allstar`` inside a known window;
        None for a regular-season date or a season with no calendar.

    """
    day = _coerce_date(game_date)
    if day is None:
        return None
    for start, end, name in SEASON_DATE_RULES.get(day.year, []):
        if date.fromisoformat(start) <= day <= date.fromisoformat(end):
            return LEAGUE_NAME_TO_SEASON_TYPE.get(name)
    return None


def has_season_calendar(year: int) -> bool:
    """Whether the exhibition/postseason windows of a season are known."""
    return year in SEASON_DATE_RULES


def season_type_from_naver_game_id(raw_game_id: object) -> str | None:
    """Return the postseason/all-star type a Naver game id prefix encodes.

    Args:
        raw_game_id: Naver ``gameId`` such as ``44441014OBLG02026``.

    Returns:
        The stage for a prefixed id, None for an ordinary date-prefixed id.

    """
    text = str(raw_game_id or "").strip()
    return NAVER_GAME_ID_PREFIX_TO_SEASON_TYPE.get(text[:4])


def season_type_from_naver_round_code(round_code: object) -> str | None:
    """Return the season type for a Naver game detail ``roundCode``."""
    return NAVER_ROUND_CODE_TO_SEASON_TYPE.get(str(round_code or "").strip().lower())


def season_type_from_kbo_series(series_value: object) -> str | None:
    """Return the season type for a KBO schedule ``#ddlSeries`` option value."""
    key = str(series_value or "").replace(" ", "").strip()
    return KBO_SERIES_VALUE_TO_SEASON_TYPE.get(key)


def resolve_effective_season_type(
    source_type: object,
    game_date: object,
    *,
    confirmed: bool = False,
) -> str | None:
    """Decide the season type a game is stored under.

    Args:
        source_type: The type the payload carries (may be a default ``regular``).
        game_date: The game date.
        confirmed: True when the source positively established ``source_type``
            (Naver id prefix / roundCode, KBO series option). A confirmed
            ``regular`` beats the calendar; an unconfirmed one is only a default.

    Returns:
        A canonical type, or None when the source said "postseason" but neither
        it nor the calendar names the stage.

    """
    source = normalize_season_type(source_type)
    calendar = calendar_season_type(game_date)

    if source == POSTSEASON:
        if calendar in POSTSEASON_STAGES:
            return calendar
        return None
    if source is not None and (source != REGULAR or confirmed):
        if calendar is not None and calendar != source:
            logger.info(
                "Season type %s from source overrides calendar %s for %s",
                source,
                calendar,
                game_date,
            )
        return source
    # No usable source signal: the calendar decides the known windows.
    return calendar or REGULAR


def season_payload_type(payload: dict[str, Any]) -> str | None:
    """Resolve the effective season type of a schedule/detail payload."""
    return resolve_effective_season_type(
        payload.get("season_type"),
        payload.get("game_date"),
        confirmed=bool(payload.get("season_type_confirmed")),
    )


__all__ = [
    "ALLSTAR",
    "EXHIBITION",
    "KBO_SERIES_VALUE_TO_SEASON_TYPE",
    "KOREAN_SERIES",
    "LEAGUE_CODE_TO_SEASON_TYPE",
    "LEAGUE_NAME_TO_SEASON_TYPE",
    "NAVER_GAME_ID_PREFIX_TO_SEASON_TYPE",
    "NAVER_ROUND_CODE_TO_SEASON_TYPE",
    "PLAYOFF",
    "POSTSEASON",
    "POSTSEASON_STAGES",
    "REGULAR",
    "REGULAR_SEASON_SPANS",
    "SEASON_DATE_RULES",
    "SEASON_TYPE_TO_LEAGUE_CODE",
    "SEASON_TYPE_TO_LEAGUE_NAME",
    "SEMI_PLAYOFF",
    "WILDCARD",
    "calendar_season_type",
    "has_season_calendar",
    "normalize_season_type",
    "resolve_effective_season_type",
    "season_payload_type",
    "season_type_from_kbo_series",
    "season_type_from_naver_game_id",
    "season_type_from_naver_round_code",
]
