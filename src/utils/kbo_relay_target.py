"""KBO LiveText Relay Target Model & Resolver.

Encapsulates canonical KBO LiveText URL generation and parameter resolution
for regular season, postseason, and Futures League games. Enforces explicit
evidence provenance (scoreboard href, GameCenter metadata, schedule metadata,
or verified target fixtures) and fails closed on missing or unevidenced parameters.
Global hardcoding of seriesId=0 is strictly prohibited.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from src.utils.team_codes import normalize_kbo_game_id

KBO_BASE_URL = "https://www.koreabaseball.com"
ALLOWED_KBO_HOSTS = ("www.koreabaseball.com", "koreabaseball.com")
LIVE_TEXT_PATH = "/Game/LiveText.aspx"
FUTURES_LIVE_TEXT_PATH = "/Futures/Schedule/LiveText.aspx"

MIN_KBO_YEAR = 1982
MAX_KBO_YEAR = 2100

LEAGUE_ID_KBO = 1
LEAGUE_ID_FUTURES = 2

SEASON_TYPE_TO_SERIES_ID: dict[str, int] = {
    "regular": 0,
    "exhibition": 1,
    "semi_playoff": 3,
    "wildcard": 4,
    "playoff": 5,
    "korean_series": 7,
}

SERIES_ID_TO_SEASON_TYPE: dict[int, str] = {v: k for k, v in SEASON_TYPE_TO_SERIES_ID.items()}


@dataclass(frozen=True)
class KboRelayTarget:
    """Immutable, validated specification of a KBO LiveText relay request target.

    Must contain explicit provenance metadata documenting the authoritative source
    from which parameters were resolved.
    """

    game_id: str
    gyear: int
    league_id: int
    series_id: int
    endpoint_path: str
    resolved_from: str

    def __post_init__(self) -> None:
        """Validate all required fields fail-closed."""
        if not self.game_id or not isinstance(self.game_id, str):
            msg = f"Invalid game_id: {self.game_id!r}"
            raise ValueError(msg)

        clean_id = self.game_id.strip()
        if not re.match(r"^\d{8}[A-Z0-9]{3,4}\d?$", clean_id):
            msg = f"Malformed game_id format: {self.game_id!r}"
            raise ValueError(msg)

        if not (MIN_KBO_YEAR <= self.gyear <= MAX_KBO_YEAR):
            msg = f"gyear out of valid KBO range ({MIN_KBO_YEAR}-{MAX_KBO_YEAR}): {self.gyear}"
            raise ValueError(msg)

        expected_year = int(clean_id[:4])
        if self.gyear != expected_year:
            msg = f"Target gyear ({self.gyear}) does not match game_id year prefix ({expected_year})"
            raise ValueError(msg)

        if self.league_id not in (LEAGUE_ID_KBO, LEAGUE_ID_FUTURES):
            msg = f"Invalid league_id ({self.league_id}); must be {LEAGUE_ID_KBO} or {LEAGUE_ID_FUTURES}"
            raise ValueError(msg)

        if self.series_id < 0:
            msg = f"Negative series_id is not allowed: {self.series_id}"
            raise ValueError(msg)

        if not self.endpoint_path or not self.endpoint_path.startswith("/"):
            msg = f"endpoint_path must start with '/': {self.endpoint_path!r}"
            raise ValueError(msg)

        if not self.endpoint_path.endswith("LiveText.aspx"):
            msg = f"endpoint_path must terminate in LiveText.aspx: {self.endpoint_path!r}"
            raise ValueError(msg)

        if not self.resolved_from or not isinstance(self.resolved_from, str):
            msg = f"resolved_from provenance must be a non-empty string: {self.resolved_from!r}"
            raise ValueError(msg)

    @property
    def is_futures(self) -> bool:
        """Check if target represents a Futures League match."""
        return self.league_id == LEAGUE_ID_FUTURES or "Futures" in self.endpoint_path

    def to_url(self) -> str:
        """Build the canonical, fully-qualified KBO LiveText URL.

        This is the single canonical source of URL generation for KBO text relays.
        Direct string assembly elsewhere in the repository is prohibited.
        """
        return (
            f"{KBO_BASE_URL}{self.endpoint_path}"
            f"?leagueId={self.league_id}"
            f"&seriesId={self.series_id}"
            f"&gameId={self.game_id}"
            f"&gyear={self.gyear}"
        )


def _parse_url_source(url_or_href: str, source_label: str) -> tuple[str, int, int, int, str]:
    """Parse and validate parameters from a raw URL or href string."""
    parsed = urlparse(url_or_href)

    if parsed.netloc and parsed.netloc not in ALLOWED_KBO_HOSTS:
        msg = f"Unauthorized host in {source_label}: {parsed.netloc!r}. Allowed: {ALLOWED_KBO_HOSTS}"
        raise ValueError(msg)

    path = parsed.path or LIVE_TEXT_PATH
    if not path.endswith("LiveText.aspx"):
        msg = f"Invalid endpoint path in {source_label}: {path!r}. Must end with LiveText.aspx"
        raise ValueError(msg)

    qs = parse_qs(parsed.query)

    for param in ("gameId", "gyear", "leagueId", "seriesId"):
        if param not in qs or not qs[param][0]:
            msg = f"R2_TARGET_METADATA_UNRESOLVED: missing '{param}' in {source_label}"
            raise ValueError(msg)

    return (
        qs["gameId"][0].strip(),
        int(qs["gyear"][0].strip()),
        int(qs["leagueId"][0].strip()),
        int(qs["seriesId"][0].strip()),
        path,
    )


def _resolve_from_fixture(fixture: dict[str, Any], resolved_from: str | None) -> KboRelayTarget:
    """Resolve target from a verified dictionary fixture."""
    fix_gid = fixture.get("game_id") or fixture.get("kbo_game_id")
    if not fix_gid:
        msg = "R2_TARGET_METADATA_UNRESOLVED: fixture missing 'game_id'"
        raise ValueError(msg)

    norm_id = normalize_kbo_game_id(str(fix_gid).strip())
    gyear = int(fixture.get("gyear") or fixture.get("year") or norm_id[:4])

    if "league_id" not in fixture or "series_id" not in fixture:
        msg = "R2_TARGET_METADATA_UNRESOLVED: fixture missing 'league_id' or 'series_id'"
        raise ValueError(msg)

    fix_league_id = int(fixture["league_id"])
    fix_series_id = int(fixture["series_id"])
    endpoint_path = fixture.get(
        "endpoint_path",
        FUTURES_LIVE_TEXT_PATH if fix_league_id == LEAGUE_ID_FUTURES else LIVE_TEXT_PATH,
    )

    return KboRelayTarget(
        game_id=norm_id,
        gyear=gyear,
        league_id=fix_league_id,
        series_id=fix_series_id,
        endpoint_path=endpoint_path,
        resolved_from=resolved_from or "verified_target_fixture",
    )


def _resolve_from_metadata(
    meta: dict[str, Any],
    game_id: str | None,
    *,
    is_gamecenter: bool,
    resolved_from: str | None,
) -> KboRelayTarget:
    """Resolve target from GameCenter or schedule metadata dictionary."""
    m_gid = meta.get("game_id") or meta.get("kbo_game_id") or game_id
    if not m_gid:
        msg = "R2_TARGET_METADATA_UNRESOLVED: metadata missing 'game_id'"
        raise ValueError(msg)

    norm_id = normalize_kbo_game_id(str(m_gid).strip())
    gyear = int(meta.get("gyear") or meta.get("year") or norm_id[:4])

    if "league_id" not in meta or "series_id" not in meta:
        msg = "R2_TARGET_METADATA_UNRESOLVED: metadata missing 'league_id' or 'series_id'"
        raise ValueError(msg)

    m_league_id = int(meta["league_id"])
    m_series_id = int(meta["series_id"])
    endpoint_path = meta.get(
        "endpoint_path",
        FUTURES_LIVE_TEXT_PATH if m_league_id == LEAGUE_ID_FUTURES else LIVE_TEXT_PATH,
    )
    label = "gamecenter_meta" if is_gamecenter else "schedule_metadata"

    return KboRelayTarget(
        game_id=norm_id,
        gyear=gyear,
        league_id=m_league_id,
        series_id=m_series_id,
        endpoint_path=endpoint_path,
        resolved_from=resolved_from or label,
    )


def _resolve_from_season_type(  # noqa: PLR0913
    game_id: str,
    *,
    season_type: str | None,
    series_id: int | None,
    league_id: int | None,
    is_futures: bool,
    resolved_from: str | None,
) -> KboRelayTarget | None:
    """Resolve target from explicit game_id and season_type/parameters."""
    norm_id = normalize_kbo_game_id(game_id.strip())
    gyear = int(norm_id[:4])

    if series_id is not None and resolved_from:
        r_series_id = int(series_id)
        r_league_id = league_id if league_id is not None else (LEAGUE_ID_FUTURES if is_futures else LEAGUE_ID_KBO)
        endpoint_path = FUTURES_LIVE_TEXT_PATH if r_league_id == LEAGUE_ID_FUTURES else LIVE_TEXT_PATH
        return KboRelayTarget(
            game_id=norm_id,
            gyear=gyear,
            league_id=r_league_id,
            series_id=r_series_id,
            endpoint_path=endpoint_path,
            resolved_from=resolved_from,
        )

    if season_type is not None:
        st_clean = season_type.strip().lower()
        if st_clean not in SEASON_TYPE_TO_SERIES_ID:
            valid_types = sorted(SEASON_TYPE_TO_SERIES_ID.keys())
            msg = f"Unmapped season_type: {season_type!r}. Must be one of: {valid_types}"
            raise ValueError(msg)
        r_series_id = SEASON_TYPE_TO_SERIES_ID[st_clean]
        r_league_id = league_id if league_id is not None else (LEAGUE_ID_FUTURES if is_futures else LEAGUE_ID_KBO)
        endpoint_path = FUTURES_LIVE_TEXT_PATH if r_league_id == LEAGUE_ID_FUTURES else LIVE_TEXT_PATH
        return KboRelayTarget(
            game_id=norm_id,
            gyear=gyear,
            league_id=r_league_id,
            series_id=r_series_id,
            endpoint_path=endpoint_path,
            resolved_from=f"season_type:{st_clean}",
        )

    return None


def resolve_kbo_relay_target(  # noqa: PLR0913
    game_id: str | None = None,
    *,
    href: str | None = None,
    scoreboard_link: str | None = None,
    gamecenter_meta: dict[str, Any] | None = None,
    schedule_meta: dict[str, Any] | None = None,
    fixture: dict[str, Any] | None = None,
    season_type: str | None = None,
    series_id: int | None = None,
    league_id: int | None = None,
    is_futures: bool = False,
    resolved_from: str | None = None,
) -> KboRelayTarget:
    """Resolve and validate a KboRelayTarget strictly from verified evidence.

    Fails closed with ValueError("R2_TARGET_METADATA_UNRESOLVED: ...") if
    authoritative provenance for league_id and series_id is not supplied.
    """
    link_source = href or scoreboard_link
    if link_source:
        label = "scoreboard_link" if scoreboard_link else "official_href"
        p_game_id, p_gyear, p_league_id, p_series_id, p_path = _parse_url_source(link_source, label)
        norm_id = normalize_kbo_game_id(p_game_id)
        return KboRelayTarget(
            game_id=norm_id,
            gyear=p_gyear,
            league_id=p_league_id,
            series_id=p_series_id,
            endpoint_path=p_path,
            resolved_from=label,
        )

    if fixture:
        return _resolve_from_fixture(fixture, resolved_from)

    if gamecenter_meta:
        return _resolve_from_metadata(gamecenter_meta, game_id, is_gamecenter=True, resolved_from=resolved_from)

    if schedule_meta:
        return _resolve_from_metadata(schedule_meta, game_id, is_gamecenter=False, resolved_from=resolved_from)

    if game_id:
        target = _resolve_from_season_type(
            game_id,
            season_type=season_type,
            series_id=series_id,
            league_id=league_id,
            is_futures=is_futures,
            resolved_from=resolved_from,
        )
        if target:
            return target

    msg = (
        "R2_TARGET_METADATA_UNRESOLVED: Cannot determine league_id/series_id without explicit evidence. "
        "Supply an official href, scoreboard_link, fixture, metadata dict, or season_type."
    )
    raise ValueError(msg)
