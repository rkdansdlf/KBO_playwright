"""Shared types and parsing helpers for external KBO stat sources."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol
from urllib.parse import parse_qs, urlparse

from src.utils.team_codes import resolve_team_code
from src.utils.type_helpers import parse_innings_to_outs

if TYPE_CHECKING:
    from collections.abc import Mapping

    from bs4 import BeautifulSoup, Tag

type ExternalMetricValue = int | float | str | None


class ExternalStatsError(RuntimeError):
    """Base exception for external statistics collection failures."""


class ExternalStatsAccessError(ExternalStatsError):
    """Report an access policy, authentication, or HTTP status failure."""


class ExternalStatsParseError(ExternalStatsError):
    """Report an unexpected or unsupported provider response."""


@dataclass(frozen=True)
class ExternalStatRecord:
    """Represent one normalized provider season-stat row."""

    provider: str
    source_key: str
    stat_type: str
    season: int
    player_name: str
    team_name: str | None
    team_code: str | None
    external_player_id: str | None
    metrics: dict[str, ExternalMetricValue]
    source_url: str
    metric_metadata: dict[str, object]
    league: str = "REGULAR"
    level: str = "KBO1"

    @property
    def source_record_key(self) -> str:
        """Return a stable identity hash for idempotent upserts."""
        identity = "|".join(
            (
                self.provider,
                self.stat_type,
                str(self.season),
                self.league,
                self.level,
                self.external_player_id or self.player_name.strip().lower(),
                self.team_code or (self.team_name or "").strip().lower(),
            ),
        )
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()


class ExternalStatsAdapter(Protocol):
    """Define the adapter contract used by the external stats crawler."""

    provider: str
    source_keys: dict[str, str]
    host: str

    def build_url(self, season: int, stat_type: str) -> str:
        """Build a provider URL for one season and stat type."""

    def parse_html(self, html: str, season: int, stat_type: str, source_url: str) -> list[ExternalStatRecord]:
        """Parse provider HTML into normalized season-stat rows."""


TEAM_ALIASES: dict[str, str] = {
    "doosan": "두산",
    "bears": "두산",
    "hanwha": "한화",
    "eagles": "한화",
    "lotte": "롯데",
    "giants": "롯데",
    "samsung": "삼성",
    "lions": "삼성",
    "kia": "KIA",
    "tigers": "KIA",
    "kiwoom": "키움",
    "heroes": "키움",
    "nexen": "넥센",
    "lg": "LG",
    "twins": "LG",
    "nc": "NC",
    "dinos": "NC",
    "kt": "KT",
    "wiz": "KT",
    "ssg": "SSG",
    "landers": "SSG",
    "sk": "SK",
    "ob": "OB",
}


def normalize_text(value: str | None) -> str:
    """Collapse provider whitespace and return an empty string for null text."""
    return " ".join((value or "").replace("\xa0", " ").split()).strip()


def normalize_header(value: str | None) -> str:
    """Normalize a table header for provider-specific alias lookup."""
    return re.sub(r"\s+", "", normalize_text(value)).lower()


def resolve_external_team_code(team_name: str | None, season: int) -> str | None:
    """Resolve Korean and common English team labels to a season-aware code."""
    normalized = normalize_text(team_name)
    if not normalized:
        return None
    normalized = re.sub(r"\s*\(kbo\)$", "", normalized, flags=re.IGNORECASE).strip()
    alias = TEAM_ALIASES.get(normalized.lower(), normalized)
    return resolve_team_code(alias, season)


def normalize_player_name(value: str | None) -> str:
    """Prefer the Korean display name when a provider shows English and Korean names."""
    normalized = normalize_text(value)
    korean_names = re.findall(r"[가-힣]{2,}", normalized)
    return korean_names[-1] if korean_names else normalized


def parse_number(value: str | None) -> ExternalMetricValue:
    """Parse a displayed stat value while retaining percentages as numeric values."""
    cleaned = normalize_text(value).replace(",", "")
    if not cleaned or cleaned in {"-", "--", "—", "N/A", "n/a"}:
        return None
    cleaned = cleaned.replace("%", "")
    try:
        number = float(cleaned)
    except ValueError:
        return cleaned
    return int(number) if number.is_integer() else number


def parse_metric_value(header: str, value: str | None) -> dict[str, ExternalMetricValue]:
    """Parse a table cell and expand baseball innings into canonical outs."""
    parsed = parse_number(value)
    if header != "innings_pitched" or parsed is None:
        return {header: parsed}
    outs = parse_innings_to_outs(normalize_text(value))
    if outs is None:
        return {header: parsed}
    return {"innings_pitched": outs / 3, "innings_outs": outs}


def extract_player_id(cell: Tag) -> str | None:
    """Extract a provider player identifier from a player link when present."""
    for link in cell.find_all("a", href=True):
        href = str(link.get("href", ""))
        query = parse_qs(urlparse(href).query)
        for key in ("playerid", "player_id", "p_no", "id"):
            if query.get(key):
                return query[key][0]
        match = re.search(r"(?:playerid|p_no|player[_-]?id)[=/]([A-Za-z0-9_-]+)", href, re.IGNORECASE)
        if match:
            return match.group(1)
        path_match = re.search(r"/players/[^/]+/(\d+)(?:/|$)", urlparse(href).path, re.IGNORECASE)
        if path_match:
            return path_match.group(1)
    return None


@dataclass(frozen=True)
class StatTable:
    """Represent a simple HTML stat table and its cells."""

    headers: list[str]
    rows: list[list[Tag]]


@dataclass(frozen=True)
class StatTableParseConfig:
    """Describe how a provider table maps into normalized records."""

    provider: str
    source_key: str
    season: int
    stat_type: str
    source_url: str
    header_map: Mapping[str, str]
    required_metric_headers: set[str]
    parser_version: str = "external-stats-v1"


def find_stat_table(
    soup: BeautifulSoup,
    required_headers: set[str],
    *,
    header_map: Mapping[str, str] | None = None,
) -> StatTable | None:
    """Find the first table containing the requested normalized headers."""
    for table in soup.find_all("table"):
        header_row = _find_header_row(table)
        if header_row is None:
            continue
        headers = [normalize_header(cell.get_text(" ", strip=True)) for cell in header_row.find_all(["th", "td"])]
        canonical_headers = {header_map.get(header, header) if header_map else header for header in headers}
        if not required_headers.issubset(canonical_headers):
            continue
        rows: list[list[Tag]] = []
        body_rows = table.select("tbody tr") or table.find_all("tr")[1:]
        for row in body_rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= len(headers):
                rows.append(cells[: len(headers)])
        if rows:
            return StatTable(headers=headers, rows=rows)
    return None


def _find_header_row(table: Tag) -> Tag | None:
    header_rows = table.select("thead tr")
    if header_rows:
        return header_rows[-1]
    for row in table.find_all("tr"):
        if row.find("th"):
            return row
    return None


def parse_stat_table(
    table: StatTable,
    config: StatTableParseConfig,
) -> list[ExternalStatRecord]:
    """Convert a provider table to normalized rows using a header map."""
    canonical_headers = [config.header_map.get(header, header) for header in table.headers]
    player_index = canonical_headers.index("player_name")
    team_index = canonical_headers.index("team_name") if "team_name" in canonical_headers else -1
    records: list[ExternalStatRecord] = []
    for cells in table.rows:
        player_cell = cells[player_index]
        player_name = normalize_player_name(player_cell.get_text(" ", strip=True))
        if not player_name or player_name.lower() in {"name", "선수", "이름"}:
            continue
        team_name = normalize_text(cells[team_index].get_text(" ", strip=True)) if team_index >= 0 else None
        metrics: dict[str, ExternalMetricValue] = {}
        for index, header in enumerate(table.headers):
            metric_key = config.header_map.get(header)
            if metric_key is None or header not in config.required_metric_headers:
                continue
            metrics.update(parse_metric_value(metric_key, cells[index].get_text(" ", strip=True)))
        if not metrics:
            continue
        records.append(
            ExternalStatRecord(
                provider=config.provider,
                source_key=config.source_key,
                stat_type=config.stat_type,
                season=config.season,
                player_name=player_name,
                team_name=team_name or None,
                team_code=resolve_external_team_code(team_name, config.season),
                external_player_id=extract_player_id(player_cell),
                metrics=metrics,
                source_url=config.source_url,
                metric_metadata={"headers": table.headers, "parser_version": config.parser_version},
            ),
        )
    return records


def ensure_stat_table(table: StatTable | None, provider: str, stat_type: str) -> StatTable:
    """Raise a parse error instead of treating an unsupported page as empty data."""
    if table is None:
        msg = f"{provider} returned no {stat_type} statistics table"
        raise ExternalStatsParseError(msg)
    return table


def query_source_keys(provider: str) -> dict[str, str]:
    """Return the DataSource key for each supported stat type."""
    return {stat_type: f"{provider}_kbo_{stat_type}" for stat_type in ("batting", "pitching")}


def source_content_hash(body: str) -> str:
    """Return a SHA-256 hash for a fetched provider response."""
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
