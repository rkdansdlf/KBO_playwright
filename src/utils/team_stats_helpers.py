"""유틸리티: team stats helpers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from bs4 import BeautifulSoup

from src.constants import KST

if TYPE_CHECKING:
    from bs4.element import Tag

ValueParser = Callable[[str, str], object | None]
PostProcessor = Callable[[dict[str, Any], dict[str, str]], None]
MIN_COMPLETED_TEAM_GAMES = 100


@dataclass(frozen=True)
class TeamStatsParseContext:
    """Static values needed to parse a team-stat table."""

    season: int
    league: str
    team_mapping: dict[str, str]
    header_map: dict[str, str]
    stat_fields: set[str]
    float_fields: set[str]
    value_parser: ValueParser | None = None
    postprocess: PostProcessor | None = None


def has_complete_team_stats(
    rows: list[dict[str, Any]],
    *,
    expected_team_ids: set[str],
    season: int,
    current_year: int | None = None,
) -> bool:
    """Return whether a parsed team-stat table covers the requested season."""
    by_team = {str(row.get("team_id")): row for row in rows if row.get("team_id")}
    if not expected_team_ids.issubset(by_team):
        return False
    if current_year is None:
        current_year = datetime.now(KST).year
    if season >= current_year:
        return True
    return all((by_team[team_id].get("games") or 0) >= MIN_COMPLETED_TEAM_GAMES for team_id in expected_team_ids)


def annotate_team_stats_source(rows: list[dict[str, Any]], source_url: str) -> list[dict[str, Any]]:
    """Attach source provenance to parsed team-stat rows."""
    for row in rows:
        extra_stats = dict(row.get("extra_stats") or {})
        extra_stats.update({"source": "kbo_team_page", "source_url": source_url})
        row["extra_stats"] = extra_stats
    return rows


def get_cell_value(cells: list[Any], index: int) -> str | None:
    """Get cell value.

    Args:
        cells: Cells.
        index: Index.
        cells: Cells.
        index: Index.
        cells: Cells.
        index: Index.

    Returns:
        The result of the operation.

    """
    if index >= len(cells):
        return None
    return str(cells[index].get_text(strip=True))


def resolve_team_id(team_name: str, team_mapping: dict[str, str]) -> str | None:
    """Resolve team id.

    Args:
        team_name: Team Name.
        team_mapping: Team Mapping.
        team_name: Team Name.
        team_mapping: Team Mapping.
        team_name: Team Name.
        team_mapping: Team Mapping.

    Returns:
        The result of the operation.

    """
    key = team_name.strip()

    if key in team_mapping:
        return team_mapping[key]
    normalized = key.replace(" ", "")
    if normalized in team_mapping:
        return team_mapping[normalized]
    return None


def parse_numeric(value: str, *, as_float: bool) -> float | int | None:
    """Parse numeric.

    Args:
        value: Value.
        as_float: As Float.
        value: Value.
        as_float: As Float.
        value: Value.

    Returns:
        The result of the operation.

    """
    cleaned = value.replace(",", "").replace("%", "")

    if cleaned in ("", "-", "N/A"):
        return None
    try:
        return float(cleaned) if as_float else int(float(cleaned))
    except ValueError:
        try:
            return float(cleaned)
        except ValueError:
            return None


def extract_team_stat_rows(table: Tag) -> list[Tag]:
    """Extract team stat rows.

    Args:
        table: Table.
        table: Table.
        table: Table.

    Returns:
        List of results.

    """
    rows = table.select("tbody tr")

    if rows:
        return rows
    return [row for row in table.select("tr") if row.find_all("td")]


def build_team_column_map(headers: list[str], header_map: dict[str, str]) -> dict[str, int]:
    """Build team column.

    Args:
        headers: Headers.
        header_map: Header Map.
        headers: Headers.
        header_map: Header Map.
        headers: Headers.
        header_map: Header Map.

    Returns:
        Dictionary mapping.

    """
    indexes: dict[str, int] = {}

    for idx, raw in enumerate(headers):
        key = raw.strip().lower()
        normalized = header_map.get(key)
        if normalized:
            indexes[normalized] = idx
    if "team_name" not in indexes:
        indexes["team_name"] = 1 if len(headers) > 1 else 0
    return indexes


def parse_team_stats_html(html: str, context: TeamStatsParseContext) -> list[dict[str, Any]]:
    """Parse team stats html.

    Args:
        html: Html.
        context: Static values needed to parse the table.

    Returns:
        List of results.

    """
    soup = BeautifulSoup(html, "lxml")

    table = soup.select_one("table.tData01") or soup.select_one("table")
    if not table:
        return []
    header_cells = table.select("thead tr th")
    if not header_cells:
        header_cells = table.select("tr th")
    headers = [cell.get_text(strip=True).lower() for cell in header_cells]
    indexes = build_team_column_map(headers, context.header_map)
    if "team_name" not in indexes:
        return []
    stat_rows = extract_team_stat_rows(table)
    results: list[dict[str, Any]] = []
    for row in stat_rows:
        payload = _parse_one_team_row(row, indexes, context)
        if payload is not None:
            results.append(payload)
    return results


def _parse_one_team_row(
    row: Tag,
    indexes: dict[str, int],
    context: TeamStatsParseContext,
) -> dict[str, Any] | None:
    """Parse one team row.

    Args:
        row: Row.
        indexes: Indexes.
        context: Static values needed to parse the table.

    Returns:
        The result of the operation.

    """
    cells = row.find_all("td")

    if len(cells) < len(indexes):
        return None
    team_name = get_cell_value(cells, indexes["team_name"])
    if not team_name:
        return None
    payload: dict[str, Any] = {
        "team_id": resolve_team_id(team_name, context.team_mapping) or team_name,
        "team_name": team_name,
        "season": context.season,
        "league": context.league,
    }
    extras, raw_values = _parse_team_row_values(payload, cells, indexes, context)
    if extras:
        payload["extra_stats"] = extras
    if context.postprocess:
        context.postprocess(payload, raw_values)
    return payload


def _parse_team_row_values(
    payload: dict[str, Any],
    cells: list[Any],
    indexes: dict[str, int],
    context: TeamStatsParseContext,
) -> tuple[dict[str, Any], dict[str, str]]:
    extras: dict[str, Any] = {}
    raw_values: dict[str, str] = {}
    for header_key, idx in indexes.items():
        if header_key == "team_name":
            continue
        value_str = get_cell_value(cells, idx)
        if value_str is None:
            continue
        raw_values[header_key] = value_str
        if context.value_parser:
            value = context.value_parser(header_key, value_str)
        else:
            value = parse_numeric(value_str, as_float=header_key in context.float_fields)
        if value is None:
            continue
        if header_key in context.stat_fields:
            payload[header_key] = value
        else:
            extras[header_key] = value
    return extras, raw_values
