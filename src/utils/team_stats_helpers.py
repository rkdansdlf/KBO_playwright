"""유틸리티: team stats helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from bs4.element import Tag


def get_cell_value(cells: list[Any], index: int) -> str | None:
    if index >= len(cells):
        return None
    return cells[index].get_text(strip=True)


def resolve_team_id(team_name: str, team_mapping: dict[str, str]) -> str | None:
    key = team_name.strip()
    if key in team_mapping:
        return team_mapping[key]
    normalized = key.replace(" ", "")
    if normalized in team_mapping:
        return team_mapping[normalized]
    return None


def parse_numeric(value: str, *, as_float: bool) -> float | int | None:
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
    rows = table.select("tbody tr")
    if rows:
        return rows
    return [row for row in table.select("tr") if row.find_all("td")]


def build_team_column_map(headers: list[str], header_map: dict[str, str]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    for idx, raw in enumerate(headers):
        key = raw.strip().lower()
        normalized = header_map.get(key)
        if normalized:
            indexes[normalized] = idx
    if "team_name" not in indexes:
        indexes["team_name"] = 1 if len(headers) > 1 else 0
    return indexes


def parse_team_stats_html(
    html: str,
    season: int,
    league: str,
    team_mapping: dict[str, str],
    header_map: dict[str, str],
    stat_fields: set[str],
    float_fields: set[str],
    *,
    value_parser: Any = None,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.tData01") or soup.select_one("table")
    if not table:
        return []
    header_cells = table.select("thead tr th")
    if not header_cells:
        header_cells = table.select("tr th")
    headers = [cell.get_text(strip=True).lower() for cell in header_cells]
    indexes = build_team_column_map(headers, header_map)
    if "team_name" not in indexes:
        return []
    stat_rows = extract_team_stat_rows(table)
    results: list[dict[str, Any]] = []
    for row in stat_rows:
        payload = _parse_one_team_row(
            row, indexes, season, league, team_mapping, stat_fields, float_fields, value_parser
        )
        if payload is not None:
            results.append(payload)
    return results


def _parse_one_team_row(
    row: Tag,
    indexes: dict[str, int],
    season: int,
    league: str,
    team_mapping: dict[str, str],
    stat_fields: set[str],
    float_fields: set[str],
    value_parser: Any,
) -> dict[str, Any] | None:
    cells = row.find_all("td")
    if len(cells) < len(indexes):
        return None
    team_name = get_cell_value(cells, indexes["team_name"])
    if not team_name:
        return None
    payload: dict[str, Any] = {
        "team_id": resolve_team_id(team_name, team_mapping) or team_name,
        "team_name": team_name,
        "season": season,
        "league": league,
    }
    extras: dict[str, Any] = {}
    for header_key, idx in indexes.items():
        if header_key == "team_name":
            continue
        value_str = get_cell_value(cells, idx)
        if value_str is None:
            continue
        if value_parser:
            value = value_parser(header_key, value_str)
        else:
            value = parse_numeric(value_str, as_float=header_key in float_fields)
        if value is None:
            continue
        if header_key in stat_fields:
            payload[header_key] = value
        else:
            extras[header_key] = value
    if extras:
        payload["extra_stats"] = extras
    return payload
