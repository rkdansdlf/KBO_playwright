from __future__ import annotations

from typing import Any


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


def parse_numeric(value: str, as_float: bool) -> float | int | None:
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
