"""
Compatibility wrapper around the new player search crawler.

Old pipelines expect a PlayerListCrawler that returns hitters/pitchers buckets.
"""

from __future__ import annotations

from typing import Any

from src.crawlers.player_search_crawler import PlayerRow, crawl_all_players
from src.utils.player_classification import PlayerCategory, classify_player


def _is_pitcher(position: str | None) -> bool:
    if not position:
        return False
    pos = position.strip().upper()
    if "투수" in pos:
        return True
    return pos in {"P", "SP", "RP", "CP"}


def _row_to_dict(row: PlayerRow) -> dict[str, Any]:
    category = classify_player({"team": row.team, "position": row.position})
    status = "active"
    staff_role = None
    if category == PlayerCategory.RETIRED:
        status = "retired"
    elif category in (PlayerCategory.MANAGER, PlayerCategory.COACH, PlayerCategory.STAFF):
        status = "staff"
        staff_role = category.value.lower()

    return {
        "player_id": str(row.player_id),
        "player_name": row.name,
        "uniform_no": row.uniform_no,
        "team": row.team,
        "position": row.position,
        "birth_date": row.birth_date,
        "height_cm": row.height_cm,
        "weight_kg": row.weight_kg,
        "career": row.career,
        "status": status,
        "staff_role": staff_role,
        "status_source": "heuristic",
    }


class PlayerListCrawler:
    """Legacy API wrapper used by init_data_collection.py and futures crawler."""

    def __init__(self, *, request_delay: float = 1.5, headless: bool = True, max_pages: int | None = None) -> None:
        """Initializes a new instance."""
        self.request_delay = request_delay
        self.headless = headless
        self.max_pages = max_pages

    async def crawl_all_players(self, _season_year: int | None = None) -> dict[str, list[dict[str, Any]]]:
        """
        Crawls all players.

        Args:
            _season_year: Season Year.

        Returns:
            Dictionary result.

        """
        rows = await crawl_all_players(
            max_pages=self.max_pages,
            headless=self.headless,
            request_delay=self.request_delay,
        )
        hitters: list[dict[str, Any]] = []
        pitchers: list[dict[str, Any]] = []
        retired: list[dict[str, Any]] = []
        staff: list[dict[str, Any]] = []
        for row in rows:
            data = _row_to_dict(row)
            category = classify_player(data)
            data["category"] = category.value

            if category == PlayerCategory.ACTIVE:
                if _is_pitcher(row.position):
                    pitchers.append(data)
                else:
                    hitters.append(data)
            elif category == PlayerCategory.RETIRED:
                retired.append(data)
            else:
                if category in (PlayerCategory.MANAGER, PlayerCategory.COACH, PlayerCategory.STAFF):
                    data["staff_role"] = category.value
                staff.append(data)

        return {
            "hitters": hitters,
            "pitchers": pitchers,
            "retired": retired,
            "staff": staff,
            "total": len(rows),
            "active_total": len(hitters) + len(pitchers),
            "retired_total": len(retired),
            "staff_total": len(staff),
        }
