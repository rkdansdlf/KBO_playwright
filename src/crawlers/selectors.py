"""Centralized CSS selectors for KBO crawler pages.

Keep selectors grouped by page or crawler domain so DOM drift can be reviewed in
one place before touching extraction logic.
"""

from __future__ import annotations

from dataclasses import dataclass

KBO_CONTENT_PREFIX = "cphContents_cphContents_cphContents_"


@dataclass(frozen=True)
class PlayerSearchSelectors:
    """Selectors for https://www.koreabaseball.com/Player/Search.aspx."""

    input: str = "input[id$='txtSearchPlayerName']"
    search_button: str = "input[id$='btnSearch']"
    table_rows: str = "table.tEx tbody tr"
    hidden_page: str = "input[id$='hfPage']"
    page_number_buttons: str = "a[id*='btnNo'], span[id*='btnNo']"
    pager_container: str = "div.paging"
    pager_next_buttons: str = "a[id$='btnNext'], a:has(img[alt='다음']), a:has-text('다음'), a[id$='btnNext10']"


@dataclass(frozen=True)
class FieldingStatsSelectors:
    """Selectors for the player defense ranking page."""

    season_dropdown: str = f"select#{KBO_CONTENT_PREFIX}ddlSeason_ddlSeason"
    team_dropdown: str = f"select#{KBO_CONTENT_PREFIX}ddlTeam_ddlTeam"
    position_dropdown: str = f"select#{KBO_CONTENT_PREFIX}ddlPos_ddlPos"
    paging: str = ".paging"
    data_table: str = "table.tData01.tt"


PLAYER_SEARCH = PlayerSearchSelectors()
FIELDING_STATS = FieldingStatsSelectors()
