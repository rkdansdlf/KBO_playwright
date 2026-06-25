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


@dataclass(frozen=True)
class GameDetailSelectors:
    """Selectors for GameCenter box score detail pages."""

    status_on: str = "li.game-cont.on p.status"
    status_typo: str = "li.game-cont.on p.staus"
    cancel_on: str = "li.game-cont.on .game-status.cancel"
    cancel_generic: str = ".game-status.cancel"

    content_boxscore_area: str = ".box-score-area"
    info_area: str = ".box-score-area, .game-info, .score-board, .record-etc"

    away_hitter_primary: str = "#tblAwayHitter1"
    away_hitter_inning: str = "#tblAwayHitter2"
    away_hitter_extra: str = "#tblAwayHitter3"
    home_hitter_primary: str = "#tblHomeHitter1"
    home_hitter_inning: str = "#tblHomeHitter2"
    home_hitter_extra: str = "#tblHomeHitter3"

    away_pitcher_primary: str = "#tblAwayPitcher"
    away_pitcher_alt: str = "#tblAwayPitcher1"
    away_pitcher_alt2: str = "#tblAwayPitcher2"
    home_pitcher_primary: str = "#tblHomePitcher"
    home_pitcher_alt: str = "#tblHomePitcher1"
    home_pitcher_alt2: str = "#tblHomePitcher2"

    sms_score: str = ".sms-score"
    score_board: str = ".score-board"

    stadium: str = "#txtStadium"
    crowd: str = "#txtCrowd"

    review_tab: str = "li[section='REVIEW']"
    hitter_fallback: str = "#tblAwayHitter1, #tblHomeHitter1, #tblAwayHitter3, #tblHomeHitter3"
    pitcher_fallback: str = "#tblAwayPitcher, #tblHomePitcher, #tblAwayPitcher1, #tblHomePitcher1"
    lineup_link: str = 'a[href*="Player/PlayerDetail"], a[href*="playerId="], a[href*="p_id="]'

    etc_table: str = "#tblEtc"

    @property
    def status_selectors(self) -> tuple[str, ...]:
        """Handles the status selectors operation.

        Returns:
            Tuple result.

        """
        return (
            self.status_on,
            self.status_typo,
            self.cancel_on,
            self.cancel_generic,
        )

    @property
    def boxscore_presence_selectors(self) -> tuple[str, ...]:
        """Handles the boxscore presence selectors operation.

        Returns:
            Tuple result.

        """
        return (
            self.away_hitter_primary,
            self.away_hitter_inning,
            self.away_hitter_extra,
            self.home_hitter_primary,
            self.home_hitter_inning,
            self.home_hitter_extra,
            self.away_pitcher_primary,
            self.away_pitcher_alt,
            self.away_pitcher_alt2,
            self.home_pitcher_primary,
            self.home_pitcher_alt,
            self.home_pitcher_alt2,
            self.sms_score,
            self.score_board,
        )


PLAYER_SEARCH = PlayerSearchSelectors()
FIELDING_STATS = FieldingStatsSelectors()
GAME_DETAIL = GameDetailSelectors()
