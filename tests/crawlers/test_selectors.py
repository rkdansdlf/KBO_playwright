from __future__ import annotations

from src.crawlers.selectors import (
    FIELDING_STATS,
    GAME_DETAIL,
    PLAYER_SEARCH,
    FieldingStatsSelectors,
    GameDetailSelectors,
    PlayerSearchSelectors,
)


class TestPlayerSearchSelectors:
    def test_instance_exists(self):
        assert isinstance(PLAYER_SEARCH, PlayerSearchSelectors)

    def test_input_selector(self):
        assert "txtSearchPlayerName" in PLAYER_SEARCH.input

    def test_table_rows_selector(self):
        assert "table.tEx" in PLAYER_SEARCH.table_rows


class TestFieldingStatsSelectors:
    def test_instance_exists(self):
        assert isinstance(FIELDING_STATS, FieldingStatsSelectors)

    def test_season_dropdown(self):
        assert "ddlSeason" in FIELDING_STATS.season_dropdown

    def test_data_table(self):
        assert "table.tData01" in FIELDING_STATS.data_table


class TestGameDetailSelectors:
    def test_instance_exists(self):
        assert isinstance(GAME_DETAIL, GameDetailSelectors)

    def test_status_selectors(self):
        selectors = GAME_DETAIL.status_selectors
        assert "li.game-cont.on p.status" in selectors
        assert len(selectors) == 4

    def test_boxscore_presence_selectors(self):
        selectors = GAME_DETAIL.boxscore_presence_selectors
        assert "#tblAwayHitter1" in selectors
        assert "#tblHomePitcher" in selectors

    def test_primary_selectors(self):
        assert "tblAwayHitter1" in GAME_DETAIL.away_hitter_primary
        assert "tblHomePitcher" in GAME_DETAIL.home_pitcher_primary
