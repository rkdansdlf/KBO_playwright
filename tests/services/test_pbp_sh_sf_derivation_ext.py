from __future__ import annotations

from unittest.mock import MagicMock

from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats, derive_sh_sf_for_game


class TestDeriveShSfForGame:
    def test_no_events_returns_empty(self):
        session = MagicMock()
        session.execute.return_value.all.side_effect = [[], []]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert result == {}

    def test_finds_sacrifice_bunt(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생번트")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert 1 in result
        assert result[1]["sh"] == 1

    def test_finds_sacrifice_fly(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생플라이")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert result[1]["sf"] == 1

    def test_counts_both_sh_and_sf(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생번트 and 희생플라이")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert result[1]["sh"] == 1
        assert result[1]["sf"] == 1

    def test_falls_back_to_name_matching(self):
        session = MagicMock()
        stats_rows = []
        event_rows = [MagicMock(batter_id=None, batter_name="Alice", description="희생번트")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert "Alice" in result
        assert result["Alice"]["sh"] == 1

    def test_skips_event_with_no_batter_info(self):
        session = MagicMock()
        stats_rows = []
        event_rows = [MagicMock(batter_id=None, batter_name=None, description="희생번트")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert result == {}


class TestApplyShSfToBattingStats:
    def test_no_derived_data_returns_0(self):
        session = MagicMock()
        session.execute.return_value.all.side_effect = [[], []]
        assert apply_sh_sf_to_batting_stats(session, "20240501LGSS0") == 0

    def test_updates_batting_stats(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생번트")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        session.execute.return_value.rowcount = 1
        result = apply_sh_sf_to_batting_stats(session, "20240501LGSS0")
        assert result > 0

    def test_commit_not_called_here(self):
        session = MagicMock()
        session.execute.return_value.all.side_effect = [[], []]
        apply_sh_sf_to_batting_stats(session, "20240501LGSS0")
        session.commit.assert_not_called()
