from __future__ import annotations

from unittest.mock import MagicMock

from src.services.pbp_sh_sf_derivation import (
    apply_sh_sf_to_batting_stats,
    count_sh_sf_from_events,
    derive_sh_sf_for_game,
)


class TestCountShSfFromEvents:
    def test_empty_rows_returns_empty(self) -> None:
        assert count_sh_sf_from_events([], {}) == {}

    def test_single_sacrifice_bunt(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None)
        result = count_sh_sf_from_events([row], {})
        assert result == {1: {"sh": 1, "sf": 0}}

    def test_single_sacrifice_fly(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="희생플라이", outs=0)
        result = count_sh_sf_from_events([row], {})
        assert result == {1: {"sh": 0, "sf": 1}}

    def test_sf_with_two_outs_not_counted(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="희생플라이", outs=2)
        result = count_sh_sf_from_events([row], {})
        assert result == {}

    def test_sf_with_three_outs_not_counted(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="희생플라이", outs=3)
        result = count_sh_sf_from_events([row], {})
        assert result == {}

    def test_both_sh_and_sf_same_player(self) -> None:
        rows = [
            MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None),
            MagicMock(batter_id=1, batter_name="Kim", description="희생플라이", outs=1),
        ]
        result = count_sh_sf_from_events(rows, {})
        assert result == {1: {"sh": 1, "sf": 1}}

    def test_multiple_players(self) -> None:
        rows = [
            MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None),
            MagicMock(batter_id=2, batter_name="Lee", description="희생플라이", outs=0),
        ]
        result = count_sh_sf_from_events(rows, {})
        assert result == {1: {"sh": 1, "sf": 0}, 2: {"sh": 0, "sf": 1}}

    def test_falls_back_to_name_when_no_batter_id(self) -> None:
        row = MagicMock(batter_id=None, batter_name="Alice", description="희생번트", outs=None)
        result = count_sh_sf_from_events([row], {})
        assert "Alice" in result
        assert result["Alice"]["sh"] == 1

    def test_uses_name_to_id_map(self) -> None:
        row = MagicMock(batter_id=None, batter_name="Bob", description="희생번트", outs=None)
        result = count_sh_sf_from_events([row], {"Bob": 42})
        assert 42 in result
        assert result[42]["sh"] == 1

    def test_skips_non_sacrifice_events(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="안타", outs=None)
        result = count_sh_sf_from_events([row], {})
        assert result == {}

    def test_skips_event_with_no_batter_info(self) -> None:
        row = MagicMock(batter_id=None, batter_name=None, description="희생번트", outs=None)
        result = count_sh_sf_from_events([row], {})
        assert result == {}

    def test_accumulates_multiple_sh(self) -> None:
        rows = [
            MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None),
            MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None),
            MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None),
        ]
        result = count_sh_sf_from_events(rows, {})
        assert result[1]["sh"] == 3

    def test_sf_with_none_outs_counted(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="희생플라이", outs=None)
        result = count_sh_sf_from_events([row], {})
        assert result[1]["sf"] == 1

    def test_empty_description_not_counted(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description="", outs=None)
        result = count_sh_sf_from_events([row], {})
        assert result == {}

    def test_none_description_not_counted(self) -> None:
        row = MagicMock(batter_id=1, batter_name="Kim", description=None, outs=None)
        result = count_sh_sf_from_events([row], {})
        assert result == {}


class TestDeriveShSfForGame:
    def test_no_events_returns_empty(self):
        session = MagicMock()
        session.execute.return_value.all.side_effect = [[], []]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert result == {}

    def test_finds_sacrifice_bunt(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생번트", outs=None)]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert 1 in result
        assert result[1]["sh"] == 1

    def test_finds_sacrifice_fly(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생플라이", outs=0)]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        result = derive_sh_sf_for_game(session, "20240501LGSS0")
        assert result[1]["sf"] == 1

    def test_counts_both_sh_and_sf(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생번트 and 희생플라이", outs=0)]
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

    def test_updates_batting_stats_by_player_id(self):
        session = MagicMock()
        stats_rows = [MagicMock(player_id=1, player_name="Kim")]
        event_rows = [MagicMock(batter_id=1, batter_name="Kim", description="희생번트")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        session.execute.return_value.rowcount = 1
        result = apply_sh_sf_to_batting_stats(session, "20240501LGSS0")
        assert result > 0

    def test_updates_batting_stats_by_name(self):
        session = MagicMock()
        stats_rows = []
        event_rows = [MagicMock(batter_id=None, batter_name="Alice", description="희생번트")]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        session.execute.return_value.rowcount = 1
        result = apply_sh_sf_to_batting_stats(session, "20240501LGSS0")
        assert result > 0

    def test_commit_not_called_here(self):
        session = MagicMock()
        session.execute.return_value.all.side_effect = [[], []]
        apply_sh_sf_to_batting_stats(session, "20240501LGSS0")
        session.commit.assert_not_called()

    def test_handles_multiple_players(self):
        session = MagicMock()
        stats_rows = [
            MagicMock(player_id=1, player_name="Kim"),
            MagicMock(player_id=2, player_name="Lee"),
        ]
        event_rows = [
            MagicMock(batter_id=1, batter_name="Kim", description="희생번트"),
            MagicMock(batter_id=2, player_name="Lee", description="희생플라이", outs=0),
        ]
        session.execute.return_value.all.side_effect = [stats_rows, event_rows]
        session.execute.return_value.rowcount = 1
        result = apply_sh_sf_to_batting_stats(session, "20240501LGSS0")
        assert result == 2
