from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from src.cli.calculate_rankings import (
    _KBO_FULL_SEASON_GAMES,
    _MIN_IP_FLOOR,
    _MIN_IP_PER_GAME,
    _MIN_PA_FLOOR,
    _MIN_PA_PER_GAME,
    _compute_min_ip_outs,
    _compute_min_pa,
    _dictify_rows,
    _games_played_in_season,
    main,
    rebuild_rankings,
)


class TestDictifyRows:
    def _make_row(self, player_id: int, **attrs):
        class Row:
            pass

        row = Row()
        row.__dict__ = {"player_id": player_id, **attrs}
        row.player_id = player_id
        return row

    def test_converts_rows_to_dicts(self):
        row1 = self._make_row(1, hits=10, _sa_instance_state="remove")
        row2 = self._make_row(2, hits=5)

        result = _dictify_rows([row1, row2], {1: "Kim", 2: "Lee"})
        assert len(result) == 2
        assert result[0]["player_name"] == "Kim"
        assert result[0]["hits"] == 10
        assert "_sa_instance_state" not in result[0]
        assert result[1]["player_name"] == "Lee"

    def test_missing_label_uses_pid(self):
        row = self._make_row(99, hits=3)
        result = _dictify_rows([row], {})
        assert result[0]["player_name"] == "99"

    def test_datetime_to_iso(self):
        dt = datetime(2025, 6, 15, 14, 30)
        row = self._make_row(1, created=dt)
        result = _dictify_rows([row], {})
        assert result[0]["created"] == "2025-06-15T14:30:00"

    def test_date_to_iso(self):
        d = date(2025, 6, 15)
        row = self._make_row(1, game_date=d)
        result = _dictify_rows([row], {})
        assert result[0]["game_date"] == "2025-06-15"

    def test_empty_rows(self):
        result = _dictify_rows([], {})
        assert result == []


class TestGamesPlayedInSeason:
    def test_returns_count(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: 50
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _games_played_in_season(mock_session, 2025)
        assert result == 50

    def test_returns_zero_when_none(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchone.return_value = None

        result = _games_played_in_season(mock_session, 2025)
        assert result == 0

    def test_returns_zero_when_count_is_none(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: None
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _games_played_in_season(mock_session, 2025)
        assert result == 0


class TestComputeMinPa:
    def test_full_season_returns_fixed(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: _KBO_FULL_SEASON_GAMES
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _compute_min_pa(mock_session, 2025)
        assert result == int(_KBO_FULL_SEASON_GAMES * _MIN_PA_PER_GAME)

    def test_partial_season_uses_dynamic(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: 72
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _compute_min_pa(mock_session, 2025)
        assert result == int(72 * _MIN_PA_PER_GAME)

    def test_partial_season_respects_floor(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: 5
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _compute_min_pa(mock_session, 2025)
        assert result == _MIN_PA_FLOOR

    def test_zero_games_uses_floor(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchone.return_value = None

        result = _compute_min_pa(mock_session, 2025)
        assert result == _MIN_PA_FLOOR


class TestComputeMinIpOuts:
    def test_full_season_returns_fixed(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: _KBO_FULL_SEASON_GAMES
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _compute_min_ip_outs(mock_session, 2025)
        assert result == int(_KBO_FULL_SEASON_GAMES * _MIN_IP_PER_GAME)

    def test_partial_season_uses_dynamic(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: 72
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _compute_min_ip_outs(mock_session, 2025)
        assert result == int(72 * _MIN_IP_PER_GAME)

    def test_partial_season_respects_floor(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: 10
        mock_session.execute.return_value.fetchone.return_value = mock_row

        result = _compute_min_ip_outs(mock_session, 2025)
        assert result == _MIN_IP_FLOOR

    def test_zero_games_uses_floor(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchone.return_value = None

        result = _compute_min_ip_outs(mock_session, 2025)
        assert result == _MIN_IP_FLOOR


class TestRebuildRankings:
    def test_returns_zero_when_no_rankings(self):
        with (
            patch("src.cli.calculate_rankings.SessionLocal") as mock_session_factory,
            patch("src.cli.calculate_rankings.RankingAggregator") as mock_agg_cls,
        ):
            mock_session = MagicMock()
            mock_session_factory.return_value.__enter__ = lambda s: mock_session
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_agg = mock_agg_cls.return_value
            mock_agg.generate_rankings.return_value = []

            result = rebuild_rankings(2025)
            assert result == 0

    def test_returns_count_when_rankings_generated(self):
        with (
            patch("src.cli.calculate_rankings.SessionLocal") as mock_session_factory,
            patch("src.cli.calculate_rankings.RankingAggregator") as mock_agg_cls,
        ):
            mock_session = MagicMock()
            mock_session_factory.return_value.__enter__ = lambda s: mock_session
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_agg = mock_agg_cls.return_value
            mock_agg.generate_rankings.return_value = [{"stat": "avg"}, {"stat": "hr"}]

            result = rebuild_rankings(2025)
            assert result == 2

    def test_deletes_existing_rankings(self):
        with (
            patch("src.cli.calculate_rankings.SessionLocal") as mock_session_factory,
            patch("src.cli.calculate_rankings.RankingAggregator") as mock_agg_cls,
        ):
            mock_session = MagicMock()
            mock_session_factory.return_value.__enter__ = lambda s: mock_session
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_agg_cls.return_value.generate_rankings.return_value = []

            rebuild_rankings(2025)
            mock_session.query.return_value.filter.return_value.delete.assert_called_once()

    def test_skips_player_basic_query_when_no_players(self):
        with (
            patch("src.cli.calculate_rankings.SessionLocal") as mock_session_factory,
            patch("src.cli.calculate_rankings.RankingAggregator") as mock_agg_cls,
        ):
            mock_session = MagicMock()
            mock_session_factory.return_value.__enter__ = lambda s: mock_session
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_agg_cls.return_value.generate_rankings.return_value = []

            rebuild_rankings(2025)
            calls = mock_session.query.call_args_list
            player_basic_calls = [c for c in calls if "PlayerBasic" in str(c)]
            assert len(player_basic_calls) == 0


class TestCalculateRankingsCLI:
    def test_required_year(self):
        with patch("src.cli.calculate_rankings.rebuild_rankings") as mock:
            mock.return_value = 0
            result = main(["--year", "2025"])
            assert result == 0
            mock.assert_called_once_with(2025)

    def test_no_year_errors(self):
        import argparse

        try:
            main([])
            raise AssertionError("Should have raised SystemExit")
        except (argparse.ArgumentError, SystemExit):
            pass
