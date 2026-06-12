from __future__ import annotations

from unittest.mock import MagicMock

from src.services.split_calculator import SituationalSplitCalculator


class TestSituationalSplitCalculator:
    def _make_session(self):
        sess = MagicMock()
        sess.__enter__.return_value = sess
        return sess

    def test_get_risp_stats_no_player(self):
        mock_session = self._make_session()
        mock_session.execute.return_value.fetchone.return_value = None
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_risp_stats(999, 2025)
        assert result["risp_avg"] is None
        assert result["risp_ab"] == 0

    def test_get_risp_stats_with_data(self):
        mock_session = self._make_session()
        mock_name_row = MagicMock()
        mock_name_row.name = "Kim"
        mock_data_row = MagicMock()
        mock_data_row.risp_ab = 20
        mock_data_row.risp_hits = 5
        mock_data_row.risp_on_base = 8
        mock_session.execute.return_value.fetchone.side_effect = [mock_name_row, mock_data_row]
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_risp_stats(1, 2025)
        assert result["risp_avg"] == 0.25
        assert result["risp_ab"] == 20
        assert result["risp_hits"] == 5

    def test_get_risp_stats_zero_ab(self):
        mock_session = self._make_session()
        mock_name_row = MagicMock()
        mock_name_row.name = "Kim"
        mock_data_row = MagicMock()
        mock_data_row.risp_ab = 0
        mock_data_row.risp_hits = 0
        mock_data_row.risp_on_base = 0
        mock_session.execute.return_value.fetchone.side_effect = [mock_name_row, mock_data_row]
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_risp_stats(1, 2025)
        assert result["risp_avg"] is None

    def test_get_lr_splits(self):
        mock_session = self._make_session()
        mock_name_row = MagicMock()
        mock_name_row.name = "Kim"
        mock_lhp_row = MagicMock()
        mock_lhp_row.throws = "L"
        mock_lhp_row.ab = 30
        mock_lhp_row.hits = 9
        mock_lhp_row.on_base_events = 5
        mock_lhp_row.obp_events = 14
        mock_rhp_row = MagicMock()
        mock_rhp_row.throws = "R"
        mock_rhp_row.ab = 100
        mock_rhp_row.hits = 25
        mock_rhp_row.on_base_events = 10
        mock_rhp_row.obp_events = 35
        mock_session.execute.return_value.fetchone.side_effect = [mock_name_row]
        mock_session.execute.return_value.fetchall.return_value = [mock_lhp_row, mock_rhp_row]
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_lr_splits(1, 2025)
        assert result["vs_lhp"]["avg"] == 0.3
        assert result["vs_lhp"]["ab"] == 30
        assert result["vs_rhp"]["avg"] == 0.25
        assert result["vs_rhp"]["ab"] == 100

    def test_get_lr_splits_no_player(self):
        mock_session = self._make_session()
        mock_session.execute.return_value.fetchone.return_value = None
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_lr_splits(999, 2025)
        assert result == {"vs_lhp": {}, "vs_rhp": {}}

    def test_get_two_out_stats(self):
        mock_session = self._make_session()
        mock_name_row = MagicMock()
        mock_name_row.name = "Kim"
        mock_data_row = MagicMock()
        mock_data_row.ab = 40
        mock_data_row.hits = 12
        mock_data_row.rbi = 8
        mock_session.execute.return_value.fetchone.side_effect = [mock_name_row, mock_data_row]
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_two_out_stats(1, 2025)
        assert result["two_out_avg"] == 0.3
        assert result["two_out_ab"] == 40
        assert result["two_out_rbi"] == 8

    def test_get_two_out_stats_no_player(self):
        mock_session = self._make_session()
        mock_session.execute.return_value.fetchone.return_value = None
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_two_out_stats(999, 2025)
        assert result["two_out_avg"] is None
        assert result["two_out_ab"] == 0

    def test_get_two_out_stats_zero_ab(self):
        mock_session = self._make_session()
        mock_name_row = MagicMock()
        mock_name_row.name = "Kim"
        mock_data_row = MagicMock()
        mock_data_row.ab = 0
        mock_data_row.hits = 0
        mock_data_row.rbi = 0
        mock_session.execute.return_value.fetchone.side_effect = [mock_name_row, mock_data_row]
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_two_out_stats(1, 2025)
        assert result["two_out_avg"] is None

    def test_get_full_splits(self):
        mock_session = self._make_session()
        mock_name_row = MagicMock()
        mock_name_row.name = "Kim"
        mock_risp_data = MagicMock()
        mock_risp_data.risp_ab = 20
        mock_risp_data.risp_hits = 5
        mock_risp_data.risp_on_base = 8
        mock_two_out_data = MagicMock()
        mock_two_out_data.ab = 40
        mock_two_out_data.hits = 12
        mock_two_out_data.rbi = 8
        mock_session.execute.return_value.fetchone.side_effect = [
            mock_name_row,  # get_risp_stats → _resolve_name
            mock_risp_data,  # get_risp_stats → query
            mock_name_row,  # get_lr_splits → _resolve_name
            mock_name_row,  # get_two_out_stats → _resolve_name
            mock_two_out_data,  # get_two_out_stats → query
        ]
        mock_session.execute.return_value.fetchall.return_value = []
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc.get_full_splits(1, 2025)
        assert result["player_id"] == 1
        assert result["season"] == 2025
        assert "risp" in result
        assert "lr_splits" in result
        assert "two_out" in result

    def test_resolve_name_returns_name(self):
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.name = "Park"
        mock_session.execute.return_value.fetchone.return_value = mock_row
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc._resolve_name(1, mock_session)
        assert result == "Park"

    def test_resolve_name_nonexistent(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchone.return_value = None
        calc = SituationalSplitCalculator(session=mock_session)
        result = calc._resolve_name(999, mock_session)
        assert result is None
