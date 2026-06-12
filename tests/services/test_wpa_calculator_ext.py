from __future__ import annotations

from unittest.mock import patch

import pytest

from src.services.wpa_calculator import WPACalculator


class TestLoadMatrix:
    def test_missing_file_logs_warning(self):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        assert calc._matrix == {}

    def test_loads_matrix_from_csv(self, tmp_path):
        csv_path = tmp_path / "win_expectancy.csv"
        csv_path.write_text("inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5000\n1,bottom,0,0,0,0.5200\n")
        calc = WPACalculator(matrix_path=str(csv_path))
        assert len(calc._matrix) == 2
        assert calc._matrix[(1, "top", 0, 0, 0)] == 0.5000
        assert calc._matrix[(1, "bottom", 0, 0, 0)] == 0.5200

    def test_handles_missing_columns_gracefully(self, tmp_path):
        csv_path = tmp_path / "bad.csv"
        csv_path.write_text("inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5000\n")
        calc = WPACalculator(matrix_path=str(csv_path))
        assert len(calc._matrix) == 1


class TestGetWinProbability:
    def test_direct_lookup(self):
        calc = WPACalculator()
        calc._matrix = {(1, "top", 0, 0, 0): 0.5000}
        prob = calc.get_win_probability(1, False, 0, 0, 0)
        assert prob == 0.5000

    def test_clamps_score_diff(self):
        calc = WPACalculator()
        calc._matrix = {(1, "top", 0, 0, 5): 0.9000}
        prob = calc.get_win_probability(1, False, 0, 0, 10)
        assert prob == 0.9000

    def test_clamps_inning(self):
        calc = WPACalculator()
        calc._matrix = {(9, "top", 0, 0, 0): 0.5000}
        prob = calc.get_win_probability(12, False, 0, 0, 0)
        assert prob == 0.5000

    def test_fallback_with_runners(self):
        calc = WPACalculator()
        calc._matrix = {(1, "top", 0, 0, 0): 0.5000}
        prob = calc.get_win_probability(1, False, 0, 1, 0)
        assert prob < 0.5000

    def test_fallback_to_formula(self):
        calc = WPACalculator()
        calc._matrix = {}
        prob = calc.get_win_probability(1, True, 0, 0, 0)
        assert 0.0 < prob < 1.0


class TestCalculateWpa:
    def test_away_team_batting(self):
        calc = WPACalculator()
        with patch.object(calc, "get_win_probability") as mock_wp:
            mock_wp.side_effect = [0.5, 0.7]
            wpa = calc.calculate_wpa(1, False, 0, 0, 0, 1, 1, 1)
            assert wpa == pytest.approx(-0.2, abs=1e-4)

    def test_home_team_batting(self):
        calc = WPACalculator()
        with patch.object(calc, "get_win_probability") as mock_wp:
            mock_wp.side_effect = [0.3, 0.6]
            wpa = calc.calculate_wpa(1, True, 0, 0, 0, 1, 3, 2)
            assert wpa == pytest.approx(0.3, abs=1e-4)

    def test_no_change(self):
        calc = WPACalculator()
        with patch.object(calc, "get_win_probability") as mock_wp:
            mock_wp.side_effect = [0.5, 0.5]
            wpa = calc.calculate_wpa(1, False, 0, 0, 0, 0, 0, 0)
            assert wpa == 0.0


class TestFallbackFormula:
    def test_home_win_in_bottom_9th(self):
        calc = WPACalculator()
        prob = calc._fallback_formula(9, True, 0, 0, 1)
        assert prob == 1.0

    def test_away_win_top_9th_3_outs(self):
        calc = WPACalculator()
        prob = calc._fallback_formula(9, False, 3, 0, 1)
        assert prob == 1.0

    def test_home_loses_bottom_9th_3_outs(self):
        calc = WPACalculator()
        prob = calc._fallback_formula(9, True, 3, 0, -1)
        assert prob == 0.0

    def test_early_game_formula(self):
        calc = WPACalculator()
        prob = calc._fallback_formula(1, False, 0, 0, 0)
        assert 0.0 < prob < 1.0

    def test_overflow_handling(self):
        calc = WPACalculator()
        prob = calc._fallback_formula(1, False, 0, 0, 100)
        assert prob >= 0.0
