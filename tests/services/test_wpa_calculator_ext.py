from __future__ import annotations

import math
from unittest.mock import patch

import pytest

from src.constants import MAX_INNINGS, MAX_OUTS
from src.services.wpa_calculator import WpaInput, WPACalculator


class TestWPACalculatorInit:
    def test_default_matrix_path(self):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        assert calc._matrix == {}

    def test_loads_matrix(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        assert len(calc._matrix) == 1
        assert calc._matrix[(1, "top", 0, 0, 0)] == 0.5


class TestGetWinProbability:
    def test_direct_lookup(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(1, is_bottom=False, outs=0, runners=0, score_diff=0)
        assert result == 0.5

    def test_score_diff_clamped_high(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,5,0.9\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(1, is_bottom=False, outs=0, runners=0, score_diff=10)
        assert result == 0.9

    def test_score_diff_clamped_low(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,-5,0.1\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(1, is_bottom=False, outs=0, runners=0, score_diff=-10)
        assert result == 0.1

    def test_inning_clamped(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n9,top,0,0,0,0.3\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(15, is_bottom=False, outs=0, runners=0, score_diff=0)
        assert result == 0.3

    def test_fallback_empty_bases(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(1, is_bottom=False, outs=0, runners=3, score_diff=0)
        assert 0.0 <= result <= 1.0

    def test_fallback_bottom_bonus(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,bottom,0,0,0,0.5\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(1, is_bottom=True, outs=0, runners=3, score_diff=0)
        assert result > 0.5

    def test_fallback_top_penalty(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        result = calc.get_win_probability(1, is_bottom=False, outs=0, runners=3, score_diff=0)
        assert result < 0.5

    def test_fallback_formula_end_game_bottom_win(self, tmp_path):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        result = calc._fallback_formula(MAX_INNINGS, is_bottom=True, outs=0, runners=0, score_diff=2)
        assert result == 1.0

    def test_fallback_formula_end_game_top_win_outs(self, tmp_path):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        result = calc._fallback_formula(MAX_INNINGS, is_bottom=False, outs=MAX_OUTS, runners=0, score_diff=2)
        assert result == 1.0

    def test_fallback_formula_end_game_bottom_loss(self, tmp_path):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        result = calc._fallback_formula(MAX_INNINGS, is_bottom=True, outs=MAX_OUTS, runners=0, score_diff=-2)
        assert result == 0.0

    def test_fallback_formula_mid_game(self, tmp_path):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        result = calc._fallback_formula(5, is_bottom=False, outs=1, runners=2, score_diff=0)
        assert 0.0 <= result <= 1.0

    def test_fallback_formula_handles_overflow(self, tmp_path):
        calc = WPACalculator(matrix_path="/nonexistent/path.csv")
        result = calc._fallback_formula(1, is_bottom=True, outs=0, runners=0, score_diff=1000)
        assert result == 1.0


class TestCalculateWpa:
    def test_bottom_batter_positive_wpa(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,bottom,0,0,0,0.5\n1,bottom,0,1,0,0.6\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        data = WpaInput(
            inning=1,
            is_bottom=True,
            outs_before=0,
            runners_before=0,
            score_diff_before=0,
            outs_after=0,
            runners_after=1,
            score_diff_after=0,
        )
        result = calc.calculate_wpa(data=data)
        assert result == 0.1

    def test_top_batter_negative_wpa(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5\n1,top,1,0,0,0.3\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        data = WpaInput(
            inning=1,
            is_bottom=False,
            outs_before=0,
            runners_before=0,
            score_diff_before=0,
            outs_after=1,
            runners_after=0,
            score_diff_after=0,
        )
        result = calc.calculate_wpa(data=data)
        assert result == pytest.approx(0.2)

    def test_no_change(self, tmp_path):
        csv_content = "inning,half,outs,runners,score_diff,win_prob\n1,top,0,0,0,0.5\n"
        p = tmp_path / "matrix.csv"
        p.write_text(csv_content)
        calc = WPACalculator(matrix_path=str(p))
        data = WpaInput(
            inning=1,
            is_bottom=False,
            outs_before=0,
            runners_before=0,
            score_diff_before=0,
            outs_after=0,
            runners_after=0,
            score_diff_after=0,
        )
        result = calc.calculate_wpa(data=data)
        assert result == 0.0


class TestWpaInputDataclass:
    def test_fields(self):
        data = WpaInput(
            inning=5,
            is_bottom=True,
            outs_before=2,
            runners_before=3,
            score_diff_before=1,
            outs_after=0,
            runners_after=5,
            score_diff_after=2,
        )
        assert data.inning == 5
        assert data.is_bottom is True
        assert data.outs_before == 2
