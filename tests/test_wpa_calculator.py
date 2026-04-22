import pytest
import os
from src.services.wpa_calculator import WPACalculator

def test_wpa_calculator_initialization():
    # Verify it can be initialized with default matrix path
    calc = WPACalculator()
    assert calc is not None
    # If matrix was loaded, it should have entries
    # The real matrix is known to exist at src/data/win_expectancy.csv
    # if it didn't load, it prints a warning but doesn't crash

def test_get_win_probability_fallbacks():
    calc = WPACalculator()
    
    # Test clamped diff and inning (clamped to 1-9, diff to -5 to 5)
    # Bottom 9, 2 outs, score diff +1 should be very high prob for home team (1.0)
    # But get_win_probability uses score_diff Home - Away
    prob = calc.get_win_probability(inning=9, is_bottom=True, outs=2, runners=0, score_diff=1)
    assert prob >= 0.5
    
    # Test extreme case (End Game)
    # Inning 9, Top, Away team winning (score_diff -1), 3 outs -> Home loses
    prob_end = calc.get_win_probability(inning=9, is_bottom=False, outs=3, runners=0, score_diff=1)
    # If it's Top 9, score_diff +1 for home, and 3 outs -> Home wins 1.0 (End of top 9, home will bat bottom)
    # Actually if 3 outs Top 9, it's end of half, should transition to bottom 9.
    # The formula says if inning >= 9 and not is_bottom and score_diff > 0 and outs == 3 -> 1.0
    assert prob_end == 1.0

def test_calculate_wpa_perspective():
    calc = WPACalculator()
    
    # 9th Inning, Bottom half (Home team batting), score tied 0-0.
    # Home team hits a walk-off HR (score_diff goes from 0 to 1)
    # WPA for home team should be significantly positive
    wpa = calc.calculate_wpa(
        inning=9, is_bottom=True,
        outs_before=2, runners_before=0, score_diff_before=0,
        outs_after=2, runners_after=0, score_diff_after=1
    )
    
    assert wpa > 0
    # A walk-off is roughly 0.5 to 1.0 WPA depending on base state

def test_fallback_formula_logic():
    calc = WPACalculator()
    # Test internal fallback formula if matrix lookup fails
    # Inning 1, Top, 0-0
    prob = calc._fallback_formula(inning=1, is_bottom=False, outs=0, runners=0, score_diff=0)
    # Should be around 0.5
    assert 0.4 <= prob <= 0.6
