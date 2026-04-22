import pytest
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

def test_batting_stat_calculator_standard():
    # Sample data for a typical hitter
    data = {
        'at_bats': 100,
        'hits': 30,
        'walks': 10,
        'hbp': 2,
        'sacrifice_flies': 3,
        'sacrifice_hits': 2,
        'doubles': 5,
        'triples': 1,
        'home_runs': 4,
        'strikeouts': 20,
        'intentional_walks': 1,
        'stolen_bases': 5,
        'caught_stealing': 2,
        'gdp': 3
    }
    
    ratios = BattingStatCalculator.calculate_ratios(data)
    
    # AVG = 30 / 100 = 0.300
    assert ratios['avg'] == 0.300
    
    # OBP = (30 + 10 + 2) / (100 + 10 + 2 + 3) = 42 / 115 = 0.3652... -> 0.365
    assert ratios['obp'] == 0.365
    
    # SLG: TB = (30-5-1-4)*1 + 5*2 + 1*3 + 4*4 = 20 + 10 + 3 + 16 = 49
    # SLG = 49 / 100 = 0.490
    assert ratios['slg'] == 0.490
    
    # OPS = 0.365 + 0.490 = 0.855
    assert ratios['ops'] == 0.855
    
    # ISO = 0.490 - 0.300 = 0.190
    assert ratios['iso'] == 0.190
    
    # BABIP = (30 - 4) / (100 - 20 - 4 + 3) = 26 / 79 = 0.3291... -> 0.329
    assert ratios['babip'] == 0.329

def test_batting_stat_calculator_zero_ab():
    data = {'at_bats': 0, 'hits': 0}
    ratios = BattingStatCalculator.calculate_ratios(data)
    assert ratios['avg'] == 0.0
    assert ratios['obp'] == 0.0
    assert ratios['slg'] == 0.0
    assert ratios['ops'] == 0.0

def test_pitching_stat_calculator_standard():
    # Sample data for a pitcher: 6 innings (18 outs), 2 ER, 5 H, 2 BB, 7 K, 1 HR, 0 HBP
    data = {
        'innings_outs': 18,
        'earned_runs': 2,
        'hits_allowed': 5,
        'walks_allowed': 2,
        'strikeouts': 7,
        'home_runs_allowed': 1,
        'hit_batters': 0
    }
    
    # Using default FIP constant 3.10
    ratios = PitchingStatCalculator.calculate_ratios(data)
    
    # IP = 18 / 3 = 6.0
    # ERA = (2 / 6) * 9 = 3.00
    assert ratios['era'] == 3.00
    
    # WHIP = (2 + 5) / 6 = 1.166... -> 1.17
    assert ratios['whip'] == 1.17
    
    # K/9 = (7 / 6) * 9 = 10.50
    assert ratios['k_per_nine'] == 10.50
    
    # BB/9 = (2 / 6) * 9 = 3.00
    assert ratios['bb_per_nine'] == 3.00
    
    # K/BB = 7 / 2 = 3.50
    assert ratios['kbb'] == 3.50
    
    # FIP = ((13*1) + (3*(2+0)) - (2*7)) / 6 + 3.10
    # FIP = (13 + 6 - 14) / 6 + 3.10 = 5 / 6 + 3.10 = 0.833... + 3.10 = 3.933... -> 3.93
    assert ratios['fip'] == 3.93

def test_pitching_stat_calculator_custom_fip():
    data = {
        'innings_outs': 27, # 9 IP
        'earned_runs': 0,
        'hits_allowed': 0,
        'walks_allowed': 0,
        'strikeouts': 9,
        'home_runs_allowed': 0,
        'hit_batters': 0
    }
    # FIP = (0 + 0 - 2*9) / 9 + 4.5 = -18/9 + 4.5 = -2 + 4.5 = 2.50
    ratios = PitchingStatCalculator.calculate_ratios(data, fip_constant=4.50)
    assert ratios['fip'] == 2.50

def test_pitching_stat_calculator_zero_ip():
    data = {'innings_outs': 0}
    ratios = PitchingStatCalculator.calculate_ratios(data)
    assert ratios['era'] == 0.0
    assert ratios['whip'] == 0.0
    assert ratios['fip'] == 0.0
