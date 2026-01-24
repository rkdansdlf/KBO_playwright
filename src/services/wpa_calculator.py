"""
WPA (Win Probability Added) Calculator Service.
Uses a Win Expectancy Matrix (CSV) to calculate the shift in win probability for each event.
"""

import os
import csv
from typing import Dict, Tuple, Optional

class WPACalculator:
    def __init__(self, matrix_path: str = None):
        """
        Initialize calculator with Win Expectancy Matrix from CSV.
        """
        if matrix_path is None:
            # Default path relative to project root
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            matrix_path = os.path.join(base_dir, 'src', 'data', 'win_expectancy.csv')
        
        self._matrix: Dict[Tuple, float] = {}
        self._load_matrix(matrix_path)
    
    def _load_matrix(self, path: str):
        """
        Load Win Expectancy Matrix from CSV.
        Expected columns: inning, half, outs, runners, score_diff, win_prob
        """
        if not os.path.exists(path):
            print(f"⚠️ Win Expectancy Matrix not found at {path}. Using fallback formula.")
            return
            
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    int(row['inning']),
                    row['half'],  # 'top' or 'bottom'
                    int(row['outs']),
                    int(row['runners']),  # Bitmask 0-7
                    int(row['score_diff'])  # Home - Away
                )
                self._matrix[key] = float(row['win_prob'])
        
        print(f"✅ Loaded {len(self._matrix)} Win Expectancy entries from {path}")

    def calculate_wpa(
        self,
        inning: int,
        is_bottom: bool,
        outs_before: int,
        runners_before: int,
        score_diff_before: int,
        outs_after: int,
        runners_after: int,
        score_diff_after: int,
    ) -> float:
        """
        Calculates WPA = WinProb(After) - WinProb(Before).
        Returns WPA from the perspective of the Batting Team.
        """
        # 1. Get WE Before (Home Perspective)
        we_before = self.get_win_probability(inning, is_bottom, outs_before, runners_before, score_diff_before)
        
        # 2. Get WE After (Home Perspective)
        we_after = self.get_win_probability(inning, is_bottom, outs_after, runners_after, score_diff_after)
        
        # 3. Calculate WPA (Batting Team Perspective)
        if is_bottom:  # Home Team batting
            wpa = we_after - we_before
        else:  # Away Team batting
            wpa = we_before - we_after
            
        return round(wpa, 4)

    def get_win_probability(
        self,
        inning: int,
        is_bottom: bool,
        outs: int,
        runners: int,
        score_diff: int
    ) -> float:
        """
        Returns probability (0.0 to 1.0) that HOME team wins.
        Uses Matrix lookup with fallback interpolation for missing keys.
        """
        half = 'bottom' if is_bottom else 'top'
        
        # Clamp score_diff to matrix range [-5, +5]
        clamped_diff = max(-5, min(5, score_diff))
        
        # Clamp inning to 1-9 (use 9 for extras)
        clamped_inning = max(1, min(9, inning))
        
        # Direct Lookup
        key = (clamped_inning, half, outs, runners, clamped_diff)
        if key in self._matrix:
            return self._matrix[key]
        
        # Fallback: Try with runners=0 (Empty bases)
        fallback_key = (clamped_inning, half, outs, 0, clamped_diff)
        if fallback_key in self._matrix:
            # Adjust slightly for runners (rough heuristic)
            base_prob = self._matrix[fallback_key]
            # Runners on base favor batting team slightly
            runner_bonus = 0.02 * bin(runners).count('1')  # +2% per runner
            if is_bottom:
                return min(1.0, base_prob + runner_bonus)
            else:
                return max(0.0, base_prob - runner_bonus)
        
        # Ultimate Fallback: Use logistic formula (legacy)
        return self._fallback_formula(clamped_inning, is_bottom, outs, runners, score_diff)
    
    def _fallback_formula(self, inning: int, is_bottom: bool, outs: int, runners: int, score_diff: int) -> float:
        """
        Legacy logistic formula for edge cases not covered by matrix.
        """
        import math
        
        # End Game Conditions
        if inning >= 9 and is_bottom and score_diff > 0:
            return 1.0
        if inning >= 9 and not is_bottom and score_diff > 0 and outs == 3:
            return 1.0
        if inning >= 9 and is_bottom and score_diff < 0 and outs == 3:
            return 0.0

        # Run Expectancy Table (simplified)
        re_table = {
            (0, 0): 0.48, (0, 1): 0.85, (0, 2): 1.10, (0, 3): 1.43,
            (0, 4): 1.35, (0, 5): 1.75, (0, 6): 1.96, (0, 7): 2.30,
            (1, 0): 0.25, (1, 1): 0.50, (1, 2): 0.66, (1, 3): 0.88,
            (1, 4): 0.95, (1, 5): 1.15, (1, 6): 1.37, (1, 7): 1.54,
            (2, 0): 0.10, (2, 1): 0.21, (2, 2): 0.31, (2, 3): 0.42,
            (2, 4): 0.36, (2, 5): 0.48, (2, 6): 0.58, (2, 7): 0.75 
        }
        
        expected_runs = re_table.get((outs, runners), 0.0)
        
        if is_bottom:
            projected_diff = score_diff + expected_runs
        else:
            projected_diff = score_diff - expected_runs

        innings_left = max(0.5, 9 - inning + (0.5 if not is_bottom else 0))
        c = 0.75
        
        try:
            exponent = -(c * projected_diff) / math.sqrt(innings_left)
            win_prob = 1 / (1 + math.exp(exponent))
        except OverflowError:
            win_prob = 1.0 if projected_diff > 0 else 0.0

        return round(win_prob, 4)
