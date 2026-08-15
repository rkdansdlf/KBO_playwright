"""Marcel-style Player Stat Projection Engine for KBO Pipeline.

Applies Tom Tango's Marcel forecasting model:
1. 3-Year Weighted Moving Average (5/4/3 weights)
2. Regression to League Mean (1200 PA for hitters, 450 IP for pitchers)
3. Aging Curve adjustment (peak age 29)
4. Park Factor adjustment
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_PEAK_AGE = 29
_AGING_GROWTH_RATE = 0.006
_AGING_DECLINE_RATE = 0.003
_HITTER_REGRESSION_PA = 1200.0
_PITCHER_REGRESSION_OUTS = 1350.0  # 450 IP * 3


@dataclass(frozen=True, slots=True)
class MarcelWeights:
    """Configurable weights for the 3 lookback seasons."""

    w_y1: float = 5.0
    w_y2: float = 4.0
    w_y3: float = 3.0


class MarcelProjectionEngine:
    """Computes transparent, reproducible player statistical projections."""

    def __init__(self, weights: MarcelWeights | None = None) -> None:
        """Initialize engine with lookback weights."""
        self.weights = weights or MarcelWeights()

    def get_aging_factor(self, age: int | None) -> float:
        """Calculate age multiplier relative to peak age 29."""
        if age is None or age <= 0:
            return 1.0
        if age <= _PEAK_AGE:
            return round(1.0 + ((_PEAK_AGE - age) * _AGING_GROWTH_RATE), 4)
        return round(1.0 - ((age - _PEAK_AGE) * _AGING_DECLINE_RATE), 4)

    def project_hitter(
        self,
        *,
        history_seasons: Sequence[Mapping[str, float]],  # [Y-1, Y-2, Y-3]
        league_rates: Mapping[str, float],  # Rate stats per PA (e.g. hits_per_pa)
        age: int | None = 28,
        park_factor: float = 1.0,
    ) -> dict[str, float]:
        """Project a hitter's rate and counting stats for the upcoming season."""
        # 1. Calculate weighted player counting stats and total weighted PA
        weights = [self.weights.w_y1, self.weights.w_y2, self.weights.w_y3]
        w_pa = 0.0
        w_ab = 0.0
        w_h = 0.0
        w_2b = 0.0
        w_3b = 0.0
        w_hr = 0.0
        w_bb = 0.0
        w_so = 0.0
        w_tb = 0.0

        for i, s in enumerate(history_seasons[:3]):
            w = weights[i]
            pa = s.get("pa", s.get("plate_appearances", 0.0))
            w_pa += pa * w
            w_ab += s.get("at_bats", s.get("ab", 0.0)) * w
            w_h += s.get("hits", 0.0) * w
            w_2b += s.get("doubles", 0.0) * w
            w_3b += s.get("triples", 0.0) * w
            w_hr += s.get("home_runs", 0.0) * w
            w_bb += s.get("walks", 0.0) * w
            w_so += s.get("strikeouts", 0.0) * w
            default_tb = s.get("hits", 0.0) + s.get("doubles", 0.0) + (s.get("home_runs", 0.0) * 3)
            w_tb += s.get("total_bases", default_tb) * w

        if w_pa == 0:
            return {}

        # 2. Regress to league average
        total_sample = w_pa + _HITTER_REGRESSION_PA
        reg_h = w_h + (_HITTER_REGRESSION_PA * league_rates.get("h_per_pa", 0.230))
        reg_hr = w_hr + (_HITTER_REGRESSION_PA * league_rates.get("hr_per_pa", 0.025))
        reg_bb = w_bb + (_HITTER_REGRESSION_PA * league_rates.get("bb_per_pa", 0.085))
        reg_so = w_so + (_HITTER_REGRESSION_PA * league_rates.get("so_per_pa", 0.180))
        reg_tb = w_tb + (_HITTER_REGRESSION_PA * league_rates.get("tb_per_pa", 0.350))
        reg_ab = w_ab + (_HITTER_REGRESSION_PA * league_rates.get("ab_per_pa", 0.880))

        # Base unadjusted rates
        rate_h = reg_h / total_sample
        rate_hr = reg_hr / total_sample
        rate_bb = reg_bb / total_sample
        rate_so = reg_so / total_sample
        rate_tb = reg_tb / total_sample
        rate_ab = reg_ab / total_sample

        # 3. Apply aging factor and park factor
        age_adj = self.get_aging_factor(age)
        final_h_rate = rate_h * age_adj * park_factor
        final_hr_rate = rate_hr * age_adj * park_factor
        final_tb_rate = rate_tb * age_adj * park_factor
        final_bb_rate = rate_bb * age_adj

        # 4. Derive slash line
        est_avg = final_h_rate / rate_ab if rate_ab > 0 else 0.0
        est_obp = final_h_rate + final_bb_rate  # approx per PA
        est_slg = final_tb_rate / rate_ab if rate_ab > 0 else 0.0
        est_ops = est_obp + est_slg
        est_woba = (0.69 * final_bb_rate + 0.89 * (final_h_rate - final_hr_rate) + 2.10 * final_hr_rate) / 1.0

        # Estimated playing time (0.5 * Y1 + 0.1 * Y2 + 200)
        y1_pa = history_seasons[0].get("pa", 0.0) if len(history_seasons) > 0 else 400.0
        y2_pa = history_seasons[1].get("pa", 0.0) if len(history_seasons) > 1 else 300.0
        projected_pa = round((0.5 * y1_pa) + (0.1 * y2_pa) + 200.0, 1)

        return {
            "projected_pa": projected_pa,
            "projected_ab": round(projected_pa * rate_ab, 1),
            "projected_hits": round(projected_pa * final_h_rate, 1),
            "projected_home_runs": round(projected_pa * final_hr_rate, 1),
            "projected_walks": round(projected_pa * final_bb_rate, 1),
            "projected_strikeouts": round(projected_pa * rate_so, 1),
            "projected_avg": round(est_avg, 3),
            "projected_obp": round(est_obp, 3),
            "projected_slg": round(est_slg, 3),
            "projected_ops": round(est_ops, 3),
            "projected_woba": round(est_woba, 3),
            "aging_factor": age_adj,
            "park_factor": park_factor,
        }

    def project_pitcher(
        self,
        *,
        history_seasons: Sequence[Mapping[str, float]],
        league_rates: Mapping[str, float],
        age: int | None = 28,
        park_factor: float = 1.0,
    ) -> dict[str, float]:
        """Project a pitcher's rate and counting stats for the upcoming season."""
        weights = [self.weights.w_y1, self.weights.w_y2, self.weights.w_y3]
        w_outs = 0.0
        w_er = 0.0
        w_so = 0.0
        w_bb = 0.0
        w_h = 0.0
        w_hr = 0.0

        for i, s in enumerate(history_seasons[:3]):
            w = weights[i]
            outs = s.get("innings_outs", s.get("ip", 0.0) * 3)
            w_outs += outs * w
            w_er += s.get("earned_runs", 0.0) * w
            w_so += s.get("strikeouts", 0.0) * w
            w_bb += s.get("walks", 0.0) * w
            w_h += s.get("hits", 0.0) * w
            w_hr += s.get("home_runs", 0.0) * w

        if w_outs == 0:
            return {}

        total_sample = w_outs + _PITCHER_REGRESSION_OUTS
        reg_er = w_er + (_PITCHER_REGRESSION_OUTS * league_rates.get("er_per_out", 0.150))
        reg_so = w_so + (_PITCHER_REGRESSION_OUTS * league_rates.get("so_per_out", 0.220))
        reg_bb = w_bb + (_PITCHER_REGRESSION_OUTS * league_rates.get("bb_per_out", 0.090))
        reg_h = w_h + (_PITCHER_REGRESSION_OUTS * league_rates.get("h_per_out", 0.280))
        reg_hr = w_hr + (_PITCHER_REGRESSION_OUTS * league_rates.get("hr_per_out", 0.028))

        rate_er = reg_er / total_sample
        rate_so = reg_so / total_sample
        rate_bb = reg_bb / total_sample
        rate_h = reg_h / total_sample
        rate_hr = reg_hr / total_sample

        # For pitching ERA: younger pitchers improve (ERA drops), older decline (ERA rises)
        age_adj = self.get_aging_factor(age)
        pitching_age_multiplier = (2.0 - age_adj) if age_adj != 1.0 else 1.0

        final_era = (rate_er * 27.0) * pitching_age_multiplier * park_factor
        final_whip = ((rate_h + rate_bb) * 3.0) * park_factor
        # FIP formula calculation with league constant 3.20
        final_fip = ((13.0 * rate_hr * 3.0) + (3.0 * rate_bb * 3.0) - (2.0 * rate_so * 3.0)) + 3.20

        # Estimated IP (0.5 * Y1 + 0.1 * Y2 + 60 IP)
        y1_outs = history_seasons[0].get("innings_outs", 300.0) if len(history_seasons) > 0 else 300.0
        y2_outs = history_seasons[1].get("innings_outs", 200.0) if len(history_seasons) > 1 else 200.0
        projected_outs = (0.5 * y1_outs) + (0.1 * y2_outs) + 180.0
        projected_ip = round(projected_outs / 3.0, 1)

        return {
            "projected_ip": projected_ip,
            "projected_era": round(final_era, 2),
            "projected_whip": round(final_whip, 2),
            "projected_fip": round(final_fip, 2),
            "projected_strikeouts": round(projected_outs * rate_so, 1),
            "projected_walks": round(projected_outs * rate_bb, 1),
            "projected_hits_allowed": round(projected_outs * rate_h, 1),
            "projected_home_runs_allowed": round(projected_outs * rate_hr, 1),
            "aging_factor": age_adj,
            "park_factor": park_factor,
        }


__all__ = ["MarcelProjectionEngine", "MarcelWeights"]
