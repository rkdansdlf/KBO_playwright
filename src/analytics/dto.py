"""Standard Data Transfer Objects (DTOs) for Sabermetrics and Advanced Baseball Analytics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LeagueConstants:
    """League-wide weights and constants for a given season and league level."""

    year: int
    level: str = "KBO1"
    woba_scale: float = 1.25
    w_bb: float = 0.69
    w_hbp: float = 0.72
    w_1b: float = 0.89
    w_2b: float = 1.27
    w_3b: float = 1.62
    w_hr: float = 2.10
    league_woba: float = 0.330
    league_era: float = 4.20
    league_obp: float = 0.340
    fip_constant: float = 3.80
    runs_per_win: float = 10.0
    runs_per_pa: float = 0.12

    def to_dict(self) -> dict[str, Any]:
        """Convert league constants to dictionary."""
        return {
            "year": self.year,
            "level": self.level,
            "woba_scale": round(self.woba_scale, 3),
            "w_bb": round(self.w_bb, 3),
            "w_hbp": round(self.w_hbp, 3),
            "w_1b": round(self.w_1b, 3),
            "w_2b": round(self.w_2b, 3),
            "w_3b": round(self.w_3b, 3),
            "w_hr": round(self.w_hr, 3),
            "league_woba": round(self.league_woba, 3),
            "league_era": round(self.league_era, 2),
            "fip_constant": round(self.fip_constant, 3),
            "runs_per_win": round(self.runs_per_win, 2),
        }


@dataclass
class BattingSabermetrics:
    """Advanced sabermetric statistics for a batter."""

    player_id: int
    season: int
    plate_appearances: int = 0
    at_bats: int = 0
    hits: int = 0
    woba: float = 0.0
    wraa: float = 0.0
    wrc: float = 0.0
    wrc_plus: float = 100.0
    babip: float = 0.0
    iso: float = 0.0
    ops_plus: float = 100.0
    offensive_runs: float = 0.0
    war: float = 0.0
    bb_pct: float = 0.0
    k_pct: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert batting sabermetrics to dictionary."""
        return {
            "player_id": self.player_id,
            "season": self.season,
            "plate_appearances": self.plate_appearances,
            "at_bats": self.at_bats,
            "hits": self.hits,
            "woba": round(self.woba, 3),
            "wraa": round(self.wraa, 2),
            "wrc": round(self.wrc, 2),
            "wrc_plus": round(self.wrc_plus, 1),
            "babip": round(self.babip, 3),
            "iso": round(self.iso, 3),
            "ops_plus": round(self.ops_plus, 1),
            "war": round(self.war, 2),
            "bb_pct": round(self.bb_pct, 1),
            "k_pct": round(self.k_pct, 1),
        }


@dataclass
class PitchingSabermetrics:
    """Advanced sabermetric statistics for a pitcher."""

    player_id: int
    season: int
    innings_pitched: float = 0.0
    earned_runs: int = 0
    era: float = 0.0
    fip: float = 0.0
    kfip: float = 0.0
    whip: float = 0.0
    era_plus: float = 100.0
    fip_minus: float = 100.0
    babip: float = 0.0
    k_per_9: float = 0.0
    bb_per_9: float = 0.0
    hr_per_9: float = 0.0
    war: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert pitching sabermetrics to dictionary."""
        return {
            "player_id": self.player_id,
            "season": self.season,
            "innings_pitched": round(self.innings_pitched, 1),
            "earned_runs": self.earned_runs,
            "era": round(self.era, 2),
            "fip": round(self.fip, 2),
            "kfip": round(self.kfip, 2),
            "whip": round(self.whip, 2),
            "era_plus": round(self.era_plus, 1),
            "fip_minus": round(self.fip_minus, 1),
            "babip": round(self.babip, 3),
            "k_per_9": round(self.k_per_9, 2),
            "bb_per_9": round(self.bb_per_9, 2),
            "hr_per_9": round(self.hr_per_9, 2),
            "war": round(self.war, 2),
        }


@dataclass
class MatchupMatrix:
    """Batter vs. Pitcher (BvP) Head-to-Head matchup summary."""

    batter_id: int
    pitcher_id: int
    plate_appearances: int = 0
    at_bats: int = 0
    hits: int = 0
    doubles: int = 0
    triples: int = 0
    home_runs: int = 0
    walks: int = 0
    strikeouts: int = 0
    hbp: int = 0
    avg: float = 0.0
    obp: float = 0.0
    slg: float = 0.0
    ops: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert matchup matrix to dictionary."""
        return {
            "batter_id": self.batter_id,
            "pitcher_id": self.pitcher_id,
            "plate_appearances": self.plate_appearances,
            "at_bats": self.at_bats,
            "hits": self.hits,
            "doubles": self.doubles,
            "triples": self.triples,
            "home_runs": self.home_runs,
            "walks": self.walks,
            "strikeouts": self.strikeouts,
            "hbp": self.hbp,
            "avg": round(self.avg, 3),
            "obp": round(self.obp, 3),
            "slg": round(self.slg, 3),
            "ops": round(self.ops, 3),
        }


@dataclass
class SplitMetrics:
    """Situational and contextual split record."""

    category: str  # "team", "stadium", "platoon", "risp", "month"
    entity_id: int  # player_id or team_code
    season: int
    split_key: str  # e.g., "vs_LG", "Jamsil", "vs_LHP", "RISP"
    sample_size: int = 0
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert split metrics to dictionary."""
        return {
            "category": self.category,
            "entity_id": self.entity_id,
            "season": self.season,
            "split_key": self.split_key,
            "sample_size": self.sample_size,
            "stats": self.stats,
        }
