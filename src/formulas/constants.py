"""League Environment Constants, Era Profiles, and Linear Weights Calibration Engine."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func

from src.formulas.models import EraProfile

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

MIN_SUPPORTED_SEASON: int = 1982
MAX_SUPPORTED_SEASON: int = 2026

_ERA_PROFILES: list[EraProfile] = [
    EraProfile(
        era_id="KBO_ERA_1982_1990_APPROX_V1",
        name="Dead Ball Era",
        season_start=1982,
        season_end=1990,
        is_calibrated=True,
        constants={
            "w_bb": 0.680,
            "w_hbp": 0.710,
            "w_1b": 0.880,
            "w_2b": 1.250,
            "w_3b": 1.580,
            "w_hr": 2.050,
            "woba_scale": 1.220,
            "lg_woba": 0.320,
            "lg_r_per_pa": 0.115,
            "lg_obp": 0.330,
            "lg_slg": 0.380,
            "lg_ops": 0.710,
            "lg_ba": 0.260,
            "lg_era": 3.650,
            "c_fip": 3.450,
        },
        provenance="Era-approximated linear weights reference V1 (1982-1990).",
    ),
    EraProfile(
        era_id="KBO_ERA_1991_2000_APPROX_V1",
        name="Expansion Era",
        season_start=1991,
        season_end=2000,
        is_calibrated=True,
        constants={
            "w_bb": 0.690,
            "w_hbp": 0.720,
            "w_1b": 0.890,
            "w_2b": 1.270,
            "w_3b": 1.620,
            "w_hr": 2.080,
            "woba_scale": 1.240,
            "lg_woba": 0.330,
            "lg_r_per_pa": 0.120,
            "lg_obp": 0.335,
            "lg_slg": 0.395,
            "lg_ops": 0.730,
            "lg_ba": 0.265,
            "lg_era": 4.200,
            "c_fip": 4.050,
        },
        provenance="Era-approximated linear weights reference V1 (1991-2000).",
    ),
    EraProfile(
        era_id="KBO_ERA_2001_2013_UNCALIBRATED",
        name="Modern Transition Era (Uncalibrated)",
        season_start=2001,
        season_end=2013,
        is_calibrated=False,
        constants=None,
        provenance="Uncalibrated interval. Requires dynamic seasonal aggregation or explicit provenance.",
    ),
    EraProfile(
        era_id="KBO_ERA_2014_2018_APPROX_V1",
        name="Offense Explosion Era",
        season_start=2014,
        season_end=2018,
        is_calibrated=True,
        constants={
            "w_bb": 0.700,
            "w_hbp": 0.730,
            "w_1b": 0.900,
            "w_2b": 1.300,
            "w_3b": 1.650,
            "w_hr": 2.150,
            "woba_scale": 1.260,
            "lg_woba": 0.355,
            "lg_r_per_pa": 0.135,
            "lg_obp": 0.355,
            "lg_slg": 0.440,
            "lg_ops": 0.795,
            "lg_ba": 0.285,
            "lg_era": 4.800,
            "c_fip": 4.450,
        },
        provenance="Era-approximated linear weights reference V1 (2014-2018).",
    ),
    EraProfile(
        era_id="KBO_ERA_2019_2026_APPROX_V1",
        name="Current Baseline Era",
        season_start=2019,
        season_end=2026,
        is_calibrated=True,
        constants={
            "w_bb": 0.690,
            "w_hbp": 0.720,
            "w_1b": 0.890,
            "w_2b": 1.270,
            "w_3b": 1.620,
            "w_hr": 2.100,
            "woba_scale": 1.240,
            "lg_woba": 0.330,
            "lg_r_per_pa": 0.120,
            "lg_obp": 0.340,
            "lg_slg": 0.410,
            "lg_ops": 0.750,
            "lg_ba": 0.270,
            "lg_era": 4.500,
            "c_fip": 3.850,
        },
        provenance="Era-approximated linear weights reference V1 (2019-2026).",
    ),
]


class LeagueConstantsEngine:
    """Master engine for computing and serving empirical and baseline league constants."""

    @classmethod
    def resolve_era_profile(cls, season: int) -> EraProfile:
        """Resolve the deterministic EraProfile for a given season with strict bounds checking."""
        if season < MIN_SUPPORTED_SEASON or season > MAX_SUPPORTED_SEASON:
            msg = f"Season {season} is out of supported range ({MIN_SUPPORTED_SEASON}~{MAX_SUPPORTED_SEASON})."
            raise ValueError(msg)

        for profile in _ERA_PROFILES:
            if profile.season_start <= season <= profile.season_end:
                return profile

        msg = f"Season {season} failed to map to any registered EraProfile."
        raise RuntimeError(msg)

    @classmethod
    def get_baseline_constants(cls, season: int) -> dict[str, float]:
        """Retrieve era-calibrated baseline weights, failing closed if the era is uncalibrated."""
        profile = cls.resolve_era_profile(season)
        if not profile.is_calibrated or profile.constants is None:
            msg = (
                f"Season {season} era '{profile.era_id}' is UNCALIBRATED. "
                "Static baseline constants unavailable without empirical dynamic aggregation."
            )
            raise ValueError(msg)
        return profile.constants.copy()

    @classmethod
    def _extract_batting_aggregates(cls, session: Session, season: int, level: str) -> object:
        """Query aggregated season batting numbers."""
        from src.models.player import PlayerSeasonBatting

        return (
            session.query(
                func.sum(PlayerSeasonBatting.plate_appearances).label("pa"),
                func.sum(PlayerSeasonBatting.at_bats).label("ab"),
                func.sum(PlayerSeasonBatting.hits).label("h"),
                func.sum(PlayerSeasonBatting.doubles).label("d2"),
                func.sum(PlayerSeasonBatting.triples).label("d3"),
                func.sum(PlayerSeasonBatting.home_runs).label("hr"),
                func.sum(PlayerSeasonBatting.walks).label("bb"),
                func.sum(PlayerSeasonBatting.intentional_walks).label("ibb"),
                func.sum(PlayerSeasonBatting.hbp).label("hbp"),
                func.sum(PlayerSeasonBatting.sacrifice_flies).label("sf"),
                func.sum(PlayerSeasonBatting.runs).label("r"),
            )
            .filter(
                PlayerSeasonBatting.season == season,
                (PlayerSeasonBatting.level == level) | (PlayerSeasonBatting.level.is_(None)),
            )
            .first()
        )

    @classmethod
    def _extract_pitching_aggregates(cls, session: Session, season: int, level: str) -> object:
        """Query aggregated season pitching numbers."""
        from src.models.player import PlayerSeasonPitching

        return (
            session.query(
                func.sum(PlayerSeasonPitching.earned_runs).label("er"),
                func.sum(PlayerSeasonPitching.innings_outs).label("outs"),
                func.sum(PlayerSeasonPitching.home_runs_allowed).label("hr"),
                func.sum(PlayerSeasonPitching.walks_allowed).label("bb"),
                func.sum(PlayerSeasonPitching.hit_batters).label("hbp"),
                func.sum(PlayerSeasonPitching.strikeouts).label("so"),
            )
            .filter(
                PlayerSeasonPitching.season == season,
                (PlayerSeasonPitching.level == level) | (PlayerSeasonPitching.level.is_(None)),
            )
            .first()
        )

    @classmethod
    def _compute_batting_derived(cls, b_row: object, baseline: dict[str, float]) -> dict[str, float]:
        """Compute league batting rate statistics from query aggregate row."""
        pa = float(getattr(b_row, "pa", 0) or 0)
        if pa <= 0:
            return {}

        ab = float(getattr(b_row, "ab", 0) or 0)
        h = float(getattr(b_row, "h", 0) or 0)
        d2 = float(getattr(b_row, "d2", 0) or 0)
        d3 = float(getattr(b_row, "d3", 0) or 0)
        hr = float(getattr(b_row, "hr", 0) or 0)
        bb = float(getattr(b_row, "bb", 0) or 0)
        ibb = float(getattr(b_row, "ibb", 0) or 0)
        hbp = float(getattr(b_row, "hbp", 0) or 0)
        sf = float(getattr(b_row, "sf", 0) or 0)
        r = float(getattr(b_row, "r", 0) or 0)

        u_bb = max(bb - ibb, 0.0)
        h1 = max(h - d2 - d3 - hr, 0.0)
        tb = h1 + 2.0 * d2 + 3.0 * d3 + 4.0 * hr

        lg_obp = (h + bb + hbp) / (ab + bb + hbp + sf) if (ab + bb + hbp + sf) > 0 else baseline["lg_obp"]
        lg_slg = tb / ab if ab > 0 else baseline["lg_slg"]
        lg_ba = h / ab if ab > 0 else baseline["lg_ba"]
        lg_ops = lg_obp + lg_slg
        lg_r_per_pa = r / pa if pa > 0 else baseline["lg_r_per_pa"]

        w_bb = baseline.get("w_bb", 0.690)
        w_hbp = baseline.get("w_hbp", 0.720)
        w_1b = baseline.get("w_1b", 0.890)
        w_2b = baseline.get("w_2b", 1.270)
        w_3b = baseline.get("w_3b", 1.620)
        w_hr = baseline.get("w_hr", 2.100)

        woba_num = w_bb * u_bb + w_hbp * hbp + w_1b * h1 + w_2b * d2 + w_3b * d3 + w_hr * hr
        woba_den = ab + u_bb + hbp + sf
        lg_woba = woba_num / woba_den if woba_den > 0 else baseline["lg_woba"]

        return {
            "lg_woba": round(lg_woba, 3),
            "lg_r_per_pa": round(lg_r_per_pa, 3),
            "lg_obp": round(lg_obp, 3),
            "lg_slg": round(lg_slg, 3),
            "lg_ops": round(lg_ops, 3),
            "lg_ba": round(lg_ba, 3),
        }

    @classmethod
    def _compute_pitching_derived(cls, p_row: object) -> dict[str, float]:
        """Compute league pitching rate statistics from query aggregate row."""
        outs = float(getattr(p_row, "outs", 0) or 0)
        if outs <= 0:
            return {}

        er = float(getattr(p_row, "er", 0) or 0)
        p_hr = float(getattr(p_row, "hr", 0) or 0)
        p_bb = float(getattr(p_row, "bb", 0) or 0)
        p_hbp = float(getattr(p_row, "hbp", 0) or 0)
        p_so = float(getattr(p_row, "so", 0) or 0)

        lg_era = (er * 27.0) / outs
        raw_fip_comp = (3.0 * (13.0 * p_hr + 3.0 * (p_bb + p_hbp) - 2.0 * p_so)) / outs
        c_fip = lg_era - raw_fip_comp

        return {
            "lg_era": round(lg_era, 3),
            "c_fip": round(c_fip, 3),
        }

    @classmethod
    def compute_league_constants(
        cls,
        session: Session,
        season: int,
        level: str = "1군",
    ) -> dict[str, float]:
        """Dynamically compute empirical seasonal linear weights and league averages from DB."""
        try:
            baseline = cls.get_baseline_constants(season)
        except ValueError:
            baseline = _ERA_PROFILES[-1].constants.copy()  # type: ignore[union-attr]

        bat_row = cls._extract_batting_aggregates(session, season, level)
        bat_derived = cls._compute_batting_derived(bat_row, baseline)

        pit_row = cls._extract_pitching_aggregates(session, season, level)
        pit_derived = cls._compute_pitching_derived(pit_row)

        return {
            **baseline,
            **bat_derived,
            **pit_derived,
        }


__all__ = [
    "MAX_SUPPORTED_SEASON",
    "MIN_SUPPORTED_SEASON",
    "LeagueConstantsEngine",
]
