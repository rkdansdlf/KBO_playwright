"""Sabermetrics Engine for calculating advanced KBO player metrics and league weights."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import func, or_

from src.analytics.dto import BattingSabermetrics, LeagueConstants, PitchingSabermetrics
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

MIN_LEAGUE_PLAYER_ID = 10_000
LEAGUE_BATTING_PA_STUB_LIMIT = 10
LEAGUE_PITCHING_OUTS_STUB_LIMIT = 10
CONSTANTS_CACHE_KEY = "kbo_sabermetrics_constants_v2"


class SabermetricsEngine:
    """Engine for computing league constants and advanced sabermetric statistics."""

    def __init__(self, session: Session) -> None:
        """Initialize the sabermetrics engine with a database session."""
        self.session = session

    def get_league_constants(self, year: int, level: str = "KBO1") -> LeagueConstants:
        """Calculate and return league-wide averages and weights for a given season."""
        cache_key = (year, level)
        session_cache = self.session.info.setdefault(CONSTANTS_CACHE_KEY, {})
        if cache_key in session_cache:
            return session_cache[cache_key]

        bat_query = (
            self.session.query(
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
                PlayerSeasonBatting.season == year,
                PlayerSeasonBatting.level == level,
                PlayerSeasonBatting.player_id >= MIN_LEAGUE_PLAYER_ID,
                or_(
                    PlayerSeasonBatting.plate_appearances > LEAGUE_BATTING_PA_STUB_LIMIT,
                    PlayerSeasonBatting.home_runs > 0,
                    PlayerSeasonBatting.walks > 0,
                ),
            )
            .one()
        )

        pit_query = (
            self.session.query(
                func.sum(PlayerSeasonPitching.innings_outs).label("outs"),
                func.sum(PlayerSeasonPitching.earned_runs).label("er"),
                func.sum(PlayerSeasonPitching.home_runs_allowed).label("hr_allowed"),
                func.sum(PlayerSeasonPitching.walks_allowed).label("bb_allowed"),
                func.sum(PlayerSeasonPitching.hit_batters).label("hbp_allowed"),
                func.sum(PlayerSeasonPitching.strikeouts).label("so"),
                func.sum(PlayerSeasonPitching.runs_allowed).label("r_allowed"),
            )
            .filter(
                PlayerSeasonPitching.season == year,
                PlayerSeasonPitching.level == level,
                PlayerSeasonPitching.player_id >= MIN_LEAGUE_PLAYER_ID,
                or_(
                    PlayerSeasonPitching.innings_outs <= LEAGUE_PITCHING_OUTS_STUB_LIMIT,
                    or_(PlayerSeasonPitching.strikeouts > 0, PlayerSeasonPitching.walks_allowed > 0),
                ),
            )
            .one()
        )

        h_1b = (bat_query.h or 0) - (bat_query.d2 or 0) - (bat_query.d3 or 0) - (bat_query.hr or 0)
        u_bb = (bat_query.bb or 0) - (bat_query.ibb or 0)
        pa = bat_query.pa or 1
        runs = bat_query.r or 0

        # Calculate wOBA weights and league wOBA
        w_bb = 0.69
        w_hbp = 0.72
        w_1b = 0.89
        w_2b = 1.27
        w_3b = 1.62
        w_hr = 2.10

        woba_num = (
            w_bb * u_bb
            + w_hbp * (bat_query.hbp or 0)
            + w_1b * h_1b
            + w_2b * (bat_query.d2 or 0)
            + w_3b * (bat_query.d3 or 0)
            + w_hr * (bat_query.hr or 0)
        )
        woba_den = (bat_query.ab or 0) + u_bb + (bat_query.hbp or 0) + (bat_query.sf or 0)
        league_woba = (woba_num / woba_den) if woba_den > 0 else 0.330

        obp_den = (bat_query.ab or 0) + (bat_query.bb or 0) + (bat_query.hbp or 0) + (bat_query.sf or 0)
        league_obp = (
            ((bat_query.h or 0) + (bat_query.bb or 0) + (bat_query.hbp or 0)) / obp_den if obp_den > 0 else 0.340
        )
        woba_scale = (league_obp / league_woba) if league_woba > 0 else 1.25

        ip = (pit_query.outs or 1) / 3.0
        league_era = (pit_query.er or 0) * 9.0 / ip if ip > 0 else 4.20
        fip_comp = (
            (
                13 * (pit_query.hr_allowed or 0)
                + 3 * ((pit_query.bb_allowed or 0) + (pit_query.hbp_allowed or 0))
                - 2 * (pit_query.so or 0)
            )
            / ip
            if ip > 0
            else 0.0
        )
        fip_constant = league_era - fip_comp
        runs_per_pa = runs / pa if pa > 0 else 0.12
        runs_per_win = max(8.0, round(runs_per_pa * 10.0 * 8.5, 2))

        consts = LeagueConstants(
            year=year,
            level=level,
            woba_scale=woba_scale,
            w_bb=w_bb,
            w_hbp=w_hbp,
            w_1b=w_1b,
            w_2b=w_2b,
            w_3b=w_3b,
            w_hr=w_hr,
            league_woba=league_woba,
            league_era=league_era,
            league_obp=league_obp,
            fip_constant=fip_constant,
            runs_per_win=runs_per_win,
            runs_per_pa=runs_per_pa,
        )
        session_cache[cache_key] = consts
        return consts

    def calculate_batting_metrics(self, stats: object, consts: LeagueConstants) -> BattingSabermetrics:
        """Calculate advanced batting sabermetrics for a single player."""
        pa = getattr(stats, "plate_appearances", 0) or 0
        ab = getattr(stats, "at_bats", 0) or 0
        h = getattr(stats, "hits", 0) or 0
        d2 = getattr(stats, "doubles", 0) or 0
        d3 = getattr(stats, "triples", 0) or 0
        hr = getattr(stats, "home_runs", 0) or 0
        bb = getattr(stats, "walks", 0) or 0
        ibb = getattr(stats, "intentional_walks", 0) or 0
        hbp = getattr(stats, "hbp", 0) or 0
        sf = getattr(stats, "sacrifice_flies", 0) or 0
        so = getattr(stats, "strikeouts", 0) or 0
        player_id = getattr(stats, "player_id", 0) or 0
        season = getattr(stats, "season", consts.year) or consts.year

        if pa == 0:
            return BattingSabermetrics(player_id=player_id, season=season)

        h_1b = h - d2 - d3 - hr
        u_bb = bb - ibb

        woba_num = (
            consts.w_bb * u_bb
            + consts.w_hbp * hbp
            + consts.w_1b * h_1b
            + consts.w_2b * d2
            + consts.w_3b * d3
            + consts.w_hr * hr
        )
        woba_den = ab + u_bb + hbp + sf
        woba = (woba_num / woba_den) if woba_den > 0 else 0.0

        wraa = ((woba - consts.league_woba) / consts.woba_scale) * pa if consts.woba_scale > 0 else 0.0
        wrc = wraa + (pa * consts.runs_per_pa)
        wrc_plus = (
            100.0 * (((wraa / pa) + consts.runs_per_pa) / consts.runs_per_pa) if consts.runs_per_pa > 0 else 100.0
        )

        babip_den = ab - so - hr + sf
        babip = ((h - hr) / babip_den) if babip_den > 0 else 0.0
        iso = ((d2 + 2 * d3 + 3 * hr) / ab) if ab > 0 else 0.0

        # Replacement level and positional baseline estimation
        rep_runs = (pa / 600.0) * 20.0  # 20 runs per 600 PA replacement level
        war = (wraa + rep_runs) / consts.runs_per_win if consts.runs_per_win > 0 else 0.0

        bb_pct = (bb / pa * 100.0) if pa > 0 else 0.0
        k_pct = (so / pa * 100.0) if pa > 0 else 0.0

        return BattingSabermetrics(
            player_id=player_id,
            season=season,
            plate_appearances=pa,
            at_bats=ab,
            hits=h,
            woba=woba,
            wraa=wraa,
            wrc=wrc,
            wrc_plus=wrc_plus,
            babip=babip,
            iso=iso,
            ops_plus=wrc_plus,
            offensive_runs=wraa,
            war=war,
            bb_pct=bb_pct,
            k_pct=k_pct,
        )

    def calculate_pitching_metrics(self, stats: object, consts: LeagueConstants) -> PitchingSabermetrics:
        """Calculate advanced pitching sabermetrics for a single pitcher."""
        outs = getattr(stats, "innings_outs", 0) or 0
        er = getattr(stats, "earned_runs", 0) or 0
        h = getattr(stats, "hits_allowed", 0) or 0
        hr = getattr(stats, "home_runs_allowed", 0) or 0
        bb = getattr(stats, "walks_allowed", 0) or 0
        ibb = getattr(stats, "intentional_walks_allowed", 0) or 0
        hbp = getattr(stats, "hit_batters", 0) or 0
        so = getattr(stats, "strikeouts", 0) or 0
        player_id = getattr(stats, "player_id", 0) or 0
        season = getattr(stats, "season", consts.year) or consts.year

        ip = outs / 3.0
        if ip <= 0:
            return PitchingSabermetrics(player_id=player_id, season=season)

        era = (er * 9.0) / ip
        fip = ((13 * hr + 3 * (bb + hbp) - 2 * so) / ip) + consts.fip_constant
        kfip = ((13 * hr + 3 * (bb - ibb + hbp) - 2 * so) / ip) + consts.fip_constant
        whip = (bb + h) / ip

        era_plus = (100.0 * consts.league_era / era) if era > 0 else 200.0
        fip_minus = (100.0 * fip / consts.league_era) if consts.league_era > 0 else 100.0

        babip_den = outs + h - so - hr
        babip = ((h - hr) / babip_den) if babip_den > 0 else 0.0

        k_per_9 = (so * 9.0) / ip
        bb_per_9 = (bb * 9.0) / ip
        hr_per_9 = (hr * 9.0) / ip

        # Pitcher WAR calculation based on FIP against league average
        rep_runs = (ip / 200.0) * 20.0
        fip_diff = (consts.league_era - fip) * (ip / 9.0)
        war = (fip_diff + rep_runs) / consts.runs_per_win if consts.runs_per_win > 0 else 0.0

        return PitchingSabermetrics(
            player_id=player_id,
            season=season,
            innings_pitched=ip,
            earned_runs=er,
            era=era,
            fip=fip,
            kfip=kfip,
            whip=whip,
            era_plus=era_plus,
            fip_minus=fip_minus,
            babip=babip,
            k_per_9=k_per_9,
            bb_per_9=bb_per_9,
            hr_per_9=hr_per_9,
            war=war,
        )

    def calculate_season_sabermetrics(self, year: int, level: str = "KBO1") -> dict[str, int]:
        """Compute and persist sabermetrics for all players in a season."""
        consts = self.get_league_constants(year, level)

        bat_rows = (
            self.session.query(PlayerSeasonBatting)
            .filter(PlayerSeasonBatting.season == year, PlayerSeasonBatting.level == level)
            .all()
        )
        pit_rows = (
            self.session.query(PlayerSeasonPitching)
            .filter(PlayerSeasonPitching.season == year, PlayerSeasonPitching.level == level)
            .all()
        )

        for b in bat_rows:
            metrics = self.calculate_batting_metrics(b, consts)
            b.woba = metrics.woba
            b.wrc_plus = metrics.wrc_plus
            b.war = metrics.war

        for p in pit_rows:
            p_metrics = self.calculate_pitching_metrics(p, consts)
            p.fip = p_metrics.fip
            p.kfip = p_metrics.kfip
            p.war = p_metrics.war

        self.session.flush()
        return {"batting_updated": len(bat_rows), "pitching_updated": len(pit_rows)}
