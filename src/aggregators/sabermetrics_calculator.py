"""sabermetrics calculator 모듈."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import func, or_

from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


MIN_LEAGUE_PLAYER_ID = 10_000
LEAGUE_BATTING_PA_STUB_LIMIT = 10
LEAGUE_PITCHING_OUTS_STUB_LIMIT = 10


class SabermetricsCalculator:
    """Service to calculate advanced Sabermetrics (wOBA, wRC+, WAR).

    using league-specific constants per season.

    """

    @staticmethod
    def get_league_constants(session: Session, year: int) -> dict[str, Any]:
        """Calculate league-wide averages and constants for a given year.

        Args:
            session: Session.
            year: Season year.

        """
        # Aggregate league batting stats

        # Filter: Exclude players that likely have incomplete data (e.g., 0 HR and 0 BB despite high PA)
        # This makes league constants more resilient to dirty data.
        bat_query = (
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
                PlayerSeasonBatting.season == year,
                PlayerSeasonBatting.player_id >= MIN_LEAGUE_PLAYER_ID,
                # Filter out obvious stubs/incomplete rows
                or_(
                    PlayerSeasonBatting.plate_appearances > LEAGUE_BATTING_PA_STUB_LIMIT,
                    PlayerSeasonBatting.home_runs > 0,
                    PlayerSeasonBatting.walks > 0,
                ),
            )
            .one()
        )

        # Aggregate league pitching stats
        pit_query = (
            session.query(
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
                PlayerSeasonPitching.player_id >= MIN_LEAGUE_PLAYER_ID,
                or_(
                    PlayerSeasonPitching.innings_outs <= LEAGUE_PITCHING_OUTS_STUB_LIMIT,
                    or_(PlayerSeasonPitching.strikeouts > 0, PlayerSeasonPitching.walks_allowed > 0),
                ),
            )
            .one()
        )

        # 1. League wOBA
        # wOBA = (0.69*uBB + 0.72*HBP + 0.89*1B + 1.27*2B + 1.62*3B + 2.10*HR) / (AB + BB - IBB + HBP + SF)
        h_1b = (bat_query.h or 0) - (bat_query.d2 or 0) - (bat_query.d3 or 0) - (bat_query.hr or 0)
        u_bb = (bat_query.bb or 0) - (bat_query.ibb or 0)
        numerator = (
            (0.69 * u_bb)
            + (0.72 * (bat_query.hbp or 0))
            + (0.89 * h_1b)
            + (1.27 * (bat_query.d2 or 0))
            + (1.62 * (bat_query.d3 or 0))
            + (2.10 * (bat_query.hr or 0))
        )
        denominator = (bat_query.ab or 0) + u_bb + (bat_query.hbp or 0) + (bat_query.sf or 0)
        lg_woba = numerator / denominator if denominator > 0 else 0.320

        # 2. wOBA Scale (League OBP / League wOBA is a common approximation)
        lg_obp = ((bat_query.h or 0) + u_bb + (bat_query.hbp or 0)) / denominator if denominator > 0 else 0.330
        woba_scale = lg_obp / lg_woba if lg_woba > 0 else 1.2

        # 3. Runs per PA
        lg_r_per_pa = (bat_query.r or 0) / (bat_query.pa or 1)

        # 4. FIP Constant
        lg_ip = (pit_query.outs or 0) / 3.0
        lg_era = ((pit_query.er or 0) / lg_ip) * 9 if lg_ip > 0 else 4.50
        raw_fip = (
            (
                (13 * (pit_query.hr_allowed or 0))
                + (3 * ((pit_query.bb_allowed or 0) + (pit_query.hbp_allowed or 0)))
                - (2 * (pit_query.so or 0))
            )
            / lg_ip
            if lg_ip > 0
            else 0
        )
        fip_constant = lg_era - raw_fip

        # 5. Runs Per Win (RPW) - Tango's formula: RPW = 10 * sqrt(RPG / 9)
        # RPG = total runs per game (both teams combined)
        # total team-games ≈ total_outs / 27, total games ≈ total_outs / 54
        total_team_games = (pit_query.outs or 0) / 27.0
        rpg = (bat_query.r or 0) * 2 / total_team_games if total_team_games > 0 else 10.0
        rpw = round(10 * (rpg / 9) ** 0.5, 2)

        # 6. League OBP and SLG for OPS+
        total_bases = (bat_query.h or 0) + (bat_query.d2 or 0) + 2 * (bat_query.d3 or 0) + 3 * (bat_query.hr or 0)
        lg_slg = total_bases / (bat_query.ab or 1)

        return {
            "lg_woba": lg_woba,
            "woba_scale": woba_scale,
            "lg_r_per_pa": lg_r_per_pa,
            "fip_constant": fip_constant,
            "rpw": rpw,
            "lg_era": lg_era,
            "lg_obp": lg_obp,
            "lg_slg": lg_slg,
        }

    @staticmethod
    def calculate_batting_metrics(stat: PlayerSeasonBatting, lg: dict[str, Any]) -> dict[str, Any]:
        """Calculate wOBA, wRC+, wRAA, and WAR for a batter.

        Args:
            stat: Stat.
            lg: Lg.

        """
        h_1b = (stat.hits or 0) - (stat.doubles or 0) - (stat.triples or 0) - (stat.home_runs or 0)

        u_bb = (stat.walks or 0) - (stat.intentional_walks or 0)

        # 1. wOBA
        numerator = (
            (0.69 * u_bb)
            + (0.72 * (stat.hbp or 0))
            + (0.89 * h_1b)
            + (1.27 * (stat.doubles or 0))
            + (1.62 * (stat.triples or 0))
            + (2.10 * (stat.home_runs or 0))
        )
        denominator = (stat.at_bats or 0) + u_bb + (stat.hbp or 0) + (stat.sacrifice_flies or 0)
        woba = round(numerator / denominator, 3) if denominator > 0 else 0.0

        # 2. wRAA (Weighted Runs Above Average)
        # wRAA = ((wOBA - League wOBA) / wOBA Scale) * PA
        wraa = round(((woba - lg["lg_woba"]) / lg["woba_scale"]) * (stat.plate_appearances or 0), 1)

        # 3. wRC+
        # wRC+ = (((wOBA - lgwOBA) / wOBA_scale) + (lgR / PA)) / (lgR / PA) * 100
        wrc_plus = (
            round((((woba - lg["lg_woba"]) / lg["woba_scale"]) + lg["lg_r_per_pa"]) / lg["lg_r_per_pa"] * 100)
            if lg["lg_r_per_pa"] > 0
            else 100
        )

        # 4. OPS+ (On-base Plus Slugging Plus)
        # OPS+ = (OBP/lgOBP + SLG/lgSLG - 1) * 100
        player_obp = stat.obp or 0
        player_slg = stat.slg or 0
        lg_obp = lg.get("lg_obp", 0.330)
        lg_slg = lg.get("lg_slg", 0.400)
        ops_plus = (
            round(((player_obp / lg_obp) + (player_slg / lg_slg) - 1) * 100, 0) if lg_obp > 0 and lg_slg > 0 else 100
        )
        ops_plus = int(ops_plus)

        # 5. Batting WAR (Simplified: wRAA / RPW)
        war = round(wraa / lg["rpw"], 2)

        return {"woba": woba, "wraa": wraa, "wrc_plus": wrc_plus, "ops_plus": ops_plus, "war": war}

    @staticmethod
    def calculate_pitching_metrics(stat: PlayerSeasonPitching, lg: dict[str, Any]) -> dict[str, Any]:
        """Calculate adjusted FIP and Pitching WAR.

        Args:
            stat: Stat.
            lg: Lg.

        """
        ip = (stat.innings_outs or 0) / 3.0

        # 1. FIP
        if ip > 0:
            fip = round(
                (
                    (13 * (stat.home_runs_allowed or 0))
                    + (3 * ((stat.walks_allowed or 0) + (stat.hit_batters or 0)))
                    - (2 * (stat.strikeouts or 0))
                )
                / ip
                + lg["fip_constant"],
                2,
            )
        else:
            fip = 0.0

        # 2. LOB% (Left On Base Percentage)
        # LOB% = (H + BB + HBP - R) / (H + BB + HBP - 1.4 * HR)
        h = stat.hits_allowed or 0
        bb = stat.walks_allowed or 0
        hbp = stat.hit_batters or 0
        r = stat.runs_allowed or 0
        hr = stat.home_runs_allowed or 0
        lob_denom = h + bb + hbp - 1.4 * hr
        lob_pct = round(((h + bb + hbp - r) / lob_denom), 3) if lob_denom > 0 else None

        # 3. Pitching WAR (FIP-based with actual league ERA)
        # WAR = (League ERA - FIP) * (IP / 9) / RPW
        lg_era = lg.get("lg_era", 4.5)
        runs_prevented = (lg_era - fip) * (ip / 9.0)
        war = round(runs_prevented / lg["rpw"], 2) if lg["rpw"] > 0 else 0.0

        return {"fip_adj": fip, "lob_pct": lob_pct, "war": war}
