from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

class SabermetricsCalculator:
    """
    Service to calculate advanced Sabermetrics (wOBA, wRC+, WAR)
    using league-specific constants per season.
    """

    @staticmethod
    def get_league_constants(session: Session, year: int) -> Dict[str, Any]:
        """
        Calculates league-wide averages and constants for a given year.
        """
        # Aggregate league batting stats
        # Filter: Exclude players that likely have incomplete data (e.g., 0 HR and 0 BB despite high PA)
        # This makes league constants more resilient to dirty data.
        bat_query = session.query(
            func.sum(PlayerSeasonBatting.plate_appearances).label('pa'),
            func.sum(PlayerSeasonBatting.at_bats).label('ab'),
            func.sum(PlayerSeasonBatting.hits).label('h'),
            func.sum(PlayerSeasonBatting.doubles).label('d2'),
            func.sum(PlayerSeasonBatting.triples).label('d3'),
            func.sum(PlayerSeasonBatting.home_runs).label('hr'),
            func.sum(PlayerSeasonBatting.walks).label('bb'),
            func.sum(PlayerSeasonBatting.intentional_walks).label('ibb'),
            func.sum(PlayerSeasonBatting.hbp).label('hbp'),
            func.sum(PlayerSeasonBatting.sacrifice_flies).label('sf'),
            func.sum(PlayerSeasonBatting.runs).label('r')
        ).filter(
            PlayerSeasonBatting.season == year,
            PlayerSeasonBatting.player_id >= 10000,
            # Filter out obvious stubs/incomplete rows
            or_(
                PlayerSeasonBatting.plate_appearances <= 10,
                or_(PlayerSeasonBatting.home_runs > 0, PlayerSeasonBatting.walks > 0)
            )
        ).one()

        # Aggregate league pitching stats
        pit_query = session.query(
            func.sum(PlayerSeasonPitching.innings_outs).label('outs'),
            func.sum(PlayerSeasonPitching.earned_runs).label('er'),
            func.sum(PlayerSeasonPitching.home_runs_allowed).label('hr_allowed'),
            func.sum(PlayerSeasonPitching.walks_allowed).label('bb_allowed'),
            func.sum(PlayerSeasonPitching.hit_batters).label('hbp_allowed'),
            func.sum(PlayerSeasonPitching.strikeouts).label('so'),
            func.sum(PlayerSeasonPitching.runs_allowed).label('r_allowed')
        ).filter(
            PlayerSeasonPitching.season == year,
            PlayerSeasonPitching.player_id >= 10000,
            or_(
                PlayerSeasonPitching.innings_outs <= 10,
                or_(PlayerSeasonPitching.strikeouts > 0, PlayerSeasonPitching.walks_allowed > 0)
            )
        ).one()

        # 1. League wOBA
        # wOBA = (0.69*uBB + 0.72*HBP + 0.89*1B + 1.27*2B + 1.62*3B + 2.10*HR) / (AB + BB – IBB + HBP + SF)
        h_1b = (bat_query.h or 0) - (bat_query.d2 or 0) - (bat_query.d3 or 0) - (bat_query.hr or 0)
        u_bb = (bat_query.bb or 0) - (bat_query.ibb or 0)
        numerator = (0.69 * u_bb) + (0.72 * (bat_query.hbp or 0)) + (0.89 * h_1b) + \
                    (1.27 * (bat_query.d2 or 0)) + (1.62 * (bat_query.d3 or 0)) + (2.10 * (bat_query.hr or 0))
        denominator = (bat_query.ab or 0) + u_bb + (bat_query.hbp or 0) + (bat_query.sf or 0)
        lg_woba = numerator / denominator if denominator > 0 else 0.320
        
        # 2. wOBA Scale (League OBP / League wOBA is a common approximation)
        lg_obp = ((bat_query.h or 0) + u_bb + (bat_query.hbp or 0)) / denominator if denominator > 0 else 0.330
        woba_scale = lg_obp / lg_woba if lg_woba > 0 else 1.2
        
        # 3. Runs per PA
        lg_r_per_pa = (bat_query.r or 0) / (bat_query.pa or 1)
        
        # 4. FIP Constant
        # FIP = ((13*HR + 3*(BB+HBP) - 2*K) / IP) + constant
        # constant = LeagueERA - (((13*lgHR + 3*(lgBB+lgHBP) - 2*lgK) / lgIP))
        lg_ip = (pit_query.outs or 0) / 3.0
        lg_era = ((pit_query.er or 0) / lg_ip) * 9 if lg_ip > 0 else 4.50
        raw_fip = ((13 * (pit_query.hr_allowed or 0)) + (3 * ((pit_query.bb_allowed or 0) + (pit_query.hbp_allowed or 0))) - (2 * (pit_query.so or 0))) / lg_ip if lg_ip > 0 else 0
        fip_constant = lg_era - raw_fip

        # 5. Runs Per Win (RPW) - Simple approximation: 9 * (League Runs / League IP) * 1.5 + 2
        # Or simply ~10 for modern KBO
        rpw = 10.0

        return {
            'lg_woba': lg_woba,
            'woba_scale': woba_scale,
            'lg_r_per_pa': lg_r_per_pa,
            'fip_constant': fip_constant,
            'rpw': rpw
        }

    @staticmethod
    def calculate_batting_metrics(stat: PlayerSeasonBatting, lg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates wOBA, wRC+, wRAA, and WAR for a batter.
        """
        h_1b = (stat.hits or 0) - (stat.doubles or 0) - (stat.triples or 0) - (stat.home_runs or 0)
        u_bb = (stat.walks or 0) - (stat.intentional_walks or 0)
        
        # 1. wOBA
        numerator = (0.69 * u_bb) + (0.72 * (stat.hbp or 0)) + (0.89 * h_1b) + \
                    (1.27 * (stat.doubles or 0)) + (1.62 * (stat.triples or 0)) + (2.10 * (stat.home_runs or 0))
        denominator = (stat.at_bats or 0) + u_bb + (stat.hbp or 0) + (stat.sacrifice_flies or 0)
        woba = round(numerator / denominator, 3) if denominator > 0 else 0.0
        
        # 2. wRAA (Weighted Runs Above Average)
        # wRAA = ((wOBA - League wOBA) / wOBA Scale) * PA
        wraa = round(((woba - lg['lg_woba']) / lg['woba_scale']) * (stat.plate_appearances or 0), 1)
        
        # 3. wRC+
        # wRC+ = (((wOBA - lgwOBA) / wOBA_scale) + (lgR / PA)) / (lgR / PA) * 100
        wrc_plus = round((((woba - lg['lg_woba']) / lg['woba_scale']) + lg['lg_r_per_pa']) / lg['lg_r_per_pa'] * 100) if lg['lg_r_per_pa'] > 0 else 100
        
        # 4. Batting WAR (Simplified: wRAA / RPW)
        war = round(wraa / lg['rpw'], 2)
        
        return {
            'woba': woba,
            'wraa': wraa,
            'wrc_plus': wrc_plus,
            'war': war
        }

    @staticmethod
    def calculate_pitching_metrics(stat: PlayerSeasonPitching, lg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates adjusted FIP and Pitching WAR.
        """
        ip = (stat.innings_outs or 0) / 3.0
        
        # 1. FIP
        if ip > 0:
            fip = round(((13 * (stat.home_runs_allowed or 0)) + (3 * ((stat.walks_allowed or 0) + (stat.hit_batters or 0))) - (2 * (stat.strikeouts or 0))) / ip + lg['fip_constant'], 2)
        else:
            fip = 0.0
            
        # 2. Pitching WAR (Simplified FIP-based)
        # (League ERA - FIP) / RPW * (IP / 9)
        # We use a baseline of league ERA for average
        lg_era = 4.5 # Default if not in lg
        # In a real model, we'd use lg_era - fip
        runs_prevented = (4.5 - fip) * (ip / 9.0)
        war = round(runs_prevented / (lg['rpw'] / 10.0), 2) # Scaled
        
        return {
            'fip_adj': fip,
            'war': war
        }
