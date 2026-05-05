from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, Integer
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.season import KboSeason
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

class TeamStatAggregator:
    """
    Service to aggregate transactional game stats into team-level season stats.
    Acts as a fallback when KBO's team record pages are unavailable.
    """

    @staticmethod
    def _get_league_name_pattern(series: str) -> str:
        series_map = {
            'regular': '정규시즌',
            'wildcard': '와일드카드',
            'semi_playoff': '준플레이오프',
            'playoff': '플레이오프',
            'korean_series': '한국시리즈'
        }
        return series_map.get(series.lower(), series)

    @staticmethod
    def aggregate_team_batting(session: Session, year: int, series: str) -> List[Dict[str, Any]]:
        """
        Aggregate team batting stats for a season/series.
        """
        pattern = TeamStatAggregator._get_league_name_pattern(series)
        print(f"🚀 Aggregating team batting stats for {year} {series}...")
        
        # We aggregate from GameBattingStat to get accurate team totals
        query = (
            session.query(
                GameBattingStat.team_code.label('team_id'),
                func.count(func.distinct(GameBattingStat.game_id)).label('games'),
                func.sum(GameBattingStat.plate_appearances).label('plate_appearances'),
                func.sum(GameBattingStat.at_bats).label('at_bats'),
                func.sum(GameBattingStat.runs).label('runs'),
                func.sum(GameBattingStat.hits).label('hits'),
                func.sum(GameBattingStat.doubles).label('doubles'),
                func.sum(GameBattingStat.triples).label('triples'),
                func.sum(GameBattingStat.home_runs).label('home_runs'),
                func.sum(GameBattingStat.rbi).label('rbi'),
                func.sum(GameBattingStat.walks).label('walks'),
                func.sum(GameBattingStat.intentional_walks).label('intentional_walks'),
                func.sum(GameBattingStat.hbp).label('hbp'),
                func.sum(GameBattingStat.strikeouts).label('strikeouts'),
                func.sum(GameBattingStat.stolen_bases).label('stolen_bases'),
                func.sum(GameBattingStat.caught_stealing).label('caught_stealing'),
                func.sum(GameBattingStat.sacrifice_hits).label('sacrifice_hits'),
                func.sum(GameBattingStat.sacrifice_flies).label('sacrifice_flies'),
                func.sum(GameBattingStat.gdp).label('gdp')
            )
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GameBattingStat.team_code)
        )
        
        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
            # Calculate team ratios
            ratios = BattingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            data.update({
                'season': year,
                'league': series.upper(),
                'source': 'FALLBACK'
            })
            results.append(data)
        
        return results

    @staticmethod
    def aggregate_team_pitching(session: Session, year: int, series: str) -> List[Dict[str, Any]]:
        """
        Aggregate team pitching stats for a season/series.
        """
        pattern = TeamStatAggregator._get_league_name_pattern(series)
        print(f"🚀 Aggregating team pitching stats for {year} {series}...")

        # 1. Base counting stats from GamePitchingStat
        query = (
            session.query(
                GamePitchingStat.team_code.label('team_id'),
                func.sum(GamePitchingStat.saves).label('saves'),
                func.sum(GamePitchingStat.holds).label('holds'),
                func.sum(GamePitchingStat.innings_outs).label('innings_outs'),
                func.sum(GamePitchingStat.hits_allowed).label('hits_allowed'),
                func.sum(GamePitchingStat.runs_allowed).label('runs_allowed'),
                func.sum(GamePitchingStat.earned_runs).label('earned_runs'),
                func.sum(GamePitchingStat.home_runs_allowed).label('home_runs_allowed'),
                func.sum(GamePitchingStat.walks_allowed).label('walks_allowed'),
                func.sum(GamePitchingStat.hit_batters).label('hit_batters'),
                func.sum(GamePitchingStat.strikeouts).label('strikeouts'),
                func.sum(GamePitchingStat.wild_pitches).label('wild_pitches'),
                func.sum(GamePitchingStat.balks).label('balks'),
                func.sum(GamePitchingStat.batters_faced).label('tbf'),
                func.sum(GamePitchingStat.pitches).label('np')
            )
            .join(Game, GamePitchingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GamePitchingStat.team_code)
        )
        
        pitching_stats_map = {row.team_id: row._asdict() for row in query.all()}

        # 2. Wins/Losses/Ties from Game table
        # We need to consider both home and away games for each team
        game_results_query = (
            session.query(
                Game.game_id,
                Game.home_team,
                Game.away_team,
                Game.winning_team,
                Game.game_status
            )
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .filter(Game.game_status == 'COMPLETED')
        )
        
        team_results = {}
        for game in game_results_query.all():
            for team in [game.home_team, game.away_team]:
                if team not in team_results:
                    team_results[team] = {'wins': 0, 'losses': 0, 'ties': 0, 'games': 0}
                
                res = team_results[team]
                res['games'] += 1
                if game.winning_team == team:
                    res['wins'] += 1
                elif game.winning_team == 'TIE' or (not game.winning_team and game.game_status == 'COMPLETED'):
                    # Handle ties (some status might be COMPLETED but no winning_team)
                    res['ties'] += 1
                elif game.winning_team: # Someone else won, so this team lost
                    res['losses'] += 1

        results = []
        for team_id, p_data in pitching_stats_map.items():
            data = {k: (v if v is not None else 0) for k, v in p_data.items()}
            
            # Merge W-L-T
            res = team_results.get(team_id, {'wins': 0, 'losses': 0, 'ties': 0, 'games': 0})
            data.update(res)
            
            data['innings_pitched'] = round(data['innings_outs'] / 3.0, 1)
            ratios = PitchingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            
            data.update({
                'season': year,
                'league': series.upper(),
                'source': 'FALLBACK'
            })
            results.append(data)

        return results
