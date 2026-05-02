from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, Integer
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.season import KboSeason
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

class SeasonStatAggregator:
    """
    Service to aggregate transactional game stats into season-level cumulative stats.
    Acts as a fallback when KBO's cumulative record pages are unavailable.
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
    def aggregate_batting_season(session: Session, player_id: int, year: int, series: str) -> Optional[Dict[str, Any]]:
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        
        query = (
            session.query(
                func.count(GameBattingStat.id).label('games'),
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
            .filter(GameBattingStat.player_id == player_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
        )
        
        row = query.one_or_none()
        if not row or row.games == 0:
            return None
            
        data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
        
        # Calculate ratios
        ratios = BattingStatCalculator.calculate_ratios(data)
        data.update(ratios)
        
        # Metadata
        data.update({
            'player_id': player_id,
            'season': year,
            'league': series.upper(),
            'source': 'FALLBACK'
        })
        
        return data

    @staticmethod
    def aggregate_batting_season_bulk(session: Session, year: int, series: str) -> List[Dict[str, Any]]:
        """
        Aggregate batting stats for all players in a season/series in a single query.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        print(f"🚀 [BULK] Aggregating batting stats for {year} {series}...")
        
        query = (
            session.query(
                GameBattingStat.player_id,
                func.count(GameBattingStat.id).label('games'),
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
            .group_by(GameBattingStat.player_id)
        )
        
        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
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
    def aggregate_pitching_season(session: Session, player_id: int, year: int, series: str) -> Optional[Dict[str, Any]]:
        pattern = SeasonStatAggregator._get_league_name_pattern(series)

        query = (
            session.query(
                func.count(GamePitchingStat.id).label('games'),
                func.sum(func.cast(GamePitchingStat.is_starting, Integer)).label('games_started'),
                func.sum(GamePitchingStat.wins).label('wins'),
                func.sum(GamePitchingStat.losses).label('losses'),
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
            .filter(GamePitchingStat.player_id == player_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
        )

        row = query.one_or_none()
        if not row or row.games == 0:
            return None

        data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
        
        # Calculate derived fields
        data['innings_pitched'] = round(data['innings_outs'] / 3.0, 1) # Simple representation
        
        # Calculate ratios
        ratios = PitchingStatCalculator.calculate_ratios(data)
        data.update(ratios)
        
        # Metadata
        data.update({
            'player_id': player_id,
            'season': year,
            'league': series.upper(),
            'source': 'FALLBACK'
        })
        
        return data

    @staticmethod
    def aggregate_pitching_season_bulk(session: Session, year: int, series: str) -> List[Dict[str, Any]]:
        """
        Aggregate pitching stats for all players in a season/series in a single query.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        print(f"🚀 [BULK] Aggregating pitching stats for {year} {series}...")

        query = (
            session.query(
                GamePitchingStat.player_id,
                func.count(GamePitchingStat.id).label('games'),
                func.sum(func.cast(GamePitchingStat.is_starting, Integer)).label('games_started'),
                func.sum(GamePitchingStat.wins).label('wins'),
                func.sum(GamePitchingStat.losses).label('losses'),
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
            .group_by(GamePitchingStat.player_id)
        )

        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
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
