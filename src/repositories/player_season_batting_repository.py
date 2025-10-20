"""
Player Season Batting Repository
UPSERT operations for player_season_batting table
"""
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.engine import SessionLocal, Engine
from src.models.player import PlayerSeasonBatting


class PlayerSeasonBattingRepository:
    """Repository for player_season_batting table operations"""

    def __init__(self):
        self.dialect = Engine.dialect.name

    def upsert_batting_stats(self, batting_stats: List[Dict[str, Any]]) -> int:
        """
        UPSERT player_season_batting records (idempotent)

        Args:
            batting_stats: List of batting stat dictionaries with keys:
                - player_id (required)
                - season (required)
                - league (default: "REGULAR")
                - level (default: "KBO1")
                - source (default: "CRAWLER")
                - team_code (optional)
                - batting stats fields (games, at_bats, hits, etc.)

        Returns:
            Number of batting stats upserted
        """
        if not batting_stats:
            return 0

        with SessionLocal() as session:
            try:
                for stats_data in batting_stats:
                    self._upsert_one(session, stats_data)
                session.commit()
                return len(batting_stats)
            except Exception as e:
                session.rollback()
                print(f"[ERROR] Error upserting batting stats: {e}")
                raise

    def _upsert_one(self, session: Session, stats_data: Dict[str, Any]):
        """UPSERT single batting stat record (SQLite/PostgreSQL compatible)"""
        data = {
            'player_id': stats_data['player_id'],
            'season': stats_data['season'],
            'league': stats_data.get('league', 'REGULAR'),
            'level': stats_data.get('level', 'KBO1'),
            'source': stats_data.get('source', 'CRAWLER'),
            'team_id': stats_data.get('team_code'),
            'games': stats_data.get('games'),
            'plate_appearances': stats_data.get('plate_appearances'),
            'at_bats': stats_data.get('at_bats'),
            'runs': stats_data.get('runs'),
            'hits': stats_data.get('hits'),
            'doubles': stats_data.get('doubles'),
            'triples': stats_data.get('triples'),
            'home_runs': stats_data.get('home_runs'),
            'rbi': stats_data.get('rbi'),
            'walks': stats_data.get('walks'),
            'intentional_walks': stats_data.get('intentional_walks'),
            'hbp': stats_data.get('hbp'),
            'strikeouts': stats_data.get('strikeouts'),
            'stolen_bases': stats_data.get('stolen_bases'),
            'caught_stealing': stats_data.get('caught_stealing'),
            'sacrifice_hits': stats_data.get('sacrifice_hits'),
            'sacrifice_flies': stats_data.get('sacrifice_flies'),
            'gdp': stats_data.get('gdp'),
            'avg': stats_data.get('avg'),
            'obp': stats_data.get('obp'),
            'slg': stats_data.get('slg'),
            'ops': stats_data.get('ops'),
            'iso': stats_data.get('iso'),
            'babip': stats_data.get('babip'),
            'extra_stats': stats_data.get('extra_stats'),
        }

        # Remove None values to avoid overwriting existing data with NULL
        data = {k: v for k, v in data.items() if v is not None}

        unique_keys = ['player_id', 'season', 'league', 'level']
        
        if self.dialect == "sqlite":
            stmt = sqlite_insert(PlayerSeasonBatting).values(**data)
            # Update all columns except unique constraint columns on conflict
            update_dict = {k: v for k, v in data.items() if k not in unique_keys}
            stmt = stmt.on_conflict_do_update(
                index_elements=unique_keys,
                set_=update_dict
            )
        else:  # PostgreSQL
            stmt = pg_insert(PlayerSeasonBatting).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k not in unique_keys}
            stmt = stmt.on_conflict_do_update(
                index_elements=unique_keys,
                set_=update_dict
            )

        session.execute(stmt)

    def get_by_player_season(self, player_id: int, season: int, 
                           league: str = "REGULAR", level: str = "KBO1") -> Optional[PlayerSeasonBatting]:
        """Get batting stats for specific player and season"""
        with SessionLocal() as session:
            return session.query(PlayerSeasonBatting).filter_by(
                player_id=player_id,
                season=season,
                league=league,
                level=level
            ).first()

    def get_by_player(self, player_id: int) -> List[PlayerSeasonBatting]:
        """Get all batting stats for a player across all seasons"""
        with SessionLocal() as session:
            return list(
                session.query(PlayerSeasonBatting)
                .filter_by(player_id=player_id)
                .order_by(PlayerSeasonBatting.season.desc())
                .all()
            )

    def get_by_season(self, season: int, league: str = "REGULAR", 
                     level: str = "KBO1", limit: int = None) -> List[PlayerSeasonBatting]:
        """Get all batting stats for a specific season"""
        with SessionLocal() as session:
            query = session.query(PlayerSeasonBatting).filter_by(
                season=season,
                league=league,
                level=level
            ).order_by(PlayerSeasonBatting.avg.desc().nulls_last())
            
            if limit:
                query = query.limit(limit)
            return list(query.all())

    def get_by_team_season(self, team_code: str, season: int, 
                          league: str = "REGULAR", level: str = "KBO1") -> List[PlayerSeasonBatting]:
        """Get batting stats for a team in a specific season"""
        with SessionLocal() as session:
            return list(
                session.query(PlayerSeasonBatting).filter_by(
                    team_code=team_code,
                    season=season,
                    league=league,
                    level=level
                ).order_by(PlayerSeasonBatting.avg.desc().nulls_last()).all()
            )

    def count(self, season: int = None) -> int:
        """Count total batting stat records (optionally by season)"""
        with SessionLocal() as session:
            query = session.query(PlayerSeasonBatting)
            if season:
                query = query.filter_by(season=season)
            return query.count()

    def delete_by_player_season(self, player_id: int, season: int, 
                               league: str = "REGULAR", level: str = "KBO1") -> bool:
        """Delete batting stats for specific player and season"""
        with SessionLocal() as session:
            try:
                deleted = session.query(PlayerSeasonBatting).filter_by(
                    player_id=player_id,
                    season=season,
                    league=league,
                    level=level
                ).delete()
                session.commit()
                return deleted > 0
            except Exception as e:
                session.rollback()
                print(f"[ERROR] Error deleting batting stats: {e}")
                raise