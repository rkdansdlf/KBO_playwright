"""
Game repository for database operations
Implements UPSERT operations for idempotent data saving
"""
from typing import List, Dict, Optional, Any
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import select, update, func

from src.db.engine import SessionLocal, Engine
from src.models.game import (
    GameSchedule,
    Game,
    GameLineup,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.validators.game_data_validator import validate_game_data


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, "", "-", "null"):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "-", "null"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _parse_duration_to_minutes(duration_text: Optional[str]) -> Optional[int]:
    if not duration_text:
        return None
    parts = duration_text.strip().split(":")
    if len(parts) == 2:
        try:
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except ValueError:
            return None
    return None


def _parse_start_time(game_date: date, time_text: Optional[str]) -> Optional[datetime]:
    if not time_text:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            dt = datetime.strptime(time_text.strip(), fmt)
            return datetime.combine(game_date, dt.time())
        except ValueError:
            continue
    return None


def _parse_player_id(raw_id: Any) -> Optional[int]:
    if raw_id is None:
        return None
    raw = str(raw_id).strip()
    if not raw.isdigit():
        return None
    try:
        return int(raw)
    except ValueError:
        return None


class GameRepository:
    """Repository for game-related database operations"""

    def __init__(self):
        self.dialect = Engine.dialect.name

    def fetch_schedules(self, status: str = "pending", limit: Optional[int] = 20) -> List[GameSchedule]:
        """Fetch schedules by crawl status ordered by date/game."""
        with SessionLocal() as session:
            stmt = select(GameSchedule).where(GameSchedule.crawl_status == status).order_by(
                GameSchedule.game_date, GameSchedule.game_id
            )
            if limit:
                stmt = stmt.limit(limit)
            return list(session.scalars(stmt))

    def save_schedules(self, schedules: List[Dict]) -> int:
        """
        Save game schedules with UPSERT (idempotent)

        Args:
            schedules: List of schedule dictionaries

        Returns:
            Number of schedules saved
        """
        saved = 0
        with SessionLocal() as session:
            try:
                for schedule in schedules:
                    self._upsert_schedule(session, schedule)
                    saved += 1
                session.commit()
                print(f"✅ Saved {saved} schedules to database")
            except Exception as e:
                session.rollback()
                print(f"❌ Error saving schedules: {e}")
                raise
        return saved

    def _upsert_schedule(self, session: Session, schedule: Dict):
        """UPSERT single schedule (SQLite/MySQL compatible)"""
        from datetime import datetime

        # Convert game_date string (YYYYMMDD) to date object
        game_date_str = schedule.get('game_date')
        if isinstance(game_date_str, str) and len(game_date_str) == 8:
            game_date = datetime.strptime(game_date_str, '%Y%m%d').date()
        else:
            game_date = game_date_str

        data = {
            'game_id': schedule['game_id'],
            'season_year': schedule.get('season_year', 2024),
            'season_type': schedule.get('season_type', 'regular'),
            'game_date': game_date,
            'game_status': schedule.get('game_status', 'scheduled'),
            'crawl_status': schedule.get('crawl_status', 'pending'),
            'home_team_code': schedule.get('home_team_code'),
            'away_team_code': schedule.get('away_team_code'),
            'doubleheader_no': schedule.get('doubleheader_no', 0),
        }

        if self.dialect == "sqlite":
            stmt = sqlite_insert(GameSchedule).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id'],
                set_={
                    'game_status': stmt.excluded.game_status,
                    'crawl_status': stmt.excluded.crawl_status,
                    'home_team_code': stmt.excluded.home_team_code,
                    'away_team_code': stmt.excluded.away_team_code,
                    'doubleheader_no': stmt.excluded.doubleheader_no,
                    'updated_at': datetime.now()
                }
            )
        else:  # MySQL
            stmt = mysql_insert(GameSchedule).values(**data)
            stmt = stmt.on_duplicate_key_update(
                game_status=stmt.inserted.game_status,
                crawl_status=stmt.inserted.crawl_status,
                home_team_code=stmt.inserted.home_team_code,
                away_team_code=stmt.inserted.away_team_code,
                doubleheader_no=stmt.inserted.doubleheader_no,
                updated_at=datetime.now()
            )

        session.execute(stmt)

    def save_game_detail(self, game_data: Dict) -> bool:
        """
        Save game detail data (metadata, lineup, stats)

        Args:
            game_data: Game data dictionary from crawler

        Returns:
            True if successful
        """
        with SessionLocal() as session:
            try:
                valid, validation_errors = validate_game_data(game_data)
                if not valid:
                    error_msg = "; ".join(validation_errors)
                    print(f"❌ Validation failed for {game_data['game_id']}: {error_msg}")
                    self.update_crawl_status(game_data['game_id'], 'failed', error_msg)
                    return False

                # Save game metadata
                self._upsert_game(session, game_data)

                # Save player stats
                self._save_player_stats(session, game_data)

                session.commit()
                print(f"✅ Saved game detail: {game_data['game_id']}")
                self.update_crawl_status(game_data['game_id'], 'crawled')
                return True
            except Exception as e:
                session.rollback()
                error_msg = str(e)
                print(f"❌ Error saving game detail: {error_msg}")
                import traceback
                traceback.print_exc()
                self.update_crawl_status(game_data['game_id'], 'failed', error_msg)
                return False

    def _upsert_game(self, session: Session, game_data: Dict):
        """UPSERT game metadata"""
        metadata = game_data.get('metadata', {})
        teams = game_data.get('teams', {})

        game_date_value = game_data.get('game_date')
        if isinstance(game_date_value, date):
            game_date_obj = game_date_value
        elif isinstance(game_date_value, str) and len(game_date_value) == 8:
            game_date_obj = datetime.strptime(game_date_value, '%Y%m%d').date()
        else:
            game_date_obj = None

        home_team = teams.get('home', {})
        away_team = teams.get('away', {})

        home_team_code = home_team.get('code') or game_data.get('home_team_code')
        away_team_code = away_team.get('code') or game_data.get('away_team_code')

        started_at = _parse_start_time(game_date_obj, metadata.get('start_time')) if game_date_obj else None
        ended_at = _parse_start_time(game_date_obj, metadata.get('end_time')) if game_date_obj else None
        duration_min = metadata.get('duration_minutes')
        if duration_min is None:
            duration_min = _parse_duration_to_minutes(metadata.get('game_time'))

        data = {
            'game_id': game_data['game_id'],
            'game_date': game_date_obj,
            'home_team_code': home_team_code,
            'away_team_code': away_team_code,
            'started_at': started_at,
            'ended_at': ended_at,
            'duration_min': duration_min,
            'attendance': metadata.get('attendance'),
            'weather': metadata.get('weather'),
            'stadium': metadata.get('stadium'),
            'home_score': _safe_int(home_team.get('score')),
            'away_score': _safe_int(away_team.get('score')),
        }

        if self.dialect == "sqlite":
            stmt = sqlite_insert(Game).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id'],
                set_={
                    'game_date': stmt.excluded.game_date,
                    'home_team_code': stmt.excluded.home_team_code,
                    'away_team_code': stmt.excluded.away_team_code,
                    'started_at': stmt.excluded.started_at,
                    'ended_at': stmt.excluded.ended_at,
                    'duration_min': stmt.excluded.duration_min,
                    'attendance': stmt.excluded.attendance,
                    'weather': stmt.excluded.weather,
                    'stadium': stmt.excluded.stadium,
                    'home_score': stmt.excluded.home_score,
                    'away_score': stmt.excluded.away_score,
                    'updated_at': datetime.now()
                }
            )
        else:  # MySQL
            stmt = mysql_insert(Game).values(**data)
            stmt = stmt.on_duplicate_key_update(
                game_date=stmt.inserted.game_date,
                home_team_code=stmt.inserted.home_team_code,
                away_team_code=stmt.inserted.away_team_code,
                started_at=stmt.inserted.started_at,
                ended_at=stmt.inserted.ended_at,
                duration_min=stmt.inserted.duration_min,
                attendance=stmt.inserted.attendance,
                weather=stmt.inserted.weather,
                stadium=stmt.inserted.stadium,
                home_score=stmt.inserted.home_score,
                away_score=stmt.inserted.away_score,
                updated_at=datetime.now()
            )

        session.execute(stmt)

    def _save_player_stats(self, session: Session, game_data: Dict):
        """Save player game statistics (hitters and pitchers)"""
        game_id = game_data['game_id']

        # Save hitters
        for team_side in ['away', 'home']:
            hitters = game_data.get('hitters', {}).get(team_side, [])
            for idx, hitter in enumerate(hitters, start=1):
                self._upsert_game_batting(session, game_id, team_side, idx, hitter)

        # Save pitchers
        for team_side in ['away', 'home']:
            pitchers = game_data.get('pitchers', {}).get(team_side, [])
            for idx, pitcher in enumerate(pitchers, start=1):
                self._upsert_game_pitching(session, game_id, team_side, idx, pitcher)

    def _upsert_game_batting(
        self,
        session: Session,
        game_id: str,
        team_side: str,
        appearance_seq: int,
        payload: Dict[str, Any],
    ) -> None:
        player_id = _parse_player_id(payload.get('player_id'))
        stats = payload.get('stats', {}) or {}

        data = {
            'game_id': game_id,
            'player_id': player_id,
            'player_name': payload.get('player_name'),
            'team_side': team_side,
            'team_code': payload.get('team_code'),
            'batting_order': payload.get('batting_order'),
            'appearance_seq': payload.get('appearance_seq', appearance_seq),
            'position': payload.get('position'),
            'is_starter': int(bool(payload.get('is_starter', False))),
            'source': payload.get('source', 'GAMECENTER'),
            'plate_appearances': _safe_int(stats.get('plate_appearances')),
            'at_bats': _safe_int(stats.get('at_bats')),
            'runs': _safe_int(stats.get('runs')),
            'hits': _safe_int(stats.get('hits')),
            'doubles': _safe_int(stats.get('doubles')),
            'triples': _safe_int(stats.get('triples')),
            'home_runs': _safe_int(stats.get('home_runs')),
            'rbi': _safe_int(stats.get('rbi')),
            'walks': _safe_int(stats.get('walks')),
            'intentional_walks': _safe_int(stats.get('intentional_walks')),
            'hbp': _safe_int(stats.get('hbp')),
            'strikeouts': _safe_int(stats.get('strikeouts')),
            'stolen_bases': _safe_int(stats.get('stolen_bases')),
            'caught_stealing': _safe_int(stats.get('caught_stealing')),
            'sacrifice_hits': _safe_int(stats.get('sacrifice_hits')),
            'sacrifice_flies': _safe_int(stats.get('sacrifice_flies')),
            'gdp': _safe_int(stats.get('gdp')),
            'avg': _safe_float(stats.get('avg')),
            'obp': _safe_float(stats.get('obp')),
            'slg': _safe_float(stats.get('slg')),
            'ops': _safe_float(stats.get('ops')),
            'iso': _safe_float(stats.get('iso')),
            'babip': _safe_float(stats.get('babip')),
            'extras': payload.get('extras'),
        }

        columns_to_update = {key: value for key, value in data.items() if key not in {'game_id', 'player_id'} and value is not None}

        if self.dialect == "sqlite":
            stmt = sqlite_insert(PlayerGameBatting).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id', 'player_id'],
                set_=columns_to_update | {'updated_at': datetime.now()}
            )
        else:
            stmt = mysql_insert(PlayerGameBatting).values(**data)
            stmt = stmt.on_duplicate_key_update(**columns_to_update, updated_at=datetime.now())

        session.execute(stmt)

    def _upsert_game_pitching(
        self,
        session: Session,
        game_id: str,
        team_side: str,
        appearance_seq: int,
        payload: Dict[str, Any],
    ) -> None:
        player_id = _parse_player_id(payload.get('player_id'))
        stats = payload.get('stats', {}) or {}

        data = {
            'game_id': game_id,
            'player_id': player_id,
            'player_name': payload.get('player_name'),
            'team_side': team_side,
            'team_code': payload.get('team_code'),
            'is_starting': int(bool(payload.get('is_starting', False))),
            'appearance_seq': payload.get('appearance_seq', appearance_seq),
            'source': payload.get('source', 'GAMECENTER'),
            'innings_outs': _safe_int(stats.get('innings_outs')),
            'hits_allowed': _safe_int(stats.get('hits_allowed')),
            'runs_allowed': _safe_int(stats.get('runs_allowed')),
            'earned_runs': _safe_int(stats.get('earned_runs')),
            'home_runs_allowed': _safe_int(stats.get('home_runs_allowed')),
            'walks_allowed': _safe_int(stats.get('walks_allowed')),
            'strikeouts': _safe_int(stats.get('strikeouts')),
            'hit_batters': _safe_int(stats.get('hit_batters')),
            'wild_pitches': _safe_int(stats.get('wild_pitches')),
            'balks': _safe_int(stats.get('balks')),
            'wins': _safe_int(stats.get('wins')),
            'losses': _safe_int(stats.get('losses')),
            'saves': _safe_int(stats.get('saves')),
            'holds': _safe_int(stats.get('holds')),
            'decision': stats.get('decision'),
            'batters_faced': _safe_int(stats.get('batters_faced')),
            'era': _safe_float(stats.get('era')),
            'whip': _safe_float(stats.get('whip')),
            'fip': _safe_float(stats.get('fip')),
            'k_per_nine': _safe_float(stats.get('k_per_nine')),
            'bb_per_nine': _safe_float(stats.get('bb_per_nine')),
            'kbb': _safe_float(stats.get('kbb')),
            'extras': payload.get('extras'),
        }

        columns_to_update = {key: value for key, value in data.items() if key not in {'game_id', 'player_id'} and value is not None}

        if self.dialect == "sqlite":
            stmt = sqlite_insert(PlayerGamePitching).values(**data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['game_id', 'player_id'],
                set_=columns_to_update | {'updated_at': datetime.now()}
            )
        else:
            stmt = mysql_insert(PlayerGamePitching).values(**data)
            stmt = stmt.on_duplicate_key_update(**columns_to_update, updated_at=datetime.now())

        session.execute(stmt)

    def update_crawl_status(self, game_id: str, status: str, error_msg: Optional[str] = None):
        """Update crawl status for a game"""
        with SessionLocal() as session:
            try:
                stmt = (
                    update(GameSchedule)
                    .where(GameSchedule.game_id == game_id)
                    .values(
                        crawl_status=status,
                        last_crawl_attempt=datetime.now(),
                        crawl_error_message=error_msg
                    )
                )
                session.execute(stmt)
                session.commit()
            except Exception as exc:  # pragma: no cover - logging path
                session.rollback()
                print(f"❌ Error updating crawl status for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # Reporting helpers
    # ------------------------------------------------------------------
    def count_schedules_by_type(self) -> Dict[str, int]:
        with SessionLocal() as session:
            rows = session.execute(
                select(GameSchedule.season_type, func.count())
                .group_by(GameSchedule.season_type)
            ).all()
            return {season_type: count for season_type, count in rows}

    def get_game_summary(self, game_id: str) -> Dict[str, Any]:
        with SessionLocal() as session:
            schedule = session.execute(
                select(GameSchedule).where(GameSchedule.game_id == game_id)
            ).scalar_one_or_none()

            game = session.execute(
                select(Game).where(Game.game_id == game_id)
            ).scalar_one_or_none()

            batting_count = session.execute(
                select(func.count()).select_from(PlayerGameBatting).where(PlayerGameBatting.game_id == game_id)
            ).scalar()

            pitching_count = session.execute(
                select(func.count()).select_from(PlayerGamePitching).where(PlayerGamePitching.game_id == game_id)
            ).scalar()

            return {
                "schedule": schedule,
                "game": game,
                "batting_rows": batting_count or 0,
                "pitching_rows": pitching_count or 0,
            }

    def get_pending_games(self, season_type: str = 'regular', limit: int = 100) -> List[Dict]:
        """
        Get games that are ready to be crawled

        Args:
            season_type: Type of season (preseason, regular, postseason)
            limit: Maximum number of games to return

        Returns:
            List of game dictionaries
        """
        with SessionLocal() as session:
            stmt = (
                select(GameSchedule)
                .where(GameSchedule.season_type == season_type)
                .where(GameSchedule.crawl_status.in_(['pending', 'ready']))
                .limit(limit)
            )
            results = session.execute(stmt).scalars().all()

            games = []
            for game in results:
                games.append({
                    'game_id': game.game_id,
                    'game_date': game.game_date,
                    'season_type': game.season_type
                })

            return games
