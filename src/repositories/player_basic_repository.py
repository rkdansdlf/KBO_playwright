"""
Player Basic Repository
UPSERT operations for player_basic table
"""
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.db.engine import SessionLocal, Engine
from src.models.player import PlayerBasic


class PlayerBasicRepository:
    """Repository for player_basic table operations"""

    def __init__(self):
        self.dialect = Engine.dialect.name

    def upsert_players(self, players: List[Dict[str, Any]]) -> int:
        """
        UPSERT player_basic records (idempotent)

        Args:
            players: List of player dictionaries with keys:
                - player_id (required)
                - name (required)
                - uniform_no, team, position (optional)
                - birth_date, birth_date_date (optional)
                - height_cm, weight_kg (optional)
                - career (optional)
                - status, staff_role, status_source (optional)

        Returns:
            Number of players upserted
        """
        if not players:
            return 0

        with SessionLocal() as session:
            try:
                payload = [self._build_payload(player_data) for player_data in players]
                unique_payload = {}
                for row in payload:
                    player_id = row.get("player_id")
                    if player_id is None:
                        continue
                    unique_payload[player_id] = row
                rows = list(unique_payload.values())
                if not rows:
                    return 0

                if self.dialect == "sqlite":
                    stmt = sqlite_insert(PlayerBasic).values(rows)
                    update_dict = {k: stmt.excluded[k] for k in rows[0].keys() if k != "player_id"}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["player_id"],
                        set_=update_dict,
                    )
                elif self.dialect == "mysql":
                    stmt = mysql_insert(PlayerBasic).values(rows)
                    update_dict = {k: stmt.inserted[k] for k in rows[0].keys() if k != "player_id"}
                    stmt = stmt.on_duplicate_key_update(update_dict)
                else:  # PostgreSQL
                    stmt = pg_insert(PlayerBasic).values(rows)
                    update_dict = {k: stmt.excluded[k] for k in rows[0].keys() if k != "player_id"}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=["player_id"],
                        set_=update_dict,
                    )

                session.execute(stmt)
                session.commit()
                return len(rows)
            except Exception as e:
                session.rollback()
                print(f"[ERROR] Error upserting players: {e}")
                raise

    def _upsert_one(self, session: Session, player_data: Dict[str, Any]):
        """UPSERT single player (SQLite/PostgreSQL compatible)"""
        data = self._build_payload(player_data)

        if self.dialect == "sqlite":
            stmt = sqlite_insert(PlayerBasic).values(**data)
            # Update all columns except player_id on conflict
            update_dict = {k: v for k, v in data.items() if k != 'player_id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id'],
                set_=update_dict
            )
        elif self.dialect == "mysql":
            stmt = mysql_insert(PlayerBasic).values(**data)
            update_dict = {k: stmt.inserted[k] for k in data.keys() if k != 'player_id'}
            stmt = stmt.on_duplicate_key_update(update_dict)
        else:  # PostgreSQL
            stmt = pg_insert(PlayerBasic).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data.keys() if k != 'player_id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['player_id'],
                set_=update_dict
            )

        session.execute(stmt)

    def _build_payload(self, player_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'player_id': player_data['player_id'],
            'name': player_data['name'],
            'uniform_no': player_data.get('uniform_no'),
            'team': player_data.get('team'),
            'position': player_data.get('position'),
            'birth_date': player_data.get('birth_date'),
            'birth_date_date': player_data.get('birth_date_date'),
            'height_cm': player_data.get('height_cm'),
            'weight_kg': player_data.get('weight_kg'),
            'career': player_data.get('career'),
            'status': player_data.get('status'),
            'staff_role': player_data.get('staff_role'),
            'status_source': player_data.get('status_source'),
        }

    def get_all(self, limit: int = None) -> List[PlayerBasic]:
        """Get all players (optionally limited)"""
        with SessionLocal() as session:
            query = session.query(PlayerBasic)
            if limit:
                query = query.limit(limit)
            return list(query.all())

    def update_statuses(self, updates: List[Dict[str, Any]]) -> int:
        """Update status/staff_role/status_source for existing players."""
        if not updates:
            return 0
        with SessionLocal() as session:
            try:
                for entry in updates:
                    player_id = entry.get("player_id")
                    if not player_id:
                        continue
                    session.query(PlayerBasic).filter_by(player_id=player_id).update(
                        {
                            "status": entry.get("status"),
                            "staff_role": entry.get("staff_role"),
                            "status_source": entry.get("status_source"),
                        }
                    )
                session.commit()
                return len(updates)
            except Exception:
                session.rollback()
                raise

    def get_by_id(self, player_id: int) -> PlayerBasic | None:
        """Get player by ID"""
        with SessionLocal() as session:
            return session.query(PlayerBasic).filter_by(player_id=player_id).first()

    def get_by_team(self, team: str, limit: int = None) -> List[PlayerBasic]:
        """Get players by team"""
        with SessionLocal() as session:
            query = session.query(PlayerBasic).filter_by(team=team)
            if limit:
                query = query.limit(limit)
            return list(query.all())

    def count(self) -> int:
        """Count total players"""
        with SessionLocal() as session:
            return session.query(PlayerBasic).count()
