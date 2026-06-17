"""
Player Basic Repository
UPSERT operations for player_basic table
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)
from sqlalchemy import case
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.db.engine import Engine, SessionLocal
from src.models.player import PlayerBasic
from src.utils.player_validation import filter_valid_player_payloads, validate_player_payload


class PlayerBasicRepository:
    """Repository for player_basic table operations"""

    def __init__(self) -> None:
        self.dialect = Engine.dialect.name
        self.last_filter_counts: Counter = Counter()

    def upsert_players(self, players: list[dict[str, Any]]) -> int:
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
        self.last_filter_counts = Counter()
        if not players:
            return 0

        with SessionLocal() as session:
            try:
                valid_players, filter_counts = filter_valid_player_payloads(players)
                self.last_filter_counts = filter_counts
                rows = self._unique_payload_rows(valid_players)
                if not rows:
                    return 0

                session.execute(self._build_upsert_stmt(rows))
                session.commit()
                return len(rows)
            except SQLAlchemyError:
                session.rollback()
                logger.exception("[ERROR] Error upserting players")
                raise

    def _upsert_one(self, session: Session, player_data: dict[str, Any]) -> None:
        """UPSERT single player (SQLite/PostgreSQL compatible)"""
        ok, reason = validate_player_payload(player_data)
        if not ok:
            self.last_filter_counts[reason or "invalid_player_payload"] += 1
            return

        data = self._build_payload(player_data)
        session.execute(self._build_upsert_stmt(data))

    def _unique_payload_rows(self, players: list[dict[str, Any]]) -> list[dict[str, Any]]:
        unique_payload = {}
        for player_data in players:
            row = self._build_payload(player_data)
            player_id = row.get("player_id")
            if player_id is not None:
                unique_payload[player_id] = row
        return list(unique_payload.values())

    def _build_upsert_stmt(self, values: dict[str, Any] | list[dict[str, Any]]):
        keys = values[0] if isinstance(values, list) else values
        if self.dialect == "sqlite":
            stmt = sqlite_insert(PlayerBasic).values(values)
            return stmt.on_conflict_do_update(
                index_elements=["player_id"],
                set_=self._build_status_preserving_update_dict(keys, stmt.excluded),
            )
        if self.dialect == "mysql":
            stmt = mysql_insert(PlayerBasic).values(values)
            update_dict = {key: stmt.inserted[key] for key in keys if key != "player_id"}
            return stmt.on_duplicate_key_update(update_dict)

        stmt = pg_insert(PlayerBasic).values(values)
        return stmt.on_conflict_do_update(
            index_elements=["player_id"],
            set_=self._build_status_preserving_update_dict(keys, stmt.excluded),
        )

    def _build_status_preserving_update_dict(self, keys: dict[str, Any], excluded: Any) -> dict[str, Any]:
        status_case = case(
            (excluded.status_source.in_(["profile", "register"]), excluded.status),
            (PlayerBasic.status_source.in_(["profile", "register"]), PlayerBasic.status),
            else_=excluded.status,
        )
        staff_role_case = case(
            (excluded.status_source.in_(["profile", "register"]), excluded.staff_role),
            (PlayerBasic.status_source.in_(["profile", "register"]), PlayerBasic.staff_role),
            else_=excluded.staff_role,
        )
        status_source_case = case(
            (excluded.status_source.in_(["profile", "register"]), excluded.status_source),
            (PlayerBasic.status_source.in_(["profile", "register"]), PlayerBasic.status_source),
            else_=excluded.status_source,
        )
        special_cases = {"status": status_case, "staff_role": staff_role_case, "status_source": status_source_case}
        return {key: special_cases.get(key, excluded[key]) for key in keys if key != "player_id"}

    def _build_payload(self, player_data: dict[str, Any]) -> dict[str, Any]:
        return {
            "player_id": player_data["player_id"],
            "name": player_data["name"],
            "uniform_no": player_data.get("uniform_no"),
            "team": player_data.get("team"),
            "position": player_data.get("position"),
            "birth_date": player_data.get("birth_date"),
            "birth_date_date": player_data.get("birth_date_date"),
            "height_cm": player_data.get("height_cm"),
            "weight_kg": player_data.get("weight_kg"),
            "career": player_data.get("career"),
            "status": player_data.get("status"),
            "staff_role": player_data.get("staff_role"),
            "status_source": player_data.get("status_source"),
            "photo_url": player_data.get("photo_url"),
            "bats": player_data.get("bats"),
            "throws": player_data.get("throws"),
            "debut_year": player_data.get("debut_year"),
            "salary_original": player_data.get("salary_original"),
            "signing_bonus_original": player_data.get("signing_bonus_original"),
            "draft_info": player_data.get("draft_info"),
            "salary_amount": player_data.get("salary_amount"),
            "salary_currency": player_data.get("salary_currency"),
            "signing_bonus_amount": player_data.get("signing_bonus_amount"),
            "signing_bonus_currency": player_data.get("signing_bonus_currency"),
            "draft_year": player_data.get("draft_year"),
            "draft_round": player_data.get("draft_round"),
            "draft_pick_overall": player_data.get("draft_pick_overall"),
            "draft_type": player_data.get("draft_type"),
            "education_path": player_data.get("education_path"),
        }

    def get_all(self, limit: int = None) -> list[PlayerBasic]:
        """Get all players (optionally limited)"""
        with SessionLocal() as session:
            query = session.query(PlayerBasic)
            if limit:
                query = query.limit(limit)
            return list(query.all())

    def update_statuses(self, updates: list[dict[str, Any]]) -> int:
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
                        },
                    )
                session.commit()
                return len(updates)
            except SQLAlchemyError:
                session.rollback()
                raise

    def get_by_id(self, player_id: int) -> PlayerBasic | None:
        """Get player by ID"""
        with SessionLocal() as session:
            return session.query(PlayerBasic).filter_by(player_id=player_id).first()

    def get_by_team(self, team: str, limit: int = None) -> list[PlayerBasic]:
        """Get players by team"""
        with SessionLocal() as session:
            query = session.query(PlayerBasic).filter_by(team=team)
            if limit:
                query = query.limit(limit)
            return list(query.all())


def save_player_basic(player_data: dict[str, Any]) -> int:
    """Helper function to save a single player's basic profile."""
    repo = PlayerBasicRepository()
    return repo.upsert_players([player_data])
