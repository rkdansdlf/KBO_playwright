"""
Repository utilities for player domain (profiles, identities, seasons).
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal, Engine
from src.models.player import (
    Player,
    PlayerIdentity,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
)
from src.parsers.player_profile_parser import PlayerProfileParsed


class PlayerRepository:
    """Persist player-related entities with SQLite/MySQL compatible UPSERT logic."""

    def __init__(self) -> None:
        self.dialect = Engine.dialect.name

    # ------------------------------------------------------------------
    # Profile / identity handling
    # ------------------------------------------------------------------
    def upsert_player_profile(
        self, kbo_player_id: str, profile: PlayerProfileParsed
    ) -> Optional[Player]:
        """
        Upsert player and primary identity based on parsed profile info.
        """
        if not kbo_player_id:
            raise ValueError("kbo_player_id is required to upsert a player profile")

        with SessionLocal() as session:
            player = self._get_or_create_player(session, kbo_player_id)
            self._apply_profile_fields(player, profile)
            self._upsert_identity(session, player, profile)
            session.commit()
            session.refresh(player)
            return player

    def _get_or_create_player(self, session: Session, kbo_player_id: str) -> Player:
        player = session.execute(
            select(Player).where(Player.kbo_person_id == kbo_player_id)
        ).scalar_one_or_none()
        if player is None:
            player = Player(kbo_person_id=kbo_player_id)
            session.add(player)
            session.flush()
        return player

    def _apply_profile_fields(self, player: Player, profile: PlayerProfileParsed) -> None:
        if profile.birth_date:
            try:
                player.birth_date = datetime.strptime(profile.birth_date, "%Y-%m-%d").date()
            except ValueError:
                pass
        player.height_cm = profile.height_cm if profile.height_cm else player.height_cm
        player.weight_kg = profile.weight_kg if profile.weight_kg else player.weight_kg
        player.bats = profile.batting_hand or player.bats
        player.throws = profile.throwing_hand or player.throws
        if profile.is_foreign is not None:
            player.is_foreign_player = bool(profile.is_foreign)
        if profile.entry_year:
            player.debut_year = profile.entry_year
        if profile.is_active is not None:
            player.status = "ACTIVE" if profile.is_active else "RETIRED"

    def _upsert_identity(
        self, session: Session, player: Player, profile: PlayerProfileParsed
    ) -> None:
        if not profile.player_name:
            return

        identity = session.execute(
            select(PlayerIdentity)
            .where(PlayerIdentity.player_id == player.id)
            .where(PlayerIdentity.name_kor == profile.player_name)
        ).scalar_one_or_none()

        if identity:
            if not identity.is_primary:
                identity.is_primary = True
        else:
            # demote existing primaries
            session.execute(
                update(PlayerIdentity)
                .where(PlayerIdentity.player_id == player.id)
                .values(is_primary=False)
            )
            identity = PlayerIdentity(
                player_id=player.id,
                name_kor=profile.player_name,
                is_primary=True,
            )
            session.add(identity)

    # ------------------------------------------------------------------
    # Season aggregates (batting/pitching)
    # ------------------------------------------------------------------
    def upsert_season_batting(self, player_id: int, season_data: Dict[str, Any]) -> None:
        self._upsert_season_stats(PlayerSeasonBatting, player_id, season_data)

    def upsert_season_pitching(self, player_id: int, season_data: Dict[str, Any]) -> None:
        self._upsert_season_stats(PlayerSeasonPitching, player_id, season_data)

    def _upsert_season_stats(
        self,
        model,
        player_id: int,
        payload: Dict[str, Any],
    ) -> None:
        if not payload:
            return

        data = dict(payload)
        data.setdefault("source", "PROFILE")

        season = data.get("season")
        if season is None:
            raise ValueError("season_data must include 'season'")

        with SessionLocal() as session:
            existing = session.execute(
                select(model).where(
                    model.player_id == player_id,
                    model.season == data.get("season"),
                    model.league == data.get("league", "REGULAR"),
                    model.level == data.get("level", "KBO1"),
                )
            ).scalar_one_or_none()

            if existing:
                for key, value in data.items():
                    setattr(existing, key, value)
            else:
                record = model(player_id=player_id, **data)
                session.add(record)

            session.commit()
