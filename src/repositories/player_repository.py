"""
Repository utilities for player domain (profiles, identities, seasons).
"""
from __future__ import annotations

from datetime import date, datetime
import re
from typing import Optional, Dict, Any, List

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal, Engine
from src.models.player import (
    Player,
    PlayerBasic,
    PlayerIdentity,
    PlayerMovement,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
)
from src.models.team import Team, TeamDailyRoster
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
        player_basic_id = self._canonical_player_basic_id(session, kbo_player_id)
        player = session.execute(
            select(Player).where(Player.kbo_person_id == kbo_player_id)
        ).scalar_one_or_none()
        if player is None:
            player = Player(kbo_person_id=kbo_player_id, player_basic_id=player_basic_id)
            session.add(player)
            session.flush()
        elif player_basic_id is not None and player.player_basic_id != player_basic_id:
            player.player_basic_id = player_basic_id
        return player

    def _canonical_player_basic_id(self, session: Session, kbo_player_id: str) -> Optional[int]:
        try:
            candidate_id = int(str(kbo_player_id).strip())
        except (TypeError, ValueError):
            return None
        exists = session.execute(
            select(PlayerBasic.player_id).where(PlayerBasic.player_id == candidate_id)
        ).scalar_one_or_none()
        return int(exists) if exists is not None else None

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
            
        # New enriched fields
        player.photo_url = profile.photo_url or player.photo_url
        player.salary_original = profile.salary_original or player.salary_original
        player.signing_bonus_original = profile.signing_bonus_original or player.signing_bonus_original
        
        # We can reconstruct draft_info or just store it if passed. 
        # For retired players, it's often in the profile text and parsed into components.
        if profile.draft_year:
            # Reconstruct for consistency with PlayerBasic if needed, or just skip if we have components
            pass

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

    # ------------------------------------------------------------------
    # Player Movements
    # ------------------------------------------------------------------
    def save_player_movements(self, movements: List[Dict[str, Any]]) -> int:
        saved_count = 0
        with SessionLocal() as session:
            for item in movements:
                # Convert date string to object if needed
                d_val = item['date']
                if isinstance(d_val, str):
                    d_val = datetime.strptime(d_val, "%Y-%m-%d").date()

                team_code = item["team_code"]
                player_name = item["player_name"]
                canonical_team_id = self._resolve_movement_team_id(session, team_code)
                if not canonical_team_id:
                    canonical_team_id = self._infer_movement_team_from_history(session, player_name, d_val.year)
                player_basic_id = self._resolve_movement_player_id(
                    session,
                    player_name,
                    canonical_team_id,
                    d_val.year,
                )
                if not canonical_team_id:
                    resolution_status = "unresolved_team"
                elif player_basic_id:
                    resolution_status = "resolved"
                else:
                    resolution_status = "unresolved_player"

                stmt = select(PlayerMovement).where(
                    PlayerMovement.movement_date == d_val,
                    PlayerMovement.team_code == team_code,
                    PlayerMovement.player_name == player_name,
                    PlayerMovement.section == item["section"],
                )
                existing = session.execute(stmt).scalar_one_or_none()
                
                if existing:
                    existing.remarks = item.get('remarks')
                    existing.canonical_team_id = canonical_team_id
                    existing.player_basic_id = player_basic_id
                    existing.resolution_status = resolution_status
                else:
                    new_rec = PlayerMovement(
                        movement_date=d_val,
                        section=item['section'],
                        team_code=team_code,
                        canonical_team_id=canonical_team_id,
                        player_basic_id=player_basic_id,
                        resolution_status=resolution_status,
                        player_name=player_name,
                        remarks=item.get('remarks'),
                    )
                    session.add(new_rec)
                saved_count += 1
            session.commit()
        return saved_count

    _TEAM_CODE_BY_NAME = {
        "KIA": "KIA",
        "기아": "KIA",
        "두산": "DB",
        "롯데": "LT",
        "삼성": "SS",
        "한화": "HH",
        "키움": "KH",
        "넥센": "NX",
        "우리": "WO",
        "SSG": "SSG",
        "SK": "SK",
        "LG": "LG",
        "KT": "KT",
        "kt": "KT",
        "NC": "NC",
        "현대": "HU",
        "해태": "HT",
        "OB": "OB",
        "쌍방울": "SL",
        "태평양": "TP",
        "청보": "CB",
        "삼미": "SM",
        "빙그레": "BE",
        "MBC": "MBC",
    }

    def _resolve_movement_team_id(self, session: Session, raw_team: str) -> Optional[str]:
        raw_team = str(raw_team or "").strip()
        candidates = [raw_team]
        mapped = self._TEAM_CODE_BY_NAME.get(raw_team)
        if mapped:
            candidates.insert(0, mapped)
        for candidate in candidates:
            if not candidate:
                continue
            exists = session.execute(
                select(Team.team_id).where(Team.team_id == candidate)
            ).scalar_one_or_none()
            if exists:
                return str(exists)
        return None

    def _infer_movement_team_from_history(self, session: Session, raw_player_name: str, season: int) -> Optional[str]:
        player_name, _position = self._split_movement_player_label(raw_player_name)
        if not player_name:
            return None
        candidate_ids = {
            int(row[0])
            for row in session.execute(
                select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
            ).fetchall()
            if row[0] is not None
        }
        if not candidate_ids:
            return None
        team_ids = set()
        for model in (PlayerSeasonBatting, PlayerSeasonPitching):
            rows = session.execute(
                select(model.team_code)
                .where(model.player_id.in_(candidate_ids), model.season <= season, model.team_code.isnot(None))
                .distinct()
            ).fetchall()
            team_ids.update(str(row[0]) for row in rows if row[0])
        if len(team_ids) != 1:
            return None
        team_id = next(iter(team_ids))
        exists = session.execute(select(Team.team_id).where(Team.team_id == team_id)).scalar_one_or_none()
        return str(exists) if exists else None

    def _resolve_movement_player_id(
        self,
        session: Session,
        raw_player_name: str,
        canonical_team_id: Optional[str],
        season: int,
    ) -> Optional[int]:
        player_name, raw_position = self._split_movement_player_label(raw_player_name)
        if not player_name:
            return None

        candidate_query = select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)
        candidate_ids = {int(row[0]) for row in session.execute(candidate_query).fetchall() if row[0] is not None}
        roster_player_id = self._unique_roster_movement_player_id(
            session,
            player_name,
            canonical_team_id,
            season,
            candidate_ids,
        )
        franchise_season_player_id = self._unique_franchise_season_player_id(
            session,
            canonical_team_id,
            season,
            candidate_ids,
        )
        if len(candidate_ids) == 1:
            return next(iter(candidate_ids))
        if not candidate_ids:
            return roster_player_id

        position_ids: set[int] = set()
        if raw_position:
            position_ids = {
                int(row[0])
                for row in session.execute(
                    candidate_query.where(PlayerBasic.position == raw_position)
                ).fetchall()
                if row[0] is not None
            }
            if len(position_ids) == 1:
                return next(iter(position_ids))

        mirror_scope_ids = position_ids or candidate_ids
        profile_mirror_ids = {
            int(row[0])
            for row in session.execute(
                select(Player.player_basic_id).where(Player.player_basic_id.in_(mirror_scope_ids))
            ).fetchall()
            if row[0] is not None
        }
        if len(profile_mirror_ids) == 1:
            return next(iter(profile_mirror_ids))
        if roster_player_id:
            return roster_player_id
        if franchise_season_player_id:
            return franchise_season_player_id

        if not canonical_team_id:
            return None

        team = session.execute(select(Team).where(Team.team_id == canonical_team_id)).scalar_one_or_none()
        team_terms = [canonical_team_id]
        if team:
            team_terms.extend(
                term
                for term in (team.team_short_name, team.team_name)
                if term and term not in team_terms
            )

        contextual_ids = set()
        for term in team_terms:
            rows = session.execute(
                select(PlayerBasic.player_id).where(
                    PlayerBasic.name == player_name,
                    PlayerBasic.team.contains(term),
                )
            ).fetchall()
            contextual_ids.update(int(row[0]) for row in rows if row[0] is not None)
        if len(contextual_ids) == 1:
            return next(iter(contextual_ids))

        for model in (PlayerSeasonBatting, PlayerSeasonPitching):
            rows = session.execute(
                select(model.player_id).where(
                    model.player_id.in_(candidate_ids),
                    model.season == season,
                    model.team_code == canonical_team_id,
                )
            ).fetchall()
            contextual_ids.update(int(row[0]) for row in rows if row[0] is not None)
        return next(iter(contextual_ids)) if len(contextual_ids) == 1 else None

    def _unique_roster_movement_player_id(
        self,
        session: Session,
        player_name: str,
        canonical_team_id: Optional[str],
        season: int,
        candidate_ids: set[int],
    ) -> Optional[int]:
        if not player_name or not canonical_team_id or not season:
            return None
        start_date = date(season, 1, 1)
        end_date = date(season + 1, 1, 1)
        roster_ids = {
            int(row[0])
            for row in session.execute(
                select(TeamDailyRoster.player_basic_id)
                .where(
                    TeamDailyRoster.team_code == canonical_team_id,
                    TeamDailyRoster.player_name == player_name,
                    TeamDailyRoster.person_type == "player",
                    TeamDailyRoster.player_basic_id.isnot(None),
                    TeamDailyRoster.roster_date >= start_date,
                    TeamDailyRoster.roster_date < end_date,
                )
                .distinct()
            ).fetchall()
            if row[0] is not None
        }
        if candidate_ids:
            roster_ids &= candidate_ids
        return next(iter(roster_ids)) if len(roster_ids) == 1 else None

    def _unique_franchise_season_player_id(
        self,
        session: Session,
        canonical_team_id: Optional[str],
        season: int,
        candidate_ids: set[int],
    ) -> Optional[int]:
        if not canonical_team_id or not season or not candidate_ids:
            return None
        franchise_id = session.execute(
            select(Team.franchise_id).where(Team.team_id == canonical_team_id)
        ).scalar_one_or_none()
        if franchise_id is None:
            return None
        franchise_team_ids = {
            str(row[0])
            for row in session.execute(
                select(Team.team_id).where(Team.franchise_id == franchise_id)
            ).fetchall()
            if row[0]
        }
        if not franchise_team_ids:
            return None

        season_ids: set[int] = set()
        for model in (PlayerSeasonBatting, PlayerSeasonPitching):
            rows = session.execute(
                select(model.player_id)
                .where(
                    model.player_id.in_(candidate_ids),
                    model.season.in_((season - 1, season)),
                    model.team_code.in_(franchise_team_ids),
                )
                .distinct()
            ).fetchall()
            season_ids.update(int(row[0]) for row in rows if row[0] is not None)
        return next(iter(season_ids)) if len(season_ids) == 1 else None

    @staticmethod
    def _split_movement_player_label(raw_player_name: str) -> tuple[str, Optional[str]]:
        raw = str(raw_player_name or "").strip()
        match = re.search(r"\(([^)]*)\)\s*$", raw)
        position = match.group(1).strip() if match else None
        player_name = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
        return player_name, position or None
