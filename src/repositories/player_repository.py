"""Repository utilities for player domain (profiles, identities, seasons)."""

from __future__ import annotations

import contextlib
import re
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import select, update

from src.constants import KST
from src.db.engine import Engine, SessionLocal
from src.models.player import (
    Player,
    PlayerBasic,
    PlayerIdentity,
    PlayerMovement,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
)
from src.models.team import Team, TeamDailyRoster

DEBUT_TIMELINE_MATCH_YEAR_DELTA = 5

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.parsers.player_profile_parser import PlayerProfileParsed


class PlayerRepository:
    """Persist player-related entities with SQLite/MySQL compatible UPSERT logic."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        self.dialect = Engine.dialect.name

    # ------------------------------------------------------------------
    # Profile / identity handling
    # ------------------------------------------------------------------
    def upsert_player_profile(self, kbo_player_id: str, profile: PlayerProfileParsed) -> Player | None:
        """Upsert player and primary identity based on parsed profile info.

        Also synchronizes status and key fields to PlayerBasic.

        Args:
            kbo_player_id: Kbo Player ID.
            profile: Profile.
            kbo_player_id: Kbo Player ID.
            profile: Profile.

        """
        if not kbo_player_id:
            msg = "kbo_player_id is required to upsert a player profile"
            raise ValueError(msg)

        with SessionLocal() as session:
            player = self._get_or_create_player(session, kbo_player_id)
            self._apply_profile_fields(player, profile)
            self._upsert_identity(session, player, profile)

            # Synchronize back to PlayerBasic
            self._sync_to_player_basic(session, kbo_player_id, profile)

            session.commit()
            session.refresh(player)
            return player

    def _sync_to_player_basic(self, session: Session, kbo_player_id: str, profile: PlayerProfileParsed) -> None:
        try:
            pid = int(str(kbo_player_id).strip())
        except (TypeError, ValueError):
            return

        basic = session.query(PlayerBasic).filter_by(player_id=pid).first()
        if basic:
            self._apply_basic_profile_fields(basic, profile)
            self._sync_basic_draft_and_career(basic, profile)
            self._sync_basic_structured_fields(basic, profile)

    def _apply_basic_profile_fields(self, basic: PlayerBasic, profile: PlayerProfileParsed) -> None:
        if profile.is_active is not None:
            basic.status = "active" if profile.is_active else "retired"
            basic.status_source = "profile"

        field_map = {
            "photo_url": profile.photo_url,
            "height_cm": profile.height_cm,
            "weight_kg": profile.weight_kg,
            "bats": profile.batting_hand,
            "throws": profile.throwing_hand,
            "debut_year": profile.entry_year,
            "salary_original": profile.salary_original,
            "signing_bonus_original": profile.signing_bonus_original,
        }
        for field, value in field_map.items():
            if value:
                setattr(basic, field, value)

        if profile.birth_date:
            basic.birth_date = profile.birth_date
            with contextlib.suppress(ValueError):
                basic.birth_date_date = datetime.strptime(profile.birth_date, "%Y-%m-%d").replace(tzinfo=KST).date()

    def _sync_basic_draft_and_career(self, basic: PlayerBasic, profile: PlayerProfileParsed) -> None:
        if profile.draft_year:
            draft_parts = [str(profile.draft_year)[2:], profile.draft_team_code or ""]
            if profile.draft_type:
                draft_parts.append(profile.draft_type)
            if profile.draft_round:
                draft_parts.append(f"{profile.draft_round}라운드")
            if profile.draft_pick_overall:
                draft_parts.append(f"{profile.draft_pick_overall}순위")
            basic.draft_info = " ".join(filter(None, draft_parts))

        if profile.education_or_career_path:
            basic.career = "-".join(profile.education_or_career_path)

    def _sync_basic_structured_fields(self, basic: PlayerBasic, profile: PlayerProfileParsed) -> None:
        basic.salary_amount = profile.salary_amount
        basic.salary_currency = profile.salary_currency
        basic.signing_bonus_amount = profile.signing_bonus_amount
        basic.signing_bonus_currency = profile.signing_bonus_currency
        basic.draft_year = profile.draft_year
        basic.draft_round = profile.draft_round
        basic.draft_pick_overall = profile.draft_pick_overall
        basic.draft_type = profile.draft_type
        basic.education_path = profile.education_path

    def _get_or_create_player(self, session: Session, kbo_player_id: str) -> Player:
        player_basic_id = self._canonical_player_basic_id(session, kbo_player_id)
        player = session.execute(select(Player).where(Player.kbo_person_id == kbo_player_id)).scalar_one_or_none()
        if player is None:
            player = Player(kbo_person_id=kbo_player_id, player_basic_id=player_basic_id)
            session.add(player)
            session.flush()
        elif player_basic_id is not None and player.player_basic_id != player_basic_id:
            player.player_basic_id = player_basic_id
        return player

    def _canonical_player_basic_id(self, session: Session, kbo_player_id: str) -> int | None:
        try:
            candidate_id = int(str(kbo_player_id).strip())
        except (TypeError, ValueError):
            return None
        exists = session.execute(
            select(PlayerBasic.player_id).where(PlayerBasic.player_id == candidate_id),
        ).scalar_one_or_none()
        return int(exists) if exists is not None else None

    def _apply_profile_fields(self, player: Player, profile: PlayerProfileParsed) -> None:
        if profile.birth_date:
            with contextlib.suppress(ValueError):
                player.birth_date = datetime.strptime(profile.birth_date, "%Y-%m-%d").replace(tzinfo=KST).date()
        player.height_cm = profile.height_cm or player.height_cm
        player.weight_kg = profile.weight_kg or player.weight_kg
        player.bats = profile.batting_hand or player.bats
        player.throws = profile.throwing_hand or player.throws
        if profile.is_foreign is not None:
            player.is_foreign_player = bool(profile.is_foreign)
        if profile.entry_year:
            player.debut_year = profile.entry_year
        if profile.is_active is not None:
            player.status = "ACTIVE" if profile.is_active else "RETIRED"

        # Enriched fields
        player.photo_url = profile.photo_url or player.photo_url
        player.salary_original = profile.salary_original or player.salary_original
        player.signing_bonus_original = profile.signing_bonus_original or player.signing_bonus_original

        # Reconstruct draft_info for relational model if components exist
        if profile.draft_year:
            draft_parts = [str(profile.draft_year)[2:], profile.draft_team_code or ""]
            if profile.draft_type:
                draft_parts.append(profile.draft_type)
            if profile.draft_round:
                draft_parts.append(f"{profile.draft_round}라운드")
            if profile.draft_pick_overall:
                draft_parts.append(f"{profile.draft_pick_overall}순위")
            player.draft_info = " ".join(filter(None, draft_parts))

        # Reconstruct career path for notes
        if profile.education_or_career_path:
            player.notes = " -> ".join(profile.education_or_career_path)

        # Structured parsed fields
        player.salary_amount = profile.salary_amount
        player.salary_currency = profile.salary_currency
        player.signing_bonus_amount = profile.signing_bonus_amount
        player.signing_bonus_currency = profile.signing_bonus_currency
        player.draft_year = profile.draft_year
        player.draft_round = profile.draft_round
        player.draft_pick_overall = profile.draft_pick_overall
        player.draft_type = profile.draft_type
        player.education_path = profile.education_path

    def _upsert_identity(self, session: Session, player: Player, profile: PlayerProfileParsed) -> None:
        if not profile.player_name:
            return

        identity = session.execute(
            select(PlayerIdentity)
            .where(PlayerIdentity.player_id == player.id)
            .where(PlayerIdentity.name_kor == profile.player_name),
        ).scalar_one_or_none()

        if identity:
            if not identity.is_primary:
                identity.is_primary = True
        else:
            # demote existing primaries
            session.execute(
                update(PlayerIdentity).where(PlayerIdentity.player_id == player.id).values(is_primary=False),
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
    def upsert_season_batting(self, player_id: int, season_data: dict[str, Any]) -> None:
        """Insert or updates season batting.

        Args:
            player_id: Player ID.
            season_data: Season Data.
            player_id: Player ID.
            season_data: Season Data.
            player_id: Player ID.
            season_data: Season Data.

        """
        self._upsert_season_stats(PlayerSeasonBatting, player_id, season_data)

    def upsert_season_pitching(self, player_id: int, season_data: dict[str, Any]) -> None:
        """Insert or updates season pitching.

        Args:
            player_id: Player ID.
            season_data: Season Data.
            player_id: Player ID.
            season_data: Season Data.
            player_id: Player ID.
            season_data: Season Data.

        """
        self._upsert_season_stats(PlayerSeasonPitching, player_id, season_data)

    def _upsert_season_stats(
        self,
        model: type[PlayerSeasonBatting | PlayerSeasonPitching],
        player_id: int,
        payload: dict[str, Any],
    ) -> None:
        if not payload:
            return

        data = dict(payload)
        data.setdefault("source", "PROFILE")

        season = data.get("season")
        if season is None:
            msg = "season_data must include 'season'"
            raise ValueError(msg)

        with SessionLocal() as session:
            existing = session.execute(
                select(model).where(
                    model.player_id == player_id,
                    model.season == data.get("season"),
                    model.league == data.get("league", "REGULAR"),
                    model.level == data.get("level", "KBO1"),
                ),
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
    def save_player_movements(self, movements: list[dict[str, Any]]) -> int:
        """Save player movements.

        Args:
            movements: Movements.
            movements: Movements.
            movements: Movements.

        Returns:
            Integer result.

        """
        saved_count = 0

        with SessionLocal() as session:
            for item in movements:
                # Convert date string to object if needed
                d_val = item["date"]
                if isinstance(d_val, str):
                    d_val = datetime.strptime(d_val, "%Y-%m-%d").replace(tzinfo=KST).date()

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
                    existing.remarks = item.get("remarks")
                    existing.canonical_team_id = canonical_team_id
                    existing.player_basic_id = player_basic_id
                    existing.resolution_status = resolution_status
                else:
                    new_rec = PlayerMovement(
                        movement_date=d_val,
                        section=item["section"],
                        team_code=team_code,
                        canonical_team_id=canonical_team_id,
                        player_basic_id=player_basic_id,
                        resolution_status=resolution_status,
                        player_name=player_name,
                        remarks=item.get("remarks"),
                    )
                    session.add(new_rec)
                saved_count += 1
            session.commit()
        return saved_count

    _TEAM_CODE_BY_NAME: ClassVar[dict[str, str]] = {
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

    def _resolve_movement_team_id(self, session: Session, raw_team: str) -> str | None:
        raw_team = str(raw_team or "").strip()
        candidates = [raw_team]
        mapped = self._TEAM_CODE_BY_NAME.get(raw_team)
        if mapped:
            candidates.insert(0, mapped)
        for candidate in candidates:
            if not candidate:
                continue
            exists = session.execute(select(Team.team_id).where(Team.team_id == candidate)).scalar_one_or_none()
            if exists:
                return str(exists)
        return None

    def _infer_movement_team_from_history(self, session: Session, raw_player_name: str, season: int) -> str | None:
        player_name, _position = self._split_movement_player_label(raw_player_name)
        if not player_name:
            return None
        candidate_ids = {
            int(row[0])
            for row in session.execute(select(PlayerBasic.player_id).where(PlayerBasic.name == player_name)).fetchall()
            if row[0] is not None
        }
        if not candidate_ids:
            return None
        team_ids = set()
        for model in (PlayerSeasonBatting, PlayerSeasonPitching):
            rows = session.execute(
                select(model.team_code)
                .where(model.player_id.in_(candidate_ids), model.season <= season, model.team_code.isnot(None))
                .distinct(),
            ).fetchall()
            team_ids.update(str(row[0]) for row in rows if row[0])
        if len(team_ids) != 1:
            return None
        team_id = next(iter(team_ids))
        exists = session.execute(select(Team.team_id).where(Team.team_id == team_id)).scalar_one_or_none()
        return str(exists) if exists else None

    @staticmethod
    def _narrow_by_position(
        candidates: list[PlayerBasic],
        raw_position: str | None,
    ) -> tuple[list[PlayerBasic], int | None]:
        if not raw_position:
            return candidates, None
        pos_matches = [candidate for candidate in candidates if candidate.position == raw_position]
        if len(pos_matches) == 1:
            return pos_matches, pos_matches[0].player_id
        return (pos_matches or candidates), None

    @staticmethod
    def _narrow_by_debut_timeline(candidates: list[PlayerBasic], season: int) -> tuple[list[PlayerBasic], int | None]:
        timeline_matches = [
            candidate
            for candidate in candidates
            if candidate.debut_year and abs(candidate.debut_year - season) <= DEBUT_TIMELINE_MATCH_YEAR_DELTA
        ]
        if len(timeline_matches) == 1:
            return timeline_matches, timeline_matches[0].player_id
        return (timeline_matches or candidates), None

    @staticmethod
    def _narrow_by_profile(session: Session, candidates: list[PlayerBasic]) -> tuple[list[PlayerBasic], int | None]:
        profile_matches = []
        for candidate in candidates:
            has_profile = session.execute(
                select(Player.id).where(Player.player_basic_id == candidate.player_id),
            ).scalar_one_or_none()
            if has_profile:
                profile_matches.append(candidate)
        if len(profile_matches) == 1:
            return profile_matches, profile_matches[0].player_id
        return (profile_matches or candidates), None

    @staticmethod
    def _unique_contextual_movement_player_id(
        session: Session,
        candidates: list[PlayerBasic],
        canonical_team_id: str | None,
    ) -> int | None:
        if not canonical_team_id:
            return None
        team = session.execute(select(Team).where(Team.team_id == canonical_team_id)).scalar_one_or_none()
        team_terms = [canonical_team_id]
        if team:
            team_terms.extend(filter(None, [team.team_short_name, team.team_name]))
        contextual_matches = [
            candidate
            for candidate in candidates
            if candidate.team and any(term in candidate.team for term in team_terms)
        ]
        return contextual_matches[0].player_id if len(contextual_matches) == 1 else None

    def _resolve_movement_player_id(
        self,
        session: Session,
        raw_player_name: str,
        canonical_team_id: str | None,
        season: int,
    ) -> int | None:
        player_name, raw_position = self._split_movement_player_label(raw_player_name)
        if not player_name or player_name == "신인":
            return None

        candidate_query = select(PlayerBasic).where(PlayerBasic.name == player_name)
        candidates = session.execute(candidate_query).scalars().all()

        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0].player_id
        return self._resolve_multi_candidate(candidates, session, player_name, canonical_team_id, season, raw_position)

    def _resolve_multi_candidate(  # noqa: PLR0913
        self,
        candidates: list,
        session: Session,
        player_name: str,
        canonical_team_id: str | None,
        season: int,
        raw_position: str | None,
    ) -> int | None:
        for narrower in (
            lambda rows: self._narrow_by_position(rows, raw_position),
            lambda rows: self._narrow_by_debut_timeline(rows, season),
            lambda rows: self._narrow_by_profile(session, rows),
        ):
            candidates, player_id = narrower(candidates)
            if player_id:
                return player_id

        roster_player_id = self._unique_roster_movement_player_id(
            session,
            player_name,
            canonical_team_id,
            season,
            {c.player_id for c in candidates},
        )
        if roster_player_id:
            return roster_player_id

        franchise_season_player_id = self._unique_franchise_season_player_id(
            session,
            canonical_team_id,
            season,
            {c.player_id for c in candidates},
        )
        if franchise_season_player_id:
            return franchise_season_player_id

        return self._unique_contextual_movement_player_id(session, candidates, canonical_team_id)

    def _unique_roster_movement_player_id(
        self,
        session: Session,
        player_name: str,
        canonical_team_id: str | None,
        season: int,
        candidate_ids: set[int],
    ) -> int | None:
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
                .distinct(),
            ).fetchall()
            if row[0] is not None
        }
        if candidate_ids:
            roster_ids &= candidate_ids
        return next(iter(roster_ids)) if len(roster_ids) == 1 else None

    def _unique_franchise_season_player_id(
        self,
        session: Session,
        canonical_team_id: str | None,
        season: int,
        candidate_ids: set[int],
    ) -> int | None:
        if not canonical_team_id or not season or not candidate_ids:
            return None
        franchise_id = session.execute(
            select(Team.franchise_id).where(Team.team_id == canonical_team_id),
        ).scalar_one_or_none()
        if franchise_id is None:
            return None
        franchise_team_ids = {
            str(row[0])
            for row in session.execute(select(Team.team_id).where(Team.franchise_id == franchise_id)).fetchall()
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
                .distinct(),
            ).fetchall()
            season_ids.update(int(row[0]) for row in rows if row[0] is not None)
        return next(iter(season_ids)) if len(season_ids) == 1 else None

    @staticmethod
    def _split_movement_player_label(raw_player_name: str) -> tuple[str, str | None]:
        raw = str(raw_player_name or "").strip()
        match = re.search(r"\(([^)]*)\)\s*$", raw)
        position = match.group(1).strip() if match else None
        player_name = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
        return player_name, position or None
