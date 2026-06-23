"""
Repository for Team related data (Roster, Info, etc.)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.models.player import PlayerBasic
from src.models.team import TeamDailyRoster

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

PLAYER_ROSTER_POSITIONS = {"투수", "포수", "내야수", "외야수", "선수"}
STAFF_ROSTER_POSITIONS = {"감독", "코치"}


class TeamRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_daily_rosters(self, rosters: list[dict[str, Any]]) -> int:
        """
        Save daily roster records with UPSERT logic.
        """

        # Deduplicate input list by unique key (date, team, player)
        # to prevent IntegrityError if the list contains duplicates
        unique_rosters = {}
        for r in rosters:
            key = (r["roster_date"], r["team_code"], r["player_id"])
            # If duplicate, keep the last one (arbitrary decision, or first?)
            unique_rosters[key] = r

        rows = list(unique_rosters.values())
        if not rows:
            return 0

        dialect = self.session.get_bind().dialect.name
        candidate_player_ids = {
            int(r["player_id"])
            for r in rows
            if self._person_type_for_position(r.get("position")) == "player" and r.get("player_id") is not None
        }
        existing_player_ids = set()
        if candidate_player_ids:
            existing_player_ids = {
                int(row[0])
                for row in self.session.query(PlayerBasic.player_id)
                .filter(PlayerBasic.player_id.in_(candidate_player_ids))
                .all()
            }

        values = []
        for r in rows:
            player_id = int(r["player_id"])
            person_type = r.get("person_type") or self._person_type_for_position(r.get("position"))
            player_basic_id = r.get("player_basic_id")
            if player_basic_id is None and person_type == "player" and player_id in existing_player_ids:
                player_basic_id = player_id
            values.append(
                {
                    "roster_date": r["roster_date"],
                    "team_code": r["team_code"],
                    "player_id": player_id,
                    "player_basic_id": player_basic_id,
                    "person_type": person_type,
                    "player_name": r["player_name"],
                    "position": r["position"],
                    "back_number": r["back_number"],
                },
            )

        if dialect == "sqlite":
            stmt = sqlite_insert(TeamDailyRoster).values(values)
            update_dict = {
                "player_name": stmt.excluded.player_name,
                "player_basic_id": stmt.excluded.player_basic_id,
                "person_type": stmt.excluded.person_type,
                "position": stmt.excluded.position,
                "back_number": stmt.excluded.back_number,
                "updated_at": text("CURRENT_TIMESTAMP"),
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["roster_date", "team_code", "player_id"],
                set_=update_dict,
            )
            self.session.execute(stmt)
        elif dialect == "mysql":
            stmt = mysql_insert(TeamDailyRoster).values(values)
            update_dict = {
                "player_name": stmt.inserted.player_name,
                "player_basic_id": stmt.inserted.player_basic_id,
                "person_type": stmt.inserted.person_type,
                "position": stmt.inserted.position,
                "back_number": stmt.inserted.back_number,
                "updated_at": text("CURRENT_TIMESTAMP"),
            }
            stmt = stmt.on_duplicate_key_update(update_dict)
            self.session.execute(stmt)
        else:
            stmt = pg_insert(TeamDailyRoster).values(values)
            update_dict = {
                "player_name": stmt.excluded.player_name,
                "player_basic_id": stmt.excluded.player_basic_id,
                "person_type": stmt.excluded.person_type,
                "position": stmt.excluded.position,
                "back_number": stmt.excluded.back_number,
                "updated_at": text("CURRENT_TIMESTAMP"),
            }
            stmt = stmt.on_conflict_do_update(
                constraint="uq_team_daily_roster",
                set_=update_dict,
            )
            self.session.execute(stmt)

        self.session.commit()
        return len(values)

    def _person_type_for_position(self, position: str | None) -> str:
        normalized = str(position or "").strip()
        if normalized in PLAYER_ROSTER_POSITIONS:
            return "player"
        if normalized in STAFF_ROSTER_POSITIONS:
            return "staff"
        return "unknown"
