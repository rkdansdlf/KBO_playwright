"""
Repository for Team related data (Roster, Info, etc.)
"""
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert

from src.models.team import TeamDailyRoster

class TeamRepository:
    def __init__(self, session: Session):
        self.session = session

    def save_daily_rosters(self, rosters: List[Dict[str, Any]]) -> int:
        """
        Save daily roster records with UPSERT logic.
        """
        
        # Deduplicate input list by unique key (date, team, player)
        # to prevent IntegrityError if the list contains duplicates
        unique_rosters = {}
        for r in rosters:
            key = (r['roster_date'], r['team_code'], r['player_id'])
            # If duplicate, keep the last one (arbitrary decision, or first?)
            unique_rosters[key] = r
            
        rows = list(unique_rosters.values())
        if not rows:
            return 0

        dialect = self.session.get_bind().dialect.name
        values = [
            {
                "roster_date": r["roster_date"],
                "team_code": r["team_code"],
                "player_id": r["player_id"],
                "player_name": r["player_name"],
                "position": r["position"],
                "back_number": r["back_number"],
            }
            for r in rows
        ]

        if dialect == "sqlite":
            stmt = sqlite_insert(TeamDailyRoster).values(values)
            update_dict = {
                "player_name": stmt.excluded.player_name,
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
