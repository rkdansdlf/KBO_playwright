"""
Repositories for team-level season statistics.
"""
from __future__ import annotations

from typing import List, Dict, Any
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import SessionLocal, Engine
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching


class BaseStatsUpsertRepository:
    """Shared UPSERT helpers for stat tables."""

    def __init__(self, model, unique_keys: List[str]):
        self.model = model
        self.unique_keys = unique_keys
        self.dialect = Engine.dialect.name

    def upsert_many(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0

        cleaned = [self._filter_none(record) for record in records]
        with SessionLocal() as session:
            try:
                for payload in cleaned:
                    stmt = self._build_insert_stmt(payload)
                    session.execute(stmt)
                session.commit()
                return len(cleaned)
            except SQLAlchemyError:
                session.rollback()
                raise

    def _build_insert_stmt(self, payload: Dict[str, Any]):
        if self.dialect == "sqlite":
            stmt = sqlite_insert(self.model).values(**payload)
            update_dict = {k: v for k, v in payload.items() if k not in self.unique_keys}
            return stmt.on_conflict_do_update(
                index_elements=self.unique_keys,
                set_=update_dict,
            )

        if self.dialect == "postgresql":
            stmt = pg_insert(self.model).values(**payload)
            update_dict = {k: stmt.excluded[k] for k in payload.keys() if k not in self.unique_keys}
            return stmt.on_conflict_do_update(
                index_elements=self.unique_keys,
                set_=update_dict,
            )

        if self.dialect == "mysql":
            stmt = mysql_insert(self.model).values(**payload)
            update_dict = {k: stmt.inserted[k] for k in payload.keys() if k not in self.unique_keys}
            return stmt.on_duplicate_key_update(**update_dict)

        # Fallback: rely on merge semantics (slower but portable)
        stmt = sqlite_insert(self.model).values(**payload)
        update_dict = {k: v for k, v in payload.items() if k not in self.unique_keys}
        return stmt.on_conflict_do_update(
            index_elements=self.unique_keys,
            set_=update_dict,
        )

    @staticmethod
    def _filter_none(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in payload.items() if v is not None}


class TeamSeasonBattingRepository(BaseStatsUpsertRepository):
    """UPSERT logic for team-level batting aggregates."""

    def __init__(self):
        super().__init__(TeamSeasonBatting, ["team_id", "season", "league"])


class TeamSeasonPitchingRepository(BaseStatsUpsertRepository):
    """UPSERT logic for team-level pitching aggregates."""

    def __init__(self):
        super().__init__(TeamSeasonPitching, ["team_id", "season", "league"])
