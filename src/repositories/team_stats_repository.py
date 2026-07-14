"""Repositories for team-level season statistics."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import Engine, SessionLocal, get_database_type
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.repositories.oracle_upsert import upsert_model_by_unique_keys


class BaseStatsUpsertRepository:
    """Shared UPSERT helpers for stat tables."""

    def __init__(self, model: type[TeamSeasonBatting | TeamSeasonPitching], unique_keys: list[str]) -> None:
        """Initialize a new instance.

        Args:
            model: Model.
            unique_keys: Unique Keys.
            model: Model.
            unique_keys: Unique Keys.

        """
        self.model = model

        self.unique_keys = unique_keys
        self.dialect = Engine.dialect.name

    def upsert_many(self, records: list[dict[str, Any]]) -> int:
        """Insert or update many.

        Args:
            records: Records.
            records: Records.
            records: Records.

        Returns:
            Integer result.

        """
        if not records:
            return 0

        # Filter fields that exist in the model to avoid CompileError
        cleaned = [self._filter_model_fields(self._filter_none(record)) for record in records]
        db_type = get_database_type()

        with SessionLocal() as session:
            try:
                if db_type == "sqlite":
                    session.execute(text("PRAGMA foreign_keys = OFF"))

                if self.dialect == "oracle":
                    for payload in cleaned:
                        upsert_model_by_unique_keys(session, self.model, payload, self.unique_keys)
                else:
                    # Group records by their column key-set so we can bulk-execute
                    # each group in one statement instead of one-per-record.
                    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
                    for payload in cleaned:
                        groups[tuple(sorted(payload.keys()))].append(payload)

                    for group in groups.values():
                        # Build stmt from the first record (representative shape)
                        stmt = self._build_insert_stmt(group[0])
                        if len(group) == 1:
                            session.execute(stmt)
                        else:
                            # Re-build as VALUES-list stmt for bulk insert
                            bulk_stmt = self._build_bulk_insert_stmt(group)
                            session.execute(bulk_stmt)

                session.commit()
                return len(cleaned)
            except SQLAlchemyError:
                session.rollback()
                raise
            finally:
                if db_type == "sqlite":
                    session.execute(text("PRAGMA foreign_keys = ON"))

    def _filter_model_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Filter out keys that are not present in the model's columns.

        Args:
            payload: Payload.
            payload: Payload.

        """
        model_columns = self.model.__table__.columns.keys()

        return {k: v for k, v in payload.items() if k in model_columns}

    def _build_insert_stmt(self, payload: dict[str, Any]) -> text | str:
        if self.dialect == "sqlite":
            stmt = sqlite_insert(self.model).values(**payload)
            update_dict = {k: v for k, v in payload.items() if k not in self.unique_keys}
            return stmt.on_conflict_do_update(
                index_elements=self.unique_keys,
                set_=update_dict,
            )

        if self.dialect == "postgresql":
            stmt = pg_insert(self.model).values(**payload)
            update_dict = {k: stmt.excluded[k] for k in payload if k not in self.unique_keys}
            return stmt.on_conflict_do_update(
                index_elements=self.unique_keys,
                set_=update_dict,
            )

        if self.dialect == "mysql":
            stmt = mysql_insert(self.model).values(**payload)
            update_dict = {k: stmt.inserted[k] for k in payload if k not in self.unique_keys}
            return stmt.on_duplicate_key_update(**update_dict)

        # Fallback: rely on merge semantics (slower but portable)
        stmt = sqlite_insert(self.model).values(**payload)
        update_dict = {k: v for k, v in payload.items() if k not in self.unique_keys}
        return stmt.on_conflict_do_update(
            index_elements=self.unique_keys,
            set_=update_dict,
        )

    def _build_bulk_insert_stmt(self, payloads: list[dict[str, Any]]) -> text | str:
        """Build a multi-row upsert statement for a list of records sharing the same key-set."""
        if self.dialect == "sqlite":
            stmt = sqlite_insert(self.model).values(payloads)
            first = payloads[0]
            update_dict = {k: v for k, v in first.items() if k not in self.unique_keys}
            return stmt.on_conflict_do_update(
                index_elements=self.unique_keys,
                set_=update_dict,
            )

        if self.dialect == "postgresql":
            stmt = pg_insert(self.model).values(payloads)
            update_dict = {k: stmt.excluded[k] for k in payloads[0] if k not in self.unique_keys}
            return stmt.on_conflict_do_update(
                index_elements=self.unique_keys,
                set_=update_dict,
            )

        if self.dialect == "mysql":
            stmt = mysql_insert(self.model).values(payloads)
            update_dict = {k: stmt.inserted[k] for k in payloads[0] if k not in self.unique_keys}
            return stmt.on_duplicate_key_update(**update_dict)

        # Fallback
        stmt = sqlite_insert(self.model).values(payloads)
        first = payloads[0]
        update_dict = {k: v for k, v in first.items() if k not in self.unique_keys}
        return stmt.on_conflict_do_update(
            index_elements=self.unique_keys,
            set_=update_dict,
        )

    @staticmethod
    def _filter_none(payload: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in payload.items() if v is not None}


class TeamSeasonBattingRepository(BaseStatsUpsertRepository):
    """upsert logic for team-level batting aggregates."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        super().__init__(TeamSeasonBatting, ["team_id", "season", "league"])


class TeamSeasonPitchingRepository(BaseStatsUpsertRepository):
    """upsert logic for team-level pitching aggregates."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        super().__init__(TeamSeasonPitching, ["team_id", "season", "league"])
