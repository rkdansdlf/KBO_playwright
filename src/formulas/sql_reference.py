"""SQL Reference Implementation for Sabermetric Formulas (Dual-Path Gate 2D).

Provides independent SQL-evaluated metrics directly queried from SQLite / ANSI SQL databases,
enabling rigorous triple-validation: Python Reference Calculator vs Production DB vs Pure SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

if TYPE_CHECKING:
    from decimal import Decimal

    from sqlalchemy.engine import Engine

SQL_DIR = Path(__file__).parent / "reference_sql"


@dataclass
class SqlMetricEvaluation:
    """Evaluation result from SQL reference oracle."""

    season: int
    player_id: int
    team_code: str
    metric: str
    sql_value: Decimal | None
    is_defined: bool


class SqlReferenceOracle:
    """Executes pure SQL metric reference queries for cross-validation."""

    @classmethod
    def evaluate_batting(
        cls,
        engine: Engine,
        season: int | None = None,
        player_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Evaluate batting metrics via independent SQL reference query."""
        sql_path = SQL_DIR / "batting_metrics.sql"
        query_text = sql_path.read_text(encoding="utf-8")

        clean_query = query_text.strip().rstrip(";")

        where_clauses = []
        params: dict[str, Any] = {}
        if season is not None:
            where_clauses.append("season = :season")
            params["season"] = season
        if player_ids:
            where_clauses.append("player_id IN :pids")
            params["pids"] = tuple(player_ids)

        if where_clauses:
            full_sql = f"SELECT * FROM ({clean_query}) WHERE {' AND '.join(where_clauses)}"  # noqa: S608
        else:
            full_sql = clean_query

        with engine.connect() as conn:
            result = conn.execute(text(full_sql), params)
            return [dict(m) for m in result.mappings()]

    @classmethod
    def evaluate_pitching(
        cls,
        engine: Engine,
        season: int | None = None,
        player_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Evaluate pitching metrics via independent SQL reference query."""
        sql_path = SQL_DIR / "pitching_metrics.sql"
        query_text = sql_path.read_text(encoding="utf-8")

        clean_query = query_text.strip().rstrip(";")

        where_clauses = []
        params: dict[str, Any] = {}
        if season is not None:
            where_clauses.append("season = :season")
            params["season"] = season
        if player_ids:
            where_clauses.append("player_id IN :pids")
            params["pids"] = tuple(player_ids)

        if where_clauses:
            full_sql = f"SELECT * FROM ({clean_query}) WHERE {' AND '.join(where_clauses)}"  # noqa: S608
        else:
            full_sql = clean_query

        with engine.connect() as conn:
            result = conn.execute(text(full_sql), params)
            return [dict(m) for m in result.mappings()]

    @classmethod
    def evaluate_fielding(
        cls,
        engine: Engine,
        season: int | None = None,
        player_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Evaluate fielding metrics via independent SQL reference query."""
        sql_path = SQL_DIR / "fielding_metrics.sql"
        query_text = sql_path.read_text(encoding="utf-8")

        clean_query = query_text.strip().rstrip(";")

        where_clauses = []
        params: dict[str, Any] = {}
        if season is not None:
            where_clauses.append("season = :season")
            params["season"] = season
        if player_ids:
            where_clauses.append("player_id IN :pids")
            params["pids"] = tuple(player_ids)

        if where_clauses:
            full_sql = f"SELECT * FROM ({clean_query}) WHERE {' AND '.join(where_clauses)}"  # noqa: S608
        else:
            full_sql = clean_query

        with engine.connect() as conn:
            result = conn.execute(text(full_sql), params)
            return [dict(m) for m in result.mappings()]
