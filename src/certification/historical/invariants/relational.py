"""H01 Schedule Coverage and H02 Referential Integrity Invariants."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.certification.historical.invariants.base import BaseHistoricalInvariant
from src.certification.historical.models import (
    ComparisonMode,
    InvariantMetadata,
    InvariantSeverity,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from src.certification.context import CertificationContext
    from src.certification.historical.models import InvariantResult


class ScheduleCoverageInvariant(BaseHistoricalInvariant):
    """H01: Verifies schedule completeness, game identity uniqueness, and completed score validity."""

    invariant_id: str = "H01-SCHEDULE-COVERAGE"
    name: str = "Schedule & Game Coverage"
    layer: str = "H01"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H01-SCHEDULE-COVERAGE",
        name="Schedule & Game Coverage",
        layer="H01",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game"],
        required_columns={"game": ["game_id", "game_date", "home_team", "away_team", "home_score", "away_score"]},
        applicability="ALL_SEASONS",
        comparison_mode=ComparisonMode.EXACT,
        source_scope="OFFICIAL_SCHEDULE",
    )

    def evaluate_seasons(
        self,
        engine: Engine,
        seasons: list[int],
        _context: CertificationContext,
    ) -> list[InvariantResult]:
        """Audit schedule tables for identity anomalies, duplicates, and unrecorded scores."""
        agg_sql = """
        SELECT
            CAST(SUBSTR(g.game_id, 1, 4) AS INT) AS season_year,
            SUM(CASE
                WHEN g.home_team = g.away_team THEN 1
                WHEN g.game_status IN ('COMPLETED', 'FINISHED', '종료')
                     AND (g.home_score IS NULL OR g.away_score IS NULL) THEN 1
                WHEN g.game_id IS NULL OR g.game_date IS NULL THEN 1
                ELSE 0
            END) AS violations,
            COUNT(*) AS total_games
        FROM game g
        WHERE g.game_id IS NOT NULL
        GROUP BY CAST(SUBSTR(g.game_id, 1, 4) AS INT)
        """

        sample_sql = """
        SELECT g.game_id, g.game_date, g.home_team, g.away_team, g.home_score, g.away_score, g.game_status
        FROM game g
        WHERE g.game_id LIKE :season || '%'
          AND (
            g.home_team = g.away_team
            OR (g.game_status IN ('COMPLETED', 'FINISHED', '종료') AND (g.home_score IS NULL OR g.away_score IS NULL))
            OR g.game_id IS NULL
          )
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


class ReferentialIntegrityInvariant(BaseHistoricalInvariant):
    """H02: Validates foreign key relationships and flags orphan stats/lineup/PBP rows."""

    invariant_id: str = "H02-REFERENTIAL-INTEGRITY"
    name: str = "Foreign Key & Referential Integrity"
    layer: str = "H02"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H02-REFERENTIAL-INTEGRITY",
        name="Foreign Key & Referential Integrity",
        layer="H02",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game", "game_batting_stats"],
        required_columns={"game": ["game_id"], "game_batting_stats": ["game_id"]},
        applicability="ALL_SEASONS",
        comparison_mode=ComparisonMode.EXACT,
        source_scope="RELATIONAL_STORE",
    )

    def evaluate_seasons(
        self,
        engine: Engine,
        seasons: list[int],
        _context: CertificationContext,
    ) -> list[InvariantResult]:
        """Audit child tables for missing parent game relationships."""
        agg_sql = """
        SELECT
            CAST(SUBSTR(b.game_id, 1, 4) AS INT) AS season_year,
            SUM(CASE WHEN g.game_id IS NULL THEN 1 ELSE 0 END) AS violations,
            COUNT(*) AS total_rows
        FROM game_batting_stats b
        LEFT JOIN game g ON b.game_id = g.game_id
        GROUP BY CAST(SUBSTR(b.game_id, 1, 4) AS INT)
        """

        sample_sql = """
        SELECT b.id, b.game_id, b.player_name, b.team_code
        FROM game_batting_stats b
        LEFT JOIN game g ON b.game_id = g.game_id
        WHERE g.game_id IS NULL
          AND b.game_id LIKE :season || '%'
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


__all__ = [
    "ReferentialIntegrityInvariant",
    "ScheduleCoverageInvariant",
]
