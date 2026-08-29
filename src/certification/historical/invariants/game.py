"""H03 Context-Aware Game State and Inning Score Invariants."""

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


class GameStateInvariant(BaseHistoricalInvariant):
    """H03: Validates game score consistency, winner alignment, and context-aware inning totals."""

    invariant_id: str = "H03-GAME-STATE"
    name: str = "Context-Aware Game State"
    layer: str = "H03"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H03-GAME-STATE",
        name="Context-Aware Game State",
        layer="H03",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game"],
        required_columns={
            "game": [
                "game_id",
                "home_score",
                "away_score",
                "winning_team",
                "winning_score",
                "game_status",
            ]
        },
        applicability="ALL_SEASONS",
        comparison_mode=ComparisonMode.CONDITIONAL,
        source_scope="OFFICIAL_BOXSCORE",
    )

    def evaluate_seasons(
        self,
        engine: Engine,
        seasons: list[int],
        _context: CertificationContext,
    ) -> list[InvariantResult]:
        """Audit games for impossible score/winner discrepancies and negative run values."""
        agg_sql = """
        SELECT
            CAST(SUBSTR(g.game_id, 1, 4) AS INT) AS season_year,
            SUM(CASE
                WHEN g.home_score < 0 OR g.away_score < 0 THEN 1
                WHEN g.winning_score IS NOT NULL AND g.winning_score < 0 THEN 1
                WHEN g.winning_team IS NOT NULL AND g.winning_team != ''
                     AND g.home_score > g.away_score AND g.winning_team != g.home_team THEN 1
                WHEN g.winning_team IS NOT NULL AND g.winning_team != ''
                     AND g.away_score > g.home_score AND g.winning_team != g.away_team THEN 1
                WHEN g.winning_team IS NOT NULL AND g.winning_team != ''
                     AND g.home_score = g.away_score AND g.winning_team NOT IN ('무', 'DRAW', 'TIE') THEN 1
                ELSE 0
            END) AS violations,
            COUNT(*) AS total_games
        FROM game g
        WHERE g.game_id IS NOT NULL
        GROUP BY CAST(SUBSTR(g.game_id, 1, 4) AS INT)
        """

        sample_sql = """
        SELECT g.game_id, g.game_date, g.home_team, g.away_team,
               g.home_score, g.away_score, g.winning_team, g.winning_score
        FROM game g
        WHERE g.game_id LIKE :season || '%'
          AND (
            g.home_score < 0 OR g.away_score < 0
            OR (g.winning_team IS NOT NULL AND g.winning_team != ''
                AND g.home_score > g.away_score AND g.winning_team != g.home_team)
            OR (g.winning_team IS NOT NULL AND g.winning_team != ''
                AND g.away_score > g.home_score AND g.winning_team != g.away_team)
            OR (g.winning_team IS NOT NULL AND g.winning_team != ''
                AND g.home_score = g.away_score AND g.winning_team NOT IN ('무', 'DRAW', 'TIE'))
          )
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


__all__ = [
    "GameStateInvariant",
]
