"""H05 Baseball Pitching Mathematical Invariants."""

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


class PitchingInvariants(BaseHistoricalInvariant):
    """H05: Validates mathematical baseball invariants on pitching boxscore statistics."""

    invariant_id: str = "H05-PITCHING-INVARIANTS"
    name: str = "Pitching Mathematical Invariants"
    layer: str = "H05"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H05-PITCHING-INVARIANTS",
        name="Pitching Mathematical Invariants",
        layer="H05",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game_pitching_stats"],
        required_columns={
            "game_pitching_stats": [
                "game_id",
                "innings_pitched",
                "hits_allowed",
                "runs_allowed",
                "earned_runs",
                "home_runs_allowed",
                "strikeouts",
                "walks_allowed",
            ]
        },
        applicability="ALL_SEASONS",
        comparison_mode=ComparisonMode.EXACT,
        source_scope="OFFICIAL_BOXSCORE",
    )

    def evaluate_seasons(
        self,
        engine: Engine,
        seasons: list[int],
        _context: CertificationContext,
    ) -> list[InvariantResult]:
        """Audit pitching records for impossible math (ER > R, HR > H, negative innings/runs)."""
        agg_sql = """
        SELECT
            COALESCE(g.season_id, CAST(SUBSTR(p.game_id, 1, 4) AS INT)) AS season_id,
            SUM(CASE
                WHEN p.innings_pitched < 0 OR p.runs_allowed < 0 OR p.earned_runs < 0
                     OR p.hits_allowed < 0 OR p.strikeouts < 0 OR p.walks_allowed < 0 THEN 1
                WHEN p.earned_runs > p.runs_allowed THEN 1
                WHEN p.home_runs_allowed > p.hits_allowed THEN 1
                ELSE 0
            END) AS violations,
            COUNT(*) AS total_pitchers
        FROM game_pitching_stats p
        LEFT JOIN game g ON p.game_id = g.game_id
        GROUP BY COALESCE(g.season_id, CAST(SUBSTR(p.game_id, 1, 4) AS INT))
        """

        sample_sql = """
        SELECT
            p.id, p.game_id, p.player_name, p.innings_pitched,
            p.hits_allowed, p.runs_allowed, p.earned_runs, p.home_runs_allowed
        FROM game_pitching_stats p
        LEFT JOIN game g ON p.game_id = g.game_id
        WHERE (g.season_id = :season OR p.game_id LIKE :season || '%')
          AND (
            p.innings_pitched < 0 OR p.runs_allowed < 0 OR p.earned_runs < 0
            OR p.hits_allowed < 0 OR p.strikeouts < 0 OR p.walks_allowed < 0
            OR p.earned_runs > p.runs_allowed
            OR p.home_runs_allowed > p.hits_allowed
          )
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


__all__ = [
    "PitchingInvariants",
]
