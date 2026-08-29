"""H04 Baseball Batting Mathematical Invariants."""

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


class BattingInvariants(BaseHistoricalInvariant):
    """H04: Validates mathematical baseball invariants on batting boxscore statistics."""

    invariant_id: str = "H04-BATTING-INVARIANTS"
    name: str = "Batting Mathematical Invariants"
    layer: str = "H04"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H04-BATTING-INVARIANTS",
        name="Batting Mathematical Invariants",
        layer="H04",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game_batting_stats"],
        required_columns={
            "game_batting_stats": [
                "game_id",
                "at_bats",
                "runs",
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "walks",
                "strikeouts",
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
        """Audit batting records for mathematical contradictions (H > AB, HR > H, negative stats)."""
        agg_sql = """
        SELECT
            COALESCE(g.season_id, CAST(SUBSTR(b.game_id, 1, 4) AS INT)) AS season_id,
            SUM(CASE
                WHEN b.at_bats < 0 OR b.runs < 0 OR b.hits < 0 OR b.doubles < 0
                     OR b.triples < 0 OR b.home_runs < 0 OR b.walks < 0 OR b.strikeouts < 0 THEN 1
                WHEN b.hits > b.at_bats THEN 1
                WHEN b.home_runs > b.hits THEN 1
                WHEN (COALESCE(b.doubles, 0) + COALESCE(b.triples, 0) + COALESCE(b.home_runs, 0)) > b.hits THEN 1
                WHEN b.plate_appearances > 0 AND b.at_bats IS NOT NULL
                     AND b.plate_appearances < b.at_bats THEN 1
                ELSE 0
            END) AS violations,
            COUNT(*) AS total_batters
        FROM game_batting_stats b
        LEFT JOIN game g ON b.game_id = g.game_id
        GROUP BY COALESCE(g.season_id, CAST(SUBSTR(b.game_id, 1, 4) AS INT))
        """

        sample_sql = """
        SELECT
            b.id, b.game_id, b.player_name, b.plate_appearances,
            b.at_bats, b.hits, b.doubles, b.triples, b.home_runs
        FROM game_batting_stats b
        LEFT JOIN game g ON b.game_id = g.game_id
        WHERE (g.season_id = :season OR b.game_id LIKE :season || '%')
          AND (
            b.at_bats < 0 OR b.runs < 0 OR b.hits < 0 OR b.doubles < 0 OR b.triples < 0 OR b.home_runs < 0
            OR b.hits > b.at_bats
            OR b.home_runs > b.hits
            OR (COALESCE(b.doubles, 0) + COALESCE(b.triples, 0) + COALESCE(b.home_runs, 0)) > b.hits
            OR (b.plate_appearances > 0 AND b.at_bats IS NOT NULL AND b.plate_appearances < b.at_bats)
          )
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


__all__ = [
    "BattingInvariants",
]
