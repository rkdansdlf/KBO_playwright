"""H06 Player Game Stats to Boxscore and Score Reconciliation."""

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


class BoxscoreReconciliationInvariant(BaseHistoricalInvariant):
    """H06: Reconciles player game stats against team final scores and boxscores."""

    invariant_id: str = "H06-BOXSCORE-RECONCILIATION"
    name: str = "Boxscore & Score Reconciliation"
    layer: str = "H06"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H06-BOXSCORE-RECONCILIATION",
        name="Boxscore & Score Reconciliation",
        layer="H06",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game", "game_batting_stats"],
        required_columns={
            "game": ["game_id", "home_score", "away_score", "game_status"],
            "game_batting_stats": ["game_id", "team_side", "runs"],
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
        """Verify that sum of player runs matches game final score for games with recorded runs."""
        agg_sql = """
        WITH game_player_runs AS (
            SELECT
                b.game_id,
                b.team_side,
                SUM(b.runs) AS total_player_runs
            FROM game_batting_stats b
            GROUP BY b.game_id, b.team_side
        ),
        game_team_scores AS (
            SELECT
                g.game_id,
                CAST(SUBSTR(g.game_id, 1, 4) AS INT) AS season_year,
                g.home_score,
                g.away_score,
                COALESCE(h_pr.total_player_runs, 0) AS home_player_runs,
                COALESCE(a_pr.total_player_runs, 0) AS away_player_runs
            FROM game g
            INNER JOIN game_player_runs h_pr ON g.game_id = h_pr.game_id AND h_pr.team_side IN ('home', 'HOME', 'H')
            INNER JOIN game_player_runs a_pr ON g.game_id = a_pr.game_id AND a_pr.team_side IN ('away', 'AWAY', 'A')
            WHERE g.game_status IN ('COMPLETED', 'FINISHED', '종료')
              AND g.home_score IS NOT NULL
              AND g.away_score IS NOT NULL
              AND (g.home_score = 0 OR h_pr.total_player_runs > 0)
              AND (g.away_score = 0 OR a_pr.total_player_runs > 0)
        )
        SELECT
            season_year,
            SUM(CASE
                WHEN home_player_runs != home_score THEN 1
                WHEN away_player_runs != away_score THEN 1
                ELSE 0
            END) AS violations,
            COUNT(*) AS checked_games
        FROM game_team_scores
        WHERE season_year IS NOT NULL
        GROUP BY season_year
        """

        sample_sql = """
        WITH game_player_runs AS (
            SELECT
                b.game_id,
                b.team_side,
                SUM(b.runs) AS total_player_runs
            FROM game_batting_stats b
            GROUP BY b.game_id, b.team_side
        )
        SELECT
            g.game_id,
            g.game_date,
            g.home_team,
            g.home_score,
            h_pr.total_player_runs AS home_player_runs,
            g.away_team,
            g.away_score,
            a_pr.total_player_runs AS away_player_runs
        FROM game g
        LEFT JOIN game_player_runs h_pr ON g.game_id = h_pr.game_id AND h_pr.team_side IN ('home', 'HOME', 'H')
        LEFT JOIN game_player_runs a_pr ON g.game_id = a_pr.game_id AND a_pr.team_side IN ('away', 'AWAY', 'A')
        WHERE g.game_id LIKE :season || '%'
          AND g.game_status IN ('COMPLETED', 'FINISHED', '종료')
          AND (
            (h_pr.total_player_runs IS NOT NULL AND h_pr.total_player_runs != g.home_score)
            OR (a_pr.total_player_runs IS NOT NULL AND a_pr.total_player_runs != g.away_score)
          )
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


__all__ = [
    "BoxscoreReconciliationInvariant",
]
