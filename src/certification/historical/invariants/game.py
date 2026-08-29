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
        """Audit games for impossible score/winner discrepancies with canonical franchise & DRAW awareness."""
        agg_sql = """
        WITH aliased_game AS (
            SELECT
                g.game_id,
                CAST(SUBSTR(g.game_id, 1, 4) AS INT) AS season_year,
                g.home_score,
                g.away_score,
                g.winning_score,
                CASE
                    WHEN g.home_team IN ('OB', 'DB') THEN 'DB'
                    WHEN g.home_team IN ('HT', 'KIA') THEN 'KIA'
                    WHEN g.home_team IN ('MBC', 'MB', 'LG') THEN 'LG'
                    WHEN g.home_team IN ('WO', 'NX', 'KH') THEN 'KH'
                    WHEN g.home_team IN ('SK', 'SSG') THEN 'SSG'
                    WHEN g.home_team IN ('BG', 'HH') THEN 'HH'
                    WHEN g.home_team IN ('SM', 'CB', 'TP', 'HD', 'HU') THEN 'HU'
                    ELSE g.home_team
                END AS h_canon,
                CASE
                    WHEN g.away_team IN ('OB', 'DB') THEN 'DB'
                    WHEN g.away_team IN ('HT', 'KIA') THEN 'KIA'
                    WHEN g.away_team IN ('MBC', 'MB', 'LG') THEN 'LG'
                    WHEN g.away_team IN ('WO', 'NX', 'KH') THEN 'KH'
                    WHEN g.away_team IN ('SK', 'SSG') THEN 'SSG'
                    WHEN g.away_team IN ('BG', 'HH') THEN 'HH'
                    WHEN g.away_team IN ('SM', 'CB', 'TP', 'HD', 'HU') THEN 'HU'
                    ELSE g.away_team
                END AS a_canon,
                CASE
                    WHEN g.winning_team IN ('OB', 'DB') THEN 'DB'
                    WHEN g.winning_team IN ('HT', 'KIA') THEN 'KIA'
                    WHEN g.winning_team IN ('MBC', 'MB', 'LG') THEN 'LG'
                    WHEN g.winning_team IN ('WO', 'NX', 'KH') THEN 'KH'
                    WHEN g.winning_team IN ('SK', 'SSG') THEN 'SSG'
                    WHEN g.winning_team IN ('BG', 'HH') THEN 'HH'
                    WHEN g.winning_team IN ('SM', 'CB', 'TP', 'HD', 'HU') THEN 'HU'
                    WHEN g.winning_team IN ('무', 'DRAW', 'TIE', 'draw', 'tie', '') THEN 'DRAW'
                    ELSE g.winning_team
                END AS w_canon
            FROM game g
            WHERE g.game_id IS NOT NULL
        )
        SELECT
            season_year,
            SUM(CASE
                WHEN home_score < 0 OR away_score < 0 THEN 1
                WHEN winning_score IS NOT NULL AND winning_score < 0 THEN 1
                WHEN w_canon IS NOT NULL AND w_canon NOT IN ('DRAW', '')
                     AND home_score > away_score AND w_canon != h_canon THEN 1
                WHEN w_canon IS NOT NULL AND w_canon NOT IN ('DRAW', '')
                     AND away_score > home_score AND w_canon != a_canon THEN 1
                WHEN w_canon IS NOT NULL AND w_canon NOT IN ('DRAW', '')
                     AND home_score = away_score AND w_canon != 'DRAW' THEN 1
                ELSE 0
            END) AS violations,
            COUNT(*) AS total_games
        FROM aliased_game
        GROUP BY season_year
        """

        sample_sql = """
        WITH aliased_game AS (
            SELECT
                g.game_id,
                g.game_date,
                g.home_team,
                g.away_team,
                g.home_score,
                g.away_score,
                g.winning_team,
                g.winning_score,
                CASE
                    WHEN g.home_team IN ('OB', 'DB') THEN 'DB'
                    WHEN g.home_team IN ('HT', 'KIA') THEN 'KIA'
                    WHEN g.home_team IN ('MBC', 'MB', 'LG') THEN 'LG'
                    WHEN g.home_team IN ('WO', 'NX', 'KH') THEN 'KH'
                    WHEN g.home_team IN ('SK', 'SSG') THEN 'SSG'
                    WHEN g.home_team IN ('BG', 'HH') THEN 'HH'
                    WHEN g.home_team IN ('SM', 'CB', 'TP', 'HD', 'HU') THEN 'HU'
                    ELSE g.home_team
                END AS h_canon,
                CASE
                    WHEN g.away_team IN ('OB', 'DB') THEN 'DB'
                    WHEN g.away_team IN ('HT', 'KIA') THEN 'KIA'
                    WHEN g.away_team IN ('MBC', 'MB', 'LG') THEN 'LG'
                    WHEN g.away_team IN ('WO', 'NX', 'KH') THEN 'KH'
                    WHEN g.away_team IN ('SK', 'SSG') THEN 'SSG'
                    WHEN g.away_team IN ('BG', 'HH') THEN 'HH'
                    WHEN g.away_team IN ('SM', 'CB', 'TP', 'HD', 'HU') THEN 'HU'
                    ELSE g.away_team
                END AS a_canon,
                CASE
                    WHEN g.winning_team IN ('OB', 'DB') THEN 'DB'
                    WHEN g.winning_team IN ('HT', 'KIA') THEN 'KIA'
                    WHEN g.winning_team IN ('MBC', 'MB', 'LG') THEN 'LG'
                    WHEN g.winning_team IN ('WO', 'NX', 'KH') THEN 'KH'
                    WHEN g.winning_team IN ('SK', 'SSG') THEN 'SSG'
                    WHEN g.winning_team IN ('BG', 'HH') THEN 'HH'
                    WHEN g.winning_team IN ('SM', 'CB', 'TP', 'HD', 'HU') THEN 'HU'
                    WHEN g.winning_team IN ('무', 'DRAW', 'TIE', 'draw', 'tie', '') THEN 'DRAW'
                    ELSE g.winning_team
                END AS w_canon
            FROM game g
            WHERE g.game_id LIKE :season || '%'
        )
        SELECT game_id, game_date, home_team, away_team, home_score, away_score, winning_team, winning_score
        FROM aliased_game
        WHERE (
            home_score < 0 OR away_score < 0
            OR (winning_score IS NOT NULL AND winning_score < 0)
            OR (w_canon IS NOT NULL AND w_canon NOT IN ('DRAW', '')
                AND home_score > away_score AND w_canon != h_canon)
            OR (w_canon IS NOT NULL AND w_canon NOT IN ('DRAW', '')
                AND away_score > home_score AND w_canon != a_canon)
            OR (w_canon IS NOT NULL AND w_canon NOT IN ('DRAW', '')
                AND home_score = away_score AND w_canon != 'DRAW')
        )
        LIMIT 20
        """

        return self._execute_aggregate_query(engine, agg_sql, sample_sql, seasons)


__all__ = [
    "GameStateInvariant",
]
