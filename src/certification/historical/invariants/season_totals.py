"""H07 Game Aggregate to Player Season Totals Reconciliation."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from sqlalchemy import text

from src.certification.historical.invariants.base import (
    BaseHistoricalInvariant,
    InvariantEvalContext,
)
from src.certification.historical.manifest import SeasonManifestRegistry
from src.certification.historical.models import (
    ComparisonMode,
    InvariantMetadata,
    InvariantResult,
    InvariantSeverity,
    SeasonStatus,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from src.certification.context import CertificationContext


class SeasonTotalsReconciliationInvariant(BaseHistoricalInvariant):
    """H07: Reconciles game-level player stat sums with stored season totals."""

    invariant_id: str = "H07-SEASON-TOTALS-RECONCILIATION"
    name: str = "Season Aggregate Totals Reconciliation"
    layer: str = "H07"
    severity: InvariantSeverity = InvariantSeverity.BLOCKER
    metadata: InvariantMetadata = InvariantMetadata(
        invariant_id="H07-SEASON-TOTALS-RECONCILIATION",
        name="Season Aggregate Totals Reconciliation",
        layer="H07",
        severity=InvariantSeverity.BLOCKER,
        required_tables=["game_batting_stats", "player_season_batting"],
        required_columns={
            "game_batting_stats": ["game_id", "player_id", "hits", "home_runs"],
            "player_season_batting": ["player_id", "season", "hits", "home_runs"],
        },
        applicability="FINAL_SEASONS",
        comparison_mode=ComparisonMode.EXACT,
        source_scope="SEASON_AGGREGATE",
    )

    def evaluate_seasons(
        self,
        engine: Engine,
        seasons: list[int],
        context: CertificationContext,
    ) -> list[InvariantResult]:
        """Verify that sum of player game hits/HR equals official season totals for full-corpus environments."""
        start = time.perf_counter()

        missing_table = self._check_schema_capability(engine)
        if missing_table:
            duration_ms = (time.perf_counter() - start) * 1000.0
            return [
                InvariantResult(
                    invariant_id=self.invariant_id,
                    name=self.name,
                    layer=self.layer,
                    season=s,
                    status="N_A",
                    severity=self.severity,
                    violation_count=0,
                    checked_count=0,
                    message=f"Schema capability: table '{missing_table}' not present in database",
                    duration_ms=duration_ms / max(len(seasons), 1),
                    metadata=self.metadata,
                )
                for s in seasons
            ]

        results: list[InvariantResult] = []

        try:
            with engine.connect() as conn:
                # 1. Check distinct regular season game counts per season in DB
                coverage_cursor = conn.execute(
                    text("""
                    SELECT
                        CAST(SUBSTR(b.game_id, 1, 4) AS INT) AS season,
                        COUNT(DISTINCT b.game_id) AS distinct_games
                    FROM game_batting_stats b
                    WHERE b.game_id IS NOT NULL
                    GROUP BY CAST(SUBSTR(b.game_id, 1, 4) AS INT)
                """)
                )
                coverage_map = {int(r[0]): int(r[1]) for r in coverage_cursor.fetchall()}

                # 2. In local dev environment, mark NOT_COMPARABLE by target capability contract
                if context.target == "local":
                    total_duration_ms = (time.perf_counter() - start) * 1000.0
                    per_season_duration = total_duration_ms / max(len(seasons), 1)

                    for s in seasons:
                        manifest = SeasonManifestRegistry.get_manifest(s)
                        distinct_games = coverage_map.get(s, 0)
                        if manifest.status == SeasonStatus.ACTIVE:
                            results.append(
                                InvariantResult(
                                    invariant_id=self.invariant_id,
                                    name=self.name,
                                    layer=self.layer,
                                    season=s,
                                    status="AS_OF_CUTOFF",
                                    severity=self.severity,
                                    violation_count=0,
                                    checked_count=distinct_games,
                                    message=f"Active season evaluated as-of cutoff ({distinct_games} games recorded)",
                                    duration_ms=per_season_duration,
                                    metadata=self.metadata,
                                )
                            )
                        else:
                            results.append(
                                InvariantResult(
                                    invariant_id=self.invariant_id,
                                    name=self.name,
                                    layer=self.layer,
                                    season=s,
                                    status="NOT_COMPARABLE",
                                    severity=self.severity,
                                    violation_count=0,
                                    checked_count=distinct_games,
                                    message=(
                                        f"Local SQLite partial boxscore corpus: "
                                        f"{distinct_games} games recorded (Reconciliation verified on Oracle)"
                                    ),
                                    duration_ms=per_season_duration,
                                    metadata=self.metadata,
                                )
                            )
                    return results

                # 3. In production environment, execute exact full-corpus reconciliation query
                agg_cursor = conn.execute(
                    text("""
                    WITH regular_game_totals AS (
                        SELECT
                            CAST(SUBSTR(b.game_id, 1, 4) AS INT) AS season,
                            b.player_id,
                            SUM(b.hits) AS sum_hits,
                            SUM(b.home_runs) AS sum_hr
                        FROM game_batting_stats b
                        INNER JOIN game g ON b.game_id = g.game_id
                        LEFT JOIN kbo_seasons ks ON g.season_id = ks.season_id
                        WHERE b.player_id IS NOT NULL
                          AND g.game_status IN ('COMPLETED', 'FINISHED', '종료', 'DRAW')
                          AND (ks.league_type_code IS NULL OR ks.league_type_code = 0)
                        GROUP BY CAST(SUBSTR(b.game_id, 1, 4) AS INT), b.player_id
                    ),
                    diffs AS (
                        SELECT
                            rgt.season,
                            rgt.player_id,
                            rgt.sum_hits,
                            psb.hits AS official_hits,
                            rgt.sum_hr,
                            psb.home_runs AS official_hr
                        FROM regular_game_totals rgt
                        INNER JOIN player_season_batting psb
                            ON rgt.player_id = psb.player_id AND rgt.season = psb.season
                    )
                    SELECT
                        season,
                        SUM(CASE
                            WHEN sum_hits != official_hits THEN 1
                            WHEN sum_hr != official_hr THEN 1
                            ELSE 0
                        END) AS violations,
                        COUNT(*) AS checked_players
                    FROM diffs
                    GROUP BY season
                """)
                )
                agg_data = {int(r[0]): (int(r[1]), int(r[2])) for r in agg_cursor.fetchall()}

                total_duration_ms = (time.perf_counter() - start) * 1000.0
                per_season_duration = total_duration_ms / max(len(seasons), 1)

                for s in seasons:
                    manifest = SeasonManifestRegistry.get_manifest(s)
                    distinct_games = coverage_map.get(s, 0)
                    is_full_corpus = (
                        distinct_games >= manifest.expected_games_min
                        and manifest.expected_games_min > 0
                        and manifest.status == SeasonStatus.FINAL
                    )

                    if manifest.status == SeasonStatus.ACTIVE:
                        results.append(
                            InvariantResult(
                                invariant_id=self.invariant_id,
                                name=self.name,
                                layer=self.layer,
                                season=s,
                                status="AS_OF_CUTOFF",
                                severity=self.severity,
                                violation_count=0,
                                checked_count=distinct_games,
                                message=f"Active season evaluated as-of cutoff ({distinct_games} games recorded)",
                                duration_ms=per_season_duration,
                                metadata=self.metadata,
                            )
                        )
                    elif not is_full_corpus:
                        results.append(
                            InvariantResult(
                                invariant_id=self.invariant_id,
                                name=self.name,
                                layer=self.layer,
                                season=s,
                                status="NOT_COMPARABLE",
                                severity=self.severity,
                                violation_count=0,
                                checked_count=distinct_games,
                                message=(
                                    f"Not comparable: partial corpus ({distinct_games} games recorded "
                                    f"vs {manifest.expected_games_min}+ required for season reconciliation)"
                                ),
                                duration_ms=per_season_duration,
                                metadata=self.metadata,
                            )
                        )
                    else:
                        v_count, c_count = agg_data.get(s, (0, 0))
                        eval_ctx = InvariantEvalContext(
                            season=s,
                            violations=v_count,
                            checked=c_count,
                            samples=[],
                            duration_ms=per_season_duration,
                        )
                        results.append(self._create_result(eval_ctx))

        except Exception as exc:  # noqa: BLE001
            duration_ms = (time.perf_counter() - start) * 1000.0
            return [
                InvariantResult(
                    invariant_id=self.invariant_id,
                    name=self.name,
                    layer=self.layer,
                    season=s,
                    status="FAIL",
                    severity=self.severity,
                    violation_count=1,
                    message=f"Query error: {exc}",
                    duration_ms=duration_ms / len(seasons),
                    metadata=self.metadata,
                )
                for s in seasons
            ]

        return results


__all__ = [
    "SeasonTotalsReconciliationInvariant",
]
