"""Historical Remediation and Certification Lineage Tracer."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from src.lineage.models import CorrectionRecord

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

_REMEDIATION_DATE = "2026-08-29"
_CODE_REVISION = "7b2f9a8c"


class CorrectionTracer:
    """Tracks historical data remediation, backfills, and normalization lineage."""

    _SYSTEM_REMEDIATIONS: ClassVar[list[CorrectionRecord]] = [
        CorrectionRecord(
            remediation_id="REM-20210523LTOB0-ZERO-SCORE",
            entity_type="game",
            entity_id="20210523LTOB0",
            affected_table="game",
            affected_count=1,
            field_name="away_score",
            original_value=None,
            corrected_value=0,
            reason="H01 Shutout Score Remediation: Lotte Giants scored 0 runs in 0-4 shutout loss to Doosan",
            source_evidence="KBO Official BoxScore URL https://www.koreabaseball.com/Schedule/Game/BoxScore.aspx?gameId=20210523LTOB0",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
        CorrectionRecord(
            remediation_id="REM-H01-CANCELLED-PLACEHOLDERS",
            entity_type="game",
            entity_id="ALL_UNPLAYED_PLACEHOLDERS",
            affected_table="game",
            affected_count=5029,
            field_name="game_status",
            original_value="COMPLETED",
            corrected_value="CANCELLED",
            reason="H01 Status Normalization: 5,029 un-played/rained out schedule stubs normalized to CANCELLED",
            source_evidence="KBO Official Schedule Cancelled/Postponed Matches Registry",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
        CorrectionRecord(
            remediation_id="REM-H01-BOXSCORE-SCORE-BACKFILL",
            entity_type="game",
            entity_id="MISSING_HEADER_SCORES_365",
            affected_table="game",
            affected_count=365,
            field_name="home_score, away_score",
            original_value=None,
            corrected_value="SUM(game_batting_stats.runs)",
            reason="H01 Score Backfill: 365 completed games backfilled from validated player batting stats",
            source_evidence="PlayerGameBatting runs aggregation per game_id",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
        CorrectionRecord(
            remediation_id="REM-H02-SYNTHETIC-ORPHAN-PURGE",
            entity_type="game_batting_stats",
            entity_id="SYNTHETIC_ORPHANS_15340",
            affected_table="game_batting_stats",
            affected_count=15340,
            field_name="row_presence",
            original_value="ORPHAN_SYNTHETIC_ROW",
            corrected_value="QUARANTINED",
            reason="H02 Test Artifact Cleanup: 15,340 synthetic mock rows quarantined from production store",
            source_evidence="Synthetic Pattern Match 'OB_타자_%' and unlinked mock IDs",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
        CorrectionRecord(
            remediation_id="REM-H03-DRAW-CANONICALIZATION",
            entity_type="game",
            entity_id="DRAW_AND_FRANCHISE_OUTCOMES_642",
            affected_table="game",
            affected_count=642,
            field_name="winning_team",
            original_value="VARYING_ALIAS",
            corrected_value="CANONICAL_DRAW_OR_FRANCHISE",
            reason="H03 Rule Canonicalization: DRAW outcomes and franchise alias mappings canonicalized",
            source_evidence="Historical franchise transformation lookup table",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
        CorrectionRecord(
            remediation_id="REM-H06-SUMMARY-ROW-PURGE",
            entity_type="game_batting_stats",
            entity_id="SUMMARY_ROWS_1068",
            affected_table="game_batting_stats",
            affected_count=1068,
            field_name="row_presence",
            original_value="TEAM_SUMMARY_AS_PLAYER",
            corrected_value="PURGED",
            reason="H06 Summary Row Cleanup: 1,068 team total rows (player_name='0') purged from 2010 boxscores",
            source_evidence="BoxScore crawler summary row filter (player_name != '0')",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
        CorrectionRecord(
            remediation_id="REM-PA-FORMULA-BACKFILL",
            entity_type="player_season_batting",
            entity_id="PA_FORMULA_SH_SF_2020",
            affected_table="player_season_batting",
            affected_count=45,
            field_name="sacrifice_hits, sacrifice_flies",
            original_value=0,
            corrected_value="DERIVED_FROM_PBP",
            reason="H04 PA Formula Backfill: Sacrifice hits and flies derived from pitch-by-pitch text relays",
            source_evidence="GamePlayByPlay event text relay descriptions",
            timestamp=_REMEDIATION_DATE,
            code_revision=_CODE_REVISION,
            reversible=True,
        ),
    ]

    def __init__(self, engine: Engine | None = None) -> None:
        """Initialize correction tracer."""
        self.engine = engine

    def get_corrections_for_entity(self, entity_type: str, entity_id: str) -> list[CorrectionRecord]:
        """Find all remediation records matching the specified entity."""
        return [
            rec
            for rec in self._SYSTEM_REMEDIATIONS
            if rec.entity_type == entity_type and (rec.entity_id == entity_id or rec.entity_id.startswith("ALL_"))
        ]

    def get_corrections_by_table(self, table_name: str) -> list[CorrectionRecord]:
        """Find all remediation records affecting the specified table."""
        return [rec for rec in self._SYSTEM_REMEDIATIONS if rec.affected_table == table_name]

    def get_total_remediated_rows(self) -> int:
        """Calculate total rows touched by declared system remediations."""
        return sum(rec.affected_count for rec in self._SYSTEM_REMEDIATIONS)

    def list_all_remediations(self) -> list[CorrectionRecord]:
        """List all system remediation and normalization records."""
        return list(self._SYSTEM_REMEDIATIONS)


__all__ = [
    "CorrectionTracer",
]
