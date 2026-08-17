"""Repository for provider-specific season statistics and projections."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from src.models.external_season_stat import ExternalSeasonStat
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.services.player_id_resolver import PlayerIdResolver

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlalchemy.orm import Session

    from src.sources.stats.base import ExternalStatRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalStatsSaveReport:
    """Summarize external rows saved and conservatively linked."""

    attempted: int = 0
    saved: int = 0
    resolved: int = 0
    unresolved_team: int = 0
    unresolved_player: int = 0

    def as_dict(self) -> dict[str, int]:
        """Return a JSON-compatible report mapping."""
        return {
            "attempted": self.attempted,
            "saved": self.saved,
            "resolved": self.resolved,
            "unresolved_team": self.unresolved_team,
            "unresolved_player": self.unresolved_player,
        }


@dataclass(frozen=True)
class ExternalStatsProjectionReport:
    """Summarize rows copied into existing season aggregates."""

    considered: int = 0
    projected: int = 0
    target_missing: int = 0

    def as_dict(self) -> dict[str, int]:
        """Return a JSON-compatible projection report mapping."""
        return {
            "considered": self.considered,
            "projected": self.projected,
            "target_missing": self.target_missing,
        }


class ExternalSeasonStatsRepository:
    """Persist provider rows and project them into existing KBO stat JSON."""

    def __init__(self, session: Session) -> None:
        """Initialize the repository with an active database session."""
        self.session = session

    def save_records(
        self,
        records: Sequence[ExternalStatRecord],
        *,
        content_hashes: dict[str, str] | None = None,
        fetched_at: datetime | None = None,
    ) -> ExternalStatsSaveReport:
        """Upsert records and resolve only unambiguous canonical players."""
        if not records:
            return ExternalStatsSaveReport()
        resolver = PlayerIdResolver(self.session, strict_game_resolution=True)
        captured_at = fetched_at or datetime.now(UTC).replace(tzinfo=None)
        report = ExternalStatsSaveReport(attempted=len(records))

        for record in records:
            existing = self.session.execute(
                select(ExternalSeasonStat).where(ExternalSeasonStat.source_record_key == record.source_record_key),
            ).scalar_one_or_none()
            player_id, status, note = self._resolve_record_player(record, resolver)
            if existing is not None and player_id is None and existing.player_id is not None:
                player_id = existing.player_id
                status = existing.resolution_status
                note = existing.resolution_note
            values = {
                "source_record_key": record.source_record_key,
                "provider": record.provider,
                "source_key": record.source_key,
                "stat_type": record.stat_type,
                "season": record.season,
                "league": record.league,
                "level": record.level,
                "external_player_id": record.external_player_id,
                "player_id": player_id,
                "player_name": record.player_name,
                "team_name": record.team_name,
                "team_code": record.team_code,
                "metrics": dict(record.metrics),
                "metric_metadata": dict(record.metric_metadata),
                "source_url": record.source_url,
                "content_hash": (content_hashes or {}).get(record.source_key),
                "fetched_at": captured_at,
                "parser_version": record.metric_metadata.get("parser_version", "external-stats-v1"),
                "resolution_status": status,
                "resolution_note": note,
            }
            if existing is None:
                self.session.add(ExternalSeasonStat(**values))
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
            report = _increment_save_report(report, status)

        self.session.flush()
        return report

    def project(
        self,
        *,
        provider: str,
        season: int,
        stat_type: str | None = None,
    ) -> ExternalStatsProjectionReport:
        """Copy selected external metrics into ``extra_stats.external_sources``."""
        query = select(ExternalSeasonStat).where(
            ExternalSeasonStat.provider == provider,
            ExternalSeasonStat.season == season,
            ExternalSeasonStat.player_id.is_not(None),
        )
        if stat_type:
            query = query.where(ExternalSeasonStat.stat_type == stat_type)
        rows = list(self.session.execute(query).scalars().all())
        projected = 0
        missing = 0
        for row in rows:
            target_model = PlayerSeasonBatting if row.stat_type == "batting" else PlayerSeasonPitching
            target = self._find_target(target_model, row)
            if target is None:
                row.resolution_status = "target_missing"
                missing += 1
                continue
            self._merge_external_payload(target, row)
            row.resolution_status = "resolved"
            projected += 1
        self.session.flush()
        return ExternalStatsProjectionReport(considered=len(rows), projected=projected, target_missing=missing)

    def _resolve_record_player(
        self,
        record: ExternalStatRecord,
        resolver: PlayerIdResolver,
    ) -> tuple[int | None, str, str | None]:
        if not record.team_code:
            return None, "unresolved_team", "provider team label did not map to a KBO team code"
        try:
            player_id = resolver.resolve_id(
                record.player_name,
                record.team_code,
                record.season,
                is_pitcher=record.stat_type == "pitching",
            )
        except (SQLAlchemyError, TypeError, ValueError):
            logger.exception("External player resolution failed for %s", record.player_name)
            return None, "unresolved_player", "player resolver failed"
        if player_id is None:
            return None, "unresolved_player", "no unambiguous KBO player match"
        return player_id, "resolved", None

    def _find_target(
        self,
        target_model: type[PlayerSeasonBatting | PlayerSeasonPitching],
        row: ExternalSeasonStat,
    ) -> PlayerSeasonBatting | PlayerSeasonPitching | None:
        if row.player_id is None or not row.team_code:
            return None
        return (
            self.session.query(target_model)
            .filter_by(
                player_id=row.player_id,
                season=row.season,
                league=row.league,
                level=row.level,
                team_code=row.team_code,
            )
            .first()
        )

    @staticmethod
    def _merge_external_payload(
        target: PlayerSeasonBatting | PlayerSeasonPitching,
        row: ExternalSeasonStat,
    ) -> None:
        extra_stats = dict(target.extra_stats) if isinstance(target.extra_stats, dict) else {}
        external_sources = extra_stats.get("external_sources")
        sources = dict(external_sources) if isinstance(external_sources, dict) else {}
        sources[row.provider] = {
            "metrics": dict(row.metrics),
            "external_player_id": row.external_player_id,
            "source_key": row.source_key,
            "source_url": row.source_url,
            "content_hash": row.content_hash,
            "fetched_at": row.fetched_at.isoformat(),
            "parser_version": row.parser_version,
        }
        extra_stats["external_sources"] = sources
        target.extra_stats = extra_stats


def overlay_external_metrics(
    session: Session,
    rows: Iterable[dict[str, object]],
    *,
    provider: str,
    season: int,
    stat_type: str,
) -> list[dict[str, object]]:
    """Overlay one explicitly selected provider onto ranking input rows."""
    external_rows = list(
        session.execute(
            select(ExternalSeasonStat).where(
                ExternalSeasonStat.provider == provider,
                ExternalSeasonStat.season == season,
                ExternalSeasonStat.stat_type == stat_type,
                ExternalSeasonStat.player_id.is_not(None),
            ),
        )
        .scalars()
        .all(),
    )
    by_key = {(row.player_id, row.team_code): row.metrics for row in external_rows}
    by_player = {row.player_id: row.metrics for row in external_rows if row.team_code is None}
    overlaid: list[dict[str, object]] = []
    for row in rows:
        copied = dict(row)
        player_id = copied.get("player_id")
        team_code = copied.get("team_code")
        metrics = by_key.get((player_id, team_code)) or by_player.get(player_id)
        if metrics:
            extra_stats = dict(copied.get("extra_stats")) if isinstance(copied.get("extra_stats"), dict) else {}
            extra_stats.update(metrics)
            copied["extra_stats"] = extra_stats
        overlaid.append(copied)
    return overlaid


def _increment_save_report(report: ExternalStatsSaveReport, status: str) -> ExternalStatsSaveReport:
    """Increment a save report without mutating the frozen dataclass."""
    return ExternalStatsSaveReport(
        attempted=report.attempted,
        saved=report.saved + 1,
        resolved=report.resolved + int(status == "resolved"),
        unresolved_team=report.unresolved_team + int(status == "unresolved_team"),
        unresolved_player=report.unresolved_player + int(status == "unresolved_player"),
    )
