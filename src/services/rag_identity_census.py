"""Build a read-only census of legacy and canonical RAG identities."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from src.constants import KST
from src.models.award import Award
from src.models.futures_schedule import FuturesGameSchedule
from src.models.game import Game, GameHighlight, GamePlayByPlay
from src.models.player import PlayerMovement
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat
from src.models.team_history import TeamHistory
from src.services.rag_index_identity import (
    PbpSourceIdentity,
    stable_award_source_row_id,
    stable_futures_source_row_id,
    stable_highlight_source_row_id,
    stable_milestone_source_row_id,
    stable_pbp_source_row_id,
    stable_player_movement_source_row_id,
    stable_splits_source_row_id,
    stable_team_history_source_row_id,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select


R2_SOURCE_TABLES = (
    "awards",
    "team_history",
    "milestone",
    "futures_schedule",
    "player_splits",
    "player_movements",
    "game_play_by_play",
    "game_highlights",
)
R2_MANIFEST_VERSION = "r2-identity-census-v1"
R2_INDEX_VERSION = "rag-v2"

_REGULAR_SEASON_CODE = 0
_PBP_KEYWORDS = (
    "홈런",
    "적시타",
    "안타",
    "삼진",
    "병살타",
    "도루",
    "결승타",
    "타점",
    "끝내기",
    "만루홈런",
    "역전타",
    "득점권",
)

SAFE_REKEY = "SAFE_REKEY"
TARGET_EXISTS_SAME_CONTENT = "TARGET_EXISTS_SAME_CONTENT"
TARGET_EXISTS_CONTENT_MISMATCH = "TARGET_EXISTS_CONTENT_MISMATCH"
TARGET_EXISTS_UNKNOWN_CONTENT = "TARGET_EXISTS_UNKNOWN_CONTENT"
SOURCE_COLLISION = "SOURCE_COLLISION"
ORPHAN_SOURCE_ROW = "ORPHAN_SOURCE_ROW"


@dataclass(frozen=True, slots=True)
class SourceIdentityRecord:
    """Represent one source primary key and its canonical RAG identity."""

    source_table: str
    source_row_id: str
    natural_source_row_id: str


@dataclass(frozen=True, slots=True)
class ExistingIdentityRow:
    """Represent the identity columns needed from one persisted RAG chunk."""

    chunk_id: int
    source_table: str
    source_row_id: str
    content_hash: str | None
    index_status: str


@dataclass(frozen=True, slots=True)
class IdentityCensusEntry:
    """Describe the disposition of one legacy numeric RAG identity."""

    source_table: str
    chunk_id: int
    legacy_source_row_id: str
    natural_source_row_id: str | None
    disposition: str
    index_status: str
    legacy_content_hash: str | None
    target_chunk_ids: tuple[int, ...] = ()
    target_content_hashes: tuple[str | None, ...] = ()
    source_record_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """Serialize one manifest entry."""
        return {
            "source_table": self.source_table,
            "chunk_id": self.chunk_id,
            "legacy_source_row_id": self.legacy_source_row_id,
            "natural_source_row_id": self.natural_source_row_id,
            "disposition": self.disposition,
            "index_status": self.index_status,
            "legacy_content_hash": self.legacy_content_hash,
            "target_chunk_ids": list(self.target_chunk_ids),
            "target_content_hashes": list(self.target_content_hashes),
            "source_record_ids": list(self.source_record_ids),
        }


@dataclass(frozen=True, slots=True)
class SourceIdentitySummary:
    """Summarize identity coverage for one RAG source table."""

    source_table: str
    source_rows: int
    legacy_numeric_rows: int
    legacy_non_numeric_rows: int
    safe_source_matches: int
    safe_rekey_candidates: int
    existing_natural_target: int
    orphan_rows: int
    collision_keys: int
    collision_rows: int
    source_rows_missing_in_index: int
    by_disposition: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        """Serialize one source summary."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IdentityCensusReport:
    """Contain the complete R2 census and its rekey manifest entries."""

    source_tables: tuple[str, ...]
    summaries: tuple[SourceIdentitySummary, ...]
    entries: tuple[IdentityCensusEntry, ...]
    manifest_version: str = R2_MANIFEST_VERSION
    target_index_version: str = R2_INDEX_VERSION

    @property
    def unsafe_entry_count(self) -> int:
        """Return the number of entries that cannot be directly rekeyed."""
        return sum(entry.disposition != SAFE_REKEY for entry in self.entries)

    def totals(self) -> dict[str, int]:
        """Aggregate the main census counters across source tables."""
        fields = (
            "source_rows",
            "legacy_numeric_rows",
            "legacy_non_numeric_rows",
            "safe_source_matches",
            "safe_rekey_candidates",
            "existing_natural_target",
            "orphan_rows",
            "collision_keys",
            "collision_rows",
            "source_rows_missing_in_index",
        )
        return {field: sum(int(getattr(summary, field)) for summary in self.summaries) for field in fields}

    def to_summary_dict(self, *, sample_limit: int = 20) -> dict[str, object]:
        """Serialize compact report data for stdout and CI gates."""
        samples = [entry.to_dict() for entry in self.entries if entry.disposition != SAFE_REKEY][: max(sample_limit, 0)]
        return {
            "manifest_version": self.manifest_version,
            "target_index_version": self.target_index_version,
            "read_only": True,
            "source_tables": list(self.source_tables),
            "totals": self.totals(),
            "unsafe_entry_count": self.unsafe_entry_count,
            "sources": [summary.to_dict() for summary in self.summaries],
            "unsafe_sample": samples,
        }

    def to_manifest_dict(self) -> dict[str, object]:
        """Serialize the complete manifest for a later apply-gated rekey."""
        entries_data = [entry.to_dict() for entry in self.entries]
        totals = self.totals()
        disposition_counts = Counter(e.disposition for e in self.entries)

        # Build manifest header
        header = self._build_manifest_header(len(self.entries), dict(disposition_counts))

        return {
            "manifest_header": header,
            "manifest_version": self.manifest_version,
            "target_index_version": self.target_index_version,
            "read_only": True,
            "source_tables": list(self.source_tables),
            "totals": totals,
            "unsafe_entry_count": self.unsafe_entry_count,
            "sources": [summary.to_dict() for summary in self.summaries],
            "entries": entries_data,
        }

    def _build_manifest_header(self, entry_count: int, disposition_counts: dict[str, int]) -> dict[str, object]:
        """Build manifest header with metadata for validation."""
        # Compute manifest SHA (excluding header)
        manifest_content = json.dumps(
            {
                "manifest_version": self.manifest_version,
                "target_index_version": self.target_index_version,
                "read_only": True,
                "source_tables": list(self.source_tables),
                "totals": self.totals(),
                "unsafe_entry_count": self.unsafe_entry_count,
                "sources": [summary.to_dict() for summary in self.summaries],
                "entries": [entry.to_dict() for entry in self.entries],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        manifest_sha = hashlib.sha256(manifest_content).hexdigest()

        # Get current git SHA
        try:
            git_sha = subprocess.run(
                ["/usr/bin/git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, cwd=Path.cwd()
            ).stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError, OSError):
            git_sha = "unknown"

        # Database fingerprint
        db_url = os.getenv("DATABASE_URL", "")
        safe_url = re.sub(r"://[^:]+:[^@]+@", "://***:***@", db_url)
        fp_content = f"{safe_url}|{os.getenv('RAG_INDEX_VERSION', 'rag-v1')}".encode()
        db_fingerprint = hashlib.sha256(fp_content).hexdigest()[:16]

        return {
            "manifest_schema_version": "r2-rekey-manifest-v1",
            "identity_schema_version": "r2",
            "generated_at": datetime.now(KST).isoformat(),
            "database_fingerprint": db_fingerprint,
            "git_commit_sha": git_sha,
            "manifest_sha256": manifest_sha,
            "expected_entry_count": entry_count,
            "expected_disposition_counts": disposition_counts,
        }


def _regular_season_ids(season: int) -> Select:
    """Return a subquery for regular-season IDs for a requested year."""
    from src.models.season import KboSeason

    return select(KboSeason.season_id).where(
        KboSeason.season_year == season,
        KboSeason.league_type_code == _REGULAR_SEASON_CODE,
    )


def _award_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield award identities from the source table."""
    statement = select(Award.id, Award.year, Award.award_type, Award.category, Award.player_name)
    if season is not None:
        statement = statement.where(Award.year == season)
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord(
            "awards",
            str(row[0]),
            stable_award_source_row_id(row[1], row[2], row[3], row[4]),
        )


def _team_history_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield team-history identities from the source table."""
    statement = select(TeamHistory.id, TeamHistory.season, TeamHistory.team_code)
    if season is not None:
        statement = statement.where(TeamHistory.season == season)
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord("team_history", str(row[0]), stable_team_history_source_row_id(row[1], row[2]))


def _milestone_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield milestone identities from the source table."""
    statement = select(
        PlayerMilestone.id,
        PlayerMilestone.season,
        PlayerMilestone.player_id,
        PlayerMilestone.milestone_category,
    )
    if season is not None:
        statement = statement.where(PlayerMilestone.season == season)
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord(
            "milestone",
            str(row[0]),
            stable_milestone_source_row_id(row[1], row[2], row[3]),
        )


def _futures_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield Futures schedule identities from the source table."""
    statement = select(FuturesGameSchedule.id, FuturesGameSchedule.game_id, FuturesGameSchedule.season)
    if season is not None:
        statement = statement.where(FuturesGameSchedule.season == season)
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord(
            "futures_schedule",
            str(row[0]),
            stable_futures_source_row_id(row[1] or str(row[0])),
        )


def _splits_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield player-split identities from the source table."""
    statement = select(
        PlayerSplitsStat.id,
        PlayerSplitsStat.season,
        PlayerSplitsStat.player_id,
        PlayerSplitsStat.split_type,
        PlayerSplitsStat.split_key,
    )
    if season is not None:
        statement = statement.where(PlayerSplitsStat.season == season)
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord(
            "player_splits",
            str(row[0]),
            stable_splits_source_row_id(row[1], row[2], row[3], row[4]),
        )


def _movement_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield player-movement identities from the source table."""
    from sqlalchemy import extract

    statement = select(
        PlayerMovement.id,
        PlayerMovement.movement_date,
        PlayerMovement.team_code,
        PlayerMovement.player_name,
        PlayerMovement.section,
    )
    if season is not None:
        statement = statement.where(extract("year", PlayerMovement.movement_date) == season)
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord(
            "player_movements",
            str(row[0]),
            stable_player_movement_source_row_id(row[1], row[2], row[3], row[4]),
        )


def _pbp_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield identities for the same keyword-filtered PBP rows as the builder."""
    keyword_filter = or_(*(GamePlayByPlay.play_description.contains(keyword) for keyword in _PBP_KEYWORDS))
    statement = select(
        GamePlayByPlay.id,
        GamePlayByPlay.game_id,
        GamePlayByPlay.inning,
        GamePlayByPlay.inning_half,
        GamePlayByPlay.pitcher_name,
        GamePlayByPlay.batter_name,
        GamePlayByPlay.play_description,
        GamePlayByPlay.event_type,
        GamePlayByPlay.result,
        GamePlayByPlay.source_row_index,
        GamePlayByPlay.source_name,
    ).join(Game, GamePlayByPlay.game_id == Game.game_id)
    statement = statement.where(GamePlayByPlay.play_description.is_not(None), keyword_filter)
    if season is not None:
        statement = statement.where(Game.season_id.in_(_regular_season_ids(season)))
    for row in session.execute(statement).yield_per(5_000):
        identity = PbpSourceIdentity(
            game_id=row[1],
            source_row_index=row[9],
            inning=row[2],
            inning_half=row[3],
            pitcher_name=row[4],
            batter_name=row[5],
            play_description=row[6],
            event_type=row[7],
            result=row[8],
            source_name=row[10],
        )
        yield SourceIdentityRecord("game_play_by_play", str(row[0]), stable_pbp_source_row_id(identity))


def _highlight_records(session: Session, season: int | None) -> Iterator[SourceIdentityRecord]:
    """Yield game-highlight identities from the source table."""
    statement = select(
        GameHighlight.id,
        GameHighlight.game_id,
        GameHighlight.highlight_type,
        GameHighlight.event_seq,
        GameHighlight.description,
    ).join(Game, GameHighlight.game_id == Game.game_id)
    if season is not None:
        statement = statement.where(Game.season_id.in_(_regular_season_ids(season)))
    for row in session.execute(statement).yield_per(1_000):
        yield SourceIdentityRecord(
            "game_highlights",
            str(row[0]),
            stable_highlight_source_row_id(row[1], row[2], row[3], row[4]),
        )


def iter_source_identity_records(
    session: Session,
    source_table: str,
    season: int | None = None,
) -> Iterator[SourceIdentityRecord]:
    """Yield canonical identities for one supported source table."""
    record_builders = {
        "awards": _award_records,
        "team_history": _team_history_records,
        "milestone": _milestone_records,
        "futures_schedule": _futures_records,
        "player_splits": _splits_records,
        "player_movements": _movement_records,
        "game_play_by_play": _pbp_records,
        "game_highlights": _highlight_records,
    }
    try:
        builder = record_builders[source_table]
    except KeyError as exc:
        message = f"unsupported R2 source table: {source_table}"
        raise ValueError(message) from exc
    yield from builder(session, season)


def _is_legacy_numeric_id(source_row_id: str) -> bool:
    """Return whether an identity is an ASCII autoincrement-style value."""
    return source_row_id.isascii() and source_row_id.isdecimal()


def _target_disposition(
    legacy: ExistingIdentityRow,
    targets: Sequence[ExistingIdentityRow],
) -> str:
    """Classify a natural target collision by content-hash evidence."""
    if not targets:
        return SAFE_REKEY
    target_hashes = [target.content_hash for target in targets]
    if legacy.content_hash and legacy.content_hash in target_hashes:
        return TARGET_EXISTS_SAME_CONTENT
    if legacy.content_hash and all(target_hash for target_hash in target_hashes):
        return TARGET_EXISTS_CONTENT_MISMATCH
    return TARGET_EXISTS_UNKNOWN_CONTENT


def _build_source_summary(
    source_table: str,
    source_records: Sequence[SourceIdentityRecord],
    entries: Sequence[IdentityCensusEntry],
    existing_rows: Sequence[ExistingIdentityRow],
) -> SourceIdentitySummary:
    """Calculate counters for one source from its manifest entries."""
    by_disposition = Counter(entry.disposition for entry in entries)
    numeric_rows = sum(_is_legacy_numeric_id(row.source_row_id) for row in existing_rows)
    source_ids = {record.source_row_id for record in source_records}
    matched_ids = {entry.legacy_source_row_id for entry in entries if entry.natural_source_row_id is not None}
    expected_groups: defaultdict[str, list[SourceIdentityRecord]] = defaultdict(list)
    for record in source_records:
        expected_groups[record.natural_source_row_id].append(record)
    collision_groups = [records for records in expected_groups.values() if len(records) > 1]

    # Consistency check: disposition counts must sum to legacy numeric rows
    total_disposition = sum(by_disposition.values())
    if total_disposition != numeric_rows:
        msg = f"Source {source_table}: disposition sum {total_disposition} != legacy numeric rows {numeric_rows}"
        raise ValueError(msg)

    summary = SourceIdentitySummary(
        source_table=source_table,
        source_rows=len(source_records),
        legacy_numeric_rows=numeric_rows,
        legacy_non_numeric_rows=len(existing_rows) - numeric_rows,
        safe_source_matches=len(matched_ids),
        safe_rekey_candidates=by_disposition[SAFE_REKEY],
        existing_natural_target=sum(bool(entry.target_chunk_ids) for entry in entries),
        orphan_rows=by_disposition[ORPHAN_SOURCE_ROW],
        collision_keys=len(collision_groups),
        collision_rows=sum(len(records) for records in collision_groups),
        source_rows_missing_in_index=len(source_ids - matched_ids),
        by_disposition=dict(sorted(by_disposition.items())),
    )

    # Additional consistency checks
    if summary.safe_source_matches > summary.source_rows:
        msg = f"Safe source matches {summary.safe_source_matches} > source rows {summary.source_rows}"
        raise ValueError(msg)
    if summary.safe_rekey_candidates > summary.legacy_numeric_rows:
        msg = (
            f"Safe rekey candidates {summary.safe_rekey_candidates} > legacy numeric rows {summary.legacy_numeric_rows}"
        )
        raise ValueError(msg)
    if summary.existing_natural_target > summary.legacy_non_numeric_rows + summary.safe_rekey_candidates:
        msg = "Existing natural target exceeds allowable threshold"
        raise ValueError(msg)
    disposition_total = (
        summary.orphan_rows
        + summary.safe_rekey_candidates
        + summary.by_disposition.get(TARGET_EXISTS_SAME_CONTENT, 0)
        + summary.by_disposition.get(TARGET_EXISTS_CONTENT_MISMATCH, 0)
        + summary.by_disposition.get(SOURCE_COLLISION, 0)
    )
    if disposition_total != summary.legacy_numeric_rows:
        msg = f"Disposition total {disposition_total} != legacy numeric rows {summary.legacy_numeric_rows}"
        raise ValueError(msg)

    return summary


def _group_source_records(
    records: Iterable[SourceIdentityRecord],
    source_tables: Sequence[str],
) -> dict[str, list[SourceIdentityRecord]]:
    """Group source projections by their RAG source table."""
    grouped = {source: [] for source in source_tables}
    for record in records:
        if record.source_table in grouped:
            grouped[record.source_table].append(record)
    return grouped


def _group_existing_rows(
    rows: Iterable[ExistingIdentityRow],
    source_tables: Sequence[str],
) -> dict[str, list[ExistingIdentityRow]]:
    """Group persisted RAG rows by their RAG source table."""
    grouped = {source: [] for source in source_tables}
    for row in rows:
        if row.source_table in grouped:
            grouped[row.source_table].append(row)
    return grouped


def _source_indexes(
    records: Sequence[SourceIdentityRecord],
) -> tuple[dict[str, list[SourceIdentityRecord]], dict[str, list[SourceIdentityRecord]]]:
    """Index source rows by legacy ID and canonical ID."""
    by_id: defaultdict[str, list[SourceIdentityRecord]] = defaultdict(list)
    by_natural_id: defaultdict[str, list[SourceIdentityRecord]] = defaultdict(list)
    for record in records:
        by_id[record.source_row_id].append(record)
        by_natural_id[record.natural_source_row_id].append(record)
    return dict(by_id), dict(by_natural_id)


def _source_entries(
    source_table: str,
    source_records: Sequence[SourceIdentityRecord],
    existing_rows: Sequence[ExistingIdentityRow],
) -> list[IdentityCensusEntry]:
    """Build manifest entries for one source table's legacy rows."""
    source_by_id, source_by_natural_id = _source_indexes(source_records)
    natural_targets: defaultdict[str, list[ExistingIdentityRow]] = defaultdict(list)
    for row in existing_rows:
        if not _is_legacy_numeric_id(row.source_row_id):
            natural_targets[row.source_row_id].append(row)

    entries: list[IdentityCensusEntry] = []
    for legacy in sorted(existing_rows, key=lambda row: (row.source_row_id, row.chunk_id)):
        if not _is_legacy_numeric_id(legacy.source_row_id):
            continue
        candidates = source_by_id.get(legacy.source_row_id, [])
        if not candidates:
            entries.append(
                IdentityCensusEntry(
                    source_table,
                    legacy.chunk_id,
                    legacy.source_row_id,
                    None,
                    ORPHAN_SOURCE_ROW,
                    legacy.index_status,
                    legacy.content_hash,
                )
            )
            continue

        # When multiple candidates share the same legacy ID, include all their natural IDs
        natural_ids = [c.natural_source_row_id for c in candidates]
        # Collect all source records for all natural IDs involved
        all_natural_records: list[SourceIdentityRecord] = []
        for nid in natural_ids:
            all_natural_records.extend(source_by_natural_id[nid])
        # Use the first natural ID as the primary one for the entry
        primary_natural_id = natural_ids[0]
        targets = natural_targets.get(primary_natural_id, [])
        disposition = (
            SOURCE_COLLISION
            if len(candidates) > 1 or len(all_natural_records) > 1
            else _target_disposition(legacy, targets)
        )
        entries.append(
            IdentityCensusEntry(
                source_table,
                legacy.chunk_id,
                legacy.source_row_id,
                primary_natural_id,
                disposition,
                legacy.index_status,
                legacy.content_hash,
                tuple(target.chunk_id for target in targets),
                tuple(target.content_hash for target in targets),
                tuple(record.source_row_id for record in all_natural_records),
            )
        )
    return entries


def build_identity_census(
    existing_rows: Iterable[ExistingIdentityRow],
    source_records: Iterable[SourceIdentityRecord],
    *,
    source_tables: Sequence[str] = R2_SOURCE_TABLES,
) -> IdentityCensusReport:
    """Compare persisted numeric identities with canonical source identities."""
    selected_sources = tuple(source_tables)
    source_by_table = _group_source_records(source_records, selected_sources)
    existing_by_table = _group_existing_rows(existing_rows, selected_sources)

    summaries: list[SourceIdentitySummary] = []
    all_entries: list[IdentityCensusEntry] = []
    for source_table in selected_sources:
        source_rows = source_by_table[source_table]
        entries = _source_entries(source_table, source_rows, existing_by_table[source_table])
        summary = _build_source_summary(source_table, source_rows, entries, existing_by_table[source_table])
        summaries.append(summary)
        all_entries.extend(entries)

    return IdentityCensusReport(
        source_tables=selected_sources,
        summaries=tuple(summaries),
        entries=tuple(sorted(all_entries, key=lambda entry: (entry.source_table, entry.chunk_id))),
    )


def validate_source_tables(source_tables: Sequence[str]) -> tuple[str, ...]:
    """Validate and deduplicate requested source tables in stable order."""
    selected = tuple(dict.fromkeys(source_tables))
    unsupported = sorted(set(selected) - set(R2_SOURCE_TABLES))
    if unsupported:
        message = f"unsupported R2 source table(s): {', '.join(unsupported)}"
        raise ValueError(message)
    return selected
