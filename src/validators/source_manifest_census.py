"""Source Manifest & Schedule Census Engine (Layer 0).

Provides external population verification by comparing KBO original schedule manifest
against the local/OCI database game population to detect un-crawled games, phantom games,
and status divergences.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


@dataclass(frozen=True)
class SourceCensusReport:
    """Census report comparing external source schedule manifest against DB game records."""

    year: int
    league_type: str
    source_game_count: int
    db_game_count: int
    missing_in_db: list[str] = field(default_factory=list)  # source_ids - db_ids
    unexpected_in_db: list[str] = field(default_factory=list)  # db_ids - source_ids
    status_mismatches: list[dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Returns True if DB game set matches source manifest with no missing or unexpected games."""
        return len(self.missing_in_db) == 0 and len(self.unexpected_in_db) == 0 and len(self.status_mismatches) == 0

    @property
    def coverage_ratio(self) -> float:
        """Returns the ratio of collected games over source expected games."""
        if self.source_game_count == 0:
            return 1.0 if self.db_game_count == 0 else 0.0
        return round(self.db_game_count / self.source_game_count, 4)

    def to_dict(self) -> dict[str, Any]:
        """Convert census report to dictionary."""
        return {
            "year": self.year,
            "league_type": self.league_type,
            "source_game_count": self.source_game_count,
            "db_game_count": self.db_game_count,
            "coverage_ratio": self.coverage_ratio,
            "ok": self.ok,
            "missing_in_db_count": len(self.missing_in_db),
            "missing_in_db_sample": self.missing_in_db[:20],
            "unexpected_in_db_count": len(self.unexpected_in_db),
            "unexpected_in_db_sample": self.unexpected_in_db[:20],
            "status_mismatches_count": len(self.status_mismatches),
            "status_mismatches_sample": self.status_mismatches[:20],
        }


def compare_source_manifest_against_db(
    source_manifest: Mapping[str, dict[str, Any]],
    db_games: Sequence[dict[str, Any]],
    *,
    year: int,
    league_type: str = "REGULAR",
) -> SourceCensusReport:
    """Compare external source schedule manifest against internal DB game records.

    Args:
        source_manifest: Mapping of {game_id: {game_id, game_date, status, home_team, away_team}}
        db_games: List of dicts representing game rows in the DB.
        year: Target year.
        league_type: Target league type.

    Returns:
        SourceCensusReport with missing/unexpected games and status mismatches.

    """
    source_ids = set(source_manifest.keys())
    db_game_map = {str(g.get("game_id")): g for g in db_games if g.get("game_id")}
    db_ids = set(db_game_map.keys())

    missing_in_db = sorted(source_ids - db_ids)
    unexpected_in_db = sorted(db_ids - source_ids)

    # Check status mismatches for shared game IDs
    shared_ids = source_ids & db_ids
    status_mismatches: list[dict[str, Any]] = []

    for gid in sorted(shared_ids):
        src_item = source_manifest[gid]
        db_item = db_game_map[gid]

        src_status = str(src_item.get("status", "")).strip().upper()
        db_status = str(db_item.get("status", "")).strip().upper()

        # Normalize common status synonyms
        src_norm = "CANCELLED" if src_status in ("취소", "우천취소", "CANCELLED") else src_status
        db_norm = "CANCELLED" if db_status in ("취소", "우천취소", "CANCELLED") else db_status

        if src_norm and db_norm and src_norm != db_norm:
            status_mismatches.append(
                {
                    "game_id": gid,
                    "source_status": src_status,
                    "db_status": db_status,
                },
            )

    return SourceCensusReport(
        year=year,
        league_type=league_type,
        source_game_count=len(source_ids),
        db_game_count=len(db_ids),
        missing_in_db=missing_in_db,
        unexpected_in_db=unexpected_in_db,
        status_mismatches=status_mismatches,
    )
