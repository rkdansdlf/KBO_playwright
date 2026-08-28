"""Repair legacy season-id values stored in RAG game metadata."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import bindparam, select
from sqlalchemy.exc import SQLAlchemyError

from src.constants import DATE_STR_LEN, GAME_ID_YEAR_END, GAME_ID_YEAR_START, KBO_FOUNDING_YEAR, KBO_MAX_VALID_SEASON
from src.db.engine import get_rag_index_session
from src.models.rag_chunk import RagChunk

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


_GAME_SOURCES = ("game", "game_lineups", "game_play_by_play", "game_highlights")
_DEFAULT_BATCH_SIZE = 1_000
_DEFAULT_SAMPLE_LIMIT = 20
_REPAIR_ROW_FIELD_COUNT = 5


@dataclass(frozen=True)
class SeasonYearRepair:
    """Represent one safe season-year metadata correction."""

    chunk_id: int
    source_table: str
    source_row_id: str
    old_season_year: int | None
    new_season_year: int

    def to_dict(self) -> dict[str, object]:
        """Serialize one planned correction."""
        return {
            "chunk_id": self.chunk_id,
            "source_table": self.source_table,
            "source_row_id": self.source_row_id,
            "old_season_year": self.old_season_year,
            "new_season_year": self.new_season_year,
        }


@dataclass(frozen=True)
class SeasonYearRepairPlan:
    """Describe a dry-run or apply operation without embedding changes."""

    scanned: int
    repairs: tuple[SeasonYearRepair, ...]
    skipped_by_reason: dict[str, int]
    by_source: dict[str, int]
    by_year: dict[int, int]
    sample_limit: int

    def to_dict(self, *, mode: str, updated: int = 0) -> dict[str, object]:
        """Serialize the plan and its operation result."""
        return {
            "mode": mode,
            "source_tables": list(_GAME_SOURCES),
            "scanned": self.scanned,
            "candidate_count": len(self.repairs),
            "updated": updated,
            "skipped": sum(self.skipped_by_reason.values()),
            "skipped_by_reason": dict(self.skipped_by_reason),
            "by_source": dict(self.by_source),
            "by_year": {str(year): count for year, count in sorted(self.by_year.items())},
            "sample": [repair.to_dict() for repair in self.repairs[: self.sample_limit]],
        }


def _metadata_mapping(value: object) -> Mapping[str, object]:
    """Normalize a JSON column value to a metadata mapping."""
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def _game_id(source_table: str, source_row_id: object, metadata: Mapping[str, object]) -> str:
    """Extract the parent game identity from a supported RAG source row."""
    row_id = str(source_row_id or "")
    if source_table == "game":
        return row_id
    if source_table == "game_lineups":
        return str(metadata.get("game_id") or row_id.split("_", 1)[0])
    return str(metadata.get("game_id") or "")


def _expected_year(source_table: str, source_row_id: object, metadata: Mapping[str, object]) -> int | None:
    """Derive the calendar year from a source-stable KBO game identity."""
    game_id = _game_id(source_table, source_row_id, metadata)
    date_prefix = game_id[GAME_ID_YEAR_START : GAME_ID_YEAR_START + DATE_STR_LEN]
    if len(date_prefix) != DATE_STR_LEN or not date_prefix.isdigit():
        return None
    year_text = game_id[GAME_ID_YEAR_START:GAME_ID_YEAR_END]
    year = int(year_text)
    if not KBO_FOUNDING_YEAR <= year <= KBO_MAX_VALID_SEASON:
        return None
    return year


def _optional_int(value: object) -> int | None:
    """Convert a stored numeric value while treating malformed values as absent."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_repair_plan(
    rows: Sequence[tuple[object, ...]],
    *,
    sample_limit: int = _DEFAULT_SAMPLE_LIMIT,
) -> SeasonYearRepairPlan:
    """Build safe season-year updates from active RAG chunk rows."""
    skipped: Counter[str] = Counter()
    by_source: Counter[str] = Counter()
    by_year: Counter[int] = Counter()
    repairs: list[SeasonYearRepair] = []

    for row in rows:
        if len(row) < _REPAIR_ROW_FIELD_COUNT:
            skipped["invalid_row"] += 1
            continue
        chunk_id, source_table_value, source_row_id, stored_year, raw_metadata = row[:5]
        source_table = str(source_table_value or "")
        if source_table not in _GAME_SOURCES:
            skipped["unsupported_source"] += 1
            continue
        metadata = _metadata_mapping(raw_metadata)
        expected = _expected_year(source_table, source_row_id, metadata)
        if expected is None:
            skipped["invalid_game_id"] += 1
            continue
        old_year = _optional_int(stored_year)
        if old_year == expected:
            skipped["already_correct"] += 1
            continue
        try:
            normalized_chunk_id = int(chunk_id)
        except (TypeError, ValueError):
            skipped["invalid_chunk_id"] += 1
            continue
        repair = SeasonYearRepair(
            chunk_id=normalized_chunk_id,
            source_table=source_table,
            source_row_id=str(source_row_id or ""),
            old_season_year=old_year,
            new_season_year=expected,
        )
        repairs.append(repair)
        by_source[source_table] += 1
        by_year[expected] += 1

    return SeasonYearRepairPlan(
        scanned=len(rows),
        repairs=tuple(repairs),
        skipped_by_reason=dict(skipped),
        by_source=dict(by_source),
        by_year=dict(by_year),
        sample_limit=max(sample_limit, 0),
    )


def apply_repair_plan(session: Session, plan: SeasonYearRepairPlan, *, batch_size: int) -> int:
    """Apply only planned column updates in bounded executemany batches."""
    if not plan.repairs:
        return 0
    statement = (
        RagChunk.__table__.update()
        .where(
            RagChunk.__table__.c.id == bindparam("repair_chunk_id"),
        )
        .values(season_year=bindparam("repair_season_year"))
    )
    repairs = plan.repairs
    for start in range(0, len(repairs), batch_size):
        batch = repairs[start : start + batch_size]
        session.execute(
            statement,
            [{"repair_chunk_id": repair.chunk_id, "repair_season_year": repair.new_season_year} for repair in batch],
        )
    return len(repairs)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse season-year repair options."""
    parser = argparse.ArgumentParser(description="Repair legacy season-id values in RAG game metadata")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Persist season_year corrections")
    mode.add_argument("--dry-run", action="store_true", help="Preview corrections without writing (default)")
    parser.add_argument("--source", action="append", choices=_GAME_SOURCES, help="Limit repair to one source table")
    parser.add_argument("--batch-size", type=int, default=_DEFAULT_BATCH_SIZE)
    parser.add_argument("--sample", type=int, default=_DEFAULT_SAMPLE_LIMIT, help="Number of planned rows to render")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    return parser.parse_args(argv)


def _validate_args(args: argparse.Namespace) -> str | None:
    """Validate bounded repair options and explicit write gates."""
    if args.batch_size <= 0:
        return "--batch-size must be positive"
    if args.sample < 0:
        return "--sample must not be negative"
    if not args.apply:
        return None
    if os.getenv("RAG_INDEX_ALLOW_WRITE") != "1":
        return "--apply requires RAG_INDEX_ALLOW_WRITE=1"
    if os.getenv("RAG_TARGET_ENV") == "production" and os.getenv("RAG_INDEX_ALLOW_PRODUCTION_WRITE") != "1":
        return "production --apply requires RAG_INDEX_ALLOW_PRODUCTION_WRITE=1"
    return None


def _load_rows(session: Session, sources: Sequence[str]) -> list[tuple[object, ...]]:
    """Load active game-family metadata columns for planning."""
    statement = select(
        RagChunk.id,
        RagChunk.source_table,
        RagChunk.source_row_id,
        RagChunk.season_year,
        RagChunk.meta,
    ).where(RagChunk.index_status == "ACTIVE", RagChunk.source_table.in_(tuple(sources)))
    return [tuple(row) for row in session.execute(statement).all()]


def _render_report(report: dict[str, object], *, as_json: bool) -> None:
    """Render a machine-readable or concise repair report."""
    if as_json:
        sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
        return
    sys.stdout.write(
        "mode={mode} scanned={scanned} candidates={candidate_count} updated={updated} skipped={skipped}\n".format(
            **report,
        ),
    )
    sys.stdout.write(f"by_source={report['by_source']} by_year={report['by_year']}\n")


def main(argv: Sequence[str] | None = None) -> int:
    """Plan or apply safe RAG season-year metadata corrections."""
    args = _parse_args(argv)
    error = _validate_args(args)
    if error:
        sys.stderr.write(f"repair_error: {error}\n")
        return 2
    sources = tuple(args.source or _GAME_SOURCES)
    try:
        with get_rag_index_session() as session:
            plan = build_repair_plan(_load_rows(session, sources), sample_limit=args.sample)
            updated = apply_repair_plan(session, plan, batch_size=args.batch_size) if args.apply else 0
    except (SQLAlchemyError, OSError, TypeError, ValueError) as exc:
        sys.stderr.write(f"repair_error: {exc}\n")
        return 2
    report = plan.to_dict(mode="apply" if args.apply else "dry-run", updated=updated)
    report["source_tables"] = list(sources)
    _render_report(report, as_json=args.as_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
