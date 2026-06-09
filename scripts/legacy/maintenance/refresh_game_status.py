#!/usr/bin/env python3
"""
Recompute and persist standardized game_status for all games.

Status rules:
  - COMPLETED: both scores are present
  - SCHEDULED: missing score and game_date is in the future, or today before detail appears
  - CANCELLED/POSTPONED: manual status from overrides/evidence for past/today games
  - CANCELLED: missing score, past, has metadata, and has no detail rows
  - UNRESOLVED_MISSING: missing score, past, and does not meet CANCELLED rule
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


import argparse
import csv
import sys
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal  # noqa: E402
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_CANCELLED as STATUS_CANCELLED,
)
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_COMPLETED as STATUS_COMPLETED,
)
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_DRAW as STATUS_DRAW,
)
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_LIVE as STATUS_LIVE,
)
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_POSTPONED as STATUS_POSTPONED,
)
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_SCHEDULED as STATUS_SCHEDULED,
)
from src.utils.game_status import (  # noqa: E402
    GAME_STATUS_UNRESOLVED as STATUS_UNRESOLVED,
)

MANUAL_STATUS_ALLOWED = {STATUS_CANCELLED, STATUS_POSTPONED}
DEFAULT_OVERRIDES_CSV = PROJECT_ROOT / "data/game_status_overrides.csv"
DEFAULT_EVIDENCE_CSV = PROJECT_ROOT / "data/game_status_schedule_evidence.csv"


def parse_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if value is None:
        raise ValueError("game_date is required")
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def derive_game_status(
    *,
    game_date: date,
    home_score: Any,
    away_score: Any,
    has_metadata: bool,
    has_inning_scores: bool,
    has_lineups: bool,
    has_batting: bool,
    has_pitching: bool,
    has_events: bool = False,
    has_pbp: bool = False,
    manual_status: str | None = None,
    today: date | None = None,
) -> str:
    today = today or date.today()
    if (
        home_score is not None
        and away_score is not None
        and (has_batting or has_pitching or (game_date < today and has_inning_scores))
    ):
        return STATUS_DRAW if home_score == away_score else STATUS_COMPLETED
    if game_date > today:
        return STATUS_SCHEDULED
    if manual_status in MANUAL_STATUS_ALLOWED:
        return manual_status

    # LIVE detection needs to be robust against pre-game data (like lineups)
    # If it's today, we only mark as LIVE if there's actual game progress evidence.
    has_progress_evidence = has_inning_scores or has_events or has_pbp or has_batting or has_pitching

    if game_date == today:
        return STATUS_LIVE if has_progress_evidence else STATUS_SCHEDULED

    if has_metadata and not (has_lineups or has_progress_evidence):
        return STATUS_CANCELLED
    return STATUS_UNRESOLVED


def _load_games_with_flags(session) -> list[dict[str, Any]]:
    rows = (
        session.execute(
            text(
                """
            SELECT
                g.game_id,
                g.game_date,
                g.home_score,
                g.away_score,
                CASE WHEN EXISTS (SELECT 1 FROM game_metadata gm WHERE gm.game_id = g.game_id) THEN 1 ELSE 0 END AS has_metadata,
                CASE WHEN EXISTS (SELECT 1 FROM game_inning_scores gis WHERE gis.game_id = g.game_id) THEN 1 ELSE 0 END AS has_inning_scores,
                CASE WHEN EXISTS (SELECT 1 FROM game_lineups gl WHERE gl.game_id = g.game_id) THEN 1 ELSE 0 END AS has_lineups,
                CASE WHEN EXISTS (SELECT 1 FROM game_batting_stats gbs WHERE gbs.game_id = g.game_id) THEN 1 ELSE 0 END AS has_batting,
                CASE WHEN EXISTS (SELECT 1 FROM game_pitching_stats gps WHERE gps.game_id = g.game_id) THEN 1 ELSE 0 END AS has_pitching,
                CASE WHEN EXISTS (SELECT 1 FROM game_events ge WHERE ge.game_id = g.game_id) THEN 1 ELSE 0 END AS has_events,
                CASE WHEN EXISTS (SELECT 1 FROM game_play_by_play pbp WHERE pbp.game_id = g.game_id) THEN 1 ELSE 0 END AS has_pbp
            FROM game g
            ORDER BY g.game_id
            """
            )
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]


def _normalize_manual_status(raw_status: Any) -> str | None:
    status = str(raw_status or "").strip().upper()
    if status in MANUAL_STATUS_ALLOWED:
        return status
    return None


def _load_manual_status_map(path: Path, source_label: str) -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    if not path.exists():
        return mapping

    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            game_id = str(row.get("game_id") or "").strip()
            if not game_id:
                continue
            status = _normalize_manual_status(row.get("resolved_status") or row.get("game_status") or row.get("status"))
            if status is None:
                continue
            mapping[game_id] = {
                "status": status,
                "source": source_label,
                "reason": str(row.get("reason") or "").strip(),
                "evidence_source": str(row.get("evidence_source") or "").strip(),
            }
    return mapping


def _ensure_game_status_column(session) -> None:
    dialect = session.bind.dialect.name
    if dialect == "sqlite":
        columns = session.execute(text("PRAGMA table_info(game)")).fetchall()
        column_names = {row[1] for row in columns}
        if "game_status" not in column_names:
            session.execute(text("ALTER TABLE game ADD COLUMN game_status VARCHAR(32)"))
            session.commit()
        return

    session.execute(
        text(
            """
            ALTER TABLE game
            ADD COLUMN IF NOT EXISTS game_status VARCHAR(32)
            """
        )
    )
    session.commit()


def refresh_game_statuses(
    *,
    dry_run: bool = False,
    output_dir: str = "data",
    overrides_csv: str = str(DEFAULT_OVERRIDES_CSV),
    evidence_csv: str = str(DEFAULT_EVIDENCE_CSV),
) -> dict[str, Any]:
    now = datetime.now()
    today = now.date()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%d_%H%M%S")
    evidence_map = _load_manual_status_map(Path(evidence_csv), "evidence")
    overrides_map = _load_manual_status_map(Path(overrides_csv), "override")
    manual_map: dict[str, dict[str, str]] = dict(evidence_map)
    manual_map.update(overrides_map)

    with SessionLocal() as session:
        _ensure_game_status_column(session)
        games = _load_games_with_flags(session)
        updates: list[dict[str, Any]] = []
        detail_rows: list[dict[str, Any]] = []
        status_counts: Counter = Counter()
        manual_source_counts: Counter = Counter()
        past_scheduled = 0
        past_missing_runs = 0

        for game in games:
            game_date = parse_date(game["game_date"])
            manual_entry = manual_map.get(str(game["game_id"]))
            status = derive_game_status(
                game_date=game_date,
                home_score=game["home_score"],
                away_score=game["away_score"],
                has_metadata=bool(game["has_metadata"]),
                has_inning_scores=bool(game["has_inning_scores"]),
                has_lineups=bool(game["has_lineups"]),
                has_batting=bool(game["has_batting"]),
                has_pitching=bool(game["has_pitching"]),
                has_events=bool(game["has_events"]),
                has_pbp=bool(game["has_pbp"]),
                manual_status=manual_entry["status"] if manual_entry else None,
                today=today,
            )
            updates.append({"game_id": game["game_id"], "game_status": status})
            status_counts[status] += 1
            if manual_entry and status == manual_entry["status"]:
                manual_source_counts[manual_entry["source"]] += 1

            is_past = game_date < today
            is_missing_runs = game["home_score"] is None or game["away_score"] is None
            if is_past and status == STATUS_SCHEDULED:
                past_scheduled += 1
            if is_past and is_missing_runs:
                past_missing_runs += 1

            detail_rows.append(
                {
                    "game_id": game["game_id"],
                    "game_date": game_date.isoformat(),
                    "home_score": game["home_score"],
                    "away_score": game["away_score"],
                    "has_metadata": int(bool(game["has_metadata"])),
                    "has_inning_scores": int(bool(game["has_inning_scores"])),
                    "has_lineups": int(bool(game["has_lineups"])),
                    "has_batting": int(bool(game["has_batting"])),
                    "has_pitching": int(bool(game["has_pitching"])),
                    "has_events": int(bool(game["has_events"])),
                    "has_pbp": int(bool(game["has_pbp"])),
                    "manual_status": manual_entry["status"] if manual_entry else "",
                    "manual_source": manual_entry["source"] if manual_entry else "",
                    "manual_reason": manual_entry["reason"] if manual_entry else "",
                    "manual_evidence_source": manual_entry["evidence_source"] if manual_entry else "",
                    "computed_status": status,
                }
            )

        if not dry_run and updates:
            session.execute(
                text(
                    """
                    UPDATE game
                    SET game_status = :game_status
                    WHERE game_id = :game_id
                    """
                ),
                updates,
            )
            session.commit()

    summary_rows = [
        {"metric": "total_games", "count": len(games)},
        {"metric": "completed", "count": status_counts[STATUS_COMPLETED]},
        {"metric": "draw", "count": status_counts[STATUS_DRAW]},
        {"metric": "live", "count": status_counts[STATUS_LIVE]},
        {"metric": "scheduled", "count": status_counts[STATUS_SCHEDULED]},
        {"metric": "cancelled", "count": status_counts[STATUS_CANCELLED]},
        {"metric": "postponed", "count": status_counts[STATUS_POSTPONED]},
        {"metric": "unresolved_missing", "count": status_counts[STATUS_UNRESOLVED]},
        {"metric": "past_scheduled", "count": past_scheduled},
        {"metric": "past_missing_runs", "count": past_missing_runs},
        {"metric": "manual_override_applied", "count": manual_source_counts["override"]},
        {"metric": "manual_evidence_applied", "count": manual_source_counts["evidence"]},
    ]

    summary_csv = output_path / f"game_status_refresh_{stamp}_summary.csv"
    detail_csv = output_path / f"game_status_refresh_{stamp}_details.csv"

    _write_csv(summary_csv, ("metric", "count"), summary_rows)
    _write_csv(
        detail_csv,
        (
            "game_id",
            "game_date",
            "home_score",
            "away_score",
            "has_metadata",
            "has_inning_scores",
            "has_lineups",
            "has_batting",
            "has_pitching",
            "has_events",
            "has_pbp",
            "manual_status",
            "manual_source",
            "manual_reason",
            "manual_evidence_source",
            "computed_status",
        ),
        detail_rows,
    )

    return {
        "summary_csv": str(summary_csv),
        "detail_csv": str(detail_csv),
        "status_counts": dict(status_counts),
        "past_scheduled": past_scheduled,
        "past_missing_runs": past_missing_runs,
        "manual_override_applied": int(manual_source_counts["override"]),
        "manual_evidence_applied": int(manual_source_counts["evidence"]),
        "dry_run": dry_run,
    }


def _write_csv(path: Path, columns: Iterable[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(columns))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh standardized game_status values")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist updates")
    parser.add_argument("--output-dir", default="data", help="Directory to write snapshot CSV files")
    parser.add_argument(
        "--overrides-csv",
        default=str(DEFAULT_OVERRIDES_CSV),
        help="Path to game_status_overrides.csv",
    )
    parser.add_argument(
        "--evidence-csv",
        default=str(DEFAULT_EVIDENCE_CSV),
        help="Path to game_status_schedule_evidence.csv",
    )
    args = parser.parse_args()

    result = refresh_game_statuses(
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        overrides_csv=args.overrides_csv,
        evidence_csv=args.evidence_csv,
    )
    logger.info("✅ game_status refresh completed")
    logger.info(f"   dry_run: {result['dry_run']}")
    logger.info(f"   summary: {result['summary_csv']}")
    logger.info(f"   details: {result['detail_csv']}")
    logger.info(
        "   counts: COMPLETED=%s SCHEDULED=%s CANCELLED=%s POSTPONED=%s UNRESOLVED_MISSING=%s",
        result['status_counts'].get(STATUS_COMPLETED, 0),
        result['status_counts'].get(STATUS_SCHEDULED, 0),
        result['status_counts'].get(STATUS_CANCELLED, 0),
        result['status_counts'].get(STATUS_POSTPONED, 0),
        result['status_counts'].get(STATUS_UNRESOLVED, 0),
    )
    logger.info(f"   past_scheduled: {result['past_scheduled']}")
    logger.info(f"   past_missing_runs: {result['past_missing_runs']}")
    logger.info(f"   manual_override_applied: {result['manual_override_applied']}")
    logger.info(f"   manual_evidence_applied: {result['manual_evidence_applied']}")


if __name__ == "__main__":
    main()
