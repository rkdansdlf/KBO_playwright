"""Regenerate LLM-ready game story summaries for selected games."""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Sequence

from sqlalchemy import and_, or_

from src.cli.daily_story_batch import (
    dump_story_json,
)
from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GameSummary
from src.services.game_story_builder import STORY_SUMMARY_TYPE, GameStoryBuilder
from src.sync.oci_sync import OCISync
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES


logger = logging.getLogger(__name__)


@dataclass
class StoryRegenReportRow:
    game_id: str
    game_date: str
    status: str
    old_hash: str = ""
    new_hash: str = ""
    changed: bool = False
    timeline_events: int = 0
    warnings: str = ""
    oci_status: str = ""
    message: str = ""

    def as_csv_row(self) -> dict:
        return {
            "game_id": self.game_id,
            "game_date": self.game_date,
            "status": self.status,
            "old_hash": self.old_hash,
            "new_hash": self.new_hash,
            "changed": str(self.changed).lower(),
            "timeline_events": self.timeline_events,
            "warnings": self.warnings,
            "oci_status": self.oci_status,
            "message": self.message,
        }


REPORT_FIELDS = [
    "game_id",
    "game_date",
    "status",
    "old_hash",
    "new_hash",
    "changed",
    "timeline_events",
    "warnings",
    "oci_status",
    "message",
]


def _short_hash(value: str | None) -> str:
    if value is None:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _load_game_ids_file(path: Path) -> list[str]:
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if value and not value.startswith("#"):
            ids.append(value)
    return ids


def _default_report_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("data/reports") / f"game_story_regen_report_{stamp}.csv"


def _default_backup_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("data/recovery") / f"game_story_regen_backup_{stamp}.csv"


def _season_filters(seasons: Iterable[int]):
    filters = []
    for season in sorted(set(seasons)):
        filters.append(
            and_(
                Game.game_date >= date(season, 1, 1),
                Game.game_date <= date(season, 12, 31),
            )
        )
    return filters


def _query_target_games(session, *, game_ids: Sequence[str], dates: Sequence[str], seasons: Sequence[int]):
    query = session.query(Game)
    filters = []
    if game_ids:
        filters.append(Game.game_id.in_(sorted(set(game_ids))))
    if dates:
        filters.append(Game.game_date.in_([_parse_date(value) for value in dates]))
    if seasons:
        filters.extend(_season_filters(seasons))
    if filters:
        query = query.filter(or_(*filters))
    return query.order_by(Game.game_date.asc(), Game.game_id.asc()).all()


def _game_batches(games: Sequence[Game], batch_size: int = 250):
    for index in range(0, len(games), batch_size):
        yield games[index : index + batch_size]


def _events_by_game(session, game_ids: Sequence[str]) -> dict[str, list[GameEvent]]:
    if not game_ids:
        return {}
    rows = (
        session.query(GameEvent)
        .filter(GameEvent.game_id.in_(list(game_ids)))
        .order_by(GameEvent.game_id.asc(), GameEvent.event_seq.asc(), GameEvent.id.asc())
        .all()
    )
    grouped: dict[str, list[GameEvent]] = {game_id: [] for game_id in game_ids}
    for row in rows:
        grouped.setdefault(row.game_id, []).append(row)
    return grouped


def _write_report(rows: Sequence[StoryRegenReportRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


def _write_backup(session, game_ids: Sequence[str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = (
        session.query(GameSummary)
        .filter(
            GameSummary.game_id.in_(sorted(set(game_ids))),
            GameSummary.summary_type == STORY_SUMMARY_TYPE,
        )
        .order_by(GameSummary.game_id.asc(), GameSummary.id.asc())
        .all()
    )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "id",
                "game_id",
                "summary_type",
                "player_id",
                "player_name",
                "detail_text",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "id": row.id,
                    "game_id": row.game_id,
                    "summary_type": row.summary_type,
                    "player_id": row.player_id,
                    "player_name": row.player_name,
                    "detail_text": row.detail_text,
                }
            )


def _sync_story_summaries(game_ids: Sequence[str], rows: Sequence[StoryRegenReportRow], *, oci_url: str, log) -> None:
    if not game_ids:
        return
    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            result = syncer.sync_review_summaries_for_games(
                list(game_ids),
                summary_type=STORY_SUMMARY_TYPE,
            )
        finally:
            syncer.close()
    status = f"synced_summary:{result.get('summary', 0)}"
    synced = set(game_ids)
    for row in rows:
        if row.game_id in synced and row.status in {"APPLIED", "UNCHANGED"}:
            row.oci_status = status
    log(f"OCI story summary sync complete: games={len(synced)} rows={result.get('summary', 0)}")


def regenerate_game_stories(
    *,
    game_ids: Sequence[str] | None = None,
    dates: Sequence[str] | None = None,
    seasons: Sequence[int] | None = None,
    apply: bool = False,
    sync_oci: bool = False,
    oci_url: str | None = None,
    report_out: Path | None = None,
    backup_out: Path | None = None,
    log=logger.info,
) -> list[StoryRegenReportRow]:
    target_game_ids = list(game_ids or [])
    target_dates = list(dates or [])
    target_seasons = list(seasons or [])
    report_path = report_out or _default_report_path()
    rows: list[StoryRegenReportRow] = []
    sync_game_ids: list[str] = []

    with SessionLocal() as session:
        games = _query_target_games(
            session,
            game_ids=target_game_ids,
            dates=target_dates,
            seasons=target_seasons,
        )
        games_by_id = {game.game_id: game for game in games}
        builder = GameStoryBuilder()

        if apply:
            backup_path = backup_out or _default_backup_path()
            _write_backup(session, [game.game_id for game in games], backup_path)
            log(f"Backed up existing game story summaries: {backup_path}")

        for requested_id in sorted(set(target_game_ids) - set(games_by_id)):
            rows.append(
                StoryRegenReportRow(
                    game_id=requested_id,
                    game_date="",
                    status="SKIPPED_GAME_NOT_FOUND",
                )
            )

        existing_summary_rows: dict[str, list[GameSummary]] = {}
        existing_summaries: dict[str, str | None] = {}
        if games:
            for row in (
                session.query(GameSummary)
                .filter(
                    GameSummary.game_id.in_([game.game_id for game in games]),
                    GameSummary.summary_type == STORY_SUMMARY_TYPE,
                )
                .order_by(GameSummary.game_id.asc(), GameSummary.id.asc())
                .all()
            ):
                existing_summary_rows.setdefault(row.game_id, []).append(row)
                existing_summaries.setdefault(row.game_id, row.detail_text)

        for game_batch in _game_batches(games):
            batch_event_map = _events_by_game(
                session,
                [game.game_id for game in game_batch if game.game_status in COMPLETED_LIKE_GAME_STATUSES],
            )
            for game in game_batch:
                game_date = game.game_date.strftime("%Y%m%d") if game.game_date else ""
                if game.game_status not in COMPLETED_LIKE_GAME_STATUSES:
                    rows.append(
                        StoryRegenReportRow(
                            game_id=game.game_id,
                            game_date=game_date,
                            status="SKIPPED_NOT_COMPLETED",
                            message=f"status={game.game_status}",
                        )
                    )
                    continue

                old_json = existing_summaries.get(game.game_id)
                story_data = builder.build(game, batch_event_map.get(game.game_id, []))
                new_json = dump_story_json(story_data)
                warnings = story_data.get("source", {}).get("warnings") or []
                changed = old_json != new_json
                row = StoryRegenReportRow(
                    game_id=game.game_id,
                    game_date=game_date,
                    status="",
                    old_hash=_short_hash(old_json),
                    new_hash=_short_hash(new_json),
                    changed=changed,
                    timeline_events=len(story_data.get("timeline") or []),
                    warnings=";".join(str(warning) for warning in warnings),
                )

                if not apply:
                    row.status = "DRY_RUN_READY" if changed else "DRY_RUN_UNCHANGED"
                    rows.append(row)
                    continue

                if changed:
                    summaries = existing_summary_rows.get(game.game_id) or []
                    if summaries:
                        for summary in summaries:
                            summary.detail_text = new_json
                    else:
                        session.add(
                            GameSummary(
                                game_id=game.game_id,
                                summary_type=STORY_SUMMARY_TYPE,
                                detail_text=new_json,
                            )
                        )
                    row.status = "APPLIED"
                else:
                    row.status = "UNCHANGED"
                rows.append(row)
                sync_game_ids.append(game.game_id)

        if apply:
            try:
                session.commit()
            except Exception:
                session.rollback()
                raise

    if sync_oci:
        if not apply:
            for row in rows:
                row.oci_status = "skipped_dry_run"
        elif not oci_url:
            for row in rows:
                if row.status in {"APPLIED", "UNCHANGED"}:
                    row.oci_status = "skipped_missing_oci_url"
        else:
            _sync_story_summaries(sync_game_ids, rows, oci_url=oci_url, log=log)

    _write_report(rows, report_path)
    log(f"Game story regeneration report: {report_path}")
    return rows


def _collect_game_ids(args) -> list[str]:
    game_ids = list(args.game_id or [])
    if args.game_ids_file:
        game_ids.extend(_load_game_ids_file(Path(args.game_ids_file)))
    return game_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate game story summaries for selected games")
    parser.add_argument("--game-id", action="append", default=[], help="Target game_id. May be repeated.")
    parser.add_argument("--game-ids-file", type=str, help="Newline-delimited file of game_ids")
    parser.add_argument("--date", action="append", default=[], help="Target date YYYYMMDD. May be repeated.")
    parser.add_argument("--season", action="append", type=int, default=[], help="Target season. May be repeated.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Report only. This is the default unless --apply is set."
    )
    parser.add_argument("--apply", action="store_true", help="Persist regenerated game story summaries locally.")
    parser.add_argument("--sync-oci", action="store_true", help="Sync successful game story rows to OCI.")
    parser.add_argument("--report-out", type=Path, help="CSV report path")
    parser.add_argument("--backup-out", type=Path, help="CSV backup path for --apply")
    args = parser.parse_args(argv)

    game_ids = _collect_game_ids(args)
    if not (game_ids or args.date or args.season):
        parser.error("Provide at least one of --game-id, --game-ids-file, --date, or --season")

    rows = regenerate_game_stories(
        game_ids=game_ids,
        dates=args.date,
        seasons=args.season,
        apply=args.apply,
        sync_oci=args.sync_oci,
        oci_url=os.getenv("OCI_DB_URL"),
        report_out=args.report_out,
        backup_out=args.backup_out,
    )
    status_counts = {}
    for row in rows:
        status_counts[row.status] = status_counts.get(row.status, 0) + 1
    logger.info(f"Done. apply={args.apply} total={len(rows)} statuses={status_counts}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
