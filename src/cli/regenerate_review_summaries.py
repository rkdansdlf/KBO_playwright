"""Regenerate postgame Coach review summaries for selected games."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Sequence

from sqlalchemy import and_, or_

from src.cli.daily_review_batch import (
    REVIEW_SUMMARY_TYPE,
    _build_review_data,
    _upsert_review_summary,
)
from src.db.engine import SessionLocal
from src.models.game import Game, GameSummary
from src.services.context_aggregator import ContextAggregator
from src.sync.oci_sync import OCISync
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.relay_text import is_relay_noise_text
from src.utils.safe_print import safe_print as print


@dataclass
class ReviewRegenReportRow:
    game_id: str
    game_date: str
    status: str
    old_hash: str = ""
    new_hash: str = ""
    changed: bool = False
    crucial_moments: int = 0
    noise_moments: int = 0
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
            "crucial_moments": self.crucial_moments,
            "noise_moments": self.noise_moments,
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
    "crucial_moments",
    "noise_moments",
    "oci_status",
    "message",
]


def _short_hash(value: str | None) -> str:
    if value is None:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _load_game_ids_file(path: Path) -> List[str]:
    ids: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if value and not value.startswith("#"):
            ids.append(value)
    return ids


def _default_report_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("data/reports") / f"review_summary_regen_report_{stamp}.csv"


def _default_backup_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("data/recovery") / f"review_summary_regen_backup_{stamp}.csv"


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


def _count_noise_moments(review_data: dict) -> int:
    moments = review_data.get("crucial_moments")
    if not isinstance(moments, list):
        return 0
    return sum(
        1
        for moment in moments
        if isinstance(moment, dict) and is_relay_noise_text(moment.get("description"))
    )


def _count_crucial_moments(review_data: dict) -> int:
    moments = review_data.get("crucial_moments")
    return len(moments) if isinstance(moments, list) else 0


def _write_report(rows: Sequence[ReviewRegenReportRow], path: Path) -> None:
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
            GameSummary.summary_type == REVIEW_SUMMARY_TYPE,
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


def _sync_review_summaries(game_ids: Sequence[str], rows: Sequence[ReviewRegenReportRow], *, oci_url: str, log) -> None:
    if not game_ids:
        return
    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            result = syncer.sync_review_summaries_for_games(list(game_ids), summary_type=REVIEW_SUMMARY_TYPE)
        finally:
            syncer.close()
    status = f"synced_summary:{result.get('summary', 0)}"
    synced = set(game_ids)
    for row in rows:
        if row.game_id in synced and row.status in {"APPLIED", "UNCHANGED"}:
            row.oci_status = status
    log(f"OCI summary sync complete: games={len(synced)} rows={result.get('summary', 0)}")


def regenerate_review_summaries(
    *,
    game_ids: Sequence[str] | None = None,
    dates: Sequence[str] | None = None,
    seasons: Sequence[int] | None = None,
    apply: bool = False,
    sync_oci: bool = False,
    oci_url: str | None = None,
    report_out: Path | None = None,
    backup_out: Path | None = None,
    log=print,
) -> List[ReviewRegenReportRow]:
    target_game_ids = list(game_ids or [])
    target_dates = list(dates or [])
    target_seasons = list(seasons or [])
    report_path = report_out or _default_report_path()
    rows: List[ReviewRegenReportRow] = []
    sync_game_ids: List[str] = []

    with SessionLocal() as session:
        games = _query_target_games(
            session,
            game_ids=target_game_ids,
            dates=target_dates,
            seasons=target_seasons,
        )
        games_by_id = {game.game_id: game for game in games}
        agg = ContextAggregator(session)

        if apply:
            backup_path = backup_out or _default_backup_path()
            _write_backup(session, [game.game_id for game in games], backup_path)
            log(f"Backed up existing review summaries: {backup_path}")

        for requested_id in sorted(set(target_game_ids) - set(games_by_id)):
            rows.append(
                ReviewRegenReportRow(
                    game_id=requested_id,
                    game_date="",
                    status="SKIPPED_GAME_NOT_FOUND",
                )
            )

        for game in games:
            game_date = game.game_date.strftime("%Y%m%d") if game.game_date else ""
            if game.game_status not in COMPLETED_LIKE_GAME_STATUSES:
                rows.append(
                    ReviewRegenReportRow(
                        game_id=game.game_id,
                        game_date=game_date,
                        status="SKIPPED_NOT_COMPLETED",
                        message=f"status={game.game_status}",
                    )
                )
                continue

            existing = (
                session.query(GameSummary)
                .filter(
                    GameSummary.game_id == game.game_id,
                    GameSummary.summary_type == REVIEW_SUMMARY_TYPE,
                )
                .order_by(GameSummary.id.asc())
                .first()
            )
            old_json = existing.detail_text if existing else None
            review_data = _build_review_data(agg, game)
            new_json = json.dumps(review_data, ensure_ascii=False)
            noise_moments = _count_noise_moments(review_data)
            changed = old_json != new_json
            row = ReviewRegenReportRow(
                game_id=game.game_id,
                game_date=game_date,
                status="",
                old_hash=_short_hash(old_json),
                new_hash=_short_hash(new_json),
                changed=changed,
                crucial_moments=_count_crucial_moments(review_data),
                noise_moments=noise_moments,
            )

            if noise_moments:
                row.status = "SKIPPED_REVIEW_MOMENT_NOISE"
                row.message = f"noise_moments={noise_moments}"
                rows.append(row)
                continue

            if not apply:
                row.status = "DRY_RUN_READY" if changed else "DRY_RUN_UNCHANGED"
                rows.append(row)
                continue

            if changed:
                _upsert_review_summary(session, game.game_id, new_json)
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
            _sync_review_summaries(sync_game_ids, rows, oci_url=oci_url, log=log)

    _write_report(rows, report_path)
    log(f"Review summary regeneration report: {report_path}")
    return rows


def _collect_game_ids(args) -> List[str]:
    game_ids = list(args.game_id or [])
    if args.game_ids_file:
        game_ids.extend(_load_game_ids_file(Path(args.game_ids_file)))
    return game_ids


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate Coach review summaries for selected games")
    parser.add_argument("--game-id", action="append", default=[], help="Target game_id. May be repeated.")
    parser.add_argument("--game-ids-file", type=str, help="Newline-delimited file of game_ids")
    parser.add_argument("--date", action="append", default=[], help="Target date YYYYMMDD. May be repeated.")
    parser.add_argument("--season", action="append", type=int, default=[], help="Target season. May be repeated.")
    parser.add_argument("--dry-run", action="store_true", help="Report only. This is the default unless --apply is set.")
    parser.add_argument("--apply", action="store_true", help="Persist regenerated review summaries locally.")
    parser.add_argument("--sync-oci", action="store_true", help="Sync successful review summary rows to OCI.")
    parser.add_argument("--report-out", type=Path, help="CSV report path")
    parser.add_argument("--backup-out", type=Path, help="CSV backup path for --apply")
    args = parser.parse_args(argv)

    game_ids = _collect_game_ids(args)
    if not (game_ids or args.date or args.season):
        parser.error("Provide at least one of --game-id, --game-ids-file, --date, or --season")

    rows = regenerate_review_summaries(
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
    print(f"Done. apply={args.apply} total={len(rows)} statuses={status_counts}")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
