"""Postgame reconciliation for games left in started-like states."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Protocol

from sqlalchemy import and_, func, or_

from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.repositories.game_repository import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_DRAW,
    LIVE_GAME_STATUSES,
    repair_game_parent_from_existing_children,
    update_game_status,
)
from src.services.game_collection_service import (
    GameCollectionResult,
    GameCollectionTarget,
    crawl_and_save_game_details,
)
from src.services.game_write_contract import GameWriteContract
from src.utils.safe_print import safe_print as print
from src.utils.team_codes import normalize_kbo_game_id


class DetailCrawler(Protocol):
    async def crawl_games(
        self,
        games: list[dict[str, str]],
        concurrency: Optional[int] = None,
        lightweight: bool = False,
    ) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class GameScoreStatusSnapshot:
    game_id: str
    game_date: str
    game_status: Optional[str]
    away_score: Optional[int]
    home_score: Optional[int]

    @property
    def score_tuple(self) -> tuple[Optional[int], Optional[int]]:
        return self.away_score, self.home_score


@dataclass(frozen=True)
class PostgameReconciliationChange:
    game_id: str
    game_date: str
    before_status: Optional[str]
    after_status: Optional[str]
    before_away_score: Optional[int]
    before_home_score: Optional[int]
    after_away_score: Optional[int]
    after_home_score: Optional[int]
    detail_status: str
    failure_reason: Optional[str] = None

    @property
    def status_changed(self) -> bool:
        return self.before_status != self.after_status

    @property
    def score_changed(self) -> bool:
        return (self.before_away_score, self.before_home_score) != (
            self.after_away_score,
            self.after_home_score,
        )


@dataclass
class PostgameReconciliationResult:
    start_date: str
    end_date: str
    candidates: int = 0
    detail_result: Optional[GameCollectionResult] = None
    changes: list[PostgameReconciliationChange] = field(default_factory=list)

    @property
    def changed_game_ids(self) -> list[str]:
        return [change.game_id for change in self.changes]


def find_postgame_reconciliation_targets(
    start_date: str,
    end_date: str,
    *,
    extra_game_ids: Optional[Iterable[str]] = None,
) -> list[GameCollectionTarget]:
    """Find recent games worth revisiting after a live/detail miss."""
    start_day = _parse_yyyymmdd(start_date)
    end_day = _parse_yyyymmdd(end_date)
    if start_day > end_day:
        start_day, end_day = end_day, start_day

    live_statuses = tuple(sorted(LIVE_GAME_STATUSES))
    completed_like = (GAME_STATUS_COMPLETED, GAME_STATUS_DRAW)
    extra_ids = {
        normalized
        for game_id in (extra_game_ids or [])
        if (normalized := normalize_kbo_game_id(game_id))
    }

    with SessionLocal() as session:
        status_expr = func.upper(func.coalesce(Game.game_status, ""))
        candidate_filter = or_(
            status_expr.in_(live_statuses),
            and_(
                status_expr.in_(completed_like),
                or_(Game.away_score.is_(None), Game.home_score.is_(None)),
            ),
        )
        rows = (
            session.query(Game.game_id, Game.game_date)
            .filter(Game.game_date >= start_day, Game.game_date <= end_day)
            .filter(candidate_filter)
            .all()
        )
        if extra_ids:
            rows.extend(
                session.query(Game.game_id, Game.game_date)
                .filter(Game.game_id.in_(extra_ids))
                .all()
            )

    targets: list[GameCollectionTarget] = []
    seen: set[str] = set()
    sorted_rows = sorted(
        rows,
        key=lambda row: (_format_game_date(row[1], fallback_game_id=row[0]), row[0]),
    )
    for game_id, game_date in sorted_rows:
        normalized = normalize_kbo_game_id(game_id)
        if not normalized or normalized in seen:
            continue
        targets.append(
            GameCollectionTarget(
                game_id=normalized,
                game_date=_format_game_date(game_date, fallback_game_id=normalized),
            )
        )
        seen.add(normalized)
    return targets


async def reconcile_postgame_range(
    start_date: str,
    end_date: str,
    *,
    detail_crawler: DetailCrawler,
    concurrency: Optional[int] = 1,
    extra_game_ids: Optional[Iterable[str]] = None,
    log: Callable[[str], None] = print,
    write_contract: Optional[GameWriteContract] = None,
    source_reason: str = "postgame_reconciliation",
) -> PostgameReconciliationResult:
    """Re-crawl started-like games and return status/score changes."""
    start_date, end_date = _normalize_range(start_date, end_date)
    targets = find_postgame_reconciliation_targets(
        start_date,
        end_date,
        extra_game_ids=extra_game_ids,
    )
    result = PostgameReconciliationResult(
        start_date=start_date,
        end_date=end_date,
        candidates=len(targets),
    )
    if not targets:
        return result

    game_ids = [target.game_id for target in targets]
    before = _load_score_status_snapshots(game_ids)
    result.detail_result = await crawl_and_save_game_details(
        targets,
        detail_crawler=detail_crawler,
        force=True,
        concurrency=concurrency,
        log=log,
        write_contract=write_contract,
        source_reason=source_reason,
    )

    for target in targets:
        item = result.detail_result.items.get(target.game_id)
        if item and not item.detail_saved and item.failure_reason == "cancelled":
            update_game_status(target.game_id, GAME_STATUS_CANCELLED)
        if _has_final_detail_rows(target.game_id):
            repair_game_parent_from_existing_children(target.game_id)

    after = _load_score_status_snapshots(game_ids)
    for game_id in game_ids:
        before_snapshot = before.get(game_id)
        after_snapshot = after.get(game_id)
        if not before_snapshot or not after_snapshot:
            continue
        if (
            before_snapshot.game_status == after_snapshot.game_status
            and before_snapshot.score_tuple == after_snapshot.score_tuple
        ):
            continue
        item = result.detail_result.items.get(game_id)
        result.changes.append(
            PostgameReconciliationChange(
                game_id=game_id,
                game_date=after_snapshot.game_date,
                before_status=before_snapshot.game_status,
                after_status=after_snapshot.game_status,
                before_away_score=before_snapshot.away_score,
                before_home_score=before_snapshot.home_score,
                after_away_score=after_snapshot.away_score,
                after_home_score=after_snapshot.home_score,
                detail_status=item.detail_status if item else "unknown",
                failure_reason=item.failure_reason if item else None,
            )
        )
    return result


def format_reconciliation_report(changes: Iterable[PostgameReconciliationChange]) -> str:
    """Return a compact text report containing only games that changed."""
    rows = list(changes)
    if not rows:
        return "No status or score changes recorded during reconciliation."

    header = "game_id | date | status | score | detail | reason"
    lines = [header, "-" * len(header)]
    for change in rows:
        lines.append(
            " | ".join(
                [
                    change.game_id,
                    change.game_date,
                    f"{_display(change.before_status)} -> {_display(change.after_status)}",
                    f"{_score(change.before_away_score, change.before_home_score)} -> "
                    f"{_score(change.after_away_score, change.after_home_score)}",
                    change.detail_status,
                    change.failure_reason or "",
                ]
            )
        )
    return "\n".join(lines)


def write_reconciliation_csv(
    changes: Iterable[PostgameReconciliationChange],
    output_path: str | Path,
) -> Path:
    import csv

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "game_id",
                "game_date",
                "before_status",
                "after_status",
                "before_away_score",
                "before_home_score",
                "after_away_score",
                "after_home_score",
                "detail_status",
                "failure_reason",
            ],
        )
        writer.writeheader()
        for change in changes:
            writer.writerow(
                {
                    "game_id": change.game_id,
                    "game_date": change.game_date,
                    "before_status": change.before_status,
                    "after_status": change.after_status,
                    "before_away_score": change.before_away_score,
                    "before_home_score": change.before_home_score,
                    "after_away_score": change.after_away_score,
                    "after_home_score": change.after_home_score,
                    "detail_status": change.detail_status,
                    "failure_reason": change.failure_reason,
                }
            )
    return path


def _load_score_status_snapshots(game_ids: Iterable[str]) -> dict[str, GameScoreStatusSnapshot]:
    ids = [normalize_kbo_game_id(game_id) for game_id in game_ids if game_id]
    ids = [game_id for game_id in ids if game_id]
    if not ids:
        return {}

    with SessionLocal() as session:
        rows = (
            session.query(
                Game.game_id,
                Game.game_date,
                Game.game_status,
                Game.away_score,
                Game.home_score,
            )
            .filter(Game.game_id.in_(ids))
            .all()
        )

    return {
        normalize_kbo_game_id(game_id): GameScoreStatusSnapshot(
            game_id=normalize_kbo_game_id(game_id),
            game_date=_format_game_date(game_date, fallback_game_id=game_id),
            game_status=game_status,
            away_score=away_score,
            home_score=home_score,
        )
        for game_id, game_date, game_status, away_score, home_score in rows
    }


def _has_final_detail_rows(game_id: str) -> bool:
    normalized = normalize_kbo_game_id(game_id)
    if not normalized:
        return False
    with SessionLocal() as session:
        return any(
            session.query(model).filter(model.game_id == normalized).first() is not None
            for model in (GameBattingStat, GamePitchingStat)
        )


def _normalize_range(start_date: str, end_date: str) -> tuple[str, str]:
    start_day = _parse_yyyymmdd(start_date)
    end_day = _parse_yyyymmdd(end_date)
    if start_day > end_day:
        start_day, end_day = end_day, start_day
    return start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d")


def _parse_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def _format_game_date(value: object, *, fallback_game_id: str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").replace("-", "").strip()
    if len(text) == 8 and text.isdigit():
        return text
    return str(fallback_game_id)[:8]


def _score(away_score: Optional[int], home_score: Optional[int]) -> str:
    return f"{_display(away_score)}-{_display(home_score)}"


def _display(value: object) -> str:
    return "-" if value is None else str(value)
