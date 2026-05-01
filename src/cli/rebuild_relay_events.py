from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from sqlalchemy import or_

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent
from src.services.wpa_calculator import WPACalculator
from src.sync.oci_sync import OCISync
from src.utils.relay_text import (
    detect_relay_event_type,
    is_relay_noise_text,
    is_relay_result_event_text,
)
from src.utils.safe_print import safe_print as print


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "recovery"
DEFAULT_SEASONS = (2024, 2025, 2026)
DEFAULT_MIN_EVENTS = 20


@dataclass
class RebuildReportRow:
    game_id: str
    status: str
    old_rows: int
    new_rows: int
    notes: str = ""
    backup_path: str = ""
    oci_status: str = "not_requested"


def rebuild_relay_events(
    *,
    seasons: Sequence[int] = DEFAULT_SEASONS,
    game_ids: Sequence[str] | None = None,
    apply: bool = False,
    sync_oci: bool = False,
    oci_sync_mode: str = "events",
    min_events: int = DEFAULT_MIN_EVENTS,
    report_out: str | Path | None = None,
    backup_out: str | Path | None = None,
    oci_url: str | None = None,
    log=print,
) -> list[RebuildReportRow]:
    season_values = tuple(int(season) for season in seasons)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    report_path = Path(report_out) if report_out else DEFAULT_REPORT_DIR / f"relay_event_rebuild_report_{timestamp}.csv"
    backup_path = Path(backup_out) if backup_out else DEFAULT_REPORT_DIR / f"relay_event_rebuild_backup_{timestamp}.csv"

    with SessionLocal() as session:
        candidate_game_ids = _load_candidate_game_ids(session, season_values, game_ids=game_ids)
        log(f"[INFO] Relay event rebuild targets={len(candidate_game_ids)} seasons={','.join(map(str, season_values))}")

        if apply and candidate_game_ids:
            _backup_existing_events(session, candidate_game_ids, backup_path)
            log(f"[INFO] Existing game_events backup written to {backup_path}")

        report_rows: list[RebuildReportRow] = []
        changed_game_ids: list[str] = []
        calculator = WPACalculator()
        for index, game_id in enumerate(candidate_game_ids, start=1):
            game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
            old_events = (
                session.query(GameEvent)
                .filter(GameEvent.game_id == game_id)
                .order_by(GameEvent.event_seq.asc(), GameEvent.id.asc())
                .all()
            )
            rebuilt_events = _rebuild_events_for_game(old_events, calculator=calculator)
            status, notes = _validate_rebuilt_events(game, rebuilt_events, min_events=min_events)
            row = RebuildReportRow(
                game_id=game_id,
                status=status,
                old_rows=len(old_events),
                new_rows=len(rebuilt_events),
                notes=notes,
                backup_path=str(backup_path) if apply else "",
                oci_status="not_requested" if sync_oci else "disabled",
            )

            if status == "READY":
                if apply:
                    session.query(GameEvent).filter(GameEvent.game_id == game_id).delete()
                    session.add_all(_build_orm_events(game_id, rebuilt_events))
                    session.commit()
                    row.status = "APPLIED"
                    changed_game_ids.append(game_id)
                else:
                    row.status = "DRY_RUN_READY"
                    session.rollback()
            else:
                session.rollback()

            report_rows.append(row)
            log(
                f"[{index}/{len(candidate_game_ids)}] {game_id} {row.status} "
                f"old={row.old_rows} new={row.new_rows} {row.notes}"
            )

    if apply and sync_oci and changed_game_ids:
        if oci_sync_mode == "specific-game":
            _sync_changed_games(changed_game_ids, report_rows, oci_url=oci_url, log=log)
        else:
            _sync_changed_events(changed_game_ids, report_rows, oci_url=oci_url, log=log)

    _write_report(report_path, report_rows)
    log(f"[INFO] Rebuild report written to {report_path}")
    return report_rows


def _load_candidate_game_ids(
    session,
    seasons: Sequence[int],
    *,
    game_ids: Sequence[str] | None = None,
) -> list[str]:
    filters = [GameEvent.game_id.like(f"{season}%") for season in seasons]
    if not filters:
        return []
    query = session.query(GameEvent.game_id).filter(or_(*filters))
    requested = _dedupe_game_ids(game_ids or [])
    if requested:
        query = query.filter(GameEvent.game_id.in_(requested))
    rows = [row[0] for row in query.distinct().order_by(GameEvent.game_id.asc()).all()]
    if not requested:
        return rows
    found = set(rows)
    return [game_id for game_id in requested if game_id in found]


def _backup_existing_events(session, game_ids: Sequence[str], backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    columns = [column.name for column in GameEvent.__table__.columns]
    with backup_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for event in (
            session.query(GameEvent)
            .filter(GameEvent.game_id.in_(list(game_ids)))
            .order_by(GameEvent.game_id.asc(), GameEvent.event_seq.asc(), GameEvent.id.asc())
            .all()
        ):
            writer.writerow({name: getattr(event, name) for name in columns})


def _rebuild_events_for_game(
    events: Sequence[GameEvent],
    *,
    calculator: WPACalculator | None = None,
) -> list[dict[str, Any]]:
    kept = [_event_to_payload(event) for event in events if _should_keep_event(event)]
    for index, event in enumerate(kept, start=1):
        event["event_seq"] = index
        event_type = str(event.get("event_type") or "").strip().lower()
        if not event_type or event_type in {"unknown", "other", "substitution"}:
            event["event_type"] = detect_relay_event_type(event.get("description"))
        if not event.get("result_code"):
            event["result_code"] = _result_from_description(event.get("description"))
    _apply_wpa_transitions(kept, calculator=calculator)
    return kept


def _should_keep_event(event: GameEvent) -> bool:
    description = event.description
    if is_relay_noise_text(description):
        return False
    event_type = str(event.event_type or "").strip().lower()
    if event_type and event_type not in {"unknown", "other", "substitution"}:
        return True
    return is_relay_result_event_text(description)


def _event_to_payload(event: GameEvent) -> dict[str, Any]:
    return {
        "event_seq": event.event_seq,
        "inning": event.inning,
        "inning_half": event.inning_half,
        "outs": event.outs,
        "batter_id": event.batter_id,
        "batter_name": event.batter_name or _batter_from_description(event.description),
        "pitcher_id": event.pitcher_id,
        "pitcher_name": event.pitcher_name,
        "description": event.description,
        "event_type": event.event_type,
        "result_code": event.result_code,
        "rbi": event.rbi,
        "bases_before": event.bases_before,
        "bases_after": event.bases_after,
        "extra_json": event.extra_json,
        "wpa": event.wpa,
        "win_expectancy_before": event.win_expectancy_before,
        "win_expectancy_after": event.win_expectancy_after,
        "score_diff": event.score_diff,
        "base_state": event.base_state,
        "home_score": event.home_score,
        "away_score": event.away_score,
    }


def _validate_rebuilt_events(
    game: Game | None,
    rebuilt_events: Sequence[dict[str, Any]],
    *,
    min_events: int,
) -> tuple[str, str]:
    if game is None:
        return "SKIPPED_MISSING_GAME", "No parent game row"
    if len(rebuilt_events) < min_events:
        return "SKIPPED_TOO_FEW_EVENTS", f"new_rows<{min_events}"
    if game.home_score is not None and game.away_score is not None:
        last_score = _last_known_score(rebuilt_events)
        if last_score is None:
            return "SKIPPED_MISSING_SCORE_STATE", "No score state on rebuilt events"
        home_score, away_score = last_score
        if int(home_score) != int(game.home_score) or int(away_score) != int(game.away_score):
            return (
                "SKIPPED_SCORE_MISMATCH",
                f"event_score={away_score}:{home_score} game_score={game.away_score}:{game.home_score}",
            )
    return "READY", ""


def _last_known_score(events: Sequence[dict[str, Any]]) -> tuple[int, int] | None:
    for event in reversed(events):
        home_score = event.get("home_score")
        away_score = event.get("away_score")
        if home_score is not None and away_score is not None:
            return int(home_score), int(away_score)
    return None


def _apply_wpa_transitions(events: list[dict[str, Any]], *, calculator: WPACalculator | None = None) -> None:
    calculator = calculator or WPACalculator()
    for index, event in enumerate(events):
        is_bottom = event.get("inning_half") == "bottom"
        if index == 0:
            outs_before, runners_before, score_diff_before = 0, 0, 0
        else:
            previous = events[index - 1]
            if previous.get("inning") != event.get("inning") or previous.get("inning_half") != event.get("inning_half"):
                outs_before, runners_before = 0, 0
            else:
                outs_before = int(previous.get("outs") or 0)
                runners_before = int(previous.get("base_state") or 0)
            score_diff_before = int(previous.get("home_score") or 0) - int(previous.get("away_score") or 0)

        inning = int(event.get("inning") or 1)
        outs_after = int(event.get("outs") or 0)
        runners_after = int(event.get("base_state") or 0)
        score_diff_after = int(event.get("home_score") or 0) - int(event.get("away_score") or 0)
        we_before = calculator.get_win_probability(
            inning,
            is_bottom,
            outs_before,
            runners_before,
            score_diff_before,
        )
        we_after = calculator.get_win_probability(
            inning,
            is_bottom,
            outs_after,
            runners_after,
            score_diff_after,
        )
        event["bases_before"] = _format_base_string(runners_before)
        event["bases_after"] = _format_base_string(runners_after)
        event["score_diff"] = score_diff_after
        event["win_expectancy_before"] = we_before
        event["win_expectancy_after"] = we_after
        event["wpa"] = round(we_after - we_before if is_bottom else we_before - we_after, 4)


def _build_orm_events(game_id: str, events: Sequence[dict[str, Any]]) -> list[GameEvent]:
    return [
        GameEvent(
            game_id=game_id,
            event_seq=event["event_seq"],
            inning=event.get("inning"),
            inning_half=event.get("inning_half"),
            outs=event.get("outs"),
            batter_id=event.get("batter_id"),
            batter_name=event.get("batter_name"),
            pitcher_id=event.get("pitcher_id"),
            pitcher_name=event.get("pitcher_name"),
            description=event.get("description"),
            event_type=event.get("event_type"),
            result_code=event.get("result_code"),
            rbi=event.get("rbi"),
            bases_before=event.get("bases_before"),
            bases_after=event.get("bases_after"),
            extra_json=event.get("extra_json"),
            wpa=event.get("wpa"),
            win_expectancy_before=event.get("win_expectancy_before"),
            win_expectancy_after=event.get("win_expectancy_after"),
            score_diff=event.get("score_diff"),
            base_state=event.get("base_state"),
            home_score=event.get("home_score"),
            away_score=event.get("away_score"),
        )
        for event in events
    ]


def _sync_changed_games(
    game_ids: Sequence[str],
    report_rows: Sequence[RebuildReportRow],
    *,
    oci_url: str | None,
    log,
) -> None:
    target_url = oci_url or os.getenv("OCI_DB_URL")
    report_by_game_id = {row.game_id: row for row in report_rows}
    if not target_url:
        for game_id in game_ids:
            report_by_game_id[game_id].oci_status = "skipped_missing_oci_url"
        return

    with SessionLocal() as sync_session:
        syncer = OCISync(target_url, sync_session)
        try:
            for game_id in game_ids:
                try:
                    syncer.sync_specific_game(game_id)
                    report_by_game_id[game_id].oci_status = "synced"
                    log(f"[OCI] Synced {game_id}")
                except Exception as exc:
                    report_by_game_id[game_id].oci_status = f"failed:{exc}"
                    log(f"[OCI] Failed {game_id}: {exc}")
        finally:
            syncer.close()


def _sync_changed_events(
    game_ids: Sequence[str],
    report_rows: Sequence[RebuildReportRow],
    *,
    oci_url: str | None,
    log,
) -> None:
    target_url = oci_url or os.getenv("OCI_DB_URL")
    report_by_game_id = {row.game_id: row for row in report_rows}
    if not target_url:
        for game_id in game_ids:
            report_by_game_id[game_id].oci_status = "skipped_missing_oci_url"
        return

    with SessionLocal() as sync_session:
        syncer = OCISync(target_url, sync_session)
        try:
            for batch in _chunked(game_ids, 200):
                syncer.target_session.query(GameEvent).filter(GameEvent.game_id.in_(list(batch))).delete(
                    synchronize_session=False
                )
                syncer.target_session.commit()
            synced = syncer._sync_simple_table(
                GameEvent,
                ["game_id", "event_seq"],
                exclude_cols=["id", "created_at"],
                filters=[GameEvent.game_id.in_(list(game_ids))],
            )
            for game_id in game_ids:
                report_by_game_id[game_id].oci_status = f"synced_events:{synced}"
            log(f"[OCI] Synced game_events for {len(game_ids)} games ({synced} rows)")
        except Exception as exc:
            for game_id in game_ids:
                report_by_game_id[game_id].oci_status = f"failed:{exc}"
            log(f"[OCI] Failed game_events batch sync: {exc}")
        finally:
            syncer.close()


def _write_report(report_path: Path, rows: Sequence[RebuildReportRow]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["game_id", "status", "old_rows", "new_rows", "notes", "backup_path", "oci_status"]
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "game_id": row.game_id,
                    "status": row.status,
                    "old_rows": row.old_rows,
                    "new_rows": row.new_rows,
                    "notes": row.notes,
                    "backup_path": row.backup_path,
                    "oci_status": row.oci_status,
                }
            )


def _batter_from_description(description: Any) -> str | None:
    text = str(description or "").strip()
    if ":" not in text:
        return None
    return text.split(":", 1)[0].strip() or None


def _result_from_description(description: Any) -> str | None:
    text = str(description or "").strip()
    if ":" not in text:
        return None
    return text.split(":", 1)[-1].strip() or None


def _format_base_string(runners: int) -> str:
    return f"{'1' if (runners & 1) else '-'}{'2' if (runners & 2) else '-'}{'3' if (runners & 4) else '-'}"


def _dedupe_game_ids(game_ids: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in game_ids:
        game_id = str(value or "").strip()
        if not game_id or game_id in seen:
            continue
        seen.add(game_id)
        result.append(game_id)
    return result


def _load_game_ids_from_file(path: str | Path | None) -> list[str]:
    if not path:
        return []
    values: list[str] = []
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            token = line.strip().split(",", 1)[0].strip()
            if not token or token.lower() == "game_id":
                continue
            values.append(token)
    return _dedupe_game_ids(values)


def _chunked(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def run(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild noisy relay game_events from existing local rows")
    parser.add_argument("--season", type=int, action="append", help="Season to rebuild. Repeatable.")
    parser.add_argument("--game-id", action="append", default=[], help="Specific game_id to rebuild. Repeatable.")
    parser.add_argument("--game-ids-file", help="File containing one game_id per line or a CSV with game_id first")
    parser.add_argument("--apply", action="store_true", help="Apply local game_events rewrites")
    parser.add_argument("--dry-run", action="store_true", help="Explicit dry-run mode; default unless --apply is set")
    parser.add_argument("--sync-oci", action="store_true", help="Sync successfully applied games to OCI")
    parser.add_argument(
        "--oci-sync-mode",
        choices=("events", "specific-game"),
        default="events",
        help="OCI sync mode for applied games. Default only replaces game_events.",
    )
    parser.add_argument("--min-events", type=int, default=DEFAULT_MIN_EVENTS, help="Minimum rebuilt event rows required")
    parser.add_argument("--report-out", type=str, help="CSV report output path")
    parser.add_argument("--backup-out", type=str, help="CSV backup output path used with --apply")
    args = parser.parse_args(argv)

    seasons = args.season or list(DEFAULT_SEASONS)
    game_ids = _dedupe_game_ids([*args.game_id, *_load_game_ids_from_file(args.game_ids_file)])
    rebuild_relay_events(
        seasons=seasons,
        game_ids=game_ids,
        apply=bool(args.apply),
        sync_oci=bool(args.sync_oci),
        oci_sync_mode=args.oci_sync_mode,
        min_events=args.min_events,
        report_out=args.report_out,
        backup_out=args.backup_out,
        log=print,
    )
    return 0


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
