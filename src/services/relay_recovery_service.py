"""Shared service for completed-game relay and play-by-play recovery."""
from __future__ import annotations

import asyncio
import csv
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.models.season import KboSeason
from src.repositories.game_repository import (
    backfill_game_play_by_play_from_existing_events,
    save_relay_data,
)
from src.sources.relay import (
    ImportRelayAdapter,
    KboRelayAdapter,
    NaverRelayAdapter,
    NormalizedRelayResult,
    RelayRecoveryOrchestrator,
    default_source_order_for_bucket,
    derive_bucket_id,
    read_manifest_entries,
)
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES
from src.utils.safe_print import safe_print as print


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST_PATH = PROJECT_ROOT / "data" / "recovery" / "source_manifest.csv"
DEFAULT_CAPABILITY_PATH = PROJECT_ROOT / "data" / "recovery" / "source_capability.csv"


@dataclass(frozen=True)
class RelayRecoveryTarget:
    game_id: str
    league_type_name: str | None = None
    bucket_id: str | None = None
    has_events: bool = False
    has_pbp: bool = False
    needs_event_recovery: bool = True
    needs_pbp_recovery: bool = True

    @classmethod
    def from_game_state(
        cls,
        *,
        game_id: str,
        league_type_name: str | None,
        bucket_id: str | None,
        has_events: bool,
        has_pbp: bool,
    ) -> "RelayRecoveryTarget":
        return cls(
            game_id=game_id,
            league_type_name=league_type_name,
            bucket_id=bucket_id or derive_bucket_id(game_id, league_type_name),
            has_events=has_events,
            has_pbp=has_pbp,
            needs_event_recovery=not has_events,
            needs_pbp_recovery=not has_pbp,
        )


@dataclass
class RelayRecoveryResult:
    total_targets: int = 0
    saved_games: int = 0
    saved_rows: int = 0
    derived_pbp_games: int = 0
    empty_games: int = 0
    report_rows: list[dict[str, Any]] = field(default_factory=list)


def parse_source_order(value: str | None) -> list[str] | None:
    if not value:
        return None
    tokens = [token.strip() for token in value.split(",") if token.strip()]
    return tokens or None


def load_game_ids_from_file(path: str | Path | None) -> list[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Game ID file not found: {file_path}")

    game_ids: list[str] = []
    seen: set[str] = set()
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            token = line.split(",", 1)[0].strip()
            if token.lower() == "game_id" or not token or token in seen:
                continue
            seen.add(token)
            game_ids.append(token)
    return game_ids


def load_relay_recovery_targets(
    *,
    season: int | None = None,
    month: int | None = None,
    date: str | None = None,
    game_ids: Iterable[str] | None = None,
    game_ids_file: str | Path | None = None,
    bucket: str | None = None,
    missing_only: bool = True,
    log: Callable[[str], None] = print,
) -> list[RelayRecoveryTarget]:
    requested_ids = _dedupe([*(game_ids or []), *load_game_ids_from_file(game_ids_file)])

    if not requested_ids and not date and not season:
        raise ValueError("Must provide season, date, game_ids, or game_ids_file")

    with SessionLocal() as session:
        rows = _load_target_rows(
            session,
            season=season,
            month=month,
            date=date,
            requested_ids=requested_ids,
        )
        row_game_ids = [row[0] for row in rows]
        if not row_game_ids:
            return []

        event_set = {
            row[0]
            for row in session.query(GameEvent.game_id)
            .filter(GameEvent.game_id.in_(row_game_ids))
            .distinct()
            .all()
        }
        pbp_set = {
            row[0]
            for row in session.query(GamePlayByPlay.game_id)
            .filter(GamePlayByPlay.game_id.in_(row_game_ids))
            .distinct()
            .all()
        }

    targets: list[RelayRecoveryTarget] = []
    skipped = 0
    for game_id, league_type_name in rows:
        has_events = game_id in event_set
        has_pbp = game_id in pbp_set
        if missing_only and has_events and has_pbp:
            skipped += 1
            continue
        targets.append(
            RelayRecoveryTarget.from_game_state(
                game_id=game_id,
                league_type_name=league_type_name,
                bucket_id=bucket,
                has_events=has_events,
                has_pbp=has_pbp,
            )
        )

    if missing_only:
        log(f"[INFO] Missing-only mode: Skipped {skipped} games already fully recovered.")
    return targets


async def recover_relay_data(
    targets: Iterable[RelayRecoveryTarget],
    *,
    dry_run: bool = False,
    source_order_override: Sequence[str] | None = None,
    import_manifest: str | Path | Iterable[str | Path] = DEFAULT_MANIFEST_PATH,
    capability_path: str | Path = DEFAULT_CAPABILITY_PATH,
    source_timeout: float = 30.0,
    allow_derived_pbp: bool = False,
    min_result_events: int | None = None,
    validate_final_score: bool = False,
    report_out: str | Path | None = None,
    sleep_seconds: float = 1.0,
    orchestrator: RelayRecoveryOrchestrator | None = None,
    log: Callable[[str], None] = print,
) -> RelayRecoveryResult:
    target_list = list(targets)
    run_result = RelayRecoveryResult(total_targets=len(target_list))
    if not target_list:
        return run_result

    orchestrator = orchestrator or build_relay_recovery_orchestrator(
        import_manifest=import_manifest,
        capability_path=capability_path,
        source_timeout=source_timeout,
    )

    bucket_map: dict[str, list[RelayRecoveryTarget]] = defaultdict(list)
    for target in target_list:
        bucket_map[target.bucket_id or derive_bucket_id(target.game_id, target.league_type_name)].append(target)

    log(f"[INFO] Total games to process: {len(target_list)}")
    if dry_run:
        log("[WARN] Dry-run mode activated. No data will be saved.")

    final_scores = (
        _load_final_scores([target.game_id for target in target_list])
        if validate_final_score
        else {}
    )

    for bucket_id, bucket_targets in bucket_map.items():
        source_order = orchestrator.source_order_for_bucket(
            bucket_id,
            source_order_override or default_source_order_for_bucket(bucket_id),
        )
        log(f"[INFO] Bucket {bucket_id}: source order = {', '.join(source_order)}")
        await orchestrator.probe_bucket(
            bucket_id,
            [target.game_id for target in bucket_targets],
            source_order,
        )

        for index, target in enumerate(bucket_targets, start=1):
            log(f"\n[PROGRESS] Bucket {bucket_id} {index}/{len(bucket_targets)}: {target.game_id}")

            if allow_derived_pbp and target.has_events and target.needs_pbp_recovery:
                saved_rows = 0 if dry_run else backfill_game_play_by_play_from_existing_events(target.game_id)
                log(
                    f"[SUCCESS] {'Would derive' if dry_run else 'Derived'} "
                    f"{saved_rows if not dry_run else 'missing'} play_by_play rows from game_events"
                )
                run_result.derived_pbp_games += 1
                run_result.saved_rows += saved_rows
                run_result.report_rows.append(
                    {
                        "game_id": target.game_id,
                        "bucket_id": bucket_id,
                        "source_name": "derived_game_events",
                        "status": "dry_run" if dry_run else "saved",
                        "saved_rows": saved_rows,
                        "has_event_state": True,
                        "has_raw_pbp": True,
                        "notes": "Derived game_play_by_play from existing game_events",
                    }
                )
                continue

            relay_result, attempts = await orchestrator.fetch_game(target.game_id, bucket_id, source_order)
            run_result.report_rows.extend(attempts)
            if relay_result.is_empty:
                run_result.empty_games += 1
                log(f"[SKIP] No relay data extracted for {target.game_id}")
                continue

            validation_failure = _validate_relay_result(
                target.game_id,
                relay_result,
                final_scores=final_scores,
                min_result_events=min_result_events,
                validate_final_score=validate_final_score,
            )
            if validation_failure:
                log(f"[SKIP] Validation failed for {target.game_id}: {validation_failure}")
                run_result.report_rows.append(
                    {
                        "game_id": target.game_id,
                        "bucket_id": bucket_id,
                        "source_name": relay_result.source_name,
                        "status": "skipped_validation",
                        "saved_rows": 0,
                        "has_event_state": relay_result.has_event_state,
                        "has_raw_pbp": relay_result.has_raw_pbp or bool(relay_result.raw_pbp_rows),
                        "notes": validation_failure,
                    }
                )
                continue

            saved_rows = _save_or_count_rows(
                target.game_id,
                relay_result,
                dry_run=dry_run,
                allow_derived_pbp=allow_derived_pbp,
            )
            if dry_run:
                log(
                    f"[DRY-RUN] Would save {saved_rows} rows from {relay_result.source_name} "
                    f"for {target.game_id}"
                )
            else:
                log(f"[SUCCESS] Saved {saved_rows} rows for {target.game_id} via {relay_result.source_name}")

            if saved_rows:
                run_result.saved_games += 1
                run_result.saved_rows += saved_rows
            run_result.report_rows.append(
                {
                    "game_id": target.game_id,
                    "bucket_id": bucket_id,
                    "source_name": relay_result.source_name,
                    "status": "dry_run" if dry_run else "saved",
                    "saved_rows": saved_rows,
                    "has_event_state": relay_result.has_event_state,
                    "has_raw_pbp": relay_result.has_raw_pbp or bool(relay_result.raw_pbp_rows),
                    "notes": relay_result.notes,
                }
            )
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

    write_relay_recovery_report(report_out, run_result.report_rows, log=log)
    return run_result


def build_relay_recovery_orchestrator(
    *,
    import_manifest: str | Path | Iterable[str | Path] = DEFAULT_MANIFEST_PATH,
    capability_path: str | Path = DEFAULT_CAPABILITY_PATH,
    source_timeout: float = 30.0,
) -> RelayRecoveryOrchestrator:
    manifest_entries = read_manifest_entries(import_manifest)
    manifest_base_dir = _manifest_base_dir(import_manifest)
    adapters = {
        "naver": NaverRelayAdapter(),
        "kbo": KboRelayAdapter(),
        "import": ImportRelayAdapter(
            manifest_entries,
            source_name="import",
            allowed_source_types={"naver", "kbo", "html_archive", "json_archive"},
            manifest_base_dir=manifest_base_dir,
        ),
        "manual": ImportRelayAdapter(
            manifest_entries,
            source_name="manual",
            allowed_source_types={"manual_text"},
            manifest_base_dir=manifest_base_dir,
        ),
    }
    return RelayRecoveryOrchestrator(
        adapters,
        capability_path=capability_path,
        timeout_seconds=source_timeout,
    )


def write_relay_recovery_report(
    report_path: str | Path | None,
    rows: list[dict[str, Any]],
    *,
    log: Callable[[str], None] = print,
) -> None:
    if not report_path:
        return
    path = Path(report_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "game_id",
        "bucket_id",
        "source_name",
        "status",
        "saved_rows",
        "has_event_state",
        "has_raw_pbp",
        "notes",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name) for name in fieldnames})
    log(f"[INFO] Recovery report written to {path}")


def _load_target_rows(
    session: Any,
    *,
    season: int | None,
    month: int | None,
    date: str | None,
    requested_ids: list[str],
) -> list[tuple[str, str | None]]:
    if requested_ids:
        found_rows = (
            session.query(Game.game_id, KboSeason.league_type_name)
            .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
            .filter(Game.game_id.in_(requested_ids))
            .all()
        )
        row_map = {game_id: league_type_name for game_id, league_type_name in found_rows}
        return [(game_id, row_map.get(game_id)) for game_id in requested_ids]

    query = (
        session.query(Game.game_id, KboSeason.league_type_name)
        .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
        .filter(Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)))
    )
    if date:
        try:
            target_dt = datetime.strptime(date, "%Y%m%d").date()
        except ValueError as exc:
            raise ValueError(f"Invalid date format: {date}. Use YYYYMMDD.") from exc
        query = query.filter(Game.game_date == target_dt)
    elif season:
        prefix = f"{season}{month:02d}" if month else str(season)
        query = query.filter(Game.game_id.like(f"{prefix}%"))
    return query.order_by(Game.game_id.asc()).all()


def _load_final_scores(game_ids: Sequence[str]) -> dict[str, tuple[int | None, int | None]]:
    if not game_ids:
        return {}
    with SessionLocal() as session:
        rows = (
            session.query(Game.game_id, Game.away_score, Game.home_score)
            .filter(Game.game_id.in_(list(game_ids)))
            .all()
        )
    return {game_id: (away_score, home_score) for game_id, away_score, home_score in rows}


def _validate_relay_result(
    game_id: str,
    relay_result: NormalizedRelayResult,
    *,
    final_scores: dict[str, tuple[int | None, int | None]],
    min_result_events: int | None,
    validate_final_score: bool,
) -> str | None:
    events = relay_result.events or []
    if min_result_events is not None and len(events) < min_result_events:
        return f"too_few_result_events:{len(events)}<{min_result_events}"

    if not validate_final_score:
        return None

    expected = final_scores.get(game_id)
    if expected is None or expected[0] is None or expected[1] is None:
        return "missing_game_final_score"

    actual = _last_event_score(events)
    if actual is None:
        return "missing_event_final_score"

    if actual != expected:
        return (
            "final_score_mismatch:"
            f"events={actual[0]}-{actual[1]} game={expected[0]}-{expected[1]}"
        )
    return None


def _last_event_score(events: Sequence[dict[str, Any]]) -> tuple[int, int] | None:
    for event in reversed(events):
        away = _coerce_int(event.get("away_score"))
        home = _coerce_int(event.get("home_score"))
        if away is not None and home is not None:
            return away, home
    return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _save_or_count_rows(
    game_id: str,
    relay_result: NormalizedRelayResult,
    *,
    dry_run: bool,
    allow_derived_pbp: bool,
) -> int:
    if dry_run:
        return len(relay_result.events) if relay_result.events else len(relay_result.raw_pbp_rows)
    return save_relay_data(
        game_id,
        relay_result.events,
        raw_pbp_rows=relay_result.raw_pbp_rows,
        source_name=relay_result.source_name,
        notes=relay_result.notes,
        allow_derived_pbp=allow_derived_pbp,
    )


def _manifest_base_dir(manifest_path: str | Path | Iterable[str | Path]) -> Path:
    if isinstance(manifest_path, Path):
        return manifest_path.resolve().parent
    if isinstance(manifest_path, str):
        token = next((part.strip() for part in manifest_path.split(",") if part.strip()), "")
        return Path(token or ".").resolve().parent
    for item in manifest_path:
        return _manifest_base_dir(item)
    return Path(".").resolve()


def _dedupe(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        game_id = str(value or "").strip()
        if not game_id or game_id in seen:
            continue
        seen.add(game_id)
        result.append(game_id)
    return result
