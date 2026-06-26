"""Shared service for completed-game relay and play-by-play recovery."""

from __future__ import annotations

import asyncio
import csv
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.db.engine import SessionLocal
from src.models.game import Game, GameEvent, GamePlayByPlay
from src.models.season import KboSeason
from src.repositories.game_repository import (
    backfill_game_play_by_play_from_existing_events,
    mark_relay_source_unavailable,
    save_relay_data,
)
from src.services.wpa_transitions import apply_wpa_transitions, event_has_wpa_state
from src.sources.relay import (
    ImportRelayAdapter,
    KboRelayAdapter,
    NaverRelayAdapter,
    NormalizedRelayResult,
    RelayRecoveryOrchestrator,
    default_source_order_for_bucket,
    derive_bucket_id,
    event_has_minimum_state,
    normalize_pbp_row,
    read_manifest_entries,
)
from src.utils.date_helpers import parse_date_str
from src.utils.game_status import (
    COMPLETED_LIKE_GAME_STATUSES,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST_PATH = PROJECT_ROOT / "data" / "recovery" / "source_manifest.csv"
DEFAULT_CAPABILITY_PATH = PROJECT_ROOT / "data" / "recovery" / "source_capability.csv"


@dataclass
class GameStateInput:
    """GameStateInput class."""

    game_id: str
    league_type_name: str | None
    bucket_id: str | None
    has_events: bool
    has_event_state: bool
    has_pbp: bool


@dataclass(frozen=True)
class RelayRecoveryTarget:
    """RelayRecoveryTarget class."""

    game_id: str
    league_type_name: str | None = None
    bucket_id: str | None = None
    has_events: bool = False
    has_event_state: bool = False
    has_pbp: bool = False
    needs_event_recovery: bool = True
    needs_pbp_recovery: bool = True

    @classmethod
    def from_game_state(
        cls,
        *,
        state: GameStateInput,
    ) -> RelayRecoveryTarget:
        """
        Handles the from game state operation.

        Returns:
            RelayRecoveryTarget instance.

        """
        return cls(
            game_id=state.game_id,
            league_type_name=state.league_type_name,
            bucket_id=state.bucket_id or derive_bucket_id(state.game_id, state.league_type_name),
            has_events=state.has_events,
            has_event_state=state.has_event_state,
            has_pbp=state.has_pbp,
            needs_event_recovery=not state.has_event_state,
            needs_pbp_recovery=not state.has_pbp,
        )


@dataclass
class RelayRecoveryResult:
    """RelayRecoveryResult class."""

    total_targets: int = 0
    saved_games: int = 0
    saved_rows: int = 0
    derived_pbp_games: int = 0
    empty_games: int = 0
    filtered_games: int = 0
    match_failed_games: int = 0
    api_failed_games: int = 0
    report_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class RelaySaveCounts:
    """RelaySaveCounts class."""

    saved_rows: int
    saved_event_rows: int = 0
    saved_pbp_rows: int = 0
    skipped_event_rows_reason: str | None = None


@dataclass
class RecoveryTargetCriteria:
    """RecoveryTargetCriteria class."""

    season: int | None = None
    month: int | None = None
    date: str | None = None
    game_ids: Iterable[str] | None = None
    game_ids_file: str | Path | None = None
    bucket: str | None = None
    missing_only: bool = True
    include_incomplete: bool = False


@dataclass
class RelayRecoveryConfig:
    """RelayRecoveryConfig class."""

    dry_run: bool = False
    source_order_override: Sequence[str] | None = None
    import_manifest: str | Path | Iterable[str | Path] = DEFAULT_MANIFEST_PATH
    capability_path: str | Path = DEFAULT_CAPABILITY_PATH
    source_timeout: float = 30.0
    allow_derived_pbp: bool = False
    min_result_events: int | None = None
    validate_final_score: bool = True
    validate_inning_continuity: bool = True
    report_out: str | Path | None = None
    sleep_seconds: float = 1.0
    log: Callable[[str], None] = logger.info


@dataclass
class RelayValidationConfig:
    """RelayValidationConfig class."""

    final_scores: dict[str, tuple[int | None, int | None]]
    min_result_events: int | None = None
    validate_final_score: bool = True
    validate_inning_continuity: bool = True


@dataclass
class RecoveryLoopContext:
    """RecoveryLoopContext class."""

    target: RelayRecoveryTarget
    bucket_id: str
    source_order: Sequence[str]
    run_result: RelayRecoveryResult
    dry_run: bool
    log: Callable[[str], None]


def parse_source_order(value: str | None) -> list[str] | None:
    """
    Parses source order.

    Args:
        value: Value.

    Returns:
        The result of the operation.

    """
    if not value:
        return None
    tokens = [token.strip() for token in value.split(",") if token.strip()]
    return tokens or None


def load_game_ids_from_file(path: str | Path | None) -> list[str]:
    """
    Loads game ids from file.

    Args:
        path: Path.

    Returns:
        List of results.

    """
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        msg = f"Game ID file not found: {file_path}"
        raise FileNotFoundError(msg)

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
    criteria: RecoveryTargetCriteria,
    *,
    log: Callable[[str], None] = logger.info,
) -> list[RelayRecoveryTarget]:
    """
    Loads relay recovery targets.

    Args:
        criteria: Criteria.

    Returns:
        List of results.

    """
    allowed_statuses = list(COMPLETED_LIKE_GAME_STATUSES)
    if criteria.include_incomplete:
        allowed_statuses.extend([GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED])

    requested_ids = _dedupe([*(criteria.game_ids or []), *load_game_ids_from_file(criteria.game_ids_file)])

    if not requested_ids and not criteria.date and not criteria.season:
        msg = "Must provide season, date, game_ids, or game_ids_file"
        raise ValueError(msg)

    with SessionLocal() as session:
        rows = _load_target_rows(
            session,
            criteria,
            allowed_statuses=allowed_statuses,
        )
        row_game_ids = [row[0] for row in rows]
        if not row_game_ids:
            return []

        event_rows = session.query(GameEvent).filter(GameEvent.game_id.in_(row_game_ids)).all()
        event_set = {row.game_id for row in event_rows}
        event_state_set = {row.game_id for row in event_rows if event_has_wpa_state(row)}
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
        has_event_state = game_id in event_state_set
        has_pbp = game_id in pbp_set
        if criteria.missing_only and has_event_state and has_pbp:
            skipped += 1
            continue
        targets.append(
            RelayRecoveryTarget(
                game_id=game_id,
                league_type_name=league_type_name,
                bucket_id=criteria.bucket or derive_bucket_id(game_id, league_type_name),
                has_events=has_events,
                has_event_state=has_event_state,
                has_pbp=has_pbp,
                needs_event_recovery=not has_event_state,
                needs_pbp_recovery=not has_pbp,
            ),
        )

    if criteria.missing_only:
        log(f"[INFO] Missing-only mode: Skipped {skipped} games already fully recovered.")
    return targets


async def recover_relay_data(
    targets: Iterable[RelayRecoveryTarget],
    config: RelayRecoveryConfig | None = None,
    orchestrator: RelayRecoveryOrchestrator | None = None,
) -> RelayRecoveryResult:
    """
    Handles the recover relay data operation.

    Args:
        targets: Targets.
        config: Config.
        orchestrator: Orchestrator.

    Returns:
        RelayRecoveryResult instance.

    """
    cfg = config or RelayRecoveryConfig()
    target_list = list(targets)
    run_result = RelayRecoveryResult(total_targets=len(target_list))
    if not target_list:
        return run_result

    orchestrator = orchestrator or build_relay_recovery_orchestrator(
        import_manifest=cfg.import_manifest,
        capability_path=cfg.capability_path,
        source_timeout=cfg.source_timeout,
    )
    bucket_map = _bucket_targets(target_list)

    cfg.log(f"[INFO] Total games to process: {len(target_list)}")
    if cfg.dry_run:
        cfg.log("[WARN] Dry-run mode activated. No data will be saved.")

    validation_config = (
        RelayValidationConfig(
            final_scores=_load_final_scores([target.game_id for target in target_list]),
            min_result_events=cfg.min_result_events,
            validate_final_score=cfg.validate_final_score,
            validate_inning_continuity=cfg.validate_inning_continuity,
        )
        if cfg.validate_final_score
        else RelayValidationConfig(
            final_scores={},
            min_result_events=cfg.min_result_events,
            validate_final_score=False,
            validate_inning_continuity=cfg.validate_inning_continuity,
        )
    )

    for bucket_id, bucket_targets in bucket_map.items():
        source_order = orchestrator.source_order_for_bucket(
            bucket_id,
            cfg.source_order_override or default_source_order_for_bucket(bucket_id),
        )
        cfg.log(f"[INFO] Bucket {bucket_id}: source order = {', '.join(source_order)}")
        await orchestrator.probe_bucket(
            bucket_id,
            [target.game_id for target in bucket_targets],
            source_order,
        )

        for index, target in enumerate(bucket_targets, start=1):
            cfg.log(f"\n[PROGRESS] Bucket {bucket_id} {index}/{len(bucket_targets)}: {target.game_id}")

            ctx = RecoveryLoopContext(
                target=target,
                bucket_id=bucket_id,
                source_order=source_order,
                run_result=run_result,
                dry_run=cfg.dry_run,
                log=cfg.log,
            )

            if _maybe_derive_pbp(ctx, cfg):
                continue

            relay_result, attempts = await orchestrator.fetch_game(
                target.game_id,
                bucket_id,
                source_order,
                validator=_relay_validator(
                    target.game_id,
                    validation_config,
                ),
            )
            run_result.report_rows.extend(attempts)
            if relay_result.is_empty:
                _handle_empty_relay_result(
                    ctx,
                    attempts,
                    relay_result,
                )
                cfg.log(f"[SKIP] No relay data extracted for {target.game_id}")
                continue

            relay_result, filter_notes, filtered_rows = _sanitize_relay_result(relay_result)
            if relay_result.is_empty:
                _handle_filtered_relay_result(ctx, relay_result, filter_notes)
                continue

            _save_relay_result(
                ctx,
                cfg,
                relay_result,
                filter_notes,
                filtered_rows,
            )
            if cfg.sleep_seconds > 0:
                await asyncio.sleep(cfg.sleep_seconds)

    write_relay_recovery_report(cfg.report_out, run_result.report_rows, log=cfg.log)
    return run_result


def _bucket_targets(targets: list[RelayRecoveryTarget]) -> dict[str, list[RelayRecoveryTarget]]:
    bucket_map: dict[str, list[RelayRecoveryTarget]] = defaultdict(list)
    for target in targets:
        bucket_map[target.bucket_id or derive_bucket_id(target.game_id, target.league_type_name)].append(target)
    return bucket_map


def _relay_validator(
    game_id: str,
    config: RelayValidationConfig,
) -> Callable[[NormalizedRelayResult], str | None]:
    def validator(relay_result: NormalizedRelayResult) -> str | None:
        """
        Handles the validator operation.

        Args:
            relay_result: Relay Result.

        Returns:
            The result of the operation.

        """
        return _validate_relay_result(
            game_id,
            relay_result,
            config,
        )

    return validator


def _maybe_derive_pbp(
    ctx: RecoveryLoopContext,
    config: RelayRecoveryConfig,
) -> bool:
    target = ctx.target
    if not (config.allow_derived_pbp and target.has_event_state and target.needs_pbp_recovery):
        return False
    saved_rows = 0 if ctx.dry_run else backfill_game_play_by_play_from_existing_events(target.game_id)
    ctx.log(
        f"[SUCCESS] {'Would derive' if ctx.dry_run else 'Derived'} "
        f"{saved_rows if not ctx.dry_run else 'missing'} play_by_play rows from game_events",
    )
    ctx.run_result.derived_pbp_games += 1
    ctx.run_result.saved_rows += saved_rows
    ctx.run_result.report_rows.append(
        {
            "game_id": target.game_id,
            "bucket_id": ctx.bucket_id,
            "source_name": "derived_game_events",
            "status": "dry_run" if ctx.dry_run else "saved",
            "saved_rows": saved_rows,
            "saved_event_rows": 0,
            "saved_pbp_rows": saved_rows,
            "skipped_event_rows_reason": None,
            "has_event_state": True,
            "has_raw_pbp": True,
            "notes": "Derived game_play_by_play from existing game_events",
        },
    )
    return True


def _handle_empty_relay_result(
    ctx: RecoveryLoopContext,
    attempts: list[dict[str, Any]],
    relay_result: NormalizedRelayResult,
) -> None:
    ctx.run_result.empty_games += 1
    failure_bucket = _classify_relay_failure(relay_result.notes)
    if failure_bucket == "relay_match_failed":
        ctx.run_result.match_failed_games += 1
    elif failure_bucket == "relay_api_failed":
        ctx.run_result.api_failed_games += 1
    if _should_mark_source_unavailable(ctx.bucket_id, attempts, relay_result):
        _mark_unavailable_relay_source(ctx, attempts, relay_result)


def _mark_unavailable_relay_source(
    ctx: RecoveryLoopContext,
    attempts: list[dict[str, Any]],
    relay_result: NormalizedRelayResult,
) -> None:
    evidence = {
        "bucket_id": ctx.bucket_id,
        "source_order": list(ctx.source_order),
        "attempts": attempts,
        "notes": relay_result.notes,
    }
    if not ctx.dry_run:
        mark_relay_source_unavailable(ctx.target.game_id, reason="public_relay_source_unavailable", evidence=evidence)
    ctx.run_result.report_rows.append(
        {
            "game_id": ctx.target.game_id,
            "bucket_id": ctx.bucket_id,
            "source_name": "none",
            "status": "source_unavailable_dry_run" if ctx.dry_run else "source_unavailable",
            "saved_rows": 0,
            "saved_event_rows": 0,
            "saved_pbp_rows": 0,
            "skipped_event_rows_reason": "public_relay_source_unavailable",
            "has_event_state": False,
            "has_raw_pbp": False,
            "notes": relay_result.notes,
        },
    )


def _handle_filtered_relay_result(
    ctx: RecoveryLoopContext,
    relay_result: NormalizedRelayResult,
    filter_notes: list[str],
) -> None:
    ctx.run_result.filtered_games += 1
    notes = ";".join(filter_notes) or "relay_rows_empty_after_filter"
    ctx.log(f"[SKIP] Relay rows filtered out for {ctx.target.game_id}: {notes}")
    ctx.run_result.report_rows.append(
        {
            "game_id": ctx.target.game_id,
            "bucket_id": ctx.bucket_id,
            "source_name": relay_result.source_name,
            "status": "skipped_filtered",
            "saved_rows": 0,
            "has_event_state": relay_result.has_event_state,
            "has_raw_pbp": relay_result.has_raw_pbp,
            "notes": notes,
        },
    )


def _save_relay_result(
    ctx: RecoveryLoopContext,
    config: RelayRecoveryConfig,
    relay_result: NormalizedRelayResult,
    filter_notes: list[str],
    filtered_rows: int,
) -> None:
    save_counts = _save_or_count_rows(
        ctx.target.game_id,
        relay_result,
        dry_run=ctx.dry_run,
        allow_derived_pbp=config.allow_derived_pbp,
    )
    _log_relay_save(ctx.target, relay_result, save_counts, dry_run=ctx.dry_run, log=ctx.log)
    if save_counts.saved_rows:
        ctx.run_result.saved_games += 1
        ctx.run_result.saved_rows += save_counts.saved_rows
    ctx.run_result.report_rows.append(
        {
            "game_id": ctx.target.game_id,
            "bucket_id": ctx.bucket_id,
            "source_name": relay_result.source_name,
            "status": "dry_run" if ctx.dry_run else ("partial_relay" if filtered_rows else "saved"),
            "saved_rows": save_counts.saved_rows,
            "saved_event_rows": save_counts.saved_event_rows,
            "saved_pbp_rows": save_counts.saved_pbp_rows,
            "skipped_event_rows_reason": save_counts.skipped_event_rows_reason,
            "has_event_state": relay_result.has_event_state,
            "has_raw_pbp": relay_result.has_raw_pbp or bool(relay_result.raw_pbp_rows),
            "notes": _join_notes(relay_result.notes, *filter_notes),
        },
    )


def _log_relay_save(
    target: RelayRecoveryTarget,
    relay_result: NormalizedRelayResult,
    save_counts: RelaySaveCounts,
    *,
    dry_run: bool,
    log: Callable[[str], None],
) -> None:
    if dry_run:
        log(
            f"[DRY-RUN] Would save events={save_counts.saved_event_rows} "
            f"pbp={save_counts.saved_pbp_rows} from {relay_result.source_name} for {target.game_id}",
        )
    else:
        log(
            f"[SUCCESS] Saved events={save_counts.saved_event_rows} "
            f"pbp={save_counts.saved_pbp_rows} for {target.game_id} via {relay_result.source_name}",
        )


def build_relay_recovery_orchestrator(
    *,
    import_manifest: str | Path | Iterable[str | Path] = DEFAULT_MANIFEST_PATH,
    capability_path: str | Path = DEFAULT_CAPABILITY_PATH,
    source_timeout: float = 30.0,
) -> RelayRecoveryOrchestrator:
    """
    Builds relay recovery orchestrator.

    Returns:
        RelayRecoveryOrchestrator instance.

    """
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
    log: Callable[[str], None] = logger.info,
) -> None:
    """
    Writes relay recovery.

    Args:
        report_path: Report file path.
        rows: Rows.

    """
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
        "saved_event_rows",
        "saved_pbp_rows",
        "skipped_event_rows_reason",
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
    session: Session,
    criteria: RecoveryTargetCriteria,
    *,
    allowed_statuses: list[str] | None = None,
) -> list[tuple[str, str | None]]:
    if not allowed_statuses:
        allowed_statuses = list(COMPLETED_LIKE_GAME_STATUSES)
    requested_ids = _dedupe([*(criteria.game_ids or []), *load_game_ids_from_file(criteria.game_ids_file)])
    if requested_ids:
        found_rows = (
            session.query(Game.game_id, KboSeason.league_type_name)
            .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
            .filter(
                Game.game_id.in_(requested_ids),
                Game.game_status.in_(tuple(allowed_statuses)),
            )
            .all()
        )
        row_map = dict(found_rows)
        return [(game_id, row_map[game_id]) for game_id in requested_ids if game_id in row_map]

    query = (
        session.query(Game.game_id, KboSeason.league_type_name)
        .outerjoin(KboSeason, KboSeason.season_id == Game.season_id)
        .filter(Game.game_status.in_(tuple(allowed_statuses)))
    )
    if criteria.date:
        try:
            target_dt = parse_date_str(criteria.date)
        except ValueError as exc:
            msg = f"Invalid date format: {criteria.date}. Use YYYYMMDD."
            raise ValueError(msg) from exc
        query = query.filter(Game.game_date == target_dt)
    elif criteria.season:
        prefix = f"{criteria.season}{criteria.month:02d}" if criteria.month else str(criteria.season)
        query = query.filter(Game.game_id.like(f"{prefix}%"))
    return query.order_by(Game.game_id.asc()).all()


def _load_final_scores(game_ids: Sequence[str]) -> dict[str, tuple[int | None, int | None]]:
    if not game_ids:
        return {}
    with SessionLocal() as session:
        rows = (
            session.query(Game.game_id, Game.away_score, Game.home_score).filter(Game.game_id.in_(list(game_ids))).all()
        )
    return {game_id: (away_score, home_score) for game_id, away_score, home_score in rows}


def _validate_relay_result(
    game_id: str,
    relay_result: NormalizedRelayResult,
    config: RelayValidationConfig,
) -> str | None:
    events = relay_result.events or []
    if config.min_result_events is not None and len(events) < config.min_result_events:
        return f"too_few_result_events:{len(events)}<{config.min_result_events}"

    if not events:
        return None

    if config.validate_inning_continuity:
        inning_error = _validate_relay_inning_continuity(events)
        if inning_error:
            return inning_error

    if not config.validate_final_score:
        return None

    return _validate_relay_final_score(game_id, events, config.final_scores)


def _validate_relay_inning_continuity(events: list[dict[str, Any]]) -> str | None:
    innings = sorted({int(e.get("inning") or 1) for e in events})
    if not innings:
        return None
    if innings[0] != 1:
        return f"missing_starting_inning:first={innings[0]}"
    for index in range(len(innings) - 1):
        if innings[index + 1] != innings[index] + 1:
            return f"missing_middle_inning:gap_between_{innings[index]}_and_{innings[index + 1]}"
    return None


def _validate_relay_final_score(
    game_id: str,
    events: list[dict[str, Any]],
    final_scores: dict[str, tuple[int | None, int | None]],
) -> str | None:
    expected = final_scores.get(game_id)
    if expected is None or expected[0] is None or expected[1] is None:
        return "missing_game_final_score"

    actual = _last_event_score(events)
    if actual is None:
        return "missing_event_final_score"

    if actual != expected:
        return f"final_score_mismatch:events={actual[0]}-{actual[1]} game={expected[0]}-{expected[1]}"
    return None


def _sanitize_relay_result(
    relay_result: NormalizedRelayResult,
) -> tuple[NormalizedRelayResult, list[str], int]:
    candidate_events = [dict(event) for event in relay_result.events or []]
    apply_wpa_transitions(candidate_events, only_missing=True)
    valid_events = [event for event in candidate_events if event_has_minimum_state(event)]
    valid_pbp_rows = [
        row for row in (_normalize_valid_pbp_row(row) for row in relay_result.raw_pbp_rows or []) if row is not None
    ]

    filtered_events = len(relay_result.events or []) - len(valid_events)
    filtered_pbp = len(relay_result.raw_pbp_rows or []) - len(valid_pbp_rows)
    notes: list[str] = []
    if filtered_events:
        notes.append(f"filtered_event_rows:{filtered_events}")
    if filtered_pbp:
        notes.append(f"filtered_pbp_rows:{filtered_pbp}")

    sanitized = NormalizedRelayResult(
        game_id=relay_result.game_id,
        source_name=relay_result.source_name,
        events=valid_events,
        raw_pbp_rows=valid_pbp_rows,
        has_event_state=bool(valid_events),
        has_raw_pbp=bool(valid_pbp_rows),
        notes=relay_result.notes,
        parser_version=relay_result.parser_version,
        source_schema_version=relay_result.source_schema_version,
        payload_hash=relay_result.payload_hash,
    )
    return sanitized, notes, filtered_events + filtered_pbp


def _normalize_valid_pbp_row(row: dict[str, Any]) -> dict[str, Any] | None:
    normalized = normalize_pbp_row(row)
    inning = _coerce_int(normalized.get("inning"))
    inning_half = normalized.get("inning_half")
    description = str(normalized.get("play_description") or "").strip()
    if inning is None or inning_half not in {"top", "bottom"} or not description:
        return None
    normalized["inning"] = inning
    normalized["play_description"] = description
    return normalized


def _classify_relay_failure(notes: str | None) -> str:
    value = str(notes or "").lower()
    if "invalid_relay_match" in value or "match" in value:
        return "relay_match_failed"
    if "relay_api_error" in value or "api" in value or "timeout" in value:
        return "relay_api_failed"
    return "relay_empty"


def _should_mark_source_unavailable(
    bucket_id: str,
    attempts: list[dict[str, Any]],
    relay_result: NormalizedRelayResult,
) -> bool:
    """Return true when a historical miss should be recorded as explainably unavailable."""
    if not bucket_id.endswith("_legacy"):
        return False
    try:
        year = int(bucket_id.split("_", 1)[0])
    except (TypeError, ValueError):
        year = 0
    if year >= 2010:
        return False

    failure_text = " ".join(
        [
            str(relay_result.notes or ""),
            *[str(attempt.get("status") or "") for attempt in attempts],
            *[str(attempt.get("notes") or "") for attempt in attempts],
        ],
    ).lower()
    transient_tokens = ("timeout", "exception", "api", "http_5", "rate", "blocked")
    return not any(token in failure_text for token in transient_tokens)


def _join_notes(*notes: str | None) -> str | None:
    tokens = [str(note).strip() for note in notes if str(note or "").strip()]
    return ";".join(tokens) or None


def _last_event_score(events: Sequence[dict[str, Any]]) -> tuple[int, int] | None:
    for event in reversed(events):
        away = _coerce_int(event.get("away_score"))
        home = _coerce_int(event.get("home_score"))
        if away is not None and home is not None:
            return away, home
    return None


def _coerce_int(value: object) -> int | None:
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
) -> RelaySaveCounts:
    event_rows = len(relay_result.events or [])
    pbp_rows = len(relay_result.raw_pbp_rows or [])
    skipped_event_rows_reason = None
    if not event_rows and relay_result.raw_pbp_rows:
        skipped_event_rows_reason = "no_valid_event_state"
    if dry_run:
        saved_rows = event_rows or pbp_rows
        return RelaySaveCounts(
            saved_rows=saved_rows,
            saved_event_rows=event_rows,
            saved_pbp_rows=pbp_rows,
            skipped_event_rows_reason=skipped_event_rows_reason,
        )
    saved_rows = save_relay_data(
        game_id,
        relay_result.events,
        raw_pbp_rows=relay_result.raw_pbp_rows,
        source_name=relay_result.source_name,
        notes=relay_result.notes,
        allow_derived_pbp=allow_derived_pbp,
        parser_version=relay_result.parser_version,
        source_schema_version=relay_result.source_schema_version,
        payload_hash=relay_result.payload_hash,
    )
    if not saved_rows:
        return RelaySaveCounts(saved_rows=0, skipped_event_rows_reason=skipped_event_rows_reason)
    return RelaySaveCounts(
        saved_rows=saved_rows,
        saved_event_rows=event_rows,
        saved_pbp_rows=pbp_rows,
        skipped_event_rows_reason=skipped_event_rows_reason,
    )


def _manifest_base_dir(manifest_path: str | Path | Iterable[str | Path]) -> Path:
    if isinstance(manifest_path, Path):
        return manifest_path.resolve().parent
    if isinstance(manifest_path, str):
        token = next((part.strip() for part in manifest_path.split(",") if part.strip()), "")
        return Path(token or ".").resolve().parent
    for item in manifest_path:
        return _manifest_base_dir(item)
    return Path.cwd()


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
