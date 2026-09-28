"""Shared helpers for game detail and relay collection workflows."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Protocol, cast

from sqlalchemy.exc import SQLAlchemyError

from src.constants import DATE_STR_LEN
from src.crawlers.failure_taxonomy import FailureCode, classify_persist_failure, stage_for_code
from src.crawlers.game_detail_outcome import (
    PARTIAL_DETAIL_REASON,
    GameDetailAttempt,
    attempt_from_result,
    error_code_for_reason,
)
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat, GameEvent, GamePitchingStat, GamePlayByPlay
from src.repositories.crawl_dead_letter_repository import DeadLetterSpec
from src.repositories.game_repository import save_game_detail, save_relay_data
from src.services.crawl_dead_letter_service import enqueue_failure
from src.services.game_detail_runs import (
    GAME_DETAIL_CRAWLER_NAME,
    GAME_DETAIL_TARGET_TYPE,
    GameDetailRunLedger,
    RunCounts,
    TerminalOutcome,
    _season_of,
    game_date_of,
)
from src.services.game_write_contract import GameWriteContract, GameWriteSource
from src.services.pbp_sh_sf_derivation import apply_sh_sf_to_batting_stats
from src.utils.team_codes import normalize_kbo_game_id
from src.validators.game_data_validator import validate_game_data

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from sqlalchemy.orm import Session

    from src.repositories.crawl_execution_repository import CrawlRunSpec

logger = logging.getLogger(__name__)

#: Everything a database write can raise, whatever the driver happens to use.
GAME_SAVE_EXCEPTIONS = (SQLAlchemyError, TimeoutError, OSError, RuntimeError, ValueError, TypeError)
LAST_MONTH_OF_YEAR = 12
ROW_FIELD_COUNT = 2

DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE = {
    "no_detail_payload",
    "incomplete_detail",
    "navigation_error",
    "timeout",
    "exception",
    "missing",
    PARTIAL_DETAIL_REASON,
    "hitter_totals_mismatch",
    "inning_score_mismatch",
}
DETAIL_COLLECTION_FAILURE_REASONS_NON_RETRYABLE = {
    "filtered",
    "save_failed",
    "detail_payload_filtered",
    "detail_save_failed",
    "cancelled",
}


class DetailCrawler(Protocol):
    """DetailCrawler class."""

    async def crawl_games(
        self,
        games: list[dict[str, str]],
        concurrency: int | None = None,
        *,
        lightweight: bool = False,
    ) -> list[dict[str, Any]]:
        """Crawl game details for the given game list.

        Args:
            games: Games.
            concurrency: Maximum number of concurrent requests.
            lightweight: Lightweight.
            games: Games.
            concurrency: Maximum number of concurrent requests.
            lightweight: Lightweight.

        """
        ...

    async def close(self) -> None:
        """Close the crawler."""
        ...


class RelayCrawler(Protocol):
    """RelayCrawler class."""

    async def crawl_game_events(self, game_id: str) -> dict[str, Any] | None:
        """Crawl game events for a given game ID.

        Args:
            game_id: Game ID.
            game_id: Game ID.

        """
        ...

    async def close(self) -> None:
        """Close the crawler."""
        ...


@dataclass(frozen=True)
class GameCollectionTarget:
    """GameCollectionTarget class."""

    game_id: str
    game_date: str

    def as_crawler_input(self) -> dict[str, str]:
        """Handle the as crawler input operation.

        Returns:
            Dictionary result.

        """
        return {"game_id": self.game_id, "game_date": self.game_date}


@dataclass(frozen=True)
class ExistingGameData:
    """ExistingGameData class."""

    has_detail: bool = False
    has_relay: bool = False


@dataclass
class GameCollectionResult:
    """GameCollectionResult class."""

    total_targets: int = 0
    detail_targets: int = 0
    detail_saved: int = 0
    detail_failed: int = 0
    detail_skipped_existing: int = 0
    relay_targets: int = 0
    relay_saved_games: int = 0
    relay_rows_saved: int = 0
    relay_missing: int = 0
    relay_skipped_existing: int = 0
    processed_game_ids: list[str] = field(default_factory=list)
    items: dict[str, GameCollectionItemResult] = field(default_factory=dict)


@dataclass
class GameCollectionConfig:
    """GameCollectionConfig class."""

    relay_crawler: RelayCrawler | None = None
    force: bool = False
    concurrency: int | None = None
    relay_requires_detail: bool = True
    should_save_detail: Callable[[dict[str, Any]], bool] | None = None
    pause_every: int | None = None
    pause_seconds: float = 0.0
    log: Callable[[str], None] = logger.info
    write_contract: GameWriteContract | None = None
    source_stage: str = "detail"
    source_crawler: str | None = None
    source_reason: str = "detail_recovery"
    relay_source_reason: str = "relay_recovery"


@dataclass
class DetailProcessingContext:
    """DetailProcessingContext class."""

    detail_crawler: DetailCrawler
    contract: GameWriteContract
    detail_source: GameWriteSource
    cfg: GameCollectionConfig
    result: GameCollectionResult
    detail_ready: set[str]


@dataclass
class RelayProcessingContext:
    """RelayProcessingContext class."""

    relay_crawler: RelayCrawler
    contract: GameWriteContract
    cfg: GameCollectionConfig
    result: GameCollectionResult


@dataclass
class GameCollectionItemResult:
    """GameCollectionItemResult class."""

    game_id: str
    game_date: str
    detail_status: str = "pending"
    relay_status: str = "not_requested"
    detail_saved: bool = False
    relay_rows_saved: int = 0
    failure_reason: str | None = None


def build_game_id_range(year: int, month: int | None) -> tuple[str, str]:
    """Build game id range.

    Args:
        year: Season year.
        month: Month.
        year: Season year.
        month: Month.
        year: Season year.
        month: Month number (1-12).

    Returns:
        Tuple result.

    """
    if month:
        start = date(year, month, 1)
        end = date(year + 1, 1, 1) if month == LAST_MONTH_OF_YEAR else date(year, month + 1, 1)
    else:
        start = date(year, 1, 1)
        end = date(year + 1, 1, 1)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def load_game_targets_from_db(year: int, month: int | None = None) -> list[GameCollectionTarget]:
    """Load game targets from db.

    Args:
        year: Season year.
        month: Month.
        year: Season year.
        month: Month.
        year: Season year.
        month: Month number (1-12).

    Returns:
        List of results.

    """
    start_id, end_id = build_game_id_range(year, month)

    with SessionLocal() as session:
        rows = (
            session.query(Game.game_id, Game.game_date)
            .filter(Game.game_id >= start_id, Game.game_id < end_id)
            .order_by(Game.game_id.asc())
            .all()
        )
    return [
        GameCollectionTarget(
            game_id=normalize_kbo_game_id(game_id),
            game_date=_format_game_date(game_date, fallback_game_id=game_id),
        )
        for game_id, game_date in rows
    ]


def load_game_targets_by_ids(game_ids: list[str]) -> list[GameCollectionTarget]:
    """game_id 목록으로 GameCollectionTarget 리스트를 조회합니다.

    Args:
        game_ids: Game Ids.
        game_ids: Game Ids.

    """
    with SessionLocal() as session:
        rows = (
            session.query(Game.game_id, Game.game_date)
            .filter(Game.game_id.in_(game_ids))
            .order_by(Game.game_id.asc())
            .all()
        )
    return [
        GameCollectionTarget(
            game_id=normalize_kbo_game_id(game_id),
            game_date=_format_game_date(game_date, fallback_game_id=game_id),
        )
        for game_id, game_date in rows
    ]


def normalize_game_targets(games: Iterable[Any]) -> list[GameCollectionTarget]:
    """Normalize game targets.

    Args:
        games: Games.
        games: Games.
        games: Games.

    Returns:
        List of results.

    """
    targets: list[GameCollectionTarget] = []

    seen: set[str] = set()
    for game in games:
        game_id = _get_value(game, "game_id")
        if not game_id:
            continue
        normalized_id = normalize_kbo_game_id(str(game_id))
        if not normalized_id or normalized_id in seen:
            continue
        game_date = _format_game_date(_get_value(game, "game_date"), fallback_game_id=normalized_id)
        targets.append(GameCollectionTarget(game_id=normalized_id, game_date=game_date))
        seen.add(normalized_id)
    return targets


def inspect_existing_game_data(targets: Iterable[GameCollectionTarget]) -> dict[str, ExistingGameData]:
    """Handle the inspect existing game data operation.

    Args:
        targets: Targets.
        targets: Targets.
        targets: Targets.

    Returns:
        Dictionary result.

    """
    target_list = list(targets)

    game_ids = [target.game_id for target in target_list]
    if not game_ids:
        return {}

    with SessionLocal() as session:
        batting_ids = _ids_with_complete_sides(session, GameBattingStat, game_ids)
        pitching_ids = _ids_with_complete_sides(session, GamePitchingStat, game_ids)
        event_ids = _ids_with_rows(session, GameEvent, game_ids)
        pbp_ids = _ids_with_rows(session, GamePlayByPlay, game_ids)

    relay_ids = event_ids | pbp_ids
    return {
        game_id: ExistingGameData(
            has_detail=game_id in batting_ids and game_id in pitching_ids,
            has_relay=game_id in relay_ids,
        )
        for game_id in game_ids
    }


async def crawl_and_save_game_details(
    games: Iterable[Any],
    *,
    detail_crawler: DetailCrawler,
    config: GameCollectionConfig | None = None,
) -> GameCollectionResult:
    """Crawl and game details.

    Args:
        games: Games.
        detail_crawler: Detail Crawler.
        config: Configuration object.
        games: Games.
        detail_crawler: Detail Crawler.
        config: Configuration object.
        games: Games.

    Returns:
        GameCollectionResult instance.

    """
    targets = normalize_game_targets(games)

    result = GameCollectionResult(total_targets=len(targets))
    result.items = {
        target.game_id: GameCollectionItemResult(game_id=target.game_id, game_date=target.game_date)
        for target in targets
    }
    if not targets:
        return result

    cfg = config or GameCollectionConfig()
    contract = cfg.write_contract or GameWriteContract(run_label="game_collection", log=cfg.log)
    detail_source = GameWriteSource(
        cfg.source_stage,
        cfg.source_crawler or detail_crawler.__class__.__name__,
        cfg.source_reason,
    )
    for target in targets:
        contract.claim_game(target.game_id, detail_source)

    exist_map = inspect_existing_game_data(targets)
    detail_ctx = DetailProcessingContext(
        detail_crawler=detail_crawler,
        contract=contract,
        detail_source=detail_source,
        cfg=cfg,
        result=result,
        detail_ready=set(),
    )
    detail_ready = await _collect_detail_phase(
        targets,
        exist_map,
        detail_ctx,
    )

    if cfg.relay_crawler:
        relay_ctx = RelayProcessingContext(
            relay_crawler=cfg.relay_crawler,
            contract=contract,
            cfg=cfg,
            result=result,
        )
        await _collect_relay_phase(
            targets,
            exist_map,
            detail_ready,
            relay_ctx,
        )

    # Derive SH/SF from PBP events for games where batting stats have them as 0
    _derive_sh_sf_for_results(result, log=cfg.log)

    if cfg.write_contract is None:
        cfg.log(contract.summary())

    return result


async def _collect_detail_phase(
    targets: list[GameCollectionTarget],
    exist_map: dict[str, ExistingGameData],
    ctx: DetailProcessingContext,
) -> set[str]:
    detail_ready: set[str] = {
        target.game_id for target in targets if exist_map.get(target.game_id, ExistingGameData()).has_detail
    }
    detail_targets = [
        target
        for target in targets
        if ctx.cfg.force or not exist_map.get(target.game_id, ExistingGameData()).has_detail
    ]
    ctx.result.detail_targets = len(detail_targets)
    ctx.result.detail_skipped_existing = len(targets) - len(detail_targets)

    _mark_skipped_detail_targets(targets, exist_map, force=ctx.cfg.force, result=ctx.result, log=ctx.cfg.log)

    if not detail_targets:
        return detail_ready

    batch_size = ctx.cfg.pause_every or 20
    total_batches = (len(detail_targets) + batch_size - 1) // batch_size
    for batch_num, b_idx in enumerate(range(0, len(detail_targets), batch_size), start=1):
        batch = detail_targets[b_idx : b_idx + batch_size]
        await _pause_between_detail_batches(b_idx, ctx.cfg.pause_seconds, ctx.detail_crawler, ctx.cfg.log)
        ctx.cfg.log(f"[*] Processing detail batch {batch_num}/{total_batches} ({len(batch)} games)...")

        # The run is opened before the fetch so the crawl time is part of the
        # duration, and closed after the write so a save failure lands on the
        # same run that did the fetching.
        run_ids = _run_ledger().open_runs([target.game_id for target in batch])

        attempts = await _crawl_detail_batch(ctx, batch)

        detail_ctx = DetailProcessingContext(
            detail_crawler=ctx.detail_crawler,
            contract=ctx.contract,
            detail_source=ctx.detail_source,
            cfg=ctx.cfg,
            result=ctx.result,
            detail_ready=detail_ready,
        )
        for index, target in enumerate(batch, start=1):
            global_index = b_idx + index
            _process_detail_target(
                target,
                attempts.get(target.game_id),
                detail_ctx,
                run_id=run_ids.get(target.game_id),
                global_index=global_index,
                total_targets=len(detail_targets),
            )

    return detail_ready


async def _crawl_detail_batch(
    ctx: DetailProcessingContext,
    batch: list[GameCollectionTarget],
) -> dict[str, GameDetailAttempt]:
    """Fetch one batch and return a typed outcome per game.

    The typed path is used when the crawler offers it. A crawler that predates
    `crawl_game_attempts` -- a test double, or a caller holding a different
    implementation of the protocol -- keeps the old untyped path, so this does not
    force the whole `DetailCrawler` contract to change at once.
    """
    inputs = [target.as_crawler_input() for target in batch]
    # Checked on the type, not the instance: a mock auto-creates any attribute,
    # so an instance check would send a test double down a path it cannot run.
    if callable(getattr(type(ctx.detail_crawler), "crawl_game_attempts", None)):
        typed = ctx.detail_crawler.crawl_game_attempts
        return {attempt.game_id: attempt for attempt in await typed(inputs, concurrency=ctx.cfg.concurrency)}

    payloads = await ctx.detail_crawler.crawl_games(inputs, concurrency=ctx.cfg.concurrency)
    synthesized: dict[str, GameDetailAttempt] = {}
    for payload in payloads:
        game_id = payload.get("game_id")
        if not game_id:
            continue
        normalized = normalize_kbo_game_id(str(game_id))
        synthesized[normalized] = attempt_from_result(normalized, payload, lightweight=False)
    for target in batch:
        synthesized.setdefault(
            target.game_id,
            attempt_from_result(target.game_id, None, lightweight=False),
        )
    return synthesized


def _run_ledger() -> GameDetailRunLedger:
    """Return a run ledger. Overridable so tests can observe the transitions."""
    return GameDetailRunLedger()


def _mark_skipped_detail_targets(
    targets: list[GameCollectionTarget],
    exist_map: dict[str, ExistingGameData],
    *,
    force: bool,
    result: GameCollectionResult,
    log: Callable[[str], None],
) -> None:
    if not result.detail_skipped_existing:
        return
    log(f"[SKIP] Detail already exists for {result.detail_skipped_existing} game(s). Use --force to recrawl.")
    for target in targets:
        if exist_map.get(target.game_id, ExistingGameData()).has_detail and not force:
            result.items[target.game_id].detail_status = "skipped_existing"


async def _pause_between_detail_batches(
    batch_start_index: int,
    pause_seconds: float,
    detail_crawler: DetailCrawler,
    log: Callable[[str], None],
) -> None:
    if batch_start_index <= 0:
        return
    if pause_seconds > 0:
        log(f"   [PAUSE] Sleeping for {pause_seconds}s between batches...")
        await asyncio.sleep(pause_seconds)
    await detail_crawler.close()


def _process_detail_target(  # noqa: PLR0913 - public batch-processing signature
    target: GameCollectionTarget,
    attempt: GameDetailAttempt | None,
    ctx: DetailProcessingContext,
    *,
    run_id: str | None,
    global_index: int,
    total_targets: int,
) -> None:
    """Process one game: decide, save, and close its run.

    The run is closed last and on its own session, so a save that rolls back
    cannot erase the record of the attempt that produced the payload.
    """
    ctx.cfg.log(f"[DETAIL] {global_index}/{total_targets} {target.game_id}")
    payload = attempt.payload if attempt is not None else None
    failure_reason = _detail_payload_failure_reason(target, payload, ctx.detail_crawler, ctx.cfg.should_save_detail)
    if failure_reason is not None:
        _mark_detail_failed(target, failure_reason, ctx.result, ctx.cfg.log)
        terminal = _close_failed_run(
            run_id,
            attempt,
            failure_reason=failure_reason[2],
            extra_reason=failure_reason[1] or None,
        )
        _enqueue_for_replay(target, terminal)
        return

    saved, persist_cause = _save_detail_payload(target, payload or {}, ctx)
    if saved:
        ctx.cfg.log("   [DB] Detail saved")
    else:
        ctx.cfg.log("   [ERROR] Detail save failed")
    terminal = _close_saved_run(run_id, attempt, saved=saved, persist_cause=persist_cause, payload=payload or {})
    _enqueue_for_replay(target, terminal)


async def replay_single_game_detail(
    game_id: str,
    spec: CrawlRunSpec,
    *,
    detail_crawler: DetailCrawler,
    config: GameCollectionConfig,
) -> TerminalOutcome | None:
    """Re-crawl one game on behalf of a dead letter.

    This is the full detail path, deliberately: a letter is queued when what was
    stored is incomplete, so replaying the lightweight path would fetch the same
    reduced page and land back on the same partial.

    It also never queues. A retry that produced a new letter would give the
    incident a second row under a second run, so the queue would grow faster than
    it drains and the original letter would stop being the thing that tracks the
    problem. The existing letter is the only record; this call just fills it in.

    Args:
        game_id: The game to re-crawl.
        spec: The run identity the dispatcher allocated, carrying the link back
            to the run that failed.
        detail_crawler: The crawler to fetch with.
        config: The detail collection settings.

    Returns:
        The recorded outcome, or None when the run was not recorded.

    """
    return await _collect_single_game_detail(
        game_id,
        spec,
        detail_crawler=detail_crawler,
        config=config,
        record_dead_letters=False,
    )


async def _collect_single_game_detail(
    game_id: str,
    spec: CrawlRunSpec,
    *,
    detail_crawler: DetailCrawler,
    config: GameCollectionConfig,
    record_dead_letters: bool,
) -> TerminalOutcome | None:
    """Fetch, write and close one game under a caller-supplied run identity."""
    target = GameCollectionTarget(game_id=game_id, game_date=game_date_of(game_id))
    result = GameCollectionResult()
    ctx = DetailProcessingContext(
        detail_crawler=detail_crawler,
        contract=GameWriteContract(),
        detail_source=GameWriteSource(),
        cfg=config,
        result=result,
        detail_ready=set(),
    )
    run_id = _run_ledger().open_run(spec)
    attempts = await _crawl_detail_batch(ctx, [target])
    terminal = _process_single_detail_target(target, attempts.get(game_id), ctx, run_id=run_id)
    if record_dead_letters:
        _enqueue_for_replay(target, terminal)
    return terminal


def _process_single_detail_target(
    target: GameCollectionTarget,
    attempt: GameDetailAttempt | None,
    ctx: DetailProcessingContext,
    *,
    run_id: str | None,
) -> TerminalOutcome | None:
    """Close one game's run, deciding and writing before recording it."""
    payload = attempt.payload if attempt is not None else None
    failure_reason = _detail_payload_failure_reason(
        target,
        payload,
        ctx.detail_crawler,
        ctx.cfg.should_save_detail,
    )
    if failure_reason is not None:
        _mark_detail_failed(target, failure_reason, ctx.result, ctx.cfg.log)
        return _close_failed_run(
            run_id,
            attempt,
            failure_reason=failure_reason[2],
            extra_reason=failure_reason[1] or None,
        )
    saved, persist_cause = _save_detail_payload(target, payload or {}, ctx)
    return _close_saved_run(
        run_id,
        attempt,
        saved=saved,
        persist_cause=persist_cause,
        payload=payload or {},
    )


def _close_saved_run(
    run_id: str | None,
    attempt: GameDetailAttempt | None,
    *,
    saved: bool,
    persist_cause: tuple[str, str] | None,
    payload: dict[str, Any],
) -> TerminalOutcome | None:
    """Close a run for a game that reached the write step, then report it.

    A game that was stored but is not a complete box score is a replayable
    outcome, not a failure and not a finished success. Both facts hold at once
    and they mean different things: `written` says usable data was stored, while
    `pending` in the queue says the completeness contract is still unmet. A
    degraded result that already wrote a row does not need fetching again for the
    data's sake -- it needs fetching again to become complete.

    Returns:
        The recorded outcome, or None when the ledger did not accept the
        transition. A run nobody could record is not a finished run.

    """
    if not run_id:
        return None
    if not saved:
        # A `False` with no exception is the quality gate declining the payload,
        # not a broken database. Calling it PERSIST_CONNECTION would blame the
        # infrastructure for a data decision.
        return _record_failure(
            run_id,
            *_save_failure_cause(persist_cause),
            counts=RunCounts(read=1, written=0, failed=1),
        )
    if _has_full_detail_rows(payload):
        if _run_ledger().record_success(run_id, counts=RunCounts(read=1, written=1)):
            return TerminalOutcome(status="success", counts=RunCounts(read=1, written=1), run_id=run_id)
        return None
    code = attempt.error_code if attempt is not None and attempt.error_code else FailureCode.VALIDATION_QUALITY.value
    return _record_failure(
        run_id,
        code,
        PARTIAL_DETAIL_REASON,
        counts=RunCounts(read=1, written=1),
        status="partial",
    )


def _record_failure(
    run_id: str,
    code: str,
    message: str,
    *,
    counts: RunCounts,
    status: str = "failed",
) -> TerminalOutcome | None:
    """Record a terminal, error-carrying outcome if the ledger accepts it.

    `status` decides the transition, never the presence of an error code: a
    partial result carries a code because it still needs replay, and reading that
    code as a failure is the mistake this function exists to prevent.

    Returns:
        The recorded outcome, or None when the transition did not commit.

    """
    ledger = _run_ledger()
    if status == "partial":
        recorded = ledger.record_partial(
            run_id,
            error_code=code,
            error_message=message,
            counts=counts,
        )
    else:
        recorded = ledger.record_failed(
            run_id,
            error_code=code,
            error_message=message,
            counts=counts,
        )
    if not recorded:
        return None
    return TerminalOutcome(
        status=status,
        error_code=code,
        error_message=message,
        counts=counts,
        run_id=run_id,
    )


def _enqueue_for_replay(
    target: GameCollectionTarget,
    terminal: TerminalOutcome | None,
) -> None:
    """Queue a game whose outcome still needs work.

    A success is never queued: a lightweight result is a degraded success by
    design, and re-fetching it would fetch the same thing again.

    `failure_stage` is always derived from the code. Inferring the stage from the
    status instead would let a record say one thing in its code and another in
    its stage.
    """
    # A terminal of None means the ledger never accepted the transition. Queueing
    # then would point a letter at a run that does not exist, and its status is
    # unknown, so there is nothing truthful to record. The unrecorded run is left
    # to run-bookkeeping monitoring.
    if terminal is None or terminal.status == "success" or not terminal.run_id:
        return
    code = terminal.error_code or FailureCode.UNKNOWN.value
    try:
        enqueue_failure(
            DeadLetterSpec(
                original_run_id=terminal.run_id,
                crawler=GAME_DETAIL_CRAWLER_NAME,
                target_type=GAME_DETAIL_TARGET_TYPE,
                target_id=target.game_id,
                game_id=target.game_id,
                season=_season_of(target.game_id),
                failure_stage=stage_for_code(code).value,
                error_code=code,
                error_message=terminal.error_message,
            ),
        )
    except Exception:
        logger.exception("Failed to enqueue dead letter for %s", target.game_id)


def _save_failure_cause(persist_cause: tuple[str, str] | None) -> tuple[str, str]:
    """Return the code and message for a write that did not succeed.

    A raised error means the database did it, and the save already classified it
    where the exception was in hand. A plain `False` means the quality gate
    declined the payload, which is a data decision and must not be reported as a
    broken connection.
    """
    if persist_cause is not None:
        return persist_cause
    return (
        FailureCode.VALIDATION_QUALITY.value,
        "detail save rejected by quality gate",
    )


def _close_failed_run(
    run_id: str | None,
    attempt: GameDetailAttempt | None,
    *,
    failure_reason: str,
    extra_reason: str | None,
) -> TerminalOutcome | None:
    """Close a run for a game that never reached the write step."""
    if not run_id:
        return None
    code = attempt.error_code if attempt is not None and attempt.error_code else None
    if code is None:
        code = error_code_for_reason(failure_reason)
    return _record_failure(run_id, code, extra_reason or failure_reason, counts=RunCounts())


def _detail_payload_failure_reason(
    target: GameCollectionTarget,
    payload: dict[str, Any] | None,
    detail_crawler: DetailCrawler,
    should_save_detail: Callable[[dict[str, Any]], bool] | None,
) -> tuple[str, str | None, str] | None:
    if not payload:
        return "crawl_failed", _get_failure_reason(detail_crawler, target.game_id), "no_detail_payload"
    if not _has_required_detail_rows(payload):
        return "filtered", _get_failure_reason(detail_crawler, target.game_id), "incomplete_detail"
    if "game_id" in payload or "teams" in payload:
        is_valid, errors, _ = validate_game_data(payload, allow_partial=not _has_full_detail_rows(payload))
        if not is_valid:
            return "filtered", errors[0] if errors else "detail_payload_invalid", "invalid_detail_payload"
    if should_save_detail and not should_save_detail(payload):
        return "filtered", "detail_payload_filtered", "filtered"
    return None


def _mark_detail_failed(
    target: GameCollectionTarget,
    failure: tuple[str, str | None, str],
    result: GameCollectionResult,
    log: Callable[[str], None],
) -> None:
    status, reason, default = failure
    result.detail_failed += 1
    item = result.items[target.game_id]
    item.detail_status = status
    item.failure_reason = _normalize_detail_failure_reason(reason, default=default)
    if default == "no_detail_payload":
        log("   [WARN] No detail payload returned")
    elif default == "incomplete_detail":
        log("   [WARN] Detail payload is missing required hitter/pitcher rows")
    else:
        log("   [WARN] Detail payload did not pass save predicate")


def _save_detail_payload(
    target: GameCollectionTarget,
    payload: dict[str, Any],
    ctx: DetailProcessingContext,
) -> tuple[bool, tuple[str, str] | None]:
    """Write one game's detail and report what happened.

    The write runs on a session this function owns, so a database error escapes
    to be classified rather than being flattened into `False`. That distinction
    is the whole point: `False` means the quality gate declined the payload, and
    a raised error means the database did, and the two need different codes.

    Returns:
        A tuple of (saved, persistence cause). The cause is None whenever the call
        returned without raising, and is already classified here because that is
        the only point where the real exception is in hand -- re-deriving it from
        the message later would invent a timeout for every error.

    """
    full_detail = _has_full_detail_rows(payload)
    with SessionLocal() as session:
        try:
            saved = save_game_detail(
                payload,
                allow_partial=not full_detail,
                write_contract=ctx.contract,
                source_stage=ctx.detail_source.stage,
                source_crawler=ctx.detail_source.crawler,
                source_reason=ctx.detail_source.reason,
                session=session,
            )
        # Any database error is classified here rather than inside the save, so a
        # PERSIST_TIMEOUT is neither lost nor invented from the message text. The
        # driver raises many unrelated types depending on the backend, so the whole
        # persistence surface is caught and classified rather than guessed at.
        except GAME_SAVE_EXCEPTIONS as exc:
            session.rollback()
            _stage, code = classify_persist_failure(exc)
            return False, (code.value, f"{type(exc).__name__}: {exc}")
        if saved:
            session.commit()
        else:
            session.rollback()

    if not saved:
        ctx.result.detail_failed += 1
        item = ctx.result.items[target.game_id]
        item.detail_status = "save_failed"
        item.failure_reason = _normalize_detail_failure_reason("detail_save_failed", default="save_failed")
        return False, None
    item = ctx.result.items[target.game_id]
    item.detail_status = "saved" if full_detail else "partial"
    item.detail_saved = full_detail
    if full_detail:
        ctx.result.detail_saved += 1
        ctx.result.processed_game_ids.append(target.game_id)
        ctx.detail_ready.add(target.game_id)
    else:
        item.failure_reason = PARTIAL_DETAIL_REASON
    return True, None


async def _collect_relay_phase(
    targets: list[GameCollectionTarget],
    exist_map: dict[str, ExistingGameData],
    detail_ready: set[str],
    ctx: RelayProcessingContext,
) -> None:
    relay_source = GameWriteSource("relay", ctx.relay_crawler.__class__.__name__, ctx.cfg.relay_source_reason)
    relay_targets = [
        target
        for target in targets
        if (ctx.cfg.force or not exist_map.get(target.game_id, ExistingGameData()).has_relay)
        and (not ctx.cfg.relay_requires_detail or target.game_id in detail_ready)
    ]
    ctx.result.relay_targets = len(relay_targets)
    ctx.result.relay_skipped_existing = len(targets) - len(relay_targets)
    if ctx.result.relay_skipped_existing:
        ctx.cfg.log(
            f"[SKIP] Relay already exists for {ctx.result.relay_skipped_existing} game(s). Use --force to recrawl.",
        )
        for target in targets:
            item = ctx.result.items[target.game_id]
            has_relay = exist_map.get(target.game_id, ExistingGameData()).has_relay
            if has_relay and not ctx.cfg.force:
                item.relay_status = "skipped_existing"
            elif ctx.cfg.relay_requires_detail and target.game_id not in detail_ready:
                item.relay_status = "skipped_no_detail"

    for index, target in enumerate(relay_targets, start=1):
        ctx.contract.claim_game(target.game_id, relay_source)
        ctx.cfg.log(f"[RELAY] {index}/{len(relay_targets)} {target.game_id}")
        relay_data = await ctx.relay_crawler.crawl_game_events(target.game_id)
        item = ctx.result.items[target.game_id]
        flat_events = list((relay_data or {}).get("events") or [])
        raw_pbp_rows = list((relay_data or {}).get("raw_pbp_rows") or [])
        if flat_events or raw_pbp_rows:
            saved_rows = save_relay_data(
                target.game_id,
                flat_events,
                raw_pbp_rows=raw_pbp_rows,
                write_contract=ctx.contract,
                source_stage=relay_source.stage,
                source_crawler=relay_source.crawler,
                source_reason=relay_source.reason,
                parser_version=(relay_data or {}).get("parser_version"),
                source_schema_version=(relay_data or {}).get("source_schema_version"),
                payload_hash=(relay_data or {}).get("payload_hash"),
                source_payload=(relay_data or {}).get("source_payload"),
            )
            ctx.result.relay_rows_saved += saved_rows
            item.relay_rows_saved = saved_rows
            if saved_rows:
                ctx.result.relay_saved_games += 1
                item.relay_status = "saved"
                if target.game_id not in ctx.result.processed_game_ids:
                    ctx.result.processed_game_ids.append(target.game_id)
                ctx.cfg.log(f"   [DB] Relay saved ({saved_rows} rows)")
            else:
                ctx.result.relay_missing += 1
                item.relay_status = "save_failed"
                item.failure_reason = item.failure_reason or "relay_save_returned_zero"
                ctx.cfg.log("   [WARN] Relay save returned 0 rows")
        else:
            ctx.result.relay_missing += 1
            item.relay_status = "missing"
            item.failure_reason = (
                item.failure_reason or _get_failure_reason(ctx.relay_crawler, target.game_id) or "no_relay_payload"
            )
            ctx.cfg.log("   [INFO] No relay data available")
        await _maybe_pause(index, ctx.cfg.pause_every, ctx.cfg.pause_seconds, ctx.cfg.log)


def _get_value(obj: object, key: str) -> object | None:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _format_game_date(value: object, *, fallback_game_id: str) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").replace("-", "").strip()
    if len(text) == DATE_STR_LEN and text.isdigit():
        return text
    return str(fallback_game_id)[:8]


def _ids_with_rows(session: Session, model: type[Any], game_ids: list[str]) -> set[str]:
    return {row[0] for row in session.query(model.game_id).filter(model.game_id.in_(game_ids)).distinct().all()}


def _ids_with_complete_sides(session: Session, model: type[Any], game_ids: list[str]) -> set[str]:
    """Return game IDs with both away and home rows for a child dataset."""
    rows = (
        session.query(model.game_id, model.team_side)
        .filter(model.game_id.in_(game_ids), model.team_side.in_(("away", "home")))
        .distinct()
        .all()
    )
    sides_by_game: dict[str, set[str]] = {}
    for row in rows:
        if len(row) < ROW_FIELD_COUNT:
            continue
        sides_by_game.setdefault(str(row[0]), set()).add(str(row[1]))
    return {game_id for game_id, sides in sides_by_game.items() if sides == {"away", "home"}}


def _get_failure_reason(crawler: object, game_id: str) -> str | None:
    getter = getattr(crawler, "get_last_failure_reason", None)
    if not callable(getter):
        return None
    try:
        result = getter(game_id)
        return cast("str | None", result)
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.warning("Failed to get last failure reason from crawler: %s", exc)
        return None


def _normalize_detail_failure_reason(raw_reason: str | None, *, default: str) -> str:
    reason = (raw_reason or "").strip().lower()
    if not reason:
        return default
    if reason in DETAIL_COLLECTION_FAILURE_REASONS_NON_RETRYABLE:
        normalized = {
            "filtered": "filtered",
            "detail_payload_filtered": "filtered",
            "save_failed": "save_failed",
            "detail_save_failed": "save_failed",
        }
        return normalized.get(reason, reason)
    if reason in DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE:
        return reason
    return default


def _has_required_detail_rows(payload: dict[str, Any]) -> bool:
    return _has_full_detail_rows(payload) or _has_partial_detail_anchor(payload)


def _has_full_detail_rows(payload: dict[str, Any]) -> bool:
    hitters = payload.get("hitters") or {}
    pitchers = payload.get("pitchers") or {}
    return (
        bool(hitters.get("away"))
        and bool(hitters.get("home"))
        and bool(pitchers.get("away"))
        and bool(pitchers.get("home"))
    )


def _has_partial_detail_anchor(payload: dict[str, Any]) -> bool:

    # Partial recovery check: must have at least team codes and SOME score or metadata info
    teams = payload.get("teams") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    metadata = payload.get("metadata") or {}

    has_teams = bool(away.get("code")) and bool(home.get("code"))
    has_scores = (
        bool(away.get("line_score"))
        or bool(home.get("line_score"))
        or away.get("score") is not None
        or home.get("score") is not None
    )
    has_metadata = bool(metadata.get("stadium")) or bool(metadata.get("attendance"))

    return has_teams and (has_scores or has_metadata)


async def _maybe_pause(
    index: int,
    pause_every: int | None,
    pause_seconds: float,
    log: Callable[[str], None],
) -> None:
    if not pause_every or pause_every <= 0 or pause_seconds <= 0:
        return
    if index % pause_every == 0:
        log(f"[PAUSE] Sleeping for {pause_seconds:g}s before continuing...")
        await asyncio.sleep(pause_seconds)


def _derive_sh_sf_for_results(result: GameCollectionResult, log: Callable[[str], None]) -> None:
    """Derive sacrifice_hits/sacrifice_flies from PBP events for collected games.

    Args:
        result: Result.
        log: Logger instance.
        result: Result.
        log: Logger instance.

    """
    updated_total = 0

    game_ids = [gid for gid, item in result.items.items() if item.detail_saved]
    if not game_ids:
        return
    with SessionLocal() as session:
        for game_id in game_ids:
            try:
                updated = apply_sh_sf_to_batting_stats(session, game_id)
                if updated:
                    updated_total += updated
            except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
                logger.exception("SH/SF derivation failed for %s", game_id)
        if updated_total:
            session.commit()
            log(f"[SH/SF] Derived {updated_total} SH/SF values from PBP events.")
        else:
            log("[SH/SF] No SH/SF values needed derivation.")
