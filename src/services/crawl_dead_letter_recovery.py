"""Deterministic recovery for dead letters stuck in the ``retrying`` state.

A retry spans two transactions around a non-transactional replay, so a crash can
leave a letter in ``retrying`` with a pre-linked ``replay_run_id``. This module
reconciles those letters against the linked execution run and converges them to
``resolved`` / ``pending`` / ``exhausted`` using the same state machine and retry
policy as the normal retry path.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from src.crawlers.failure_taxonomy import FailureCode, FailureStage
from src.db.engine import SessionLocal
from src.models.crawl_dead_letter import DlqStatus
from src.models.crawl_execution import RUN_STATUS_SUCCESS
from src.repositories.crawl_dead_letter_repository import CrawlDeadLetterRepository
from src.repositories.crawl_execution_repository import CrawlExecutionRepository
from src.services.crawl_dead_letter_service import CrawlDeadLetterService

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_STALE_RETRYING_SECONDS = 1800

ACTION_RESOLVED = "resolved"
ACTION_RESCHEDULED = "rescheduled"
ACTION_EXHAUSTED = "exhausted"
ACTION_ORPHAN_RESCHEDULED = "orphan_rescheduled"
ACTION_INVARIANT_MISSING = "invariant_missing"
ACTION_SKIPPED_ACTIVE = "skipped_active"
ACTION_FAILED = "failed"


@dataclass(frozen=True)
class RecoveryResult:
    """Outcome of reconciling one stuck dead letter."""

    dlq_id: str
    replay_run_id: str | None
    before_status: str
    after_status: str
    action: str
    reason: str


@dataclass(frozen=True)
class _Outcome:
    """Local replay-outcome shape satisfying ``ReplayOutcomeLike``."""

    success: bool
    replay_run_id: str
    status: str
    error_message: str | None = None
    error_code: str | None = None
    failure_stage: str | None = None


@dataclass(frozen=True)
class _RunSnapshot:
    """Plain view of a linked execution run, safe to use after session close."""

    status: str
    error_code: str | None
    error_message: str | None


def _utcnow() -> datetime:
    """Return a naive UTC timestamp matching the rest of the schema."""
    return datetime.now(UTC).replace(tzinfo=None)


def _env_seconds(name: str, default: int) -> int:
    """Read a positive integer env override, falling back to the default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _missing_outcome(replay_run_id: str) -> _Outcome:
    return _Outcome(
        success=False,
        replay_run_id=replay_run_id,
        status="missing",
        error_message="replay run not found",
        error_code=FailureCode.REPLAY_RUN_MISSING.value,
        failure_stage=FailureStage.ORCHESTRATE.value,
    )


def _action_for(after_status: str, preferred: str | None) -> str:
    if after_status == DlqStatus.RESOLVED.value:
        return ACTION_RESOLVED
    if after_status == DlqStatus.EXHAUSTED.value:
        return ACTION_EXHAUSTED
    if after_status == DlqStatus.PENDING.value:
        return preferred or ACTION_RESCHEDULED
    return ACTION_RESCHEDULED


def _build_outcome(replay_run_id: str | None, snapshot: _RunSnapshot | None) -> tuple[_Outcome, str | None, str]:
    """Map the linked run state to a replay outcome and preferred action."""
    if replay_run_id is None:
        return _missing_outcome(""), ACTION_INVARIANT_MISSING, "replay_run_id is NULL"
    if snapshot is None:
        return _missing_outcome(replay_run_id), ACTION_ORPHAN_RESCHEDULED, "replay run not found"
    if snapshot.status == RUN_STATUS_SUCCESS:
        return _Outcome(success=True, replay_run_id=replay_run_id, status=RUN_STATUS_SUCCESS), None, "replay succeeded"
    return (
        _Outcome(
            success=False,
            replay_run_id=replay_run_id,
            status=snapshot.status,
            error_message=snapshot.error_message,
            error_code=snapshot.error_code,
            failure_stage=FailureStage.ORCHESTRATE.value,
        ),
        None,
        f"replay {snapshot.status}",
    )


def recover_stuck_retrying(
    *,
    stale_before: datetime | None = None,
    limit: int = 100,
    session_factory: Callable[[], Session] | None = None,
) -> list[RecoveryResult]:
    """Reconcile dead letters stuck in ``retrying`` since ``stale_before``.

    Each letter is handled in its own transaction so a single failure cannot
    abort the batch. Active (still running) replays are left untouched.
    """
    factory: Callable[[], Session] = session_factory or SessionLocal
    cutoff = stale_before or _utcnow() - timedelta(
        seconds=_env_seconds("DLQ_STALE_RETRYING_SECONDS", DEFAULT_STALE_RETRYING_SECONDS),
    )

    with factory() as session:
        letters = CrawlDeadLetterRepository(session).get_stale_retrying(stale_before=cutoff, limit=limit)
        candidates = [(letter.dlq_id, letter.replay_run_id) for letter in letters]
        run_ids = {run_id for _, run_id in candidates if run_id}
        runs = CrawlExecutionRepository(session).get_by_run_ids(run_ids)
        snapshots = {
            run_id: _RunSnapshot(status=run.status, error_code=run.error_code, error_message=run.error_message)
            for run_id, run in runs.items()
        }

    results: list[RecoveryResult] = []
    for dlq_id, replay_run_id in candidates:
        snapshot = snapshots.get(replay_run_id) if replay_run_id else None
        results.append(_recover_one(factory, dlq_id, replay_run_id, snapshot))
    return results


def _recover_one(
    factory: Callable[[], Session],
    dlq_id: str,
    replay_run_id: str | None,
    snapshot: _RunSnapshot | None,
) -> RecoveryResult:
    try:
        with factory() as session:
            service = CrawlDeadLetterService(session)
            letter = service.repository.get_by_dlq_id(dlq_id)
            if letter is None:
                return RecoveryResult(
                    dlq_id, replay_run_id, "missing", "missing", ACTION_INVARIANT_MISSING, "letter disappeared"
                )
            before = letter.status

            if snapshot is not None and snapshot.status == "running":
                return RecoveryResult(
                    dlq_id, replay_run_id, before, before, ACTION_SKIPPED_ACTIVE, "replay still running"
                )

            outcome, preferred, reason = _build_outcome(replay_run_id, snapshot)
            result = service.finalize_retry(dlq_id, outcome)
            session.commit()
            after = result.status.value
            return RecoveryResult(dlq_id, replay_run_id, before, after, _action_for(after, preferred), reason)
    except Exception:
        logger.exception("Failed to recover stuck dead letter dlq_id=%s", dlq_id)
        return RecoveryResult(dlq_id, replay_run_id, "unknown", "unknown", ACTION_FAILED, "recovery error")
