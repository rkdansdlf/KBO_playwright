"""Best-effort L3 query-event recording for the RAG answer endpoint.

The recorder owns its **own short transaction**, exactly like
:class:`src.notifications.recorder.DeliveryRecorder`:

* the observation is real once retrieval has happened, so it survives a later
  failure in answer construction and a rollback of the request's session;
* a failed audit write must never turn a successful answer into an error, so
  persistence errors are contained, counted and logged without query text.

Nothing in this module stores the raw question. A whitespace-normalised sha256
and a character count are enough to count distinct questions and to spot a
repeating caller. Classifications are about *what was served*, never about what
the user meant.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError

from src.models.rag_query_event import (
    RAG_QUERY_OUTCOME_DOCUMENT_PROSE,
    RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS,
    RAG_QUERY_OUTCOME_EMPTY,
    RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED,
    RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK,
    RAG_QUERY_OUTCOME_UNKNOWN,
    RagQueryEvent,
)
from src.services.query_router import STRUCTURED_SOURCE_TABLES
from src.utils.metrics import record_rag_query_log_failure

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger("src.services.rag_query_log")

#: Persistence errors are swallowed so the API answer is never contaminated.
AUDIT_EXCEPTIONS = (
    SQLAlchemyError,
    OSError,
    RuntimeError,
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    ImportError,
)

_ENABLE_ENV = "RAG_QUERY_LOG_ENABLED"
_SAMPLE_ENV = "RAG_QUERY_LOG_SAMPLE_RATE"
_RETENTION_ENV = "RAG_QUERY_LOG_RETENTION_DAYS"
DEFAULT_RETENTION_DAYS = 90
_SAMPLE_STEPS = 10_000


class SessionFactory(Protocol):
    """Callable returning a fresh, independent database session."""

    def __call__(self) -> Session:
        """Open a fresh session owned by the caller."""
        ...


@dataclass(frozen=True)
class QueryEventDraft:
    """The classification derived from a routing decision and served chunks."""

    route: str
    intent: str | None
    outcome: str
    source_table: str | None
    result_count: int
    structured_candidates: int
    fallback_used: bool


def default_session_factory() -> Session:
    """Return a new application session without importing the engine eagerly."""
    from src.db.engine import SessionLocal

    return SessionLocal()


def hash_query(query: str) -> str:
    """Hash the whitespace-normalised question; the raw text is never retained."""
    normalised = " ".join(query.split())
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


def _source_table_of(result: object) -> str | None:
    """Read the source table from a served chunk without copying a key convention.

    Structured results carry it in ``meta``. Dense/BM25 results carry it as the
    prefix of the ``source_table:source_row_id`` key that the retriever itself
    builds. A chunk with neither is genuinely unattributable, and is reported as
    ``unknown`` rather than guessed.
    """
    meta = getattr(result, "meta", None)
    if isinstance(meta, Mapping):
        declared = meta.get("source_table")
        if isinstance(declared, str) and declared:
            return declared
    chunk_id = getattr(result, "chunk_id", None)
    if isinstance(chunk_id, str) and ":" in chunk_id:
        prefix = chunk_id.split(":", 1)[0]
        if prefix:
            return prefix
    return None


def _dominant_source_table(results: Sequence[object]) -> str | None:
    """Return the most frequent source table, or ``None`` on a tie or no evidence."""
    tables = [table for result in results if (table := _source_table_of(result)) is not None]
    if not tables:
        return None
    ranked = Counter(tables).most_common(2)
    if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
        return None
    return ranked[0][0]


def _is_structured(result: object) -> bool:
    """Whether a structured row was actually served, not merely attempted."""
    meta = getattr(result, "meta", None)
    if isinstance(meta, Mapping) and meta.get("structured") is True:
        return True
    provenance = getattr(result, "provenance", None)
    return isinstance(provenance, Mapping) and provenance.get("type") == "structured_db"


def classify_query_event(
    *,
    route: str,
    intent: str | None,
    results: Sequence[object],
    structured_candidates: int,
    fallback_used: bool,
) -> QueryEventDraft:
    """Classify one request by route and by what was actually served.

    The order matters: an empty answer is not prose, a structured fallback is
    not a structured answer, and an unattributable table list is ``unknown`` —
    never quietly folded into the prose bucket.
    """
    count = len(results)
    if count == 0:
        outcome = RAG_QUERY_OUTCOME_EMPTY
        source_table = None
    elif structured_candidates > 0 and not fallback_used and any(_is_structured(r) for r in results):
        outcome = RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED
        source_table = _dominant_source_table(results)
    elif fallback_used:
        outcome = RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK
        source_table = _dominant_source_table(results)
    else:
        source_table = _dominant_source_table(results)
        if source_table is None:
            outcome = RAG_QUERY_OUTCOME_UNKNOWN
        elif source_table in STRUCTURED_SOURCE_TABLES:
            outcome = RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS
        else:
            outcome = RAG_QUERY_OUTCOME_DOCUMENT_PROSE

    return QueryEventDraft(
        route=route,
        intent=intent,
        outcome=outcome,
        source_table=source_table,
        result_count=count,
        structured_candidates=structured_candidates,
        fallback_used=fallback_used,
    )


def _enabled() -> bool:
    """Read the enable flag at call time so a running process can be muted."""
    return os.getenv(_ENABLE_ENV, "1").strip().lower() not in {"0", "false", "no", "off"}


def _sample_rate() -> float:
    """Read and clamp the sample rate at call time."""
    try:
        rate = float(os.getenv(_SAMPLE_ENV, "1"))
    except ValueError:
        return 1.0
    return min(1.0, max(0.0, rate))


class RagQueryEventRecorder:
    """Persist query-event rows in an independent transaction."""

    def __init__(
        self,
        session_factory: SessionFactory | None = None,
        *,
        enabled: bool | None = None,
        sample_rate: float | None = None,
    ) -> None:
        """Initialize the recorder; ``None`` options are resolved from the environment."""
        self._session_factory = session_factory or default_session_factory
        self._enabled = enabled
        self._sample_rate = sample_rate

    def _should_record(self) -> bool:
        if self._enabled is False or not _enabled():
            return False
        rate = self._sample_rate if self._sample_rate is not None else _sample_rate()
        rate = min(1.0, max(0.0, rate))
        if rate <= 0.0:
            return False
        return rate >= 1.0 or secrets.randbelow(_SAMPLE_STEPS) < int(rate * _SAMPLE_STEPS)

    def record(
        self,
        query: str,
        draft: QueryEventDraft,
        *,
        latency_ms: float | None = None,
        recorded_at: datetime | None = None,
    ) -> bool:
        """Write one row, returning whether it was persisted. Never raises."""
        if not self._should_record():
            return False
        # Naive UTC, matching the rest of the operational ledger.
        moment = recorded_at or datetime.now(UTC).replace(tzinfo=None)
        try:
            with self._session_factory() as session:
                session.add(
                    RagQueryEvent(
                        created_at=moment,
                        query_hash=hash_query(query),
                        query_chars=len(query),
                        route=draft.route,
                        intent=draft.intent,
                        outcome=draft.outcome,
                        source_table=draft.source_table,
                        result_count=draft.result_count,
                        structured_candidates=draft.structured_candidates,
                        fallback_used=draft.fallback_used,
                        latency_ms=latency_ms,
                    ),
                )
                session.commit()
        except AUDIT_EXCEPTIONS:
            record_rag_query_log_failure()
            logger.exception("RAG query-event audit write failed; the answer is preserved")
            return False
        return True


_default_recorder = RagQueryEventRecorder()


def record_query_request(
    query: str,
    *,
    plan: object,
    results: Sequence[object],
    retrieval: Mapping[str, object],
    recorder: RagQueryEventRecorder | None = None,
) -> bool:
    """Classify and log one ``rag_ask`` call; failures never escape."""
    try:
        route = str(getattr(getattr(plan, "route", None), "value", "DOCUMENT"))
        intent_obj = getattr(plan, "intent", None)
        intent = getattr(intent_obj, "value", None) if intent_obj is not None else None
        latency = retrieval.get("latency_ms")
        if isinstance(latency, Mapping):
            latency = latency.get("total")
        draft = classify_query_event(
            route=route,
            intent=intent if isinstance(intent, str) else None,
            results=results,
            structured_candidates=int(retrieval.get("structured_candidates", 0) or 0),
            fallback_used=bool(retrieval.get("fallback_used", False)),
        )
        return (recorder or _default_recorder).record(
            query,
            draft,
            latency_ms=float(latency) if isinstance(latency, (int, float)) else None,
        )
    except AUDIT_EXCEPTIONS:
        record_rag_query_log_failure()
        logger.exception("RAG query-event classification failed; the answer is preserved")
        return False


def prune_rag_query_events(session: Session, *, before: datetime) -> int:
    """Delete query events older than ``before``; caller owns the commit."""
    return int(session.execute(delete(RagQueryEvent).where(RagQueryEvent.created_at < before)).rowcount or 0)
