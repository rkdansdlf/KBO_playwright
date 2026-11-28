"""Read-only L3 traffic report: what real ``rag_ask`` calls actually did.

This answers a different question from ``kbo rag necessity``. That tool asks
"could this curated case have been a SQL lookup?" -- the answer is about 30
golden cases. This tool asks "what did the queries sent to the API look like?"
-- the answer depends on traffic that has accumulated since deployment.

The two share verdict bands (imported, not copied) but not the measurement
basis. The report says so explicitly, because a reader who merges them would
read a curated 30-case ratio as if it were real traffic.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import distinct, func, select
from sqlalchemy.exc import SQLAlchemyError

from src.cli.rag.audit_retrieval_necessity import NecessityVerdict, decide_verdict
from src.db.engine import get_db_session
from src.models.rag_query_event import (
    RAG_QUERY_OUTCOME_DOCUMENT_PROSE,
    RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS,
    RAG_QUERY_OUTCOME_EMPTY,
    RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED,
    RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK,
    RAG_QUERY_OUTCOME_UNKNOWN,
    RagQueryEvent,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

DEFAULT_DAYS = 30
DEFAULT_MIN_EVENTS = 100


@dataclass(frozen=True)
class TrafficReport:
    """Aggregated query-window counts and the derived verdict."""

    days: int
    since: datetime
    total: int
    distinct_queries: int
    counts: dict[str, int]
    min_events: int
    verdict: NecessityVerdict
    undetermined_reason: str | None = None

    @property
    def unknown(self) -> int:
        """Unclassifiable rows -- reported separately, never folded into prose."""
        return self.counts.get(RAG_QUERY_OUTCOME_UNKNOWN, 0)

    @property
    def classified(self) -> int:
        """Rows whose served chunks had an attributable source."""
        return self.total - self.unknown

    def shares(self) -> dict[str, float]:
        """Ratios over explicit denominators; no denominator is silently merged."""
        if self.total == 0:
            return {}
        prose = self.counts.get(RAG_QUERY_OUTCOME_DOCUMENT_PROSE, 0)
        structured = self.counts.get(RAG_QUERY_OUTCOME_STRUCTURED_ANSWERED, 0)
        table_rows = self.counts.get(RAG_QUERY_OUTCOME_DOCUMENT_TABLE_ROWS, 0)

        def ratio(numerator: int, denominator: int) -> float:
            return round(numerator / denominator, 4) if denominator else 0.0

        result: dict[str, float] = {
            "unknown_share": ratio(self.unknown, self.total),
            "empty_share": ratio(self.counts.get(RAG_QUERY_OUTCOME_EMPTY, 0), self.total),
            "structured_answered_share": ratio(structured, self.total),
            "structured_fallback_share": ratio(self.counts.get(RAG_QUERY_OUTCOME_STRUCTURED_FALLBACK, 0), self.total),
            "document_table_rows_share": ratio(table_rows, self.total),
            "document_prose_share": ratio(prose, self.total),
            "prose_share_of_classified": ratio(prose, self.classified),
            "sql_answerable_share": ratio(structured + table_rows, self.classified),
        }
        return result

    def to_dict(self) -> dict[str, object]:
        """Serialize for ``--json`` and for machine-readable evidence artifacts."""
        return {
            "basis": "served_chunks",
            "window_days": self.days,
            "since": self.since.isoformat(),
            "total": self.total,
            "distinct_queries": self.distinct_queries,
            "counts": dict(sorted(self.counts.items())),
            "unknown": self.unknown,
            "classified": self.classified,
            "shares": self.shares(),
            "verdict": self.verdict.value,
            "undetermined_reason": self.undetermined_reason,
            "min_events": self.min_events,
        }


def collect_traffic(
    session: Session,
    *,
    days: int = DEFAULT_DAYS,
    min_events: int = DEFAULT_MIN_EVENTS,
) -> TrafficReport:
    """Group stored events by outcome; the only write here is none at all."""
    if days <= 0:
        message = "--days must be positive"
        raise ValueError(message)
    if min_events < 0:
        message = "--min-events must not be negative"
        raise ValueError(message)

    since = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=days)
    window = RagQueryEvent.created_at >= since
    rows = session.execute(
        select(RagQueryEvent.outcome, func.count()).where(window).group_by(RagQueryEvent.outcome)
    ).all()
    counts = {str(outcome): int(count) for outcome, count in rows}
    distinct_queries = int(
        session.execute(select(func.count(distinct(RagQueryEvent.query_hash))).where(window)).scalar_one()
    )
    total = sum(counts.values())

    verdict = decide_verdict(
        total=total,
        classified=total - counts.get(RAG_QUERY_OUTCOME_UNKNOWN, 0),
        prose=counts.get(RAG_QUERY_OUTCOME_DOCUMENT_PROSE, 0),
        unknown=counts.get(RAG_QUERY_OUTCOME_UNKNOWN, 0),
    )
    reason = None
    if total < min_events:
        verdict = NecessityVerdict.UNDETERMINED
        reason = f"only {total} event(s) in window, below --min-events {min_events}"

    return TrafficReport(
        days=days,
        since=since,
        total=total,
        distinct_queries=distinct_queries,
        counts=counts,
        min_events=min_events,
        verdict=verdict,
        undetermined_reason=reason,
    )


def _render_summary(report: TrafficReport) -> str:
    """Render the counts with the measurement limits attached."""
    lines = [
        f"RAG traffic (last {report.days}d, since {report.since.isoformat()})",
        f"  total={report.total}  distinct_queries={report.distinct_queries}",
        f"  verdict={report.verdict.value}  basis=served_chunks",
    ]
    if report.undetermined_reason:
        lines.append(f"  reason={report.undetermined_reason}")
    for outcome, count in sorted(report.counts.items()):
        lines.append(f"  {outcome}: {count}")
    for name, value in report.shares().items():
        lines.append(f"  {name}: {value}")
    lines.extend(
        [
            "  note: unknown rows are excluded from the classified denominator, never counted as prose.",
            "  note: table rows returned by search are a proxy for SQL-answerability, not proof of intent.",
            "  note: only rag_ask traffic is observed; CLI and direct DB access are invisible.",
        ]
    )
    return "\n".join(lines)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report observed RAG query routing (read-only).")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Window length in days.")
    parser.add_argument(
        "--min-events",
        type=int,
        default=DEFAULT_MIN_EVENTS,
        help="Refuse to conclude below this many events.",
    )
    parser.add_argument("--json", action="store_true", help="Output JSON format.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Aggregate the query ledger without writing anything."""
    args = _parse_args(argv)
    try:
        with get_db_session() as session:
            report = collect_traffic(session, days=args.days, min_events=args.min_events)
    except (SQLAlchemyError, OSError, RuntimeError, ValueError) as exc:
        sys.stderr.write(f"traffic_error: {exc}\n")
        return 2

    if args.json:
        sys.stdout.write(json.dumps(report.to_dict(), ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(_render_summary(report) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
