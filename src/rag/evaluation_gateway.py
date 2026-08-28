"""RAG Evaluation Gateway for benchmarking hybrid retrieval against golden queries."""

from __future__ import annotations

import json
import logging
import statistics
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.rag.dto import (
    RagEvaluationMetrics,
    RagEvaluationReport,
    RagGoldenQuery,
    RagLatencyBreakdown,
    RetrievalQuery,
)
from src.rag.evaluation import RagEvaluator
from src.rag.retrievers.hybrid import UnifiedHybridRetriever

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

    from src.rag.base_retriever import BaseRetriever

logger = logging.getLogger(__name__)

DEFAULT_GOLDEN_PATH = Path("Docs/references/rag_golden_queries.json")


class RagEvaluationGateway:
    """Gateway for executing automated RAG retrieval benchmarks and SLA validation."""

    def __init__(
        self,
        session: Session,
        *,
        retriever: BaseRetriever | None = None,
    ) -> None:
        """Initialize evaluation gateway with database session and retriever."""
        self.session = session
        self.retriever = retriever or UnifiedHybridRetriever(session)

    def load_golden_queries(self, path: str | Path = DEFAULT_GOLDEN_PATH) -> list[RagGoldenQuery]:
        """Load and parse golden query definitions from JSON file."""
        golden_file = Path(path)
        if not golden_file.exists():
            msg = f"Golden query file not found: {golden_file}"
            raise FileNotFoundError(msg)

        with golden_file.open(encoding="utf-8") as f:
            raw_data = json.load(f)

        if not isinstance(raw_data, list):
            msg = f"Golden query file must contain a JSON array, got {type(raw_data).__name__}"
            raise TypeError(msg)

        queries: list[RagGoldenQuery] = []
        for item in raw_data:
            q_id = str(item.get("id") or item.get("query_id") or f"query_{len(queries) + 1}")
            query_text = str(item.get("query") or item.get("query_text") or "")
            relevant_chunk_ids = (
                item.get("relevant_chunk_ids") or item.get("relevantChunkIds") or item.get("target_chunk_ids") or []
            )
            filters = item.get("filters") or {}
            tags = item.get("tags") or []
            category = item.get("category")

            queries.append(
                RagGoldenQuery(
                    id=q_id,
                    query=query_text,
                    relevant_chunk_ids=list(relevant_chunk_ids),
                    filters=filters,
                    tags=list(tags),
                    category=category,
                )
            )

        logger.info("Loaded %d golden queries from %s", len(queries), golden_file)
        return queries

    @staticmethod
    def calculate_latency_percentiles(durations_ms: Sequence[float]) -> RagLatencyBreakdown:
        """Compute latency percentiles (p50, p90, p95, p99, max, avg) from duration samples."""
        if not durations_ms:
            return RagLatencyBreakdown()

        sorted_ms = sorted(durations_ms)
        n = len(sorted_ms)

        def _percentile(p: float) -> float:
            k = (n - 1) * p
            f = int(k)
            c = min(f + 1, n - 1)
            d = k - f
            return sorted_ms[f] + d * (sorted_ms[c] - sorted_ms[f])

        return RagLatencyBreakdown(
            p50_ms=_percentile(0.50),
            p90_ms=_percentile(0.90),
            p95_ms=_percentile(0.95),
            p99_ms=_percentile(0.99),
            max_ms=max(sorted_ms),
            avg_ms=statistics.mean(sorted_ms),
        )

    def evaluate_golden_set(  # noqa: PLR0913
        self,
        queries: Sequence[RagGoldenQuery] | None = None,
        *,
        golden_path: str | Path | None = None,
        top_k: int = 5,
        max_queries: int | None = None,
        min_recall: float = 0.85,
        min_mrr: float = 0.70,
        max_p95_ms: float = 500.0,
    ) -> RagEvaluationReport:
        """Run end-to-end evaluation against golden query dataset and validate SLAs."""
        if queries is None:
            path = golden_path or DEFAULT_GOLDEN_PATH
            queries = self.load_golden_queries(path)

        if max_queries is not None and max_queries > 0:
            queries = queries[:max_queries]

        if not queries:
            return RagEvaluationReport(
                metrics=RagEvaluationMetrics(),
                latency=RagLatencyBreakdown(),
                total_evaluated=0,
                top_k=top_k,
                sla_passed=True,
                sla_violations=[],
                details=[],
            )

        batch_retrieved: list[list[str]] = []
        batch_relevant: list[list[str]] = []
        latencies_ms: list[float] = []
        details: list[dict[str, Any]] = []

        logger.info("Beginning RAG benchmark evaluation for %d queries (top_k=%d)", len(queries), top_k)

        for q in queries:
            t0 = time.perf_counter()
            query_obj = RetrievalQuery(
                query_text=q.query,
                top_k=top_k,
                category=q.category,
                filters=q.filters or None,
            )

            result = self.retriever.retrieve(query_obj)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            latencies_ms.append(elapsed_ms)

            retrieved_chunk_ids = [cand.chunk_id for cand in result.candidates]
            batch_retrieved.append(retrieved_chunk_ids)
            batch_relevant.append(q.relevant_chunk_ids)

            query_metrics = RagEvaluator.evaluate_query(
                retrieved_chunk_ids,
                q.relevant_chunk_ids,
                k=top_k,
            )

            details.append(
                {
                    "id": q.id,
                    "query": q.query,
                    "target_ids": q.relevant_chunk_ids,
                    "retrieved_ids": retrieved_chunk_ids,
                    "hit": bool(query_metrics.hit_rate > 0),
                    "recall": query_metrics.recall_at_k,
                    "mrr": query_metrics.mrr,
                    "elapsed_ms": round(elapsed_ms, 2),
                }
            )

        # Compute aggregate metrics
        overall_metrics = RagEvaluator.evaluate_batch(
            batch_retrieved,
            batch_relevant,
            k=top_k,
        )
        latency_breakdown = self.calculate_latency_percentiles(latencies_ms)

        # Validate SLAs
        sla_violations: list[str] = []
        if overall_metrics.recall_at_k < min_recall:
            sla_violations.append(
                f"Recall@{top_k} ({overall_metrics.recall_at_k:.4f}) below SLA target ({min_recall:.4f})"
            )
        if overall_metrics.mrr < min_mrr:
            sla_violations.append(f"MRR ({overall_metrics.mrr:.4f}) below SLA target ({min_mrr:.4f})")
        if latency_breakdown.p95_ms > max_p95_ms:
            sla_violations.append(
                f"Latency p95 ({latency_breakdown.p95_ms:.1f}ms) exceeded SLA limit ({max_p95_ms:.1f}ms)"
            )

        sla_passed = len(sla_violations) == 0

        logger.info(
            "Evaluation complete: evaluated=%d recall@%d=%.4f mrr=%.4f p95=%.1fms sla_passed=%s",
            len(queries),
            top_k,
            overall_metrics.recall_at_k,
            overall_metrics.mrr,
            latency_breakdown.p95_ms,
            sla_passed,
        )

        return RagEvaluationReport(
            metrics=overall_metrics,
            latency=latency_breakdown,
            total_evaluated=len(queries),
            top_k=top_k,
            sla_passed=sla_passed,
            sla_violations=sla_violations,
            details=details,
        )


__all__ = ["DEFAULT_GOLDEN_PATH", "RagEvaluationGateway"]
