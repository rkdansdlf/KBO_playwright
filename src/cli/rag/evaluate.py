"""CLI command for benchmarking RAG retrieval against golden queries."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING

from src.db.engine import get_db_session
from src.rag.evaluation_gateway import DEFAULT_GOLDEN_PATH, RagEvaluationGateway

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Execute RAG benchmark evaluation CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Benchmark KBO RAG Retrieval Accuracy & Latency")
    parser.add_argument(
        "--golden-path",
        "-g",
        type=str,
        default=str(DEFAULT_GOLDEN_PATH),
        help=f"Path to golden queries JSON (default: {DEFAULT_GOLDEN_PATH})",
    )
    parser.add_argument("--top-k", "-k", type=int, default=5, help="Top K evaluation depth (default: 5)")
    parser.add_argument("--limit", "-n", type=int, default=None, help="Max queries to evaluate")
    parser.add_argument("--min-recall", type=float, default=0.85, help="Minimum Recall@K SLA target (default: 0.85)")
    parser.add_argument("--min-mrr", type=float, default=0.70, help="Minimum MRR SLA target (default: 0.70)")
    parser.add_argument("--max-p95-ms", type=float, default=500.0, help="Maximum p95 latency ms SLA (default: 500ms)")
    parser.add_argument("--strict", action="store_true", help="Exit with code 1 if SLA is violated")
    parser.add_argument("--json", action="store_true", help="Output report in JSON format")

    args = parser.parse_args(argv)

    with get_db_session() as session:
        gateway = RagEvaluationGateway(session)
        report = gateway.evaluate_golden_set(
            golden_path=args.golden_path,
            top_k=args.top_k,
            max_queries=args.limit,
            min_recall=args.min_recall,
            min_mrr=args.min_mrr,
            max_p95_ms=args.max_p95_ms,
        )

    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
    else:
        print("=" * 70)  # noqa: T201
        print("📊 [KBO RAG 검색 품질 & 성능 벤치마크 리포트]")  # noqa: T201
        print("=" * 70)  # noqa: T201
        print(f"• 총 평가 쿼리 수: {report.total_evaluated}개 (Top-K: {report.top_k})")  # noqa: T201
        print(f"• Recall@{report.top_k}:      {report.metrics.recall_at_k * 100:.2f}%")  # noqa: T201
        print(f"• Precision@{report.top_k}:   {report.metrics.precision_at_k * 100:.2f}%")  # noqa: T201
        print(f"• MRR:               {report.metrics.mrr:.4f}")  # noqa: T201
        print(f"• NDCG@{report.top_k}:        {report.metrics.ndcg:.4f}")  # noqa: T201
        print(f"• Hit Rate:          {report.metrics.hit_rate * 100:.2f}%")  # noqa: T201
        print("-" * 70)  # noqa: T201
        print(f"• 지연 시간 p50:      {report.latency.p50_ms:.1f}ms")  # noqa: T201
        print(f"• 지연 시간 p95:      {report.latency.p95_ms:.1f}ms")  # noqa: T201
        print(f"• 지연 시간 Max:      {report.latency.max_ms:.1f}ms")  # noqa: T201
        print(f"• 지연 시간 Avg:      {report.latency.avg_ms:.1f}ms")  # noqa: T201
        print("-" * 70)  # noqa: T201
        status_str = "✅ 통과 (PASSED)" if report.sla_passed else "❌ 위반 (FAILED)"
        print(f"• SLA 판정:          {status_str}")  # noqa: T201

        if not report.sla_passed:
            print("\n[SLA 위반 항목]:")  # noqa: T201
            for v in report.sla_violations:
                print(f"  - ⚠️ {v}")  # noqa: T201
        print("=" * 70)  # noqa: T201

    if args.strict and not report.sla_passed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
