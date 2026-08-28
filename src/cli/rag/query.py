"""CLI command for interactive RAG hybrid retrieval."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import TYPE_CHECKING

from src.db.engine import get_db_session
from src.rag.dto import RetrievalQuery
from src.rag.retrievers.hybrid import UnifiedHybridRetriever

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Execute hybrid retrieval query CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Query KBO Hybrid Vector/BM25 Knowledge Base")
    parser.add_argument("query", nargs="?", type=str, default="", help="Search query string")
    parser.add_argument("--query", "-q", dest="query_opt", type=str, default="", help="Search query string (option)")
    parser.add_argument("--top-k", "-k", type=int, default=5, help="Top K candidates (default: 5)")
    parser.add_argument("--category", "-c", type=str, default=None, help="Target category filter")
    parser.add_argument("--json", action="store_true", help="Output result in JSON format")

    args = parser.parse_args(argv)
    query_text = (args.query_opt or args.query).strip()

    if not query_text:
        parser.print_help()
        return 1

    with get_db_session() as session:
        retriever = UnifiedHybridRetriever(session)
        query_obj = RetrievalQuery(
            query_text=query_text,
            top_k=args.top_k,
            category=args.category,
        )
        result = retriever.retrieve(query_obj)

    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))  # noqa: T201
        return 0

    print("=" * 70)  # noqa: T201
    header = f"🔍 [KBO RAG 검색]: '{result.query.query_text}' ({result.elapsed_ms:.1f}ms, {len(result.candidates)}건)"
    print(header)  # noqa: T201
    print("=" * 70)  # noqa: T201

    if not result.candidates:
        print("일치하는 검색 결과를 찾지 못했습니다.")  # noqa: T201
        return 0

    for i, cand in enumerate(result.candidates, start=1):
        print(f"\n[{i}] {cand.title or '제목 없음'} (Score: {cand.score:.4f} | Chunk ID: {cand.chunk_id})")  # noqa: T201
        print("-" * 70)  # noqa: T201
        print(cand.content.strip())  # noqa: T201
        if cand.source_url:
            print(f"출처: {cand.source_url}")  # noqa: T201

    print("\n" + "=" * 70)  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
