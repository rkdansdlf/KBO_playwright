"""CLI entrypoint to query RAG knowledge base."""

from __future__ import annotations

# ruff: noqa: T201
import argparse
import json
import logging
import sys

from src.db.engine import get_rag_index_session
from src.services.rag_search_engine import RagSearchEngine

logger = logging.getLogger(__name__)


def main() -> None:
    """Run CLI."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Query KBO RAG Knowledge Base")
    parser.add_argument("--query", type=str, required=True, help="Search query string")
    parser.add_argument("--top-k", type=int, default=5, help="Top K results")
    parser.add_argument("--json", action="store_true", help="Output raw JSON format")

    args = parser.parse_args()

    with get_rag_index_session() as session:
        engine = RagSearchEngine(session)
        result = engine.answer_question(query=args.query, top_k=args.top_k)

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print("=" * 60)
            print(f"[RAG 질문]: {result['query']}")
            print("=" * 60)
            print(result["answer"])
            print("-" * 60)
            print(f"[출처 URL ({len(result['sources'])}개)]:")
            for src in result["sources"]:
                print(f"  - {src}")
            print("=" * 60)


if __name__ == "__main__":
    main()
