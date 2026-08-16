"""Validate and optionally index the reproducible RAG evaluation corpus."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.services.rag_eval_corpus import (
    index_eval_corpus,
    load_eval_documents,
    validate_eval_corpus_files,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DOCUMENTS = _ROOT / "tests" / "fixtures" / "rag_corpus" / "documents.json"
_DEFAULT_GOLDEN = _ROOT / "tests" / "fixtures" / "rag_corpus" / "golden_queries.json"


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the evaluation corpus and write it only with explicit opt-in."""
    parser = argparse.ArgumentParser(description="Bootstrap the reproducible RAG evaluation corpus")
    parser.add_argument("--documents", type=Path, default=_DEFAULT_DOCUMENTS)
    parser.add_argument("--golden", type=Path, default=_DEFAULT_GOLDEN)
    parser.add_argument("--apply", action="store_true", help="Write the corpus to primary and pgvector databases")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    args = parser.parse_args(argv)

    validation = validate_eval_corpus_files(args.documents, args.golden)
    payload: dict[str, object] = {
        "documents": str(args.documents),
        "golden": str(args.golden),
        "apply": args.apply,
        "validation": validation.to_dict(),
    }
    if not validation.is_valid:
        payload["error"] = "evaluation corpus or golden references are invalid"
        _render(payload, as_json=args.as_json)
        return 2

    if not args.apply:
        _render(payload, as_json=args.as_json)
        return 0
    if os.getenv("RAG_EVAL_ALLOW_WRITE") != "1":
        payload["error"] = "--apply requires RAG_EVAL_ALLOW_WRITE=1"
        _render(payload, as_json=args.as_json)
        return 2

    from src.db.engine import get_db_session, init_db
    from src.db.vector_engine import get_vector_session, init_vector_db, is_pgvector_available

    if not is_pgvector_available():
        payload["error"] = "pgvector is unavailable"
        _render(payload, as_json=args.as_json)
        return 2
    init_db()
    init_vector_db()
    documents = load_eval_documents(args.documents)
    with get_db_session() as primary_session, get_vector_session() as vector_session:
        report = index_eval_corpus(primary_session, vector_session, documents, apply=True)
    payload["index"] = report.to_dict()
    _render(payload, as_json=args.as_json)
    return 0


def _render(payload: dict[str, object], *, as_json: bool) -> None:
    if as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return
    if "error" in payload:
        sys.stdout.write(f"error={payload['error']}\n")
    validation = payload.get("validation")
    if isinstance(validation, dict):
        sys.stdout.write(
            f"documents={validation.get('document_count')} chunks={validation.get('chunk_count')} "
            f"queries={validation.get('query_count')} valid={validation.get('valid')}\n"
        )
    index = payload.get("index")
    if isinstance(index, dict):
        sys.stdout.write(
            f"primary_upserted={index.get('primary_upserted')} vector_upserted={index.get('vector_upserted')}\n"
        )


if __name__ == "__main__":
    raise SystemExit(main())
