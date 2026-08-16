"""Apply one explicit RAG source update or delete to both indexes."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.services.rag_index_propagation import propagate_index_delete, propagate_index_update

if TYPE_CHECKING:
    from collections.abc import Sequence

    from src.services.hybrid_retriever import EmbeddingProvider


def main(argv: Sequence[str] | None = None) -> int:
    """Propagate an explicit source mutation, with dry-run as the default."""
    parser = argparse.ArgumentParser(description="Propagate one RAG source mutation")
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--source-row-id", required=True)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--delete", action="store_true", help="Mark the source deleted")
    operation.add_argument("--payload", type=Path, help="JSON update payload containing title/content/document_type")
    parser.add_argument("--purge", action="store_true", help="Purge rows after delete; only with --delete")
    parser.add_argument("--apply", action="store_true", help="Persist the mutation; dry-run is the default")
    parser.add_argument(
        "--embedding-mode",
        choices=("configured", "deterministic"),
        default="configured",
        help="Embedding provider for an update payload",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    args = parser.parse_args(argv)
    if args.purge and not args.delete:
        parser.error("--purge requires --delete")

    if args.delete:
        payload: dict[str, Any] = {}
    else:
        payload = _load_payload(args.payload, args.source_table, args.source_row_id)
        if payload is None:
            return 2

    plan: dict[str, Any] = {
        "source_key": f"{args.source_table}:{args.source_row_id}",
        "operation": "delete" if args.delete else "update",
        "apply": args.apply,
        "purge": args.purge,
    }
    if not args.apply:
        _render(plan, as_json=args.as_json)
        return 0

    from src.db.engine import get_db_session
    from src.db.vector_engine import get_vector_session, is_pgvector_available

    if not is_pgvector_available():
        plan["error"] = "pgvector is unavailable"
        _render(plan, as_json=args.as_json)
        return 2
    with get_db_session() as primary_session, get_vector_session() as vector_session:
        if args.delete:
            result = propagate_index_delete(
                primary_session,
                vector_session,
                args.source_table,
                args.source_row_id,
                purge=args.purge,
            )
        else:
            embedder = _embedding_service(args.embedding_mode)
            result = propagate_index_update(
                primary_session,
                vector_session,
                payload,
                embedder.get_embedding(f"{payload['title']}\n{payload['content']}"),
            )
    plan["result"] = result.to_dict()
    _render(plan, as_json=args.as_json)
    return 0


def _load_payload(path: Path | None, source_table: str, source_row_id: str) -> dict[str, Any] | None:
    """Load and validate one source update payload."""
    if path is None:
        sys.stderr.write("payload_error: --payload is required for an update\n")
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"payload_error: {exc}\n")
        return None
    if not isinstance(payload, dict):
        sys.stderr.write("payload_error: update payload must be a JSON object\n")
        return None
    required = {"title", "content", "document_type"}
    missing = sorted(required - set(payload))
    if missing:
        sys.stderr.write(f"payload_error: missing fields: {', '.join(missing)}\n")
        return None
    payload["source_table"] = source_table
    payload["source_row_id"] = source_row_id
    return payload


def _embedding_service(mode: str) -> EmbeddingProvider:
    """Build the requested embedding provider."""
    if mode == "deterministic":
        from src.services.rag_eval_corpus import DeterministicEmbeddingService

        return DeterministicEmbeddingService()
    from src.services.embedding_service import EmbeddingService

    return EmbeddingService()


def _render(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return
    if payload.get("error"):
        sys.stdout.write(f"error={payload['error']}\n")
    else:
        sys.stdout.write(
            f"operation={payload['operation']} source_key={payload['source_key']} apply={payload['apply']}\n"
        )


if __name__ == "__main__":
    raise SystemExit(main())
