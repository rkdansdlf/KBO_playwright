"""Build a deterministic evaluation corpus through the production RAG path."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.constants import KST
from src.parsers.text_transformer import TextTransformer
from src.services.rag_index_identity import chunk_content_hash, current_index_version
from src.services.rag_index_propagation import publish_index_batch

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


EVAL_EMBEDDING_DIMENSION = 1536
EVAL_EMBEDDING_MODEL = "rag-eval-hash-v1"
_TOKEN_PATTERN = re.compile(r"[0-9A-Za-z\uac00-\ud7a3]+")


@dataclass(frozen=True)
class EvalIndexReport:
    """Summarize a fixture corpus indexing attempt."""

    document_count: int
    chunk_count: int
    primary_upserted: int
    vector_upserted: int
    index_version: str
    embedding_model: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize the indexing report."""
        return {
            "document_count": self.document_count,
            "chunk_count": self.chunk_count,
            "primary_upserted": self.primary_upserted,
            "vector_upserted": self.vector_upserted,
            "index_version": self.index_version,
            "embedding_model": self.embedding_model,
        }


@dataclass(frozen=True)
class EvalCorpusValidationReport:
    """Summarize fixture corpus and golden-query identity validation."""

    document_count: int
    chunk_count: int
    query_count: int
    missing_chunk_ids: tuple[str, ...]

    @property
    def is_valid(self) -> bool:
        """Return whether every golden query references an indexed fixture chunk."""
        return not self.missing_chunk_ids and self.query_count > 0 and self.chunk_count > 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize the validation report."""
        return {
            "document_count": self.document_count,
            "chunk_count": self.chunk_count,
            "query_count": self.query_count,
            "missing_chunk_ids": list(self.missing_chunk_ids),
            "valid": self.is_valid,
        }


class DeterministicEmbeddingService:
    """Generate reproducible hashed-token vectors without a network provider."""

    model_name = EVAL_EMBEDDING_MODEL
    dimension = EVAL_EMBEDDING_DIMENSION

    def get_embedding(self, text: str) -> list[float]:
        """Generate one normalized hashed-token embedding."""
        return self.get_embeddings_batch([text])[0]

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate normalized hashed-token embeddings for a batch."""
        return [_hashed_embedding(text) for text in texts]


def load_eval_documents(path: str | Path) -> list[dict[str, Any]]:
    """Load and validate evaluation source documents from a JSON array."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        message = "Evaluation corpus must be a JSON array"
        raise TypeError(message)

    documents: list[dict[str, Any]] = []
    for index, document in enumerate(payload, start=1):
        if not isinstance(document, Mapping):
            message = f"Evaluation corpus item {index} must be an object"
            raise TypeError(message)
        required = {"source_table", "source_row_id", "title", "content", "document_type"}
        missing = sorted(required - set(document))
        if missing:
            message = f"Evaluation corpus item {index} missing fields: {', '.join(missing)}"
            raise ValueError(message)
        documents.append(dict(document))
    return documents


def validate_eval_corpus_files(
    documents_path: str | Path,
    golden_queries_path: str | Path,
) -> EvalCorpusValidationReport:
    """Validate golden IDs against chunks produced by the production transformer."""
    from src.services.retrieval_evaluation import load_golden_queries

    documents = load_eval_documents(documents_path)
    chunks = build_eval_chunks(documents)
    available_ids = {f"{chunk['source_table']}:{chunk['source_row_id']}" for chunk in chunks}
    queries = load_golden_queries(golden_queries_path)
    missing = sorted(
        {chunk_id for query in queries for chunk_id in query.relevant_chunk_ids if chunk_id not in available_ids}
    )
    return EvalCorpusValidationReport(len(documents), len(chunks), len(queries), tuple(missing))


def build_eval_chunks(
    documents: Iterable[Mapping[str, Any]],
    transformer: TextTransformer | None = None,
) -> list[dict[str, Any]]:
    """Chunk evaluation documents using the same transformer as production."""
    chunker = transformer or TextTransformer()
    chunks: list[dict[str, Any]] = []
    for document in documents:
        source_table = str(document["source_table"])
        source_row_id = str(document["source_row_id"])
        document_type = str(document["document_type"])
        base_meta = dict(document.get("meta") or {})
        base_meta.update(
            {
                "category": base_meta.get("category", document_type),
                "document_type": document_type,
                "source_table": source_table,
                "source_row_id": source_row_id,
            }
        )
        raw_document = {
            "title": str(document["title"]),
            "content": str(document["content"]),
            "meta": base_meta,
        }
        transformed = chunker.chunk_document(raw_document)
        for chunk_index, transformed_chunk in enumerate(transformed, start=1):
            meta = dict(transformed_chunk.get("meta") or {})
            stable_row_id = f"{source_row_id}:{chunk_index}"
            meta.update(
                {
                    "category": base_meta["category"],
                    "document_type": document_type,
                    "source_table": source_table,
                    "source_row_id": stable_row_id,
                }
            )
            chunks.append(
                {
                    "source_table": source_table,
                    "source_row_id": stable_row_id,
                    "title": transformed_chunk.get("title"),
                    "content": transformed_chunk["content"],
                    "team_id": document.get("team_id"),
                    "player_id": document.get("player_id"),
                    "season_year": document.get("season_year"),
                    "document_type": document_type,
                    "game_date": _coerce_date(document.get("game_date")),
                    "published_at": document.get("published_at"),
                    "source_url": document.get("source_url"),
                    "language": document.get("language", "ko"),
                    "league_type_code": document.get("league_type_code"),
                    "meta": meta,
                }
            )
    return chunks


def index_eval_corpus(
    primary_session: Session,
    vector_session: Session,
    documents: Iterable[Mapping[str, Any]],
    *,
    apply: bool = False,
    embedding_service: DeterministicEmbeddingService | None = None,
) -> EvalIndexReport:
    """Index fixture documents into sparse and vector stores with shared identity."""
    document_list = list(documents)
    chunks = build_eval_chunks(document_list)
    version = current_index_version()
    embedder = embedding_service or DeterministicEmbeddingService()
    if not apply:
        return EvalIndexReport(len(document_list), len(chunks), 0, 0, version, embedder.model_name)

    embeddings = embedder.get_embeddings_batch([_embedding_text(chunk) for chunk in chunks])
    now = datetime.now(KST)
    payloads: list[dict[str, Any]] = []
    for chunk, embedding in zip(chunks, embeddings, strict=True):
        payload = dict(chunk)
        payload.update(
            {
                "embedding": embedding,
                "content_hash": chunk_content_hash(chunk.get("title"), chunk["content"]),
                "index_version": version,
                "indexed_at": now,
            }
        )
        payloads.append(payload)

    primary_count = publish_index_batch(primary_session, vector_session, payloads)
    return EvalIndexReport(
        len(document_list),
        len(chunks),
        primary_count,
        len(chunks),
        version,
        embedder.model_name,
    )


def eval_source_key(source_table: str, source_row_id: str, chunk_index: int = 1) -> str:
    """Return the stable key used by sparse, vector, and golden-query results."""
    return f"{source_table}:{source_row_id}:{chunk_index}"


def _embedding_text(chunk: Mapping[str, Any]) -> str:
    return f"{chunk.get('title') or ''}\n{chunk.get('content') or ''}"


def _coerce_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value:
        return date.fromisoformat(value)
    return None


def _hashed_embedding(text: str) -> list[float]:
    vector = [0.0] * EVAL_EMBEDDING_DIMENSION
    tokens = _TOKEN_PATTERN.findall(text.lower())
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:8], "big") % EVAL_EMBEDDING_DIMENSION
        vector[index] += 1.0
    norm = math.sqrt(sum(value * value for value in vector))
    if norm:
        return [value / norm for value in vector]
    return vector
