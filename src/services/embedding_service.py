"""
Service to fetch vector embeddings from Gemini API or OpenRouter API.
"""


# ruff: noqa: PLR2004from __future__ import annotations

import contextlib
import logging
import os
from http import HTTPStatus

import httpx
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

EMBEDDING_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)
EMBEDDING_HTTP_EXCEPTIONS = (httpx.HTTPError, ValueError, TypeError, RuntimeError, OSError)


class EmbeddingService:
    """
    Connects to external embedding providers to generate vector arrays for chunk texts.
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            logger.warning("⚠️ Warning: GEMINI_API_KEY is not configured in environment.")

    def adjust_embedding_dimension(self, embedding: list[float], target_dim: int = 256) -> list[float]:
        """
        Truncates or pads embedding list to target_dim.
        If truncating, L2 normalization is applied.
        """
        if not embedding:
            return [0.0] * target_dim

        current_dim = len(embedding)
        if current_dim == target_dim:
            return embedding

        if current_dim > target_dim:
            truncated = embedding[:target_dim]
            import math

            norm = math.sqrt(sum(x * x for x in truncated))
            if norm > 1e-9:
                return [x / norm for x in truncated]
            return truncated
        return embedding + [0.0] * (target_dim - current_dim)

    def _compute_hash(self, text: str) -> str:
        import hashlib

        # Normalize whitespace to make hash robust to minor formatting changes
        cleaned = " ".join(text.split()).strip()
        return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()

    def get_embedding(self, text: str) -> list[float]:
        """
        Generates embedding for a single text string.
        """
        results = self.get_embeddings_batch([text])
        return results[0] if results else [0.0] * 256

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Generates embeddings for a batch of text strings, utilizing a local SQLite cache.
        """
        if not texts:
            return []

        model_name = self._model_name()
        hashes = [self._compute_hash(t) for t in texts]
        cached_map = self._load_cached_embeddings(hashes, model_name)
        missing_indices, missing_texts = self._missing_embedding_inputs(texts, hashes, cached_map)

        if missing_texts:
            new_embeddings = self._fetch_missing_embeddings(missing_texts)
            self._save_cached_embeddings(hashes, missing_indices, model_name, new_embeddings)
            self._merge_new_embeddings(cached_map, hashes, missing_indices, new_embeddings)

        return [cached_map[h] for h in hashes]

    def _model_name(self) -> str:
        if self.api_key and self.api_key.startswith("sk-or-v1-"):
            return os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")
        return "models/text-embedding-004"

    def _load_cached_embeddings(self, hashes: list[str], model_name: str) -> dict[str, list[float]]:
        cached_map = {}
        try:
            import json

            from sqlalchemy import select

            from src.db.engine import SessionLocal
            from src.models.embedding_cache import EmbeddingCache

            with SessionLocal() as session:
                stmt = select(EmbeddingCache).where(
                    EmbeddingCache.text_hash.in_(hashes),
                    EmbeddingCache.model_name == model_name,
                )
                for row in session.scalars(stmt).all():
                    emb = row.embedding
                    if isinstance(emb, str):
                        with contextlib.suppress(json.JSONDecodeError, TypeError):
                            emb = json.loads(emb)
                    cached_map[row.text_hash] = emb
        except EMBEDDING_DB_EXCEPTIONS:
            logger.exception("⚠️ Warning: Embedding cache lookup error (continuing without cache)")
        return cached_map

    def _missing_embedding_inputs(
        self,
        texts: list[str],
        hashes: list[str],
        cached_map: dict[str, list[float]],
    ) -> tuple[list[int], list[str]]:
        missing_indices = []
        missing_texts = []
        for idx, text_hash in enumerate(hashes):
            if text_hash not in cached_map:
                missing_indices.append(idx)
                missing_texts.append(texts[idx])
        return missing_indices, missing_texts

    def _fetch_missing_embeddings(self, missing_texts: list[str]) -> list[list[float]]:
        if not self.api_key:
            logger.error("❌ GEMINI_API_KEY missing. Returning zero-vectors as fallback.")
            return [[0.0] * 256 for _ in missing_texts]
        if self.api_key.startswith("sk-or-v1-"):
            raw_embeddings = self._fetch_openrouter_embeddings(missing_texts)
        else:
            raw_embeddings = self._fetch_google_embeddings(missing_texts)
        return [self.adjust_embedding_dimension(emb) for emb in raw_embeddings]

    def _save_cached_embeddings(
        self,
        hashes: list[str],
        missing_indices: list[int],
        model_name: str,
        new_embeddings: list[list[float]],
    ) -> None:
        try:
            from src.db.engine import SessionLocal
            from src.models.embedding_cache import EmbeddingCache

            with SessionLocal() as session:
                for idx, emb in enumerate(new_embeddings):
                    text_hash = hashes[missing_indices[idx]]
                    existing = session.get(EmbeddingCache, (text_hash, model_name))
                    if not existing:
                        session.add(EmbeddingCache(text_hash=text_hash, model_name=model_name, embedding=emb))
                session.commit()
        except EMBEDDING_DB_EXCEPTIONS:
            logger.exception("⚠️ Warning: Failed to save to embedding cache")

    def _merge_new_embeddings(
        self,
        cached_map: dict[str, list[float]],
        hashes: list[str],
        missing_indices: list[int],
        new_embeddings: list[list[float]],
    ) -> None:
        for idx, emb in enumerate(new_embeddings):
            cached_map[hashes[missing_indices[idx]]] = emb

    def _fetch_openrouter_embeddings(self, texts: list[str]) -> list[list[float]]:
        """
        Calls OpenRouter's OpenAI-compatible embeddings endpoint.
        """
        url = "https://openrouter.ai/api/v1/embeddings"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

        # Default to openai/text-embedding-3-small which returns 1536-dimensional vectors
        model = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")

        payload = {"model": model, "input": texts, "dimensions": 256}

        try:
            with httpx.Client(headers=headers, timeout=30.0) as client:
                res = client.post(url, json=payload)
                if res.status_code == HTTPStatus.OK:
                    data = res.json()
                    # OpenRouter / OpenAI format: {"data": [{"embedding": [...]}, ...]}
                    records = data.get("data", [])
                    # Make sure they are returned in order
                    sorted_records = sorted(records, key=lambda x: x.get("index", 0))
                    return [item.get("embedding") for item in sorted_records]
                logger.error("❌ OpenRouter Embedding API returned status %s: %s", res.status_code, res.text)
        except EMBEDDING_HTTP_EXCEPTIONS:
            logger.exception("❌ Exception fetching OpenRouter embeddings")

        # Fallback empty vectors
        return [[0.0] * 256 for _ in texts]

    def _fetch_google_embeddings(self, texts: list[str]) -> list[list[float]]:
        """
        Calls standard Google Gemini AI Studio Embeddings API.
        """
        # Google text-embedding-004 supports batching via batchEmbedContents
        url = f"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents?key={self.api_key}"
        headers = {"Content-Type": "application/json"}

        requests_payload = []
        for text in texts:
            requests_payload.append(  # noqa: PERF401
                {
                    "model": "models/text-embedding-004",
                    "content": {"parts": [{"text": text}]},
                    "outputDimensionality": 256,
                },
            )

        payload = {"requests": requests_payload}

        try:
            with httpx.Client(headers=headers, timeout=30.0) as client:
                res = client.post(url, json=payload)
                if res.status_code == HTTPStatus.OK:
                    data = res.json()
                    # Google format: {"embeddings": [{"values": [...]}, ...]}
                    embeddings_data = data.get("embeddings", [])
                    return [item.get("values", []) for item in embeddings_data]
                logger.error("❌ Google Embedding API returned status %s: %s", res.status_code, res.text)
        except EMBEDDING_HTTP_EXCEPTIONS:
            logger.exception("❌ Exception fetching Google embeddings")

        # Fallback empty vectors (Google text-embedding-004 dimensions = 768, target = 256)
        return [[0.0] * 256 for _ in texts]
